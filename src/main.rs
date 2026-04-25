// Force link advapi32 on Windows (needed by zeromq-src for
// InitializeSecurityDescriptor / SetSecurityDescriptorDacl)
#[cfg(windows)]
#[link(name = "advapi32")]
extern "system" {}

//
// Minimal reproducer for zmq_poll() blocking forever on Windows ARM64.
//
// On Windows ARM64 (aarch64-pc-windows-msvc), zmq_poll() with a non-zero
// timeout blocks forever even when data is available on the socket.
// zmq_poll() with timeout=0 (non-blocking) works correctly.
//
// The underlying cause is that zeromq-src builds libzmq with
// ZMQ_POLL_BASED_ON_POLL, which maps to WSAPoll on Windows. WSAPoll has
// a long history of bugs (see https://daniel.haxx.se/blog/2012/10/10/wsapoll-is-broken/)
// and appears to not wake up reliably on Windows ARM64.
//
// Run with: cargo run
//
// Expected (all platforms except Windows ARM64):
//   All tests pass, completes in ~1 second.
//
// Observed on Windows ARM64:
//   "Test 2: poll with 3s timeout" hangs forever.
//

use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::Instant;

fn main() {
    println!("zmq version: {:?}", zmq::version());
    println!(
        "target: {} / {}",
        std::env::consts::OS,
        std::env::consts::ARCH
    );
    println!();

    test_inproc_pair();
    test_tcp_pubsub();
    test_delayed_send();

    println!();
    println!("All tests passed.");
}

/// Test with inproc PAIR sockets (used internally by libzmq for cross-thread
/// signaling via the mailbox/signaler mechanism).
fn test_inproc_pair() {
    println!("=== inproc PAIR sockets ===");

    let ctx = zmq::Context::new();
    let sender = ctx.socket(zmq::PAIR).unwrap();
    let receiver = ctx.socket(zmq::PAIR).unwrap();

    sender.bind("inproc://reprex-pair").unwrap();
    receiver.connect("inproc://reprex-pair").unwrap();

    // Send a message before polling so it's already available
    sender.send("hello", 0).unwrap();
    std::thread::sleep(Duration::from_millis(50));

    run_poll_tests(&receiver, "inproc PAIR");
}

/// Test with TCP PUB/SUB sockets (used by Jupyter's IOPub channel).
fn test_tcp_pubsub() {
    println!();
    println!("=== TCP PUB/SUB sockets ===");

    let ctx = zmq::Context::new();
    let publisher = ctx.socket(zmq::PUB).unwrap();
    let subscriber = ctx.socket(zmq::SUB).unwrap();

    publisher.bind("tcp://127.0.0.1:*").unwrap();
    let endpoint = publisher.get_last_endpoint().unwrap().unwrap();
    subscriber.set_subscribe(b"").unwrap();
    subscriber.connect(&endpoint).unwrap();

    // PUB/SUB subscription propagation can be slow, especially on
    // Windows ARM. Send repeatedly until the subscriber sees a message.
    let start = Instant::now();
    loop {
        publisher.send("hello", 0).unwrap();
        // Use poll(0) + sleep, not poll(100), because poll with
        // non-zero timeout is the exact bug we're reproducing.
        std::thread::sleep(Duration::from_millis(100));
        if subscriber.poll(zmq::POLLIN, 0).unwrap() > 0 {
            // Consume the message
            let _ = subscriber.recv_msg(0).unwrap();
            println!("  (subscription propagated in {:?})", start.elapsed());
            break;
        }
        if start.elapsed() > Duration::from_secs(5) {
            println!("  FAIL — subscription never propagated after 5s");
            std::process::exit(1);
        }
    }

    // Now send the actual test message
    publisher.send("hello", 0).unwrap();
    std::thread::sleep(Duration::from_millis(50));

    run_poll_tests(&subscriber, "TCP PUB/SUB");
}

/// Test where data arrives AFTER poll is already blocking. This is the
/// actual scenario in the kernel: the R thread sends a Stopped event
/// while the test is already waiting in poll() for it.
fn test_delayed_send() {
    println!();
    println!("=== Delayed send (data arrives while poll is blocking) ===");

    let ctx = zmq::Context::new();
    let sender = ctx.socket(zmq::PAIR).unwrap();
    let receiver = ctx.socket(zmq::PAIR).unwrap();

    sender.bind("inproc://reprex-delayed").unwrap();
    receiver.connect("inproc://reprex-delayed").unwrap();

    // Test 3: Start polling BEFORE data is sent
    print!("  Test 3: poll(5000) with data arriving 200ms later ... ");
    let done = std::sync::Arc::new(AtomicBool::new(false));
    let done2 = done.clone();

    // Producer: send after a short delay
    let producer = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(200));
        sender.send("delayed", 0).unwrap();
    });

    // Watchdog
    let watchdog = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_secs(5));
        if !done2.load(Ordering::Relaxed) {
            eprintln!();
            eprintln!("  HANG DETECTED — poll(5000) did not wake when data arrived");
            eprintln!("  This is the scenario that breaks the kernel: a blocking");
            eprintln!("  poll never wakes for data sent by another thread.");
            std::process::exit(2);
        }
    });

    let start = Instant::now();
    let ready = receiver.poll(zmq::POLLIN, 5000).unwrap();
    let elapsed = start.elapsed();
    done.store(true, Ordering::Relaxed);

    producer.join().unwrap();
    drop(watchdog);

    if ready > 0 && elapsed < Duration::from_secs(2) {
        println!("OK ({elapsed:?}), woke up promptly after send");
    } else if ready > 0 {
        println!("SLOW ({elapsed:?}), data arrived but poll was slow to wake");
    } else {
        println!("FAIL — poll(5000) timed out, took {elapsed:?}");
        std::process::exit(1);
    }

    let _ = receiver.recv_msg(0).unwrap();
}

fn run_poll_tests(socket: &zmq::Socket, label: &str) {
    // Test 1: Non-blocking poll (timeout=0) — expected to work everywhere
    print!("  Test 1: poll with timeout=0 (non-blocking) ... ");
    let start = Instant::now();
    let ready = socket.poll(zmq::POLLIN, 0).unwrap();
    let elapsed = start.elapsed();
    if ready > 0 {
        println!("OK ({elapsed:?}), data is available");
    } else {
        println!("FAIL — poll(0) says no data available for {label}");
        std::process::exit(1);
    }

    // Test 2: Blocking poll with timeout — hangs on Windows ARM64
    print!("  Test 2: poll with 3s timeout ... ");
    let done = std::sync::Arc::new(AtomicBool::new(false));
    let done2 = done.clone();

    // Watchdog: if poll doesn't return within 5s, report and abort
    let watchdog = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_secs(5));
        if !done2.load(Ordering::Relaxed) {
            eprintln!();
            eprintln!("  HANG DETECTED — poll(3000) did not return within 5s");
            eprintln!("  This confirms the WSAPoll bug on this platform.");
            eprintln!();
            eprintln!("  zmq_poll() with a non-zero timeout blocks forever");
            eprintln!("  even though data is available (poll(0) sees it).");
            std::process::exit(2);
        }
    });

    let start = Instant::now();
    let ready = socket.poll(zmq::POLLIN, 3000).unwrap();
    let elapsed = start.elapsed();
    done.store(true, Ordering::Relaxed);
    drop(watchdog);

    if ready > 0 {
        println!("OK ({elapsed:?}), data is available");
    } else {
        println!("FAIL — poll(3000) timed out for {label}, took {elapsed:?}");
        std::process::exit(1);
    }

    // Consume the message so the next test starts clean
    let _ = socket.recv_msg(0).unwrap();
}
