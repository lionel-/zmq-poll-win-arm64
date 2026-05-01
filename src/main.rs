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
//   Test 2 hangs forever (watchdog fires after 5s).
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

    test_tcp_pair();
    test_tcp_xpub_sub();
    test_tcp_delayed_send();
    test_multi_socket_poll();
    test_shell_poll();

    println!();
    println!("All tests passed.");
}

/// Test with TCP PAIR sockets — simplest TCP socket type for testing
/// poll semantics without subscription complexity.
fn test_tcp_pair() {
    println!("=== TCP PAIR sockets ===");

    let ctx = zmq::Context::new();
    let sender = ctx.socket(zmq::PAIR).unwrap();
    let receiver = ctx.socket(zmq::PAIR).unwrap();

    sender.bind("tcp://127.0.0.1:*").unwrap();
    let endpoint = sender.get_last_endpoint().unwrap().unwrap();
    receiver.connect(&endpoint).unwrap();

    std::thread::sleep(Duration::from_millis(100));
    sender.send("hello", 0).unwrap();
    std::thread::sleep(Duration::from_millis(50));

    run_poll_tests(&receiver, "TCP PAIR");
}

/// Test with TCP XPUB/SUB sockets — the actual socket types used by
/// Jupyter's IOPub channel. Uses the same subscription synchronization
/// as the kernel: subscribe before connect, then wait for the XPUB to
/// receive the subscription notification before sending.
fn test_tcp_xpub_sub() {
    println!();
    println!("=== TCP XPUB/SUB sockets (IOPub) ===");

    let ctx = zmq::Context::new();
    let xpub = ctx.socket(zmq::XPUB).unwrap();
    let sub = ctx.socket(zmq::SUB).unwrap();

    // Subscribe BEFORE connect (same order as the kernel)
    sub.set_subscribe(b"").unwrap();

    xpub.bind("tcp://127.0.0.1:*").unwrap();
    let endpoint = xpub.get_last_endpoint().unwrap().unwrap();
    sub.connect(&endpoint).unwrap();

    // Wait for the subscription to arrive at the XPUB side
    print!("  Waiting for subscription ... ");
    let start = Instant::now();
    loop {
        if xpub.poll(zmq::POLLIN, 0).unwrap() > 0 {
            let _ = xpub.recv_msg(0).unwrap();
            println!("OK ({:?})", start.elapsed());
            break;
        }
        if start.elapsed() > Duration::from_secs(5) {
            println!("FAIL — XPUB never received subscription after 5s");
            std::process::exit(1);
        }
        std::thread::sleep(Duration::from_millis(10));
    }

    xpub.send("hello", 0).unwrap();
    std::thread::sleep(Duration::from_millis(50));

    run_poll_tests(&sub, "TCP XPUB/SUB");
}

/// The actual kernel scenario: poll is already blocking when data
/// arrives from another thread over TCP.
fn test_tcp_delayed_send() {
    println!();
    println!("=== TCP PAIR delayed send (data arrives while poll is blocking) ===");

    let ctx = zmq::Context::new();
    let sender = ctx.socket(zmq::PAIR).unwrap();
    let receiver = ctx.socket(zmq::PAIR).unwrap();

    sender.bind("tcp://127.0.0.1:*").unwrap();
    let endpoint = sender.get_last_endpoint().unwrap().unwrap();
    receiver.connect(&endpoint).unwrap();

    std::thread::sleep(Duration::from_millis(100));

    print!("  poll(5000) with data arriving 200ms later ... ");
    let done = std::sync::Arc::new(AtomicBool::new(false));
    let done2 = done.clone();

    // Producer: send after a short delay
    std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(200));
        sender.send("delayed", 0).unwrap();
    });

    // Watchdog
    std::thread::spawn(move || {
        std::thread::sleep(Duration::from_secs(5));
        if !done2.load(Ordering::Relaxed) {
            eprintln!();
            eprintln!(
                "  HANG DETECTED \u{2014} poll(5000) did not wake when data arrived"
            );
            eprintln!("  This is the scenario that breaks the kernel: a blocking");
            eprintln!("  poll never wakes for data sent by another thread.");
            std::process::exit(2);
        }
    });

    let start = Instant::now();
    let ready = receiver.poll(zmq::POLLIN, 5000).unwrap();
    let elapsed = start.elapsed();
    done.store(true, Ordering::Relaxed);

    if ready > 0 && elapsed < Duration::from_secs(2) {
        println!("OK ({elapsed:?})");
    } else if ready > 0 {
        println!("SLOW ({elapsed:?}), poll was slow to wake");
    } else {
        println!("FAIL \u{2014} poll(5000) timed out, took {elapsed:?}");
        std::process::exit(1);
    }

    let _ = receiver.recv_msg(0).unwrap();
}

/// Matches `socket_bridge_thread`: zmq_poll on [inproc PAIR, ROUTER TCP,
/// XPUB TCP] with timeout=-1. Notification arrives on inproc from another
/// thread while poll is blocking.
fn test_multi_socket_poll() {
    println!();
    println!("=== Multi-socket poll (bridge thread pattern, timeout=-1) ===");

    let ctx = zmq::Context::new();

    // Inproc PAIR for cross-thread notifications (like outbound_notif_socket)
    let notif_tx = ctx.socket(zmq::PAIR).unwrap();
    let notif_rx = ctx.socket(zmq::PAIR).unwrap();
    notif_rx.bind("inproc://bridge-notif").unwrap();
    notif_tx.connect("inproc://bridge-notif").unwrap();

    // TCP ROUTER (like stdin_socket in the bridge thread)
    let router = ctx.socket(zmq::ROUTER).unwrap();
    router.bind("tcp://127.0.0.1:*").unwrap();
    let ep1 = router.get_last_endpoint().unwrap().unwrap();
    let dealer = ctx.socket(zmq::DEALER).unwrap();
    dealer.connect(&ep1).unwrap();

    // TCP XPUB (like iopub_socket in the bridge thread)
    let xpub = ctx.socket(zmq::XPUB).unwrap();
    xpub.bind("tcp://127.0.0.1:*").unwrap();
    let ep2 = xpub.get_last_endpoint().unwrap().unwrap();
    let sub = ctx.socket(zmq::SUB).unwrap();
    sub.set_subscribe(b"").unwrap();
    sub.connect(&ep2).unwrap();

    std::thread::sleep(Duration::from_millis(100));

    print!("  zmq_poll([inproc PAIR, ROUTER, XPUB], -1), notif 200ms later ... ");
    let done = std::sync::Arc::new(AtomicBool::new(false));
    let done2 = done.clone();

    std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(200));
        notif_tx.send("wake", 0).unwrap();
    });

    std::thread::spawn(move || {
        std::thread::sleep(Duration::from_secs(5));
        if !done2.load(Ordering::Relaxed) {
            eprintln!();
            eprintln!("  HANG DETECTED \u{2014} zmq_poll(-1) did not wake for inproc notification");
            eprintln!("  This is the bridge thread pattern: poll on inproc + TCP");
            eprintln!("  with infinite timeout never wakes for inproc data.");
            std::process::exit(2);
        }
    });

    let mut poll_items = vec![
        notif_rx.as_poll_item(zmq::POLLIN),
        router.as_poll_item(zmq::POLLIN),
        xpub.as_poll_item(zmq::POLLIN),
    ];

    let start = Instant::now();
    let n = zmq::poll(&mut poll_items, -1).unwrap();
    let elapsed = start.elapsed();
    done.store(true, Ordering::Relaxed);

    if n > 0 && elapsed < Duration::from_secs(2) {
        println!("OK ({elapsed:?})");
    } else {
        println!("FAIL \u{2014} zmq_poll took {elapsed:?}");
        std::process::exit(1);
    }

    let _ = notif_rx.recv_msg(0).unwrap();
}

/// Matches `Shell::listen_blocking`: zmq_poll on [ROUTER TCP, inproc PAIR]
/// with timeout=-1. Notification arrives on inproc from another thread.
fn test_shell_poll() {
    println!();
    println!("=== Shell poll (ROUTER + inproc PAIR, timeout=-1) ===");

    let ctx = zmq::Context::new();

    // TCP ROUTER (shell socket)
    let router = ctx.socket(zmq::ROUTER).unwrap();
    router.bind("tcp://127.0.0.1:*").unwrap();
    let ep = router.get_last_endpoint().unwrap().unwrap();
    let dealer = ctx.socket(zmq::DEALER).unwrap();
    dealer.connect(&ep).unwrap();

    // Inproc PAIR (comm notification socket)
    let notif_tx = ctx.socket(zmq::PAIR).unwrap();
    let notif_rx = ctx.socket(zmq::PAIR).unwrap();
    notif_rx.bind("inproc://shell-comm-notif").unwrap();
    notif_tx.connect("inproc://shell-comm-notif").unwrap();

    std::thread::sleep(Duration::from_millis(100));

    print!("  zmq_poll([ROUTER, inproc PAIR], -1), notif 200ms later ... ");
    let done = std::sync::Arc::new(AtomicBool::new(false));
    let done2 = done.clone();

    std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(200));
        notif_tx.send("", 0).unwrap();
    });

    std::thread::spawn(move || {
        std::thread::sleep(Duration::from_secs(5));
        if !done2.load(Ordering::Relaxed) {
            eprintln!();
            eprintln!("  HANG DETECTED \u{2014} zmq_poll(-1) did not wake for comm notification");
            eprintln!("  This is the Shell::listen pattern: ROUTER + inproc PAIR");
            eprintln!("  with infinite timeout never wakes for inproc data.");
            std::process::exit(2);
        }
    });

    let mut poll_items = vec![
        router.as_poll_item(zmq::POLLIN),
        notif_rx.as_poll_item(zmq::POLLIN),
    ];

    let start = Instant::now();
    let n = zmq::poll(&mut poll_items, -1).unwrap();
    let elapsed = start.elapsed();
    done.store(true, Ordering::Relaxed);

    if n > 0 && elapsed < Duration::from_secs(2) {
        println!("OK ({elapsed:?})");
    } else {
        println!("FAIL \u{2014} zmq_poll took {elapsed:?}");
        std::process::exit(1);
    }

    let _ = notif_rx.recv_msg(0).unwrap();
}

fn run_poll_tests(socket: &zmq::Socket, label: &'static str) {
    // Test 1: Non-blocking poll (timeout=0) — works everywhere
    print!("  Test 1: poll with timeout=0 (non-blocking) ... ");
    let ready = socket.poll(zmq::POLLIN, 0).unwrap();
    if ready > 0 {
        println!("OK, data is available");
    } else {
        println!("FAIL — poll(0) says no data available for {label}");
        std::process::exit(1);
    }

    // Test 2: Blocking poll with timeout — hangs on Windows ARM64
    print!("  Test 2: poll with 3s timeout ... ");
    let done = std::sync::Arc::new(AtomicBool::new(false));
    let done2 = done.clone();

    // Watchdog: if poll doesn't return within 5s, report and abort
    std::thread::spawn(move || {
        std::thread::sleep(Duration::from_secs(5));
        if !done2.load(Ordering::Relaxed) {
            eprintln!();
            eprintln!("  HANG DETECTED — poll(3000) did not return within 5s for {label}");
            eprintln!("  This confirms the WSAPoll bug on this platform.");
            std::process::exit(2);
        }
    });

    let start = Instant::now();
    let ready = socket.poll(zmq::POLLIN, 3000).unwrap();
    let elapsed = start.elapsed();
    done.store(true, Ordering::Relaxed);

    if ready > 0 {
        println!("OK ({elapsed:?})");
    } else {
        println!("FAIL — poll(3000) timed out for {label}, took {elapsed:?}");
        std::process::exit(1);
    }

    // Consume the message
    let _ = socket.recv_msg(0).unwrap();
}
