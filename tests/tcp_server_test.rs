//! Adapted port of `upstream/tests/junit-functional/org/jgroups/tests/TcpServerTest.java`
//! (`testCloseOnBlockedSend`, guarding JGRP-2350: closing a connection whose peer never reads must
//! not hang forever).
//!
//! **Adaptation, and why:** upstream's regression is specific to blocking I/O (`TcpConnection`
//! wraps a plain `Socket`; a writer *thread* blocks inside `OutputStream.write()` once the kernel
//! send buffer fills, and the bug was that `close()` could then deadlock waiting for that thread).
//! This port's connections are tokio tasks, not blocked OS threads, and `TcpServer::stop()` never
//! takes the per-connection write mutex a stuck send might be holding — so the specific deadlock
//! upstream guards against cannot occur here by construction (there is no lock ordering between
//! `stop()` and a send in flight for it to invert). What *is* still worth asserting, and is the
//! practically-equivalent property a caller cares about, is that `stop()` completes promptly even
//! while a connection to an unresponsive peer has a send stuck against a full socket buffer —
//! i.e. one slow peer cannot wedge shutdown of the whole server.

use std::io::Read;
use std::net::{Ipv4Addr, TcpListener as StdTcpListener};
use std::time::Duration;

use jgroups::transport::tcp::server::TcpServer;
use jgroups::wire::address::Address;

#[tokio::test]
async fn test_close_on_blocked_send() {
    // A raw listener that accepts once and then never reads — the "peer that never drains its
    // socket buffer" from the upstream test's `acceptor` (`srv_sock.accept()` then an infinite
    // sleep loop without reading).
    let listener = StdTcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    let listen_port = listener.local_addr().unwrap().port();
    let accepted = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        // Never read; just hold the socket open until the test drops it.
        let mut discard = [0u8; 1];
        // A short, bounded read attempt so the thread eventually exits when the test ends and
        // the peer closes, rather than blocking the test process at exit.
        stream.set_read_timeout(Some(Duration::from_secs(30))).ok();
        let _ = stream.read(&mut discard);
    });

    let (server, _rx) = TcpServer::bind(Ipv4Addr::LOCALHOST.into(), 0, 0, false, true).await.unwrap();
    let dest = Address::ip4(127, 0, 0, 1, listen_port);

    // Fire off writes until the kernel buffer fills and one of them is stuck pending — the upstream
    // `writer` thread's `for(;;) client.send(array,...)` loop, bounded here so a healthy run (buffer
    // never fills, all writes complete) still finishes the test rather than looping forever.
    let big_chunk = vec![0u8; 8192 * 4];
    let writer_server = server.clone();
    let writer_dest = dest.clone();
    let writer = tokio::spawn(async move {
        for _ in 0..4096 {
            if writer_server.send(Some(&writer_dest), &big_chunk).await.is_err() {
                break;
            }
        }
    });

    // Give the writer time to run into a full buffer (upstream waits for 2s of continuous
    // blocked-write evidence before triggering the close).
    tokio::time::sleep(Duration::from_millis(1500)).await;

    // The property under test: stop() itself returns promptly, regardless of whether the writer
    // task above is currently stuck on a full send buffer to the non-reading peer.
    let stop_result = tokio::time::timeout(Duration::from_secs(5), server.stop()).await;
    assert!(stop_result.is_ok(), "TcpServer::stop() must not hang because one peer never reads");

    writer.abort();
    drop(accepted); // the accept()'d thread's read() will time out and exit on its own
}
