//! The TCP handshake and length-prefixed framing every connection (outbound or accepted) uses
//! after the socket is open. Port of `upstream/src/org/jgroups/blocks/cs/TcpConnection.java:85-123`
//! (`connect`), `:218-272` (`sendLocalAddress`/`readPeerAddress`) and `:131-175`
//! (`send`/`doSend`, the length-prefixed frame).

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};

use crate::wire::address::{self, Address};
use crate::wire::version;

/// `Connection.cookie`, `blocks/cs/Connection.java:13`: `{'b','e','l','a'}`.
pub const COOKIE: [u8; 4] = *b"bela";

/// `BaseServer.OK`/`FAIL`, `blocks/cs/BaseServer.java:75-76`.
pub const ACK_OK: [u8; 4] = [1, 2, 3, 4];
/// `BaseServer.FAIL`: the answer to a connection that already exists.
pub const ACK_FAIL: [u8; 4] = [4, 3, 2, 1];

/// Why a connection's handshake failed.
#[derive(Debug)]
pub enum HandshakeError {
    /// The socket failed.
    Io(std::io::Error),
    /// The peer did not start with JGroups's cookie: it is not a JGroups node.
    BadCookie,
    /// The peer's JGroups version is not binary compatible with this one.
    IncompatibleVersion(u16),
    /// The peer refused the connection with [`ACK_FAIL`].
    AckFailed,
    /// The peer answered with bytes that are neither [`ACK_OK`] nor [`ACK_FAIL`].
    UnexpectedAck(Vec<u8>),
}
impl From<std::io::Error> for HandshakeError {
    fn from(e: std::io::Error) -> HandshakeError {
        HandshakeError::Io(e)
    }
}
impl std::fmt::Display for HandshakeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HandshakeError::Io(e) => write!(f, "I/O error during TCP handshake: {e}"),
            HandshakeError::BadCookie => write!(f, "cookie mismatch: peer is not a JGroups node"),
            HandshakeError::IncompatibleVersion(v) => {
                write!(f, "incompatible JGroups wire version: {} (ours: {})", version::print(*v), version::print(version::VERSION))
            }
            HandshakeError::AckFailed => write!(f, "peer rejected the connection (FAIL ack)"),
            HandshakeError::UnexpectedAck(b) => write!(f, "unexpected ack bytes: {b:?}"),
        }
    }
}
impl std::error::Error for HandshakeError {}

/// `TcpConnection.sendLocalAddress`, `:222-238`: cookie, then `short version`, `short addr_size`,
/// then the local `IpAddress` body (no flags byte — `local_addr.writeTo(os)` calls the concrete
/// type's own `writeTo`, not the generic flags-byte dispatch every other address goes through).
pub async fn write_handshake(out: &mut OwnedWriteHalf, local_addr: &Address) -> Result<(), HandshakeError> {
    let mut body = Vec::new();
    local_addr.write_to(&mut body);

    let mut frame = Vec::with_capacity(4 + 2 + 2 + body.len());
    frame.extend_from_slice(&COOKIE);
    frame.extend_from_slice(&version::VERSION.to_be_bytes());
    frame.extend_from_slice(&(body.len() as u16).to_be_bytes());
    frame.extend_from_slice(&body);
    out.write_all(&frame).await?;
    out.flush().await?;
    Ok(())
}

/// `TcpConnection.readPeerAddress`, `:244-272`: reads and checks the cookie, checks the version
/// (major/minor only — `version_check_micro=false`, matching upstream's own default), reads (and
/// discards, per upstream's own comment "only needed by NioConnection") the address-size short,
/// then the peer's `IpAddress` body.
pub async fn read_handshake(input: &mut OwnedReadHalf) -> Result<Address, HandshakeError> {
    let mut cookie = [0u8; 4];
    input.read_exact(&mut cookie).await?;
    if cookie != COOKIE {
        return Err(HandshakeError::BadCookie);
    }
    let mut version_buf = [0u8; 2];
    input.read_exact(&mut version_buf).await?;
    let peer_version = u16::from_be_bytes(version_buf);
    if !version::is_binary_compatible(peer_version, version::VERSION, true, false) {
        return Err(HandshakeError::IncompatibleVersion(peer_version));
    }
    let mut size_buf = [0u8; 2];
    input.read_exact(&mut size_buf).await?; // addr_size, unused here
    let addr_size = u16::from_be_bytes(size_buf) as usize;
    let mut body = vec![0u8; addr_size];
    input.read_exact(&mut body).await?;
    let mut r = crate::wire::stream::WireReader::new(&body);
    // a malformed peer address fails the handshake (upstream's `readPeerAddress` throws), never the acceptor
    address::read_ip_address_body(&mut r).map_err(|e| HandshakeError::Io(std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string())))
}

/// `TcpConnection.doSend`, `:168-175`: a 4-byte big-endian length, then the payload. Used both for
/// ordinary messages and for the OK/FAIL ack bytes (`TcpServer.Acceptor.handleAccept`,
/// `:126-135`, sends the ack through the very same `conn.send()`).
pub async fn write_frame(out: &mut OwnedWriteHalf, data: &[u8]) -> std::io::Result<()> {
    out.write_all(&(data.len() as i32).to_be_bytes()).await?;
    out.write_all(data).await?;
    out.flush().await
}

/// `TcpConnection.Receiver.run`, `:300-306`: `int len=in.readInt()` then that many bytes.
pub async fn read_frame(input: &mut OwnedReadHalf) -> std::io::Result<Vec<u8>> {
    let mut len_buf = [0u8; 4];
    input.read_exact(&mut len_buf).await?;
    let len = i32::from_be_bytes(len_buf) as usize;
    let mut buf = vec![0u8; len];
    input.read_exact(&mut buf).await?;
    Ok(buf)
}

/// Reads and interprets a 4-byte-framed OK/FAIL ack (`TcpConnection.connect`, `:103-113`).
pub async fn read_ack(input: &mut OwnedReadHalf) -> Result<(), HandshakeError> {
    let bytes = read_frame(input).await?;
    if bytes == ACK_OK {
        Ok(())
    } else if bytes == ACK_FAIL {
        Err(HandshakeError::AckFailed)
    } else {
        Err(HandshakeError::UnexpectedAck(bytes))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::{TcpListener, TcpStream};

    fn ip4(a: u8, b: u8, c: u8, d: u8, port: u16) -> Address {
        Address::ip4(a, b, c, d, port)
    }

    #[tokio::test]
    async fn handshake_roundtrips_over_a_real_loopback_socket() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let client_addr = ip4(127, 0, 0, 1, 4242);
        let client_addr_clone = client_addr.clone();
        let client = tokio::spawn(async move {
            let stream = TcpStream::connect(addr).await.unwrap();
            let (_r, mut w) = stream.into_split();
            write_handshake(&mut w, &client_addr_clone).await.unwrap();
        });

        let (stream, _) = listener.accept().await.unwrap();
        let (mut r, _w) = stream.into_split();
        let peer = read_handshake(&mut r).await.unwrap();
        client.await.unwrap();
        assert_eq!(peer, client_addr);
    }

    #[tokio::test]
    async fn bad_cookie_is_rejected() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let stream = TcpStream::connect(addr).await.unwrap();
            let (_r, mut w) = stream.into_split();
            w.write_all(b"nope").await.unwrap();
            w.write_all(&[0u8; 4]).await.unwrap();
            w.flush().await.unwrap();
        });
        let (stream, _) = listener.accept().await.unwrap();
        let (mut r, _w) = stream.into_split();
        let err = read_handshake(&mut r).await.unwrap_err();
        assert!(matches!(err, HandshakeError::BadCookie));
    }

    #[tokio::test]
    async fn incompatible_version_is_rejected() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let stream = TcpStream::connect(addr).await.unwrap();
            let (_r, mut w) = stream.into_split();
            w.write_all(&COOKIE).await.unwrap();
            w.write_all(&99u16.to_be_bytes()).await.unwrap(); // major=0, minor=1: incompatible
            w.write_all(&0u16.to_be_bytes()).await.unwrap();
            w.flush().await.unwrap();
        });
        let (stream, _) = listener.accept().await.unwrap();
        let (mut r, _w) = stream.into_split();
        let err = read_handshake(&mut r).await.unwrap_err();
        assert!(matches!(err, HandshakeError::IncompatibleVersion(99)));
    }

    #[tokio::test]
    async fn frame_roundtrips() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let payload = b"hello world".to_vec();
        let payload_clone = payload.clone();
        tokio::spawn(async move {
            let stream = TcpStream::connect(addr).await.unwrap();
            let (_r, mut w) = stream.into_split();
            write_frame(&mut w, &payload_clone).await.unwrap();
        });
        let (stream, _) = listener.accept().await.unwrap();
        let (mut r, _w) = stream.into_split();
        let got = read_frame(&mut r).await.unwrap();
        assert_eq!(got, payload);
    }

    #[tokio::test]
    async fn empty_frame_roundtrips() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let stream = TcpStream::connect(addr).await.unwrap();
            let (_r, mut w) = stream.into_split();
            write_frame(&mut w, &[]).await.unwrap();
        });
        let (stream, _) = listener.accept().await.unwrap();
        let (mut r, _w) = stream.into_split();
        let got = read_frame(&mut r).await.unwrap();
        assert!(got.is_empty());
    }

    #[tokio::test]
    async fn ack_ok_and_fail_are_distinguished() {
        for (bytes, expect_ok) in [(ACK_OK, true), (ACK_FAIL, false)] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            tokio::spawn(async move {
                let stream = TcpStream::connect(addr).await.unwrap();
                let (_r, mut w) = stream.into_split();
                write_frame(&mut w, &bytes).await.unwrap();
            });
            let (stream, _) = listener.accept().await.unwrap();
            let (mut r, _w) = stream.into_split();
            let result = read_ack(&mut r).await;
            assert_eq!(result.is_ok(), expect_ok);
        }
    }
}
