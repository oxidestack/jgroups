//! An unofficial Rust port of [JGroups](https://github.com/belaban/JGroups) 5.2.28.Final, the Java toolkit for
//! reliable cluster communication: channels, group membership, failure detection, reliable delivery and the TCP
//! transport.
//!
//! A [`channel::JChannel`] is built from a JGroups stack configuration, the same XML a Java node reads, and joins a
//! cluster by name. Its members agree on a [`wire::view::View`] of the cluster, and a message sent to the cluster or to
//! one member arrives once, in order per sender. The bytes on the wire are JGroups's own, so a Rust node and a Java
//! node of the same version join one cluster.
//!
//! The protocols ported are those in the stacks this crate was written for: `TCP` and `SHARED_LOOPBACK` (an
//! in-process transport for tests), `TCPPING` and `DNS_PING` discovery, `MERGE3`, `FD_ALL3`, `VERIFY_SUSPECT2`,
//! `NAKACK2`, `UNICAST3`, `STABLE`, `GMS`, `UFC`, `MFC` and `FRAG2`. A protocol outside that list fails
//! [`JChannel::new`](channel::JChannel::new) with a message naming it. Within that scope, a difference from
//! JGroups's behaviour is a bug.
//!
//! # Example
//!
//! Two channels on the in-process transport join one cluster and one sends the other a message:
//!
//! ```
//! use jgroups::channel::JChannel;
//!
//! # #[tokio::main(flavor = "multi_thread")]
//! # async fn main() -> Result<(), jgroups::channel::ChannelError> {
//! let config = r#"<config>
//!     <SHARED_LOOPBACK/><SHARED_LOOPBACK_PING/>
//!     <NAKACK2/><UNICAST3/><STABLE/><GMS print_local_addr="false"/>
//! </config>"#;
//!
//! let a = JChannel::new(config).await?;
//! let b = JChannel::new(config).await?;
//! a.connect("crate-docs").await?;
//! b.connect("crate-docs").await?;
//!
//! b.send(Some(a.local_address().clone()), b"hello".to_vec()).await?;
//! let (from, payload) = a.receive().await.expect("a message");
//! assert_eq!(from, *b.local_address());
//! assert_eq!(payload, b"hello");
//!
//! b.close().await;
//! a.close().await;
//! # Ok(())
//! # }
//! ```
//!
//! Over the network, the stack names `TCP` and a discovery protocol instead, as a JGroups configuration does:
//! `<TCP bind_port="7800"/><TCPPING initial_hosts="10.0.0.1[7800],10.0.0.2[7800]"/>`, with `${VAR:default}`
//! substituted from the environment and system properties as JGroups does ([`conf::parse`]).
//!
//! # Modules
//!
//! - [`channel`]: [`JChannel`](channel::JChannel), the application's handle: connect, send, receive, view.
//! - [`conf`]: the XML stack configuration and its variable substitution.
//! - [`stack`]: the protocol chain and how a configuration becomes one.
//! - [`protocols`]: the protocols above the transport.
//! - [`transport`]: `TCP`, `SHARED_LOOPBACK` and the transport core they share.
//! - [`wire`]: the binary wire format: addresses, messages, headers, views.
//! - [`util`]: the runtime helpers the protocols share (digests, tables, credits).

#![warn(missing_docs)]

pub mod channel;
pub mod conf;
pub mod protocols;
pub mod stack;
pub mod transport;
pub mod util;
pub mod wire;
