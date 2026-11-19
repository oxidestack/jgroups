//! `TcpPing`: the `stack::AsyncProtocol` face of `TCPPING`. Wraps the
//! free functions in `super` (`send_discovery_requests`/`handle_ping_message`) and
//! `TcpPingConfig` (`tcpping.rs`) behind the chain-of-responsibility shape
//! `stack::build_stack` assembles, so `crate::channel` no longer matches `PingHeader`s or threads
//! discovery responses through itself.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, Mutex};

use crate::stack::{AsyncProtocol, BoxFuture};
use crate::transport::tcp::Tcp;
use crate::wire::address::Address;
use crate::wire::header::HeaderValue;
use crate::wire::ids::PROT_ID_TCPPING;
use crate::wire::message::{Message, Payload};

use super::{handle_ping_message, send_discovery_requests, tcpping::TcpPingConfig, Discovery, DiscoveryEvent, PingData, Responses};

/// `TCPPING` (`protocols/TCPPING.java`): discovery by asking a static list of hosts.
pub struct TcpPing {
    tcp: Arc<Tcp>,
    config: TcpPingConfig,
    local_logical: Address,
    cluster_name: Mutex<Option<String>>,
    /// Shared with `protocols::gms::Gms` (set at construction by `stack::build_stack`): whether
    /// *this* node currently believes itself to be the coordinator, for the `PingData.coord` flag
    /// on a `GET_MBRS_RSP` (`Discovery.java`'s response construction reads `gms.isCoord()`
    /// indirectly; we don't have a live `gms` reference here to avoid a reference cycle, so `Gms`
    /// just keeps this flag current instead — see `Gms::set_is_coord`).
    is_coord: Arc<AtomicBool>,
    response_tx: Mutex<Option<mpsc::UnboundedSender<PingData>>>,
}

impl TcpPing {
    /// Builds the protocol over `tcp`.
    pub fn new(tcp: Arc<Tcp>, config: TcpPingConfig, local_logical: Address, is_coord: Arc<AtomicBool>) -> TcpPing {
        TcpPing { tcp, config, local_logical, cluster_name: Mutex::new(None), is_coord, response_tx: Mutex::new(None) }
    }

    /// Sets the cluster name its discovery requests carry and incoming ones are filtered on.
    pub async fn set_cluster_name(&self, name: &str) {
        *self.cluster_name.lock().await = Some(name.to_string());
    }

    /// `TCPPING.findMembers` + the blocking half of `Discovery.findMembers`/`Responses.waitFor`:
    /// sends a `GET_MBRS_REQ` to every configured initial host and collects responses for up to
    /// `timeout` (or until `break_on_coord_rsp` sees a coordinator response). Moved here unchanged
    /// from `crate::channel::JChannel::connect`; `protocols::gms::Gms::join` is now the
    /// only caller.
    pub async fn find_members(&self, cluster_name: &str, break_on_coord_rsp: bool, timeout: Duration) -> Responses {
        self.set_cluster_name(cluster_name).await;
        let (targets, _unresolved) = self.config.resolve_initial_hosts();
        let local_physical = self.tcp.local_address().clone();

        let (tx, mut rx) = mpsc::unbounded_channel();
        *self.response_tx.lock().await = Some(tx);
        send_discovery_requests(&self.tcp, &targets, &self.local_logical, &local_physical, cluster_name, true, None, PROT_ID_TCPPING).await;

        let mut responses = Responses::new(0, break_on_coord_rsp);
        let deadline = tokio::time::Instant::now() + timeout;
        while let Ok(Some(pd)) = tokio::time::timeout_at(deadline, rx.recv()).await {
            if responses.add_response(pd, false) {
                break;
            }
        }
        *self.response_tx.lock().await = None;
        responses
    }

    async fn handle_ping(&self, hdr: crate::wire::header::PingHeader, msg: &Message, sender_physical: Address) {
        let payload = match &msg.payload {
            Payload::Bytes { array: Some(b), offset, length } => &b[*offset..*offset + *length],
            _ => &[][..],
        };
        let cluster_name = self.cluster_name.lock().await.clone().unwrap_or_default();
        let is_coord = self.is_coord.load(Ordering::SeqCst);
        let event = handle_ping_message(
            &self.tcp,
            &hdr,
            payload,
            &sender_physical,
            &self.local_logical,
            self.tcp.local_address(),
            &cluster_name,
            is_coord,
            PROT_ID_TCPPING,
        )
        .await;
        match event {
            DiscoveryEvent::Response(pd) => {
                if let Some(sender) = &pd.sender {
                    self.tcp.remember_physical(sender.clone(), sender_physical).await;
                }
                if let Some(tx) = &*self.response_tx.lock().await {
                    let _ = tx.send(pd);
                }
            }
            DiscoveryEvent::RequestAnswered(Some(pd)) | DiscoveryEvent::RequestIgnored(Some(pd)) => {
                if let Some(sender) = &pd.sender {
                    self.tcp.remember_physical(sender.clone(), sender_physical).await;
                }
            }
            DiscoveryEvent::RequestAnswered(None) | DiscoveryEvent::RequestIgnored(None) | DiscoveryEvent::ClusterMismatch | DiscoveryEvent::Malformed => {}
        }
    }

    /// [`TcpPingConfig::resolve_initial_hosts`] of this protocol's configuration.
    #[allow(dead_code)]
    pub fn resolve_initial_hosts(&self) -> (Vec<SocketAddr>, std::collections::HashSet<String>) {
        self.config.resolve_initial_hosts()
    }
}

/// `protocols::discovery::Discovery` face of `TcpPing` — what `protocols::gms::Gms::join` actually
/// calls (see `discovery::mod`'s "Discovery-protocol-agnostic seam" comment) so `Gms` can hold this, `DnsPing`, or `SharedLoopbackDiscovery` behind one `Arc<dyn Discovery>`:
/// a thin wrapper over the inherent `find_members` above, unchanged behaviour. Direct calls to
/// `tcp_ping.find_members(...)` on a concrete `&TcpPing` keep resolving to the inherent method (it
/// shadows the trait one), so this is additive, not a behaviour change for any existing caller.
impl Discovery for TcpPing {
    fn find_members<'a>(&'a self, cluster_name: &'a str, break_on_coord_rsp: bool, timeout: Duration) -> BoxFuture<'a, Responses> {
        Box::pin(async move { TcpPing::find_members(self, cluster_name, break_on_coord_rsp, timeout).await })
    }
}

impl AsyncProtocol for TcpPing {
    fn name(&self) -> &'static str {
        "TCPPING"
    }

    fn up_message<'a>(&'a self, sender_physical: Address, msg: Message) -> BoxFuture<'a, Option<(Address, Message)>> {
        Box::pin(async move {
            if let Some(HeaderValue::Ping(hdr)) = msg.get_header(PROT_ID_TCPPING) {
                let hdr = hdr.clone();
                self.handle_ping(hdr, &msg, sender_physical).await;
                None
            } else {
                Some((sender_physical, msg))
            }
        })
    }
}
