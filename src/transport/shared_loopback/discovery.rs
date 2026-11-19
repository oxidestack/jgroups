//! `SharedLoopbackDiscovery`: the `protocols::discovery::Discovery` face of `SHARED_LOOPBACK_PING`
//! (`upstream/src/org/jgroups/protocols/SHARED_LOOPBACK_PING.java`). Unlike `TcpPing`, it never puts
//! a `PingHeader`-carrying message on any wire: `findMembers` reads the registered membership
//! straight out of `SHARED_LOOPBACK`'s own routing table (`Event.GET_PING_DATA`, answered
//! synchronously — there is no discovery round to wait out, matching upstream's doc comment
//! "doesn't send discovery requests and responses, but fetches discovery information directly from
//! SHARED_LOOPBACK"). So this type is never an `stack::AsyncProtocol` chain member (nothing would
//! ever call its `up_message`) — it only exists as the `Arc<dyn Discovery>` handle
//! `protocols::gms::protocol::Gms` holds, built directly by `stack::build_stack`.

use std::time::Duration;

use crate::protocols::discovery::{Discovery, Responses};
use crate::stack::BoxFuture;

use super::transport::discovery_responses_for;

/// Stateless: `find_members` takes `cluster_name` as a plain argument (`Gms::join_internal` already
/// has it), so there is nothing to cache between calls — contrast `TcpPing`, which must remember its
/// own cluster name to answer an *incoming* wire request asynchronously.
pub struct SharedLoopbackDiscovery;

impl SharedLoopbackDiscovery {
    /// The discovery of `SHARED_LOOPBACK_PING`.
    pub fn new() -> SharedLoopbackDiscovery {
        SharedLoopbackDiscovery
    }
}

impl Default for SharedLoopbackDiscovery {
    fn default() -> SharedLoopbackDiscovery {
        SharedLoopbackDiscovery::new()
    }
}

impl Discovery for SharedLoopbackDiscovery {
    /// `SHARED_LOOPBACK_PING.findMembers` (`:30-36`): every member currently registered under
    /// `cluster_name`, self included (unlike `TCPPING.findMembers`, which never pings itself) —
    /// exactly what lets a lone first node's own registration-time coordinator flag (see the parent
    /// module's doc comment) reach `Gms::join_internal` through its own discovery response.
    /// `break_on_coord_rsp`/`timeout` are accepted for interface parity with `Discovery::find_members`
    /// but unused: every response is already available, there is nothing to break out of early or
    /// wait for.
    fn find_members<'a>(&'a self, cluster_name: &'a str, break_on_coord_rsp: bool, _timeout: Duration) -> BoxFuture<'a, Responses> {
        Box::pin(async move {
            let mut responses = Responses::new(0, break_on_coord_rsp);
            for pd in discovery_responses_for(cluster_name) {
                responses.add_response(pd, false);
            }
            responses
        })
    }
}

#[cfg(test)]
mod tests {
    use super::super::transport::SharedLoopback;
    use super::*;
    use crate::transport::Transport;
    use crate::wire::address::Address;
    use std::sync::atomic::AtomicBool;
    use std::sync::Arc;

    fn coord_flag() -> Arc<AtomicBool> {
        Arc::new(AtomicBool::new(false))
    }

    #[tokio::test]
    async fn no_registered_members_is_an_empty_result() {
        let d = SharedLoopbackDiscovery::new();
        let responses = d.find_members("shared_loopback_discovery::empty", true, Duration::from_millis(50)).await;
        assert!(responses.is_empty());
    }

    #[tokio::test]
    async fn a_lone_registrant_sees_itself_as_coordinator() {
        let cluster = "shared_loopback_discovery::lone_registrant";
        let local = Address::random_uuid();
        let (sl, _rx) = SharedLoopback::new(local.clone(), coord_flag());
        sl.connect(cluster);

        let d = SharedLoopbackDiscovery::new();
        let responses = d.find_members(cluster, true, Duration::from_millis(50)).await;
        assert_eq!(responses.len(), 1);
        assert!(responses.is_coord(&local));

        sl.stop().await;
    }

    #[tokio::test]
    async fn a_second_registrant_is_not_coordinator_and_sees_both_members() {
        let cluster = "shared_loopback_discovery::second_registrant";
        let local_a = Address::random_uuid();
        let local_b = Address::random_uuid();
        let (a, _a_rx) = SharedLoopback::new(local_a.clone(), coord_flag());
        let (b, _b_rx) = SharedLoopback::new(local_b.clone(), coord_flag());
        a.connect(cluster);
        b.connect(cluster);

        let d = SharedLoopbackDiscovery::new();
        let responses = d.find_members(cluster, true, Duration::from_millis(50)).await;
        assert_eq!(responses.len(), 2);
        assert!(responses.is_coord(&local_a));
        assert!(!responses.is_coord(&local_b));

        a.stop().await;
        b.stop().await;
    }
}
