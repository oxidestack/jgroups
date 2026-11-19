//! A stack XML that names GMS or STABLE explicitly: the chain is the
//! XML's own order, validated as `Configurator.sanityCheck` validates it
//! (`upstream/src/org/jgroups/stack/Configurator.java:243-278`). Upstream's own `ConfiguratorTest`
//! covers `ProtocolStack` insertion/removal on a live UDP channel, which this crate has no
//! counterpart for; the sanity-check behaviour is covered here and by `stack::plan`'s unit tests.

use std::time::Duration;

use jgroups::channel::JChannel;

async fn two_nodes_unicast(xml: &str, cluster: &str) {
    let a = JChannel::new(xml).await.unwrap();
    a.connect(cluster).await.unwrap();
    let b = JChannel::new(xml).await.unwrap();
    let view = b.connect(cluster).await.unwrap();
    assert_eq!(view.size(), 2);

    let (a_addr, b_addr) = (a.local_address().clone(), b.local_address().clone());
    a.send(Some(b_addr.clone()), b"ping".to_vec()).await.unwrap();
    b.send(Some(a_addr.clone()), b"pong".to_vec()).await.unwrap();
    let (from, payload) = tokio::time::timeout(Duration::from_secs(2), b.receive()).await.expect("unicast A->B arrives").unwrap();
    assert_eq!((from, payload), (a_addr, b"ping".to_vec()));
    let (from, payload) = tokio::time::timeout(Duration::from_secs(2), a.receive()).await.expect("unicast B->A arrives").unwrap();
    assert_eq!((from, payload), (b_addr, b"pong".to_vec()));
    a.disconnect().await;
    b.disconnect().await;
}

#[tokio::test]
async fn explicit_full_canonical_chain_delivers_unicast() {
    let xml = "<config><SHARED_LOOPBACK/><SHARED_LOOPBACK_PING/><NAKACK2/><UNICAST3/><STABLE/><GMS join_timeout=\"500\"/></config>";
    two_nodes_unicast(xml, "stack_config::full").await;
}

// GMS alone must get NAKACK2/UNICAST3 below it: placed above GMS, unicast never arrived.
#[tokio::test]
async fn explicit_gms_alone_gets_the_defaulted_layers_below_it() {
    two_nodes_unicast("<config><SHARED_LOOPBACK/><SHARED_LOOPBACK_PING/><GMS join_timeout=\"500\"/></config>", "stack_config::gms_only").await;
}

#[tokio::test]
async fn explicit_stable_alone_gets_the_defaulted_layers_around_it() {
    two_nodes_unicast("<config><SHARED_LOOPBACK/><SHARED_LOOPBACK_PING/><STABLE desired_avg_gossip=\"0\"/></config>", "stack_config::stable_only").await;
}

#[tokio::test]
async fn gms_below_nakack2_fails_as_jgroups_fails() {
    let e = JChannel::new("<config><SHARED_LOOPBACK/><SHARED_LOOPBACK_PING/><GMS/><NAKACK2/><UNICAST3/><STABLE/></config>").await.err().expect("must be rejected");
    assert_eq!(e.0, "events [GET_DIGEST SET_DIGEST] are required by GMS, but not provided by any of the protocols below it");
}

#[tokio::test]
async fn stable_below_nakack2_fails_as_jgroups_fails() {
    let e = JChannel::new("<config><SHARED_LOOPBACK/><SHARED_LOOPBACK_PING/><STABLE/><NAKACK2/><UNICAST3/><GMS/></config>").await.err().expect("must be rejected");
    assert_eq!(e.0, "events [GET_DIGEST] are required by STABLE, but not provided by any of the protocols below it");
}

// The rejection happens before the TCP transport binds: the port is still free afterwards.
#[tokio::test]
async fn a_rejected_tcp_stack_binds_nothing() {
    let port = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap().port()
    };
    let xml = format!(
        "<config><TCP bind_addr=\"127.0.0.1\" bind_port=\"{port}\" port_range=\"0\"/><TCPPING initial_hosts=\"\" port_range=\"0\"/><GMS/><NAKACK2/></config>"
    );
    let e = JChannel::new(&xml).await.err().expect("must be rejected");
    assert!(e.0.contains("required by GMS"), "{}", e.0);
    std::net::TcpListener::bind(("127.0.0.1", port)).expect("port was never bound");
}

#[tokio::test]
async fn duplicate_gms_fails_as_jgroups_fails() {
    let e = JChannel::new("<config><SHARED_LOOPBACK/><SHARED_LOOPBACK_PING/><NAKACK2/><GMS/><GMS/></config>").await.err().expect("must be rejected");
    assert_eq!(e.0, "Protocol ID 12 (name=GMS) is duplicate; IDs have to be unique");
}

// A user-supplied stack (MockServer's `clusterTransportConfig`): a protocol this crate cannot
// run is refused at startup, on either transport, instead of being skipped.
#[tokio::test]
async fn an_unsupported_protocol_fails_startup_on_both_transports() {
    for xml in [
        "<config><SHARED_LOOPBACK/><SHARED_LOOPBACK_PING/><FD_SOCK/><GMS/></config>",
        "<config><TCP bind_addr=\"127.0.0.1\" bind_port=\"0\" port_range=\"0\"/><MPING/><GMS/></config>",
        "<config><UDP/><PING/><GMS/></config>",
    ] {
        let e = JChannel::new(xml).await.err().unwrap_or_else(|| panic!("{xml} must be rejected"));
        assert!(e.0.starts_with("unsupported protocol "), "{xml}: {}", e.0);
    }
}
