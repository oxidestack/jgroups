//! Shared TP-core logic: the down-path send/loopback decision and up-path own-message/version
//! filtering every concrete transport (`tcp::Tcp`, `shared_loopback::SharedLoopback`) needs before/after it moves bytes (a socket for `Tcp`, a direct in-process channel
//! push for `SharedLoopback` — same framing either way, see [`decode_received`]). Port of
//! `upstream/src/org/jgroups/protocols/TP.java:1025-1058` (`down(Message)`) and `:1225-1268`
//! (`receive`/`versionMatch`).
//!
//! Not ported here: the bundler (`TransferQueueBundler`), `MaxOneThreadPerSender` delivery, the
//! logical-to-physical `logical_addr_cache` (`LazyRemovalCache`, 2000 entries/360s expiry/60s
//! reaper) — `Tcp`'s addresses are physical (`IpAddress`) end to end, so there is no logical
//! address to resolve there (that arrives with GMS/UUID-based membership); the
//! equivalent mapping for `SharedLoopback` is the identity function (its own module doc comment
//! explains why — routing there is keyed by logical UUID directly, matching upstream's own
//! `SHARED_LOOPBACK.routing_table`).

use crate::wire::address::Address;
use crate::wire::header::TpHeader;
use crate::wire::message::{self, Message};
use crate::wire::stream::WireReader;
use crate::wire::version;

/// What `TP.down(Message)` decides for one outgoing message (`TP.java:1037-1043`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SendDecision {
    /// Send it out over the wire (to one peer if `dest` is set, to every connection if not —
    /// `BasicTCP`'s "sendToAll becomes N unicasts", `BasicTCP.java:82`).
    pub send: bool,
    /// Deliver it directly back up the local stack without touching the network.
    pub loop_back: bool,
}

/// Port of the `multicast`/`do_send`/`loop_back` computation in `TP.down(Message)`. `dest`/`src`
/// are the message's own fields (already set by the caller via [`set_source_and_header`]); `dont_loopback` is
/// `Message.TransientFlag.DONT_LOOPBACK`; `local_physical_addr` is this node's own physical
/// address (`TP.java:1040`, the `dest instanceof PhysicalAddress && dest.equals(local_physical_addr)`
/// branch — in this crate every address in scope other than a bare `UUID` join address *is*
/// physical, so the check is really just address equality).
pub fn decide_send(
    dest: Option<&Address>,
    src: Option<&Address>,
    dont_loopback: bool,
    local_physical_addr: Option<&Address>,
) -> SendDecision {
    let multicast = dest.is_none();
    let mut do_send = multicast || dest != src;
    let mut loop_back = (multicast || dest == src) && !dont_loopback;

    // `dest instanceof PhysicalAddress` (`TP.java:1040`): a logical `UUID` destination never takes this
    // branch, even over `SHARED_LOOPBACK` where this crate's "physical" address is the logical one — so a
    // `DONT_LOOPBACK` unicast to self stays undelivered (UnicastUnitTest#testMessagesToSelf).
    if let (Some(d), Some(local)) = (dest, local_physical_addr)
        && matches!(d, Address::IpAddress { .. })
        && d == local
    {
        loop_back = true;
        do_send = false;
    }
    SendDecision { send: do_send, loop_back }
}

/// `TP.down(Message)`'s header/source attachment (`:1026-1029`), before [`decide_send`] runs.
/// `putHeader` (`Headers.java:114-141`): replaces any existing header under the same protocol id
/// rather than appending a duplicate — `Message::put_header` already has that behaviour.
/// `TP.down` does `putHeader(this.id, hdr)` (`TP.java:1027`), and `TP.init` sets `this.id` to the id
/// of `TP` itself (`TP.java:695`: `ClassConfigurator.getProtocolId(TP.class)` = 57), not the concrete
/// transport's (`TCP` = 17): every JGroups transport carries its `TpHeader` under 57. Without the cluster name set on the
/// transport no `TpHeader` is attached at all, and a Java peer drops the message (`JGRP000027 ... hdr
/// is null`).
pub fn set_source_and_header(msg: &mut Message, local_addr: Address, cluster_name: &str) {
    msg.put_header(crate::wire::ids::PROT_ID_TP, crate::wire::header::HeaderValue::Tp(TpHeader::new(cluster_name)));
    msg.src = Some(local_addr);
}

/// `TP.receive(Address, ...)`'s own-message drop (`:1229-1230`, "drop message from self; it has
/// already been looped back up").
pub fn is_from_self(sender: &Address, local_physical_addr: Option<&Address>) -> bool {
    Some(sender) == local_physical_addr
}

/// `TP.receive(Address sender, DataInput in)` (`:1251-1268`): own-message drop, then version check,
/// then decode. Shared by every concrete transport's `decode_received`. Batches (`is_message_list`) are out of
/// scope.
pub fn decode_received(sender: Address, bytes: &[u8], local_physical_addr: Option<&Address>) -> Option<(Address, Message)> {
    if is_from_self(&sender, local_physical_addr) {
        return None;
    }
    let mut r = WireReader::new(bytes);
    // `TP.receive` catches whatever reading the frame throws and drops it (`IncomingMsgFailure`)
    let (peer_version, _multicast, msg) = message::try_read_message(&mut r).ok()?;
    if !version::is_binary_compatible(peer_version, version::VERSION, true, false) {
        return None;
    }
    Some((sender, msg))
}

/// `TP.receive` with message batches (`handleMessageBatch`, `TP.java:1270-1280`): a frame whose flags
/// carry `LIST` (JGroups' bundler packs several messages into one frame) yields every message in
/// it, OOB ones first as upstream processes the OOB batch before the regular one; a plain frame
/// yields at most one. The `TpHeader` is left out of batched messages (the list header carries the
/// cluster name), nothing here reads it back.
pub fn decode_received_all(sender: Address, bytes: &[u8], local_physical_addr: Option<&Address>) -> Vec<(Address, Message)> {
    if is_from_self(&sender, local_physical_addr) || bytes.len() < 3 {
        return Vec::new();
    }
    const LIST: u8 = 1;
    if bytes[2] & LIST == 0 {
        return decode_received(sender, bytes, local_physical_addr).into_iter().collect();
    }
    let mut r = WireReader::new(bytes);
    let Ok((peer_version, _multicast, reg, oob)) = crate::wire::batch::try_read_message_batch(&mut r) else { return Vec::new() };
    if !version::is_binary_compatible(peer_version, version::VERSION, true, false) {
        return Vec::new();
    }
    oob.into_iter().chain(reg).flat_map(|b| b.iter().cloned().collect::<Vec<_>>()).map(|m| (sender.clone(), m)).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn a() -> Address {
        Address::ip4(10, 0, 0, 1, 7800)
    }
    fn b() -> Address {
        Address::ip4(10, 0, 0, 2, 7800)
    }

    #[test]
    fn unicast_to_other_sends_without_loopback() {
        let d = decide_send(Some(&b()), Some(&a()), false, Some(&a()));
        assert!(d.send);
        assert!(!d.loop_back);
    }

    #[test]
    fn unicast_to_self_loops_back_and_does_not_send() {
        let d = decide_send(Some(&a()), Some(&a()), false, Some(&a()));
        assert!(!d.send);
        assert!(d.loop_back);
    }

    // TP.java:1040: only a PhysicalAddress destination is forced to loop back; a logical UUID to self honours
    // DONT_LOOPBACK even when the transport's physical address is that same UUID (SHARED_LOOPBACK here).
    #[test]
    fn logical_unicast_to_self_with_dont_loopback_is_dropped() {
        let me = Address::uuid(1, 2);
        let d = decide_send(Some(&me), Some(&me), true, Some(&me));
        assert!(!d.send);
        assert!(!d.loop_back);
        // a physical destination equal to the local physical address always loops back
        let d = decide_send(Some(&a()), Some(&Address::uuid(1, 2)), true, Some(&a()));
        assert!(!d.send);
        assert!(d.loop_back);
    }

    #[test]
    fn multicast_sends_and_loops_back() {
        let d = decide_send(None, Some(&a()), false, Some(&a()));
        assert!(d.send);
        assert!(d.loop_back);
    }

    #[test]
    fn multicast_with_dont_loopback_only_sends() {
        let d = decide_send(None, Some(&a()), true, Some(&a()));
        assert!(d.send);
        assert!(!d.loop_back);
    }

    #[test]
    fn dest_matches_local_physical_addr_even_without_src_set() {
        // The `dest instanceof PhysicalAddress && dest.equals(local_physical_addr)` branch fires
        // independent of src (e.g. a message with no src yet set, sent to our own physical addr).
        let d = decide_send(Some(&a()), None, false, Some(&a()));
        assert!(!d.send);
        assert!(d.loop_back);
    }

    #[test]
    fn is_from_self_matches_physical_addr() {
        assert!(is_from_self(&a(), Some(&a())));
        assert!(!is_from_self(&b(), Some(&a())));
    }
}
