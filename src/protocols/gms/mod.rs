//! GMS (Group Membership Service): the join tracer bullet. Port of
//! `upstream/src/org/jgroups/protocols/pbcast/{GMS,GmsImpl,ClientGmsImpl,CoordGmsImpl,
//! ParticipantGmsImpl,ViewHandler,JoinRsp}.java`, without flush.
//!
//! `view_handler` is a faithful, standalone port of `ViewHandler<R>`; `queue` is the async variant `Gms` drives its
//! request processing with, and `merge` holds the `Merger`. This module also carries the small
//! pure-logic pieces of `ClientGmsImpl`/`CoordGmsImpl` that are meaningfully unit-testable without
//! network I/O; the async join/coordinator orchestration itself lives in `crate::channel`.

pub mod merge;
pub mod merger;
pub mod protocol;
pub mod queue;
pub mod view_handler;

pub use protocol::{Gms, GmsConfig, GmsError};
pub use view_handler::ViewHandler;

use crate::protocols::discovery::PingData;
use crate::stack::BoxFuture;
use crate::util::membership::compute_new_membership;
use crate::wire::address::Address;
use crate::wire::view::View;

/// The view-change hook of flow control and fragmentation: nothing upstream is a literal `ViewListener` class — `FlowControl`/`FRAG2`
/// each override `Protocol.up(Event)`/`down(Event)`'s `VIEW_CHANGE` case directly
/// (`FlowControl.java:197,236`, `FRAG2.java:99-104,120-125`) because every upstream `Protocol` sees
/// every `Event`, membership included, by construction (`down_prot.down(evt)`/`up_prot.up(evt)`
/// propagate through the *entire* chain regardless of whether a given protocol cares). This crate's
/// `AsyncProtocol`/`AsyncProtocolStack` never grew that generic event-propagation path
/// — nothing else needs one — so `Gms`, the sole owner of "a new view was just
/// decided", calls registered listeners directly instead. `Ufc`/`Mfc`/`Frag2` implement it;
/// `stack::build_stack` constructs them before `Gms` and passes them to `Gms::new`.
pub trait ViewListener: Send + Sync {
    /// Called with the members of each new view, before the application sees it.
    fn on_view_change<'a>(&'a self, members: &'a [Address]) -> BoxFuture<'a, ()>;
}

/// `Receiver.viewAccepted(View)` (`Receiver.java`) / `MembershipListener`: the application-facing
/// view callback, called from [`Gms::install_view`] for every installed view. `subgroups` is `Some`
/// exactly when the view is a `MergeView` (the former partitions that merged), `None` for an
/// ordinary view; a merge is how an application (e.g. Infinispan's `ClusterTopologyManager`, which
/// treats a merge as `RECOVERING_CLUSTER`) tells a healed partition from a join or leave.
pub trait MembershipListener: Send + Sync {
    /// Called with each installed view; `subgroups` holds the merged partitions' views for a merge view.
    fn view_accepted<'a>(&'a self, view: &'a View, subgroups: Option<&'a [View]>) -> BoxFuture<'a, ()>;
}

/// `GmsImpl.Request`: what `ViewHandler<Request>` queues (`protocols/pbcast/GmsImpl.java:78-159`).
/// `Merge` is omitted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Request {
    /// `Request.JOIN`: `mbr` asks to join.
    Join {
        /// The member that asks to join.
        mbr: Address,
    },
    /// `Request.LEAVE`: `mbr` asks to leave.
    Leave {
        /// The member that asks to leave.
        mbr: Address,
    },
    /// `Request.SUSPECT`: `mbr` is suspected to have crashed.
    Suspect {
        /// The member that is suspected.
        mbr: Address,
    },
    /// `Request.COORD_LEAVE`: the coordinator itself is leaving.
    CoordLeave,
    /// `Request.MERGE`: the different views MERGE3 detected, keyed by the member that reported each.
    Merge {
        /// Each reporting member's view.
        views: std::collections::HashMap<Address, View>,
    },
}

impl Request {
    /// `GmsImpl.Request.getType()`: `JOIN=1, LEAVE=2, COORD_LEAVE=3, SUSPECT=4, MERGE=5`
    /// (`GmsImpl.java:79-84`); `BaseLeaveTest` sorts queued requests by it.
    pub fn type_id(&self) -> i32 {
        match self {
            Request::Join { .. } => 1,
            Request::Leave { .. } => 2,
            Request::CoordLeave => 3,
            Request::Suspect { .. } => 4,
            Request::Merge { .. } => 5,
        }
    }

    /// `GmsImpl.Request.canBeProcessedTogether`, `GmsImpl.java:152-158`: JOIN/LEAVE/SUSPECT can be
    /// coalesced into one batch; `COORD_LEAVE` (and, upstream, `MERGE`) cannot be coalesced with
    /// anything, including another `COORD_LEAVE` — each one closes its own single-request batch.
    pub fn can_be_processed_together(&self, other: &Request) -> bool {
        fn coalescable(r: &Request) -> bool {
            matches!(r, Request::Join { .. } | Request::Leave { .. } | Request::Suspect { .. })
        }
        coalescable(self) && coalescable(other)
    }
}

/// `ClientGmsImpl.getCoords(Iterable<PingData>)`, `ClientGmsImpl.java:228-242`: the addresses of
/// every discovery response flagged as coordinator, deduplicated, insertion order preserved.
/// Returns an empty `Vec` where upstream returns `null` (no coord responses) — callers distinguish
/// "no coordinator found" the same way, by checking `is_empty()`.
pub fn get_coords(responses: &[PingData]) -> Vec<Address> {
    let mut coords = Vec::new();
    for r in responses {
        if r.is_coord()
            && let Some(addr) = &r.sender
            && !coords.contains(addr)
        {
            coords.push(addr.clone());
        }
    }
    coords
}

/// `ClientGmsImpl.firstOfAllClients`, `ClientGmsImpl.java:162-182`, minus the retry-sleep side
/// effect (the caller decides what to do about "not me" — see `channel.rs`'s join loop): sorts
/// the joiner plus every responding client's address by `Address::compare_to` and returns whether
/// `joiner` sorts first (i.e. whether *this* node should become the singleton coordinator).
pub fn is_first_of_all_clients(joiner: &Address, responses: &[PingData]) -> bool {
    let mut candidates: Vec<Address> = vec![joiner.clone()];
    for r in responses {
        if let Some(addr) = &r.sender
            && !candidates.contains(addr)
        {
            candidates.push(addr.clone());
        }
    }
    candidates.sort_by(|a, b| a.compare_to(b));
    candidates.first() == Some(joiner)
}

/// `GMS.getNextView(Collection, Collection, Collection)`, `GMS.java:441-468`, minus the
/// `tmp_members`/`joining`/`leaving` bookkeeping those lines also do (`channel.rs`'s coordinator
/// state owns that directly — see its doc comment): computes the new membership and view id from
/// the current view, a set of joiners/leavers/suspects, and the running Lamport clock (`ltime`).
/// `local_addr` is `GMS.local_addr` — the fallback coordinator when the computed membership is
/// empty (a node leaving its own singleton view).
pub fn next_view_id_and_members(
    current_view_id: &crate::wire::header::ViewId,
    ltime: i64,
    local_addr: &Address,
    current_members: &[Address],
    joiners: &[Address],
    leavers: &[Address],
    suspects: &[Address],
) -> (crate::wire::header::ViewId, Vec<Address>, i64) {
    let new_ltime = current_view_id.id.max(ltime) + 1;
    let members = compute_new_membership(current_members, joiners, leavers, suspects);
    let new_coord = members.first().cloned().unwrap_or_else(|| local_addr.clone());
    let view_id = crate::wire::header::ViewId { creator: Some(new_coord), id: new_ltime };
    (view_id, members, new_ltime)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn a(n: i64) -> Address {
        Address::uuid(n, n)
    }

    fn ping(sender: Address, coord: bool) -> PingData {
        PingData::new(Some(sender), true).coord(coord)
    }

    #[test]
    fn request_coalescing_matches_gmsimpl_request() {
        let j1 = Request::Join { mbr: a(1) };
        let j2 = Request::Join { mbr: a(2) };
        let l1 = Request::Leave { mbr: a(1) };
        let s1 = Request::Suspect { mbr: a(1) };
        assert!(j1.can_be_processed_together(&j2));
        assert!(j1.can_be_processed_together(&l1));
        assert!(l1.can_be_processed_together(&s1));
        assert!(!j1.can_be_processed_together(&Request::CoordLeave));
        assert!(!Request::CoordLeave.can_be_processed_together(&Request::CoordLeave));
    }

    #[test]
    fn get_coords_collects_only_coordinator_flagged_responses() {
        let responses = vec![ping(a(1), false), ping(a(2), true), ping(a(3), true), ping(a(2), true)];
        let coords = get_coords(&responses);
        assert_eq!(coords, vec![a(2), a(3)]); // dedup, insertion order
    }

    #[test]
    fn get_coords_empty_when_no_coord_flagged() {
        let responses = vec![ping(a(1), false), ping(a(2), false)];
        assert!(get_coords(&responses).is_empty());
    }

    // ClientGmsImpl#firstOfAllClients: the numerically-first UUID among joiner + all client
    // responses becomes the singleton coordinator.
    #[test]
    fn first_of_all_clients_picks_the_numerically_smallest_uuid() {
        let joiner = a(5);
        let responses = vec![ping(a(1), false), ping(a(9), false)];
        assert!(!is_first_of_all_clients(&joiner, &responses)); // a(1) < a(5)

        let responses2 = vec![ping(a(9), false), ping(a(20), false)];
        assert!(is_first_of_all_clients(&joiner, &responses2)); // a(5) is smallest
    }

    #[test]
    fn next_view_id_and_members_advances_ltime_and_picks_first_member_as_coord() {
        let creator = a(1);
        let current_view_id = crate::wire::header::ViewId { creator: Some(creator.clone()), id: 3 };
        let (vid, members, ltime) =
            next_view_id_and_members(&current_view_id, 3, &creator, std::slice::from_ref(&creator), &[a(2)], &[], &[]);
        assert_eq!(vid.id, 4);
        assert_eq!(ltime, 4);
        assert_eq!(members, vec![creator.clone(), a(2)]);
        assert_eq!(vid.creator, Some(creator));
    }

    #[test]
    fn next_view_id_and_members_falls_back_to_local_addr_when_membership_empties() {
        let creator = a(1);
        let current_view_id = crate::wire::header::ViewId { creator: Some(creator.clone()), id: 0 };
        let (vid, members, _) = next_view_id_and_members(
            &current_view_id,
            0,
            &creator,
            std::slice::from_ref(&creator),
            &[],
            std::slice::from_ref(&creator),
            &[],
        );
        assert!(members.is_empty());
        assert_eq!(vid.creator, Some(creator));
    }
}
