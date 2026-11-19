//! `Merger`: reconciles independently-formed views of the same cluster name. Port of
//! `upstream/src/org/jgroups/protocols/pbcast/Merger.java`, scoped to what this crate can
//! faithfully exercise:
//!
//! **Fully ported** (no network I/O, no flush, no digests — pure view-shape logic): the merge
//! participant/coordinator helpers `Util.determineMergeParticipants`/`determineMergeCoords`/
//! `determineActualMergeCoords` and `Merger.sanitizeViews`, plus `Merger.determineMergeLeader`'s
//! deterministic election. `tests/junit-functional/org/jgroups/tests/MergerTest.java`'s 7 methods
//! (no network I/O there either — it calls these helpers directly) are ported verbatim below.
//!
//! **The merge itself**: `Merger.merge` -> `MergeTask` ->
//! `MERGE_REQ`/`MERGE_RSP`/`GET_DIGEST_REQ`/`GET_DIGEST_RSP`/`INSTALL_MERGE_VIEW` relayed through every subgroup
//! coordinator runs in `gms::merge` (the `impl Gms` blocks); this module holds its pure parts: the merge-coordinator
//! helpers, `sanitizeViews`, `consolidateMergeData` and `consolidateDigests`. Flush (`startFlush`, `INSTALL_MERGE_VIEW_OK`,
//! `merge_ack_collector`) is not ported: no MockServer stack contains FLUSH.

use std::collections::{HashMap, HashSet};

use crate::wire::address::Address;
use crate::util::digest as digest_ops;
use crate::util::membership::compute_merge_membership;
use crate::wire::header::ViewId;
use crate::wire::view::{Digest, View};

/// `Merger.sanitizeViews(Map<Address,View>)`, `:195-218`: removes members from a subview who
/// don't have *us* (the map's key) in *their* subview — "A and B don't have C in their views" ->
/// C's subview shrinks to just `{C}`. Mutates in place like upstream.
pub fn sanitize_views(map: &mut HashMap<Address, View>) {
    let snapshot: HashMap<Address, Vec<Address>> = map.iter().map(|(k, v)| (k.clone(), v.members.clone())).collect();
    for (key, view) in map.iter_mut() {
        let mut members = view.members.clone();
        let mut modified = false;
        members.retain(|val| {
            if val == key {
                return true; // "we can always talk to ourself"
            }
            match snapshot.get(val) {
                Some(their_members) if !their_members.contains(key) => {
                    modified = true;
                    false
                }
                _ => true,
            }
        });
        if modified {
            *view = View { view_id: view.view_id.clone(), members };
        }
    }
}

/// `Util.determineMergeCoords(Map<Address,View>)`, `Util.java:2922-2934`: every distinct view
/// creator.
pub fn determine_merge_coords(map: &HashMap<Address, View>) -> HashSet<Address> {
    map.values().filter_map(|v| v.creator().cloned()).collect()
}

/// `Util.determineActualMergeCoords(Map<Address,View>)`, `Util.java:2938-2950`: only entries
/// where the *sender* (map key) is also the first member of its own reported view.
pub fn determine_actual_merge_coords(map: &HashMap<Address, View>) -> HashSet<Address> {
    map.iter()
        .filter(|(sender, view)| view.members.first() == Some(*sender))
        .map(|(sender, _)| sender.clone())
        .collect()
}

/// `Util.determineMergeParticipants(Map<Address,View>)`, `Util.java:2889-2911`: every subview
/// coordinator, plus any address that appears in some subview but not in its own coordinator's
/// reported subview (a member whose coordinator doesn't (yet) list it — the "outside a merge
/// partition" case upstream's doc comment describes).
pub fn determine_merge_participants(map: &HashMap<Address, View>) -> HashSet<Address> {
    let mut all_addrs: HashSet<Address> = HashSet::new();
    for v in map.values() {
        all_addrs.extend(v.members.iter().cloned());
    }
    let mut coords: HashSet<Address> = determine_merge_coords(map);
    for coord in &coords {
        if let Some(view) = map.get(coord) {
            for m in &view.members {
                all_addrs.remove(m);
            }
        }
    }
    coords.extend(all_addrs);
    coords
}

/// `Merger.determineMergeLeader(Map<Address,View>)`, `:224-235`: the merge coordinators (actual
/// coords if any exist, else any subview coordinator — JGRP-2092), sorted by `Address::compare_to`,
/// first one wins ("establish a deterministic order, so that coords can elect leader").
pub fn determine_merge_leader(views: &HashMap<Address, View>) -> Option<Address> {
    let mut coords: Vec<Address> = determine_actual_merge_coords(views).into_iter().collect();
    if coords.is_empty() {
        coords = determine_merge_coords(views).into_iter().collect();
    }
    if coords.is_empty() {
        return None;
    }
    coords.sort_by(|a, b| a.compare_to(b));
    coords.into_iter().next()
}

/// `MergeData` (`pbcast/MergeData.java`): what a `MERGE_RSP`/`INSTALL_MERGE_VIEW` carries. `subgroups` is `Some` when
/// `view` is a `MergeView`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MergeData {
    /// The subgroup coordinator that sent it.
    pub sender: Option<Address>,
    /// The sender's view, or the merged view to install.
    pub view: Option<View>,
    /// The subgroup views when `view` is a merge view.
    pub subgroups: Option<Vec<View>>,
    /// The sender's digest.
    pub digest: Option<Digest>,
    /// Whether the sender rejected the merge request.
    pub merge_rejected: bool,
}

impl MergeData {
    /// `new MergeData(sender, view, digest)`, with no subgroups and not rejected.
    pub fn new(sender: Option<Address>, view: Option<View>, digest: Option<Digest>) -> MergeData {
        MergeData { sender, view, subgroups: None, digest, merge_rejected: false }
    }
}

/// `Merger.determineMergeCoords(Map<Address,View>)`, `Merger.java:240-258` (the protected one, not `Util`'s): each subview
/// coordinator and the members of the views it created, in order; then every merge participant that is not a coordinator,
/// with itself as the only member.
pub fn determine_merge_coords_with_members(views: &HashMap<Address, View>) -> HashMap<Address, Vec<Address>> {
    let mut retval: HashMap<Address, Vec<Address>> = HashMap::new();
    // iterate in a stable order so that the member order does not depend on the map's
    let mut ordered: Vec<(&Address, &View)> = views.iter().collect();
    ordered.sort_by(|a, b| a.0.compare_to(b.0));
    for (_, view) in ordered {
        let Some(coord) = view.creator() else { continue };
        let members = retval.entry(coord.clone()).or_default();
        for mbr in &view.members {
            if !members.contains(mbr) {
                members.push(mbr.clone());
            }
        }
    }
    let participants = determine_merge_participants(views);
    for p in participants {
        retval.entry(p.clone()).or_insert_with(|| vec![p]);
    }
    retval
}

/// `MergeTask.consolidateDigests`: for each member of the new view, the maximum of the highest-delivered and highest-received
/// seqnos over all digests.
pub fn consolidate_digests(new_view: &View, merge_rsps: &[MergeData]) -> Digest {
    let mut retval = digest_ops::unset(&new_view.members);
    for data in merge_rsps {
        if let Some(d) = &data.digest {
            digest_ops::merge_digest(&mut retval, d);
        }
    }
    retval
}

/// `MergeTask.consolidateMergeData`, `Merger.java:604-669`: one `MergeView` and one digest out of the subgroup coordinators'
/// responses; `None` when no coordinator remains or a member of the new view has no digest.
pub fn consolidate_merge_data(local: &Address, merge_rsps: &[MergeData], mut subviews: Vec<View>) -> Option<MergeData> {
    let mut logical_time: i64 = 0; // for the new view id
    let mut sub_mbrships: Vec<Vec<Address>> = Vec::new();
    let mut digest_membership: HashSet<Address> = HashSet::new(); // members as seen by the digests

    for tmp_data in merge_rsps {
        if let Some(tmp_view) = &tmp_data.view {
            logical_time = logical_time.max(tmp_view.view_id.id);
            sub_mbrships.push(tmp_view.members.clone());
        }
        if let Some(d) = &tmp_data.digest {
            digest_membership.extend(d.members());
        }
    }

    // remove all members from the new view for which we didn't get a digest
    if !digest_membership.is_empty() {
        for coll in &mut sub_mbrships {
            coll.retain(|m| digest_membership.contains(m));
        }
    }

    let mut merged_mbrs = compute_merge_membership(&sub_mbrships);

    // second line of defence: nothing outside the subviews' members
    let all_members: HashSet<&Address> = sub_mbrships.iter().flatten().collect();
    merged_mbrs.retain(|m| all_members.contains(m));

    let new_coord = merged_mbrs.first()?.clone();

    // remove views from the subviews whose creator are not in the new membership
    subviews.retain(|v| {
        logical_time = logical_time.max(v.view_id.id);
        v.creator().is_none_or(|c| merged_mbrs.contains(c))
    });
    subviews.sort_by(|a, b| match (a.creator(), b.creator()) {
        (Some(x), Some(y)) => x.compare_to(y).then(a.view_id.id.cmp(&b.view_id.id)),
        (None, Some(_)) => std::cmp::Ordering::Less,
        (Some(_), None) => std::cmp::Ordering::Greater,
        (None, None) => std::cmp::Ordering::Equal,
    });

    let new_view = View { view_id: ViewId { creator: Some(new_coord), id: logical_time + 1 }, members: merged_mbrs };
    let new_digest = consolidate_digests(&new_view, merge_rsps);
    if !digest_ops::all_set(&new_digest) {
        return None;
    }
    Some(MergeData { sender: Some(local.clone()), view: Some(new_view), subgroups: Some(subviews), digest: Some(new_digest), merge_rejected: false })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn a(n: i64) -> Address {
        Address::uuid(n, n)
    }

    fn view(coord: Address, members: &[Address]) -> View {
        View::create(coord, 1, members.to_vec())
    }

    // MergerTest#testSimpleMerge / #testSimpleMerge2:
    // A: AB, B: AB, C: CD, D: CD -> sanitizing changes nothing (everyone agrees).
    #[test]
    fn simple_merge_sanitization_is_a_no_op_when_subviews_agree() {
        let (av, bv, cv, dv) = (a(1), a(2), a(3), a(4));
        let mut map = HashMap::new();
        map.insert(av.clone(), view(av.clone(), &[av.clone(), bv.clone()]));
        map.insert(bv.clone(), view(av.clone(), &[av.clone(), bv.clone()]));
        map.insert(cv.clone(), view(cv.clone(), &[cv.clone(), dv.clone()]));
        map.insert(dv.clone(), view(cv.clone(), &[cv.clone(), dv.clone()]));
        assert_eq!(map.len(), 4);
        sanitize_views(&mut map);
        assert_eq!(map.len(), 4);
        assert_eq!(map[&av].size(), 2);
        assert_eq!(map[&bv].size(), 2);
        assert_eq!(map[&cv].size(), 2);
        assert_eq!(map[&dv].size(), 2);
    }

    // MergerTest#testOverlappingMerge: A: ABC, B: BC, C: BC -> A's subview shrinks to {A} (B and C
    // don't list A).
    #[test]
    fn overlapping_merge_drops_a_member_neither_side_acknowledges() {
        let (av, bv, cv) = (a(1), a(2), a(3));
        let mut map = HashMap::new();
        map.insert(av.clone(), view(av.clone(), &[av.clone(), bv.clone(), cv.clone()]));
        map.insert(bv.clone(), view(bv.clone(), &[bv.clone(), cv.clone()]));
        map.insert(cv.clone(), view(bv.clone(), &[bv.clone(), cv.clone()]));
        sanitize_views(&mut map);
        assert_eq!(map.len(), 3);
        assert_eq!(map[&av].size(), 1);
        assert_eq!(map[&bv].size(), 2);
        assert_eq!(map[&cv].size(), 2);
    }

    // MergerTest#testOverlappingMerge1: A: BAC, B: BC, C: BC -> only B is an *actual* merge coord
    // (view creator == first member AND the sender who reported it); A and C are participants.
    #[test]
    fn overlapping_merge1_has_one_actual_coord_and_two_participants() {
        let (av, bv, cv) = (a(1), a(2), a(3));
        let mut map = HashMap::new();
        map.insert(av.clone(), view(bv.clone(), &[bv.clone(), av.clone(), cv.clone()]));
        map.insert(bv.clone(), view(bv.clone(), &[bv.clone(), cv.clone()]));
        map.insert(cv.clone(), view(bv.clone(), &[bv.clone(), cv.clone()]));

        let merge_coords = determine_merge_coords(&map);
        assert_eq!(merge_coords.len(), 1);
        assert!(merge_coords.contains(&bv));

        let participants = determine_merge_participants(&map);
        assert_eq!(participants.len(), 2);
    }

    // MergerTest#testOverlappingMerge2: A: ABCDEF, X: XYZ -> 2 distinct subview coords, 2
    // participants (each coord represents its own subgroup).
    #[test]
    fn overlapping_merge2_two_disjoint_partitions() {
        let (av, bv, cv, dv, ev, fv) = (a(1), a(2), a(3), a(4), a(5), a(6));
        let (xv, yv, zv) = (a(24), a(25), a(26));
        let mut map = HashMap::new();
        map.insert(av.clone(), view(av.clone(), &[av.clone(), bv.clone(), cv.clone(), dv.clone(), ev.clone(), fv.clone()]));
        map.insert(xv.clone(), view(xv.clone(), &[xv.clone(), yv.clone(), zv.clone()]));

        let merge_coords = determine_merge_coords(&map);
        assert_eq!(merge_coords.len(), 2);
        let participants = determine_merge_participants(&map);
        assert_eq!(participants.len(), 2);
    }

    // MergerTest#testOverlappingMerge3: A: AB, B: BC, C: CD, D: DE -> a chain; sanitizing drops
    // each end's neighbour that doesn't reciprocate, except D (who is acknowledged by both C's and
    // its own subview membership expectations) keeps 2.
    #[test]
    fn overlapping_merge3_chain_of_pairs() {
        let (av, bv, cv, dv, ev) = (a(1), a(2), a(3), a(4), a(5));
        let mut map = HashMap::new();
        map.insert(av.clone(), view(av.clone(), &[av.clone(), bv.clone()]));
        map.insert(bv.clone(), view(bv.clone(), &[bv.clone(), cv.clone()]));
        map.insert(cv.clone(), view(cv.clone(), &[cv.clone(), dv.clone()]));
        map.insert(dv.clone(), view(dv.clone(), &[dv.clone(), ev.clone()]));
        sanitize_views(&mut map);
        assert_eq!(map.len(), 4);
        assert_eq!(map[&av].size(), 1);
        assert_eq!(map[&bv].size(), 1);
        assert_eq!(map[&cv].size(), 1);
        assert_eq!(map[&dv].size(), 2);
    }

    // MergerTest#testOverlappingMerge4 (JGRP-1451): A: ACB, B: ACB, C: ACB, D: BACD -> A/B/C all
    // agree and keep size 3; D is dropped by all three (none of A/B/C list D), so D's own subview
    // shrinks to just {D}.
    #[test]
    fn overlapping_merge4_jgrp_1451() {
        let (av, bv, cv, dv) = (a(1), a(2), a(3), a(4));
        let mut map = HashMap::new();
        map.insert(av.clone(), view(av.clone(), &[av.clone(), cv.clone(), bv.clone()]));
        map.insert(bv.clone(), view(av.clone(), &[av.clone(), cv.clone(), bv.clone()]));
        map.insert(cv.clone(), view(av.clone(), &[av.clone(), cv.clone(), bv.clone()]));
        map.insert(dv.clone(), view(dv.clone(), &[bv.clone(), av.clone(), cv.clone(), dv.clone()]));
        sanitize_views(&mut map);
        assert_eq!(map.len(), 4);
        assert_eq!(map[&av].size(), 3);
        assert_eq!(map[&bv].size(), 3);
        assert_eq!(map[&cv].size(), 3);
        assert_eq!(map[&dv].size(), 1);
    }

    #[test]
    fn determine_merge_leader_picks_the_numerically_smallest_actual_coord() {
        let (av, bv, cv, dv) = (a(1), a(2), a(3), a(4));
        let mut map = HashMap::new();
        map.insert(av.clone(), view(av.clone(), &[av.clone(), bv.clone()]));
        map.insert(cv.clone(), view(cv.clone(), &[cv.clone(), dv.clone()]));
        assert_eq!(determine_merge_leader(&map), Some(av));
    }

    #[test]
    fn determine_merge_leader_falls_back_to_any_coord_when_no_actual_coord_exists() {
        // Neither entry's sender is the first member of its own reported view (JGRP-2092 case).
        let (av, bv, cv) = (a(1), a(2), a(3));
        let mut map = HashMap::new();
        map.insert(av.clone(), view(bv.clone(), &[bv.clone(), av.clone()]));
        map.insert(cv.clone(), view(bv.clone(), &[bv.clone(), cv.clone()]));
        assert_eq!(determine_merge_leader(&map), Some(bv));
    }

    #[test]
    fn determine_merge_leader_none_for_empty_views() {
        assert_eq!(determine_merge_leader(&HashMap::new()), None);
    }
}
