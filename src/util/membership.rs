//! `org.jgroups.Membership`: an ordered, deduplicated list of `Address`es. Used internally by GMS
//! (`GMS.members`/`GMS.tmp_members`) — never exposed to application code, per upstream's own doc
//! comment.
//!
//! Port of `upstream/src/org/jgroups/Membership.java`. `synchronized(members)` blocks become plain
//! `&mut self` methods (Rust's borrow checker already gives single-writer exclusivity; nothing in
//! this crate shares a live `Membership` across threads — GMS state is owned by one async task,
//! see `channel.rs`).

use std::cmp::Ordering;

use crate::wire::address::Address;

/// `Membership`: an ordered list of members with no duplicates.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Membership {
    members: Vec<Address>,
}

impl Membership {
    /// An empty membership.
    pub fn new() -> Membership {
        Membership::default()
    }

    /// `new Membership(Collection<Address>)`, `Membership.java:34-37`.
    pub fn from_members(initial: impl IntoIterator<Item = Address>) -> Membership {
        let mut m = Membership::new();
        m.add_all(initial);
        m
    }

    /// `Membership.getMembers()`, `Membership.java:53-58`: a defensive copy.
    pub fn members(&self) -> Vec<Address> {
        self.members.clone()
    }

    /// `Membership.add(Address)`, `Membership.java:64-73`: a no-op if already present.
    pub fn add(&mut self, mbr: Address) -> &mut Membership {
        if !self.members.contains(&mbr) {
            self.members.push(mbr);
        }
        self
    }

    /// `Membership.add(Collection<Address>)`, `Membership.java:86-90`.
    pub fn add_all(&mut self, mbrs: impl IntoIterator<Item = Address>) -> &mut Membership {
        for mbr in mbrs {
            self.add(mbr);
        }
        self
    }

    /// `Membership.remove(Address)`, `Membership.java:98-105`.
    pub fn remove(&mut self, mbr: &Address) -> &mut Membership {
        self.members.retain(|m| m != mbr);
        self
    }

    /// `Membership.remove(Collection<Address>)`, `Membership.java:112-119`.
    pub fn remove_all(&mut self, mbrs: &[Address]) -> &mut Membership {
        self.members.retain(|m| !mbrs.contains(m));
        self
    }

    /// `Membership.retainAll(Collection<Address>)`, `Membership.java:121-128`.
    pub fn retain_all(&mut self, mbrs: &[Address]) -> &mut Membership {
        self.members.retain(|m| mbrs.contains(m));
        self
    }

    /// `Membership.clear()`, `Membership.java:134-139`.
    pub fn clear(&mut self) -> &mut Membership {
        self.members.clear();
        self
    }

    /// `Membership.set(Collection<Address>)`, `Membership.java:148-153`.
    pub fn set(&mut self, mbrs: impl IntoIterator<Item = Address>) -> &mut Membership {
        self.clear();
        self.add_all(mbrs)
    }

    /// `Membership.set(Membership)`, `Membership.java:162-169`.
    pub fn set_from(&mut self, other: &Membership) -> &mut Membership {
        self.clear();
        self.add_all(other.members.iter().cloned())
    }

    /// `Membership.merge(Collection<Address>, Collection<Address>)`, `Membership.java:183-188`:
    /// remove suspects first, then add the new members — order matters (a member that is both
    /// suspected and rejoining ends up present).
    pub fn merge(&mut self, new_mbrs: impl IntoIterator<Item = Address>, suspects: &[Address]) -> &mut Membership {
        self.remove_all(suspects);
        self.add_all(new_mbrs)
    }

    /// `Membership.contains(Address)`, `Membership.java:197-202`.
    pub fn contains(&self, mbr: &Address) -> bool {
        self.members.contains(mbr)
    }

    /// `Membership.sort()`, `Membership.java:205-210`: ascending `Address::compare_to`.
    pub fn sort(&mut self) -> &mut Membership {
        self.members.sort_by(|a, b| a.compare_to(b));
        self
    }

    /// `Membership.copy()`, `Membership.java:217-219`.
    pub fn copy(&self) -> Membership {
        self.clone()
    }

    /// The number of members.
    pub fn size(&self) -> usize {
        self.members.len()
    }

    /// Whether there is no member.
    pub fn is_empty(&self) -> bool {
        self.members.is_empty()
    }

    /// `Membership.elementAt(int)`, `Membership.java:240-244`. Panics on an out-of-range index,
    /// matching upstream's `ArrayIndexOutOfBoundsException`.
    pub fn element_at(&self, index: usize) -> &Address {
        &self.members[index]
    }

    /// `Membership.getFirst()`, `Membership.java:246-250`.
    pub fn get_first(&self) -> Option<&Address> {
        self.members.first()
    }

    /// `Membership.isCoord(Address)`, `Membership.java:252-256`.
    pub fn is_coord(&self, mbr: &Address) -> bool {
        self.get_first().is_some_and(|first| first == mbr)
    }

    /// `Membership.getPrevious(Address)`, `Membership.java:263-271` + `Util.pickPrevious(List,
    /// Object)`, `Util.java:3057-3070`: the circular predecessor, or `None` if `mbr` is absent or
    /// there are fewer than 2 members.
    pub fn get_previous(&self, mbr: &Address) -> Option<&Address> {
        pick_previous(&self.members, mbr)
    }

    /// `Membership.getNext(Address)`, `Membership.java:278-286` + `Util.pickNext(List, Object)`,
    /// `Util.java:3013-3023`: the circular successor.
    pub fn get_next(&self, mbr: &Address) -> Option<&Address> {
        pick_next(&self.members, mbr)
    }
}

fn pick_previous<'a>(members: &'a [Address], mbr: &Address) -> Option<&'a Address> {
    if members.len() < 2 {
        return None;
    }
    let i = members.iter().position(|m| m == mbr)?;
    let prev_index = if i == 0 { members.len() - 1 } else { i - 1 };
    let candidate = &members[prev_index];
    if candidate == mbr { None } else { Some(candidate) }
}

fn pick_next<'a>(members: &'a [Address], mbr: &Address) -> Option<&'a Address> {
    if members.len() < 2 {
        return None;
    }
    let i = members.iter().position(|m| m == mbr)?;
    let next_index = (i + 1) % members.len();
    let candidate = &members[next_index];
    if candidate == mbr { None } else { Some(candidate) }
}

/// `DefaultMembershipPolicy.getNewMembership`, `GMS.java` (nested class `DefaultMembershipPolicy`,
/// also known as `computeNewMembership`): removes
/// suspected and left members from the current list, then appends new joiners in the order given.
/// Used by both `CoordGmsImpl.handleMembershipChange` (`getNextView`) and
/// `ParticipantGmsImpl.wouldIBeCoordinator`.
pub fn compute_new_membership(
    current: &[Address],
    joiners: &[Address],
    leavers: &[Address],
    suspects: &[Address],
) -> Vec<Address> {
    let mut retval: Vec<Address> =
        current.iter().filter(|m| !leavers.contains(m) && !suspects.contains(m)).cloned().collect();
    for j in joiners {
        if !retval.contains(j) {
            retval.push(j.clone());
        }
    }
    retval
}

/// `DefaultMembershipPolicy.getNewMembership(Collection<Collection<Address>>)` (`GMS.java`, the merge policy): the new
/// coordinator is the smallest of the subviews' coordinators, then every member follows in the order of its subview,
/// duplicates dropped. Empty when no subview has a member (upstream throws).
pub fn compute_merge_membership(subviews: &[Vec<Address>]) -> Vec<Address> {
    let mut coords: Vec<Address> = Vec::new();
    for subview in subviews {
        if let Some(first) = subview.first()
            && !coords.contains(first)
        {
            coords.push(first.clone());
        }
    }
    coords.sort_by(|a, b| a.compare_to(b));
    let Some(new_coord) = coords.first().cloned() else { return Vec::new() };
    let mut new_mbrs = vec![new_coord];
    for subview in subviews {
        for m in subview {
            if !new_mbrs.contains(m) {
                new_mbrs.push(m.clone());
            }
        }
    }
    new_mbrs
}

/// Convenience used by ordering call sites that don't have a `Membership` handy — sorts a plain
/// address list the way `Membership::sort` does.
pub fn sort_addresses(addrs: &mut [Address]) {
    addrs.sort_by(|a, b| a.compare_to(b));
}

/// Compares two addresses the way JGroups orders members (`Address.compareTo`).
pub fn cmp(a: &Address, b: &Address) -> Ordering {
    a.compare_to(b)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn a(n: i64) -> Address {
        Address::uuid(n, n)
    }

    // MembershipTest#testConstructor / #testAddVectorDupl: duplicates are silently dropped.
    #[test]
    fn constructor_and_add_dedupe() {
        let (a1, a2) = (a(1), a(2));
        let m = Membership::from_members([a1.clone(), a2.clone(), a2.clone()]);
        assert_eq!(m.size(), 2);
        assert!(m.contains(&a1));
        assert!(m.contains(&a2));
    }

    #[test]
    fn copy_is_independent() {
        let mut m1 = Membership::from_members([a(1), a(2)]);
        let m2 = m1.copy();
        m1.add(a(3));
        assert_eq!(m2.size(), 2);
        assert_eq!(m1.size(), 3);
    }

    // MembershipTest#testAdd
    #[test]
    fn add_dedupes_across_calls() {
        let mut m = Membership::new();
        let (a1, a2) = (a(1), a(2));
        m.add(a1.clone()).add(a2.clone()).add(a2.clone());
        assert_eq!(m.size(), 2);
        assert!(m.contains(&a1));
        assert!(m.contains(&a2));
    }

    // MembershipTest#testAddVectorDupl
    #[test]
    fn add_all_after_add_dedupes() {
        let (a1, a2, a4, a5) = (a(1), a(2), a(4), a(5));
        let mut m = Membership::new();
        m.add(a5.clone()).add(a1.clone()).add_all([a1.clone(), a2.clone(), a2.clone(), a4.clone(), a5.clone()]);
        assert_eq!(m.size(), 4);
        for x in [&a1, &a2, &a4, &a5] {
            assert!(m.contains(x));
        }
    }

    // MembershipTest#testRemove
    #[test]
    fn remove_drops_one_member() {
        let mut m = Membership::from_members([a(1), a(2), a(3), a(4), a(5)]);
        m.remove(&a(2));
        assert_eq!(m.size(), 4);
        assert!(!m.contains(&a(2)));
    }

    // MembershipTest#testGetFirst / #testIsCoord
    #[test]
    fn first_and_coord() {
        let mut m = Membership::new();
        assert_eq!(m.get_first(), None);
        assert!(!m.is_coord(&a(1)));
        m.add(a(1));
        assert_eq!(m.get_first(), Some(&a(1)));
        assert!(m.is_coord(&a(1)));
        assert!(!m.is_coord(&a(2)));
        m.add(a(2));
        assert!(!m.is_coord(&a(2))); // a(1) is still first
    }

    // MembershipTest#testGetNext / #testGetPrevious
    #[test]
    fn next_and_previous_are_circular() {
        let mut m = Membership::new();
        assert_eq!(m.get_next(&a(1)), None);
        m.add(a(1));
        assert_eq!(m.get_next(&a(1)), None); // size < 2
        assert_eq!(m.get_previous(&a(1)), None);
        m.add(a(2));
        assert_eq!(m.get_next(&a(1)), Some(&a(2)));
        assert_eq!(m.get_next(&a(2)), Some(&a(1))); // wraps around
        assert_eq!(m.get_previous(&a(1)), Some(&a(2))); // wraps around
        assert_eq!(m.get_previous(&a(2)), Some(&a(1)));
    }

    // MembershipTest#testSet
    #[test]
    fn set_replaces_contents() {
        let mut m = Membership::from_members([a(1), a(2), a(4), a(5)]);
        m.set([a(1), a(2)]);
        assert_eq!(m.size(), 2);
        assert!(m.contains(&a(1)));
        assert!(m.contains(&a(2)));
        assert!(!m.contains(&a(4)));
    }

    // MembershipTest#testSet2
    #[test]
    fn set_from_replaces_contents() {
        let mut m1 = Membership::new();
        m1.add_all([a(1), a(2), a(4)]);
        let mut m2 = Membership::from_members([a(5)]);
        m2.set_from(&m1);
        assert_eq!(m2.size(), 3);
        assert!(!m2.contains(&a(5)));
    }

    // MembershipTest#testMerge
    #[test]
    fn merge_removes_suspects_then_adds() {
        let mut m = Membership::from_members([a(1), a(2), a(3), a(4)]);
        m.merge([a(5)], &[a(2), a(3)]);
        assert_eq!(m.size(), 3);
        assert!(m.contains(&a(1)));
        assert!(m.contains(&a(4)));
        assert!(m.contains(&a(5)));
    }

    // MembershipTest#testSort
    #[test]
    fn sort_orders_by_address_compare_to() {
        let mut m = Membership::from_members([a(3), a(4), a(2), a(1), a(5)]);
        assert_eq!(m.size(), 5);
        m.sort();
        for i in 0..4 {
            assert_eq!(m.element_at(i).compare_to(m.element_at(i + 1)), Ordering::Less);
        }
    }

    #[test]
    fn retain_all_and_remove_all() {
        let mut m = Membership::from_members([a(1), a(2), a(3)]);
        m.retain_all(&[a(1), a(3)]);
        assert_eq!(m.size(), 2);
        assert!(!m.contains(&a(2)));
        m.remove_all(&[a(1)]);
        assert_eq!(m.size(), 1);
        assert!(m.contains(&a(3)));
    }

    #[test]
    fn compute_new_membership_appends_joiners_after_removing_leavers_and_suspects() {
        let current = [a(1), a(2), a(3)];
        let joiners = [a(4)];
        let leavers = [a(2)];
        let suspects = [a(3)];
        let next = compute_new_membership(&current, &joiners, &leavers, &suspects);
        assert_eq!(next, vec![a(1), a(4)]);
    }

    #[test]
    fn compute_new_membership_never_duplicates_an_existing_joiner() {
        let current = [a(1)];
        let joiners = [a(1)];
        let next = compute_new_membership(&current, &joiners, &[], &[]);
        assert_eq!(next, vec![a(1)]);
    }
}
