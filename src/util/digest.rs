//! `upstream/src/org/jgroups/util/MutableDigest.java`: the mutable operations NAKACK2/STABLE need
//! on top of `wire::view::Digest` (`util/Digest.java`, ported byte-exact for
//! GMS's `VIEW` payload — see that module's doc comment). Kept as free functions over
//! `wire::view::Digest` rather than a separate `MutableDigest` type: upstream's split exists so a
//! `Digest` can be handed out `@Immutable` while a `MutableDigest` (always a distinct, fixed-size
//! copy — `MutableDigest(Address[])`) is mutated locally; this crate's `Digest` is a plain
//! `Vec`-backed struct with no immutability enforcement to route around, so the extra type would
//! add a wrapper with no behavioural difference. `wire::view::Digest` itself is left untouched
//! (its own module owns the wire format); everything below only reads/writes its public `entries`.

use crate::wire::address::Address;
use crate::wire::view::Digest;

/// `MutableDigest(Address[] members)`, `MutableDigest.java:18-20`: a digest naming exactly
/// `members`, every seqno `-1` ("not yet set" — `Digest.java`'s own convention, checked by
/// `all_set`/`non_set_members` below).
pub fn unset(members: &[Address]) -> Digest {
    Digest { entries: members.iter().map(|m| (m.clone(), -1, -1)).collect() }
}

/// `MutableDigest.set(Address, long, long)`, `MutableDigest.java:29-37`: updates the seqnos for an
/// existing member in place. A no-op if `member` isn't already one of `digest`'s entries — upstream
/// never grows a `MutableDigest` past the fixed member set it was constructed with.
pub fn set(digest: &mut Digest, member: &Address, hd: i64, hr: i64) {
    if let Some(entry) = digest.entries.iter_mut().find(|(m, _, _)| m == member) {
        entry.1 = hd;
        entry.2 = hr;
    }
}

/// `MutableDigest.set(Digest)`, `MutableDigest.java:70-74`: calls `set` for every entry of `other`.
pub fn set_from(digest: &mut Digest, other: &Digest) {
    for (member, hd, hr) in other.entries.clone() {
        set(digest, &member, hd, hr);
    }
}

/// `MutableDigest.merge(Address, long, long)`, `MutableDigest.java:90-96`: like `set`, but takes
/// the per-field max with whatever `digest` already has for `member` (a no-op `get` of `None`
/// falls back to the incoming values verbatim, matching `entry==null` upstream).
pub fn merge(digest: &mut Digest, member: &Address, hd: i64, hr: i64) {
    let (new_hd, new_hr) = match digest.get(member) {
        Some((cur_hd, cur_hr)) => (cur_hd.max(hd), cur_hr.max(hr)),
        None => (hd, hr),
    };
    set(digest, member, new_hd, new_hr);
}

/// `MutableDigest.merge(Digest)`, `MutableDigest.java:79-84`.
pub fn merge_digest(digest: &mut Digest, other: &Digest) {
    for (member, hd, hr) in other.entries.clone() {
        merge(digest, &member, hd, hr);
    }
}

/// `MutableDigest.allSet()`, `MutableDigest.java:44-48`.
pub fn all_set(digest: &Digest) -> bool {
    digest.entries.iter().all(|(_, hd, _)| *hd != -1)
}

/// `MutableDigest.getNonSetMembers()`, `MutableDigest.java:52-59`.
pub fn non_set_members(digest: &Digest) -> Vec<Address> {
    digest.entries.iter().filter(|(_, hd, _)| *hd == -1).map(|(m, _, _)| m.clone()).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::address::Address;
    use crate::wire::stream::WireReader;

    fn addr(n: i64) -> Address {
        Address::uuid(n, n)
    }

    #[test]
    fn unset_starts_every_member_at_minus_one() {
        let d = unset(&[addr(1), addr(2)]);
        assert!(!all_set(&d));
        assert_eq!(non_set_members(&d), vec![addr(1), addr(2)]);
    }

    #[test]
    fn set_updates_an_existing_member_only() {
        let mut d = unset(&[addr(1), addr(2)]);
        set(&mut d, &addr(1), 5, 10);
        assert_eq!(d.get(&addr(1)), Some((5, 10)));
        assert_eq!(d.get(&addr(2)), Some((-1, -1)));
        assert!(!all_set(&d)); // member 2 still unset

        // Setting an address absent from the digest is a no-op (no new entry grown).
        set(&mut d, &addr(99), 1, 1);
        assert_eq!(d.capacity(), 2);
    }

    #[test]
    fn all_set_once_every_member_has_a_real_hd() {
        let mut d = unset(&[addr(1), addr(2)]);
        set(&mut d, &addr(1), 0, 0);
        set(&mut d, &addr(2), 0, 0);
        assert!(all_set(&d));
        assert!(non_set_members(&d).is_empty());
    }

    #[test]
    fn merge_takes_the_per_field_max() {
        let mut d = Digest::one(addr(1), 5, 10);
        merge(&mut d, &addr(1), 3, 20);
        assert_eq!(d.get(&addr(1)), Some((5, 20))); // max(5,3)=5, max(10,20)=20
    }

    #[test]
    fn merge_digest_folds_every_entry() {
        let mut d = Digest { entries: vec![(addr(1), 2, 2), (addr(2), 0, 0)] };
        let incoming = Digest { entries: vec![(addr(1), 5, 1), (addr(2), 1, 1)] };
        merge_digest(&mut d, &incoming);
        assert_eq!(d.get(&addr(1)), Some((5, 2)));
        assert_eq!(d.get(&addr(2)), Some((1, 1)));
    }

    #[test]
    fn set_from_overwrites_rather_than_maxing() {
        let mut d = Digest::one(addr(1), 50, 50);
        let incoming = Digest { entries: vec![(addr(1), 3, 3)] };
        set_from(&mut d, &incoming);
        assert_eq!(d.get(&addr(1)), Some((3, 3)));
    }

    // ---- DigestTest, method by method (upstream/tests/junit-functional/org/jgroups/tests/DigestTest.java) ----
    // Java-only, not ported: testConstructor2 (`new Digest(Address[], long[])` length check: this crate's Digest
    // is a Vec of triples, a length mismatch is unrepresentable), the copy-constructor/immutability tests
    // (testCopyConstructor*, testImmutability*, testImmutablity, testCopy, testCopy2: `Clone` and value semantics
    // make them tautologies; one check of value independence is kept below).

    fn upstream_members() -> (Address, Address, Address, Digest) {
        let (a1, a2, a3) = (addr(1), addr(2), addr(3));
        let d = Digest {
            entries: vec![(a1.clone(), 500, 501), (a2.clone(), 26, 26), (a3.clone(), 25, 33)],
        };
        (a1, a2, a3, d)
    }

    #[test]
    fn upstream_digest_constructor_and_capacity() {
        let (a1, a2, a3, d) = upstream_members();
        let md = unset(&[a1, a2, a3]);
        assert_eq!(md.capacity(), 3);
        assert_eq!(d.capacity(), 3);
    }

    #[test]
    fn upstream_digest_contains_and_contains_all() {
        let (a1, a2, a3, d) = upstream_members();
        assert!(d.contains(&a1) && d.contains(&a2) && d.contains(&a3));
        let mut digest = unset(&[a1.clone(), a2.clone(), a3.clone()]);
        set(&mut digest, &a1, 1, 1);
        set(&mut digest, &a2, 1, 1);
        // containsAll is about membership, not about seqnos having been set.
        assert!([&a1, &a2, &a3].iter().all(|m| digest.contains(m)));
        set(&mut digest, &a3, 1, 1);
        assert!([&a1, &a2, &a3].iter().all(|m| digest.contains(m)));
    }

    #[test]
    fn upstream_digest_equals_family() {
        let (a1, a2, a3, d) = upstream_members();
        assert_eq!(d, d.clone());
        // testEquals2
        let mut md = d.clone();
        assert_eq!(d, md);
        md = unset(&[a1.clone(), a2.clone(), a3.clone()]);
        set(&mut md, &a1, 1, 2);
        set(&mut md, &a2, 3, 4);
        assert_ne!(d, md);
        // testEquals3
        let same = Digest {
            entries: vec![(a1.clone(), 500, 501), (a2.clone(), 26, 26), (a3.clone(), 25, 33)],
        };
        assert_eq!(d, same);
        let differs = Digest {
            entries: vec![(a1.clone(), 500, 501), (a2.clone(), 26, 26), (a3.clone(), 25, 37)],
        };
        assert_ne!(d, differs);
        let mut partial = unset(&[a1.clone(), a2.clone(), a3.clone()]);
        set(&mut partial, &a1, 500, 501);
        set(&mut partial, &a2, 26, 26);
        assert_ne!(d, partial);
    }

    #[test]
    fn upstream_digest_set_get_and_independence() {
        let (a1, a2, a3, d) = upstream_members();
        // testSet
        let mut md = unset(&[a1.clone(), a2.clone(), a3.clone()]);
        set(&mut md, &a2, 200, 201);
        assert_eq!(md.get(&a2), Some((200, 201)));
        // testImmutablity: mutating a copy leaves the original alone
        let mut md = d.clone();
        let before = d.get(&a1);
        set(&mut md, &a1, 1, 1);
        assert_eq!(d.get(&a1), before);
        // testGet / testHighSeqnoAt / testHighSeqnoSeenAt
        assert_eq!(d.get(&a1), Some((500, 501)));
        assert_eq!(d.get(&a2), Some((26, 26)));
        assert_eq!(d.get(&a3), Some((25, 33)));
        // testSetHighestDeliveredAndSeenSeqnoAt
        set(&mut md, &a1, 10, 20);
        assert_eq!(md.get(&a1), Some((10, 20)));
    }

    #[test]
    fn upstream_digest_set_digest_ignores_non_members() {
        let (a1, a2, a3, d) = upstream_members();
        // testSetDigest
        let mut md = unset(&[a1.clone(), a2.clone(), a3.clone()]);
        let tmp = md.clone();
        set_from(&mut md, &tmp);
        assert_eq!(md.capacity(), 3);
        // testSetDigest2: entries for addresses outside the digest are ignored
        let mut tmp = unset(&[a1.clone(), a2.clone(), a3.clone()]);
        set(&mut tmp, &addr(90), 2, 3);
        set(&mut tmp, &addr(91), 2, 3);
        set(&mut tmp, &a2, 2, 3);
        set(&mut tmp, &a3, 2, 3);
        let mut md = unset(&[a1.clone(), a2.clone(), a3.clone()]);
        set_from(&mut md, &tmp);
        assert_eq!(md.capacity(), 3);
        // testReplace
        let mut digest = unset(&[a1.clone(), a2.clone(), a3.clone()]);
        set(&mut digest, &a1, 1, 1);
        set(&mut digest, &a2, 2, 2);
        let mut md = unset(&[a1.clone(), a2.clone(), a3.clone()]);
        set_from(&mut md, &digest);
        assert_eq!(digest, md);
        // testMutableCopy: a non-member set is ignored
        let mut md = unset(&[a1, a2, a3]);
        let copy = md.clone();
        set(&mut md, &addr(77), 500, 1000);
        assert_eq!(md, copy);
        let _ = d;
    }

    #[test]
    fn upstream_digest_increment_high_seqno() {
        let (a1, a2, a3, _) = upstream_members();
        let mut md = unset(&[a1.clone(), a2.clone(), a3.clone()]);
        set(&mut md, &a1, 1, 100);
        set(&mut md, &a2, 3, 300);
        set(&mut md, &a3, 7, 700);
        for a in [&a1, &a2, &a3] {
            let tmp = md.get(a).unwrap().0;
            set(&mut md, a, tmp + 1, tmp + 1);
            assert_eq!(md.get(a).unwrap().0, tmp + 1);
        }
    }

    #[test]
    fn upstream_digest_all_set() {
        let mbrs: Vec<Address> = (10..15).map(addr).collect();
        let mut dig = unset(&mbrs);
        assert!(!all_set(&dig));
        for index in [1usize, 3, 4] {
            set(&mut dig, &mbrs[index], index as i64, index as i64 + 1);
        }
        assert!(!all_set(&dig));
        assert_eq!(non_set_members(&dig), vec![mbrs[0].clone(), mbrs[2].clone()]);
        for index in [0usize, 2] {
            set(&mut dig, &mbrs[index], index as i64, index as i64 + 1);
        }
        assert!(all_set(&dig));
        assert!(non_set_members(&dig).is_empty());

        let (a1, a2, a3, d) = upstream_members();
        let members = vec![a1, a2, a3];
        let mut dig = unset(&members);
        assert!(!all_set(&dig));
        assert_eq!(non_set_members(&dig), members);
        set_from(&mut dig, &d);
        assert!(all_set(&dig));
        assert!(non_set_members(&dig).is_empty());
    }

    #[test]
    fn upstream_digest_merge_family() {
        let (a1, a2, a3, d) = upstream_members();
        // testMerge
        let mut digest = unset(&[a1.clone(), a2.clone(), a3.clone()]);
        set(&mut digest, &a1, 499, 502);
        set(&mut digest, &a2, 26, 27);
        set(&mut digest, &a3, 26, 35);
        merge_digest(&mut digest, &d);
        assert_eq!(digest.capacity(), 3);
        assert_eq!(digest.get(&a1), Some((500, 502)));
        assert_eq!(digest.get(&a2), Some((26, 27)));
        assert_eq!(digest.get(&a3), Some((26, 35)));

        // testNonConflictingMerge: a digest over a larger view keeps its extra members
        let (ip1, ip2) = (addr(40), addr(41));
        let mut cons = unset(&[a1.clone(), a2.clone(), a3.clone(), ip1.clone(), ip2.clone()]);
        set(&mut cons, &ip1, 10, 10);
        set(&mut cons, &ip2, 20, 20);
        merge_digest(&mut cons, &d);
        assert_eq!(cons.capacity(), 5);
        assert_eq!(cons.get(&ip1), Some((10, 10)));
        assert_eq!(cons.get(&ip2), Some((20, 20)));
        assert_eq!(cons.get(&a1), Some((500, 501)));
        assert_eq!(cons.get(&a2), Some((26, 26)));
        assert_eq!(cons.get(&a3), Some((25, 33)));

        // testConflictingMerge
        let mut new_d = unset(&[a1.clone(), a2.clone(), a3.clone()]);
        set(&mut new_d, &a1, 450, 501);
        set(&mut new_d, &a3, 28, 35);
        let mut md = d.clone();
        merge_digest(&mut md, &new_d);
        assert_eq!(md.capacity(), 3);
        assert_eq!(md.get(&a1), Some((500, 501)));
        assert_eq!(md.get(&a2), Some((26, 26)));
        assert_eq!(md.get(&a3), Some((28, 35)));
    }

    #[test]
    fn upstream_digest_streamable_roundtrips() {
        let (_, _, _, d) = upstream_members();
        // testStreamable / testViewBasedMarshalling / testViewId / testSerializedSize
        let mut buf = Vec::new();
        d.write_to(true, &mut buf);
        assert_eq!(buf.len(), d.serialized_size(true));
        let tmp = Digest::read_from(true, None, &mut WireReader::new(&buf));
        assert_eq!(d, tmp);

        // testViewBasedMarshallingLargeView
        let entries: Vec<(Address, i64, i64)> = (0..1000i64)
            .map(|i| {
                let hd = (i * 2_654_435_761) % i32::MAX as i64;
                (addr(1000 + i), hd, hd + (i * 7919) % 1000)
            })
            .collect();
        let digest = Digest { entries };
        let mut buf = Vec::new();
        digest.write_to(true, &mut buf);
        let digest1 = Digest::read_from(true, None, &mut WireReader::new(&buf));
        assert_eq!(digest, digest1);
        assert_eq!(digest.capacity(), digest1.capacity());
    }
}
