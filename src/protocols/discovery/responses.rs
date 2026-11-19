//! A minimal, non-blocking stand-in for `org.jgroups.util.Responses` (`Discovery`'s discovery-round
//! collector). Not a literal `Lock`/`CondVar` port: upstream's blocks the calling thread
//! (`waitFor`); this crate's discovery always runs inside an async task (`channel.rs`'s join loop,
//! `tokio::time::timeout`-bounded), so there is nothing to block — the caller just stops feeding
//! responses in and reads them out once its own bounded wait ends. The *decision* logic
//! (`addResponse`'s new-vs-overwrite rule, `isCoord`, early completion on a coord response or
//! reaching the expected count) is ported faithfully; only the thread-synchronization shell is
//! left out as unneeded machinery (`Responses.java:1-16,66-88,113-121`).

use crate::protocols::discovery::PingData;
use crate::wire::address::Address;

/// The `PingData` answers of one discovery round, at most one per sender.
pub struct Responses {
    entries: Vec<PingData>,
    num_expected: usize,
    break_on_coord_rsp: bool,
}

impl Responses {
    /// `new Responses(int num_expected_rsps, boolean break_on_coord_rsp, int initial_capacity)`,
    /// `Responses.java:34-40` (the capacity argument is a Java-only preallocation hint, not
    /// modelled here — `Vec` grows on its own).
    pub fn new(num_expected: usize, break_on_coord_rsp: bool) -> Responses {
        Responses { entries: Vec::new(), num_expected, break_on_coord_rsp }
    }

    /// `Responses.addResponse(PingData, boolean)`, `Responses.java:69-88`: replaces an existing
    /// response from the same sender only when `overwrite` is set, or when the new one is a coord
    /// response and the existing one wasn't (`JGRP-1179`); otherwise appends. Returns whether this
    /// call just completed the round (reached `num_expected`, or `break_on_coord_rsp` and this was
    /// a coordinator response) — the caller's cue to stop waiting, mirroring `_done()`'s effect on
    /// `isDone()` without the condition-variable wakeup (nothing is parked waiting on it here).
    pub fn add_response(&mut self, rsp: PingData, overwrite: bool) -> bool {
        let is_coord_rsp = rsp.is_coord();
        let mut changed = false;
        match self.entries.iter().position(|e| e.sender == rsp.sender) {
            None => {
                self.entries.push(rsp);
                changed = true;
            }
            Some(i) => {
                if overwrite || (is_coord_rsp && !self.entries[i].is_coord()) {
                    self.entries[i] = rsp;
                    changed = true;
                }
            }
        }
        changed && ((self.num_expected > 0 && self.entries.len() >= self.num_expected) || (self.break_on_coord_rsp && is_coord_rsp))
    }

    /// Whether no answer arrived.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// The number of answers.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// `Responses.isCoord(Address)`, `Responses.java:110-113`.
    pub fn is_coord(&self, addr: &Address) -> bool {
        self.entries.iter().any(|e| e.is_coord() && e.sender.as_ref() == Some(addr))
    }

    /// The answers, in arrival order.
    pub fn iter(&self) -> impl Iterator<Item = &PingData> {
        self.entries.iter()
    }

    /// The answers, in arrival order.
    pub fn into_vec(self) -> Vec<PingData> {
        self.entries
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn a(n: i64) -> Address {
        Address::uuid(n, n)
    }
    fn pd(sender: Address, coord: bool) -> PingData {
        PingData::new(Some(sender), true).coord(coord)
    }

    #[test]
    fn empty_when_nothing_added() {
        let r = Responses::new(0, false);
        assert!(r.is_empty());
        assert_eq!(r.len(), 0);
    }

    #[test]
    fn new_sender_is_appended_and_duplicate_is_ignored_without_overwrite() {
        let mut r = Responses::new(0, false);
        assert!(!r.add_response(pd(a(1), false), false));
        assert_eq!(r.len(), 1);
        r.add_response(pd(a(1), false), false); // same sender, no overwrite -> not replaced, no growth
        assert_eq!(r.len(), 1);
    }

    #[test]
    fn a_coord_response_overwrites_a_non_coord_one_even_without_overwrite_flag() {
        let mut r = Responses::new(0, false);
        r.add_response(pd(a(1), false), false);
        r.add_response(pd(a(1), true), false);
        assert!(r.is_coord(&a(1)));
    }

    #[test]
    fn done_when_expected_count_reached() {
        let mut r = Responses::new(2, false);
        assert!(!r.add_response(pd(a(1), false), false));
        assert!(r.add_response(pd(a(2), false), false));
    }

    #[test]
    fn done_immediately_on_a_coord_response_when_break_on_coord_rsp() {
        let mut r = Responses::new(0, true);
        assert!(!r.add_response(pd(a(1), false), false));
        assert!(r.add_response(pd(a(2), true), false));
    }

    #[test]
    fn is_coord_false_for_unknown_address() {
        let r = Responses::new(0, false);
        assert!(!r.is_coord(&a(9)));
    }
}
