//! `upstream/src/org/jgroups/util/SeqnoList.java`: a bitset of missing seqnos, `index = seqno -
//! offset` into a `util::fixed_size_bitset::FixedSizeBitSet`. This is the payload NAKACK2's and
//! UNICAST3's `XMIT_REQ` carry (an `ObjectMessage(SeqnoList)`), delivered as
//! `wire::message::Payload::Object(ObjectPayload::SeqnoList(..))` (magic id 15,
//! `upstream/conf/jg-magic-map.xml:18`).

use crate::util::fixed_size_bitset::FixedSizeBitSet;
use crate::wire::stream::{WireError, WireReader, WireResult};

/// `SeqnoList`: a set of seqnos above an offset, kept as a bitset, which is how retransmission requests list missing messages.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SeqnoList {
    bits: FixedSizeBitSet,
    offset: i64,
}

impl SeqnoList {
    /// `SeqnoList(int size, long offset)`, `SeqnoList.java:30-33`.
    pub fn new(size: usize, offset: i64) -> SeqnoList {
        SeqnoList { bits: FixedSizeBitSet::new(size), offset }
    }

    /// `index(long)`, `SeqnoList.java:160`: `seqno - offset`. Signed — `remove_higher_than`/
    /// `remove_lower_than` below rely on this going negative for an out-of-range seqno, exactly
    /// like the Java `int index()` can.
    fn index(&self, seqno: i64) -> i64 {
        seqno - self.offset
    }

    fn seqno(&self, index: usize) -> i64 {
        self.offset + index as i64
    }

    /// `add(long)`, `SeqnoList.java:44-47`.
    pub fn add(&mut self, seqno: i64) -> &mut SeqnoList {
        let idx = self.index(seqno);
        self.bits.set(idx as usize);
        self
    }

    /// `add(long, long)`, `SeqnoList.java:57-60`: a seqno range, inclusive.
    pub fn add_range(&mut self, from: i64, to: i64) -> &mut SeqnoList {
        self.bits.set_range(self.index(from) as usize, self.index(to) as usize);
        self
    }

    /// `removeHigherThan(long)`, `SeqnoList.java:64-71`.
    pub fn remove_higher_than(&mut self, seqno: i64) -> &mut SeqnoList {
        let size = self.bits.size();
        if size == 0 {
            return self;
        }
        let from = self.index(seqno + 1).max(0);
        let to = size as i64 - 1;
        if from <= to {
            self.bits.clear_range(from as usize, to as usize);
        }
        self
    }

    /// `removeLowerThan(long)`, `SeqnoList.java:74-79`.
    pub fn remove_lower_than(&mut self, seqno: i64) -> &mut SeqnoList {
        let idx = self.index(seqno - 1);
        if idx >= 0 {
            self.bits.clear_range(0, idx as usize);
        }
        self
    }

    /// `getLast()`, `SeqnoList.java:83-86`.
    pub fn last(&self) -> Option<i64> {
        let size = self.bits.size();
        if size == 0 {
            return None;
        }
        let idx = self.bits.previous_set_bit(size - 1);
        if idx == -1 { None } else { Some(self.seqno(idx as usize)) }
    }

    /// `getFirst()`, `SeqnoList.java:89-92`.
    pub fn first(&self) -> Option<i64> {
        let idx = self.bits.next_set_bit(0);
        if idx < 0 { None } else { Some(self.seqno(idx as usize)) }
    }

    /// Whether the list holds no seqno.
    pub fn is_empty(&self) -> bool {
        self.bits.cardinality() == 0
    }

    /// The number of seqnos in the list.
    pub fn size(&self) -> usize {
        self.bits.cardinality()
    }

    /// The seqnos, ascending.
    pub fn iter(&self) -> SeqnoListIter<'_> {
        SeqnoListIter { list: self, index: 0 }
    }

    /// The seqnos, ascending.
    pub fn to_vec(&self) -> Vec<i64> {
        self.iter().collect()
    }

    /// `writeTo`, `SeqnoList.java:101-106`: `int size, long offset, words...`.
    pub fn write_to(&self, out: &mut Vec<u8>) {
        use crate::wire::stream::WireWrite;
        out.write_i32(self.bits.size() as i32);
        out.write_i64(self.offset);
        for w in self.bits.words() {
            out.write_i64(*w as i64);
        }
    }

    /// `readFrom`, `SeqnoList.java:108-115`.
    pub fn read_from(inp: &mut WireReader<'_>) -> SeqnoList {
        SeqnoList::try_read_from(inp).unwrap_or_else(|e| panic!("{e}"))
    }

    /// [`SeqnoList::read_from`] for bytes from the network: a negative size (upstream's `new FixedSizeBitSet`
    /// throws `NegativeArraySizeException`) or fewer words than the size needs is an error.
    pub fn try_read_from(inp: &mut WireReader<'_>) -> WireResult<SeqnoList> {
        let size = inp.try_read_i32()?;
        if size < 0 {
            return Err(WireError::Invalid(format!("SeqnoList size {size} is negative")));
        }
        let size = size as usize;
        let offset = inp.try_read_i64()?;
        let num_words = if size == 0 { 1 } else { (size - 1) / 64 + 1 };
        if num_words * 8 > inp.remaining() {
            return Err(WireError::Underflow { need: num_words * 8, remain: inp.remaining() });
        }
        let words = (0..num_words).map(|_| inp.try_read_i64().map(|w| w as u64)).collect::<WireResult<Vec<u64>>>()?;
        Ok(SeqnoList { bits: FixedSizeBitSet::from_parts(size, words), offset })
    }

    /// `serializedSize`, `SeqnoList.java:94-98`: `4 (size) + 8*(words.len()+1)`.
    pub fn serialized_size(&self) -> usize {
        4 + (self.bits.words().len() + 1) * 8
    }
}

/// The iterator [`SeqnoList::iter`] returns.
pub struct SeqnoListIter<'a> {
    list: &'a SeqnoList,
    index: usize,
}

impl Iterator for SeqnoListIter<'_> {
    type Item = i64;
    fn next(&mut self) -> Option<i64> {
        let size = self.list.bits.size();
        if self.index >= size {
            return None;
        }
        let next = self.list.bits.next_set_bit(self.index);
        if next < 0 || next as usize >= size {
            self.index = size;
            return None;
        }
        self.index = next as usize + 1;
        Some(self.list.seqno(next as usize))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn add_and_iterate() {
        let mut l = SeqnoList::new(20, 100);
        l.add(101).add(105).add(110);
        assert_eq!(l.to_vec(), vec![101, 105, 110]);
        assert_eq!(l.size(), 3);
        assert!(!l.is_empty());
    }

    #[test]
    fn add_range() {
        let mut l = SeqnoList::new(20, 100);
        l.add_range(102, 106);
        assert_eq!(l.to_vec(), vec![102, 103, 104, 105, 106]);
    }

    #[test]
    fn first_and_last() {
        let l = SeqnoList::new(20, 100);
        assert_eq!(l.first(), None);
        assert_eq!(l.last(), None);
        let mut l = SeqnoList::new(20, 100);
        l.add(105).add(101).add(110);
        assert_eq!(l.first(), Some(101));
        assert_eq!(l.last(), Some(110));
    }

    #[test]
    fn remove_higher_than() {
        let mut l = SeqnoList::new(20, 100);
        l.add_range(100, 119);
        l.remove_higher_than(105);
        assert_eq!(l.to_vec(), (100..=105).collect::<Vec<_>>());
    }

    #[test]
    fn remove_lower_than() {
        let mut l = SeqnoList::new(20, 100);
        l.add_range(100, 119);
        l.remove_lower_than(110);
        assert_eq!(l.to_vec(), (110..=119).collect::<Vec<_>>());
    }

    #[test]
    fn empty_list_reports_no_first_last() {
        let l = SeqnoList::new(0, 0);
        assert_eq!(l.first(), None);
        assert_eq!(l.last(), None);
        assert!(l.is_empty());
    }

    #[test]
    fn roundtrip_across_word_boundary() {
        let mut l = SeqnoList::new(200, 1000);
        l.add(1000).add(1063).add(1064).add(1199);
        let mut out = Vec::new();
        l.write_to(&mut out);
        assert_eq!(out.len(), l.serialized_size());
        let mut inp = WireReader::new(&out);
        let back = SeqnoList::read_from(&mut inp);
        assert_eq!(back, l);
        assert_eq!(back.to_vec(), vec![1000, 1063, 1064, 1199]);
    }

    #[test]
    fn roundtrip_empty() {
        let l = SeqnoList::new(50, 5000);
        let mut out = Vec::new();
        l.write_to(&mut out);
        let mut inp = WireReader::new(&out);
        assert_eq!(SeqnoList::read_from(&mut inp), l);
    }

    // Cross-checked against the real jgroups-5.2.28.Final jar (the vector harness's
    // `upstream-tools/vector-harness/src/main/java/Main.java`'s `seqnolist.*` cases) —
    // not hand-derived: wire formats are always recorded from a real JGroups.
    #[test]
    fn matches_upstream_vectors() {
        let v = crate::wire::vectors::load();
        macro_rules! check {
            ($name:expr, $list:expr) => {{
                let vec = crate::wire::vectors::get(&v, $name);
                let mut out = Vec::new();
                $list.write_to(&mut out);
                assert_eq!(out, vec.bytes, $name);
                assert_eq!(out.len() as i64, vec.size, $name);
                let mut inp = WireReader::new(&vec.bytes);
                assert_eq!(SeqnoList::read_from(&mut inp), $list, $name);
            }};
        }

        let mut small = SeqnoList::new(20, 1000);
        small.add(1000).add(1005).add(1010);
        check!("seqnolist.small", small);

        check!("seqnolist.empty", SeqnoList::new(50, 5000));

        let mut word_boundary = SeqnoList::new(200, 1000);
        word_boundary.add(1000).add(1063).add(1064).add(1199);
        check!("seqnolist.wordboundary", word_boundary);

        let mut range = SeqnoList::new(20, 100);
        range.add_range(102, 106);
        check!("seqnolist.range", range);
    }

    // ---- SeqnoListTest, method by method (upstream/tests/junit-functional/org/jgroups/tests/SeqnoListTest.java) ----

    fn add_all(l: &mut SeqnoList, seqnos: &[i64]) {
        for s in seqnos {
            l.add(*s);
        }
    }

    #[test]
    fn upstream_test_addition() {
        let mut l = SeqnoList::new(16, 5000);
        l.add(5001).add_range(5005, 5010).add(5015);
        assert_eq!(l.size(), 8);
        assert_eq!(l.last(), Some(5015));
    }

    #[test]
    fn upstream_test_add_with_offset() {
        let mut l = SeqnoList::new(100, 300000);
        l.add_range(300000, 300050);
        assert_eq!(l.size(), 51);
        assert_eq!(l.last(), Some(300050));
        l.remove_higher_than(300020);
        assert_eq!(l.size(), 21);
    }

    #[test]
    fn upstream_test_add2() {
        let mut l = SeqnoList::new(1, 500);
        l.add_range(500, 500);
        assert_eq!(l.size(), 1);
    }

    #[test]
    fn upstream_test_get_last() {
        let mut l = SeqnoList::new(20, 0);
        assert_eq!(l.last(), None); // Java: -1
        l.add(0);
        assert_eq!(l.last(), Some(0));
        l.add_range(5, 7);
        assert_eq!(l.last(), Some(7));
        l.add_range(15, 19);
        assert_eq!(l.last(), Some(19));

        let mut l = SeqnoList::new(15, 0);
        l.add(3).add_range(5, 10);
        assert_eq!(l.last(), Some(10));
    }

    #[test]
    fn upstream_test_get_first() {
        let mut l = SeqnoList::new(20, 0);
        assert_eq!(l.first(), None);
        l.add(0);
        assert_eq!(l.first(), Some(0));
        l.add_range(5, 7);
        assert_eq!(l.first(), Some(0));

        let mut l = SeqnoList::new(15, 0);
        l.add(3).add_range(5, 10);
        assert_eq!(l.first(), Some(3));

        let mut l = SeqnoList::new(20, 5);
        assert_eq!(l.first(), None);
        l.add(5);
        assert_eq!(l.first(), Some(5));
        l.remove_lower_than(6);
        assert!(l.is_empty());
        l.add(6);
        assert_eq!(l.first(), Some(6));
        assert_eq!(l.last(), Some(6));

        let mut l = SeqnoList::new(100, 5);
        l.add(88);
        assert_eq!(l.size(), 1);
        assert_eq!(l.first(), Some(88));
        assert_eq!(l.last(), Some(88));
    }

    #[test]
    fn upstream_test_remove_higher_than() {
        let mut l = SeqnoList::new(20, 0);
        l.add(1).add_range(10, 15);
        l.remove_higher_than(15);
        assert_eq!(l.size(), 7);
        l.remove_higher_than(14);
        assert_eq!(l.size(), 6);
        l.remove_higher_than(3);
        assert_eq!(l.size(), 1);
    }

    #[test]
    fn upstream_test_remove_higher_than2() {
        let mut l = SeqnoList::new(20, 0);
        l.add(1).add_range(10, 15);
        l.remove_higher_than(10);
        assert_eq!(l.size(), 2);
    }

    #[test]
    fn upstream_test_remove_higher_than_with_offset() {
        let mut l = SeqnoList::new(20, 5);
        add_all(&mut l, &[6, 7, 9, 10, 15, 20]);
        assert_eq!(l.size(), 6);
        l.remove_higher_than(10);
        assert_eq!(l.size(), 4);
        l.remove_higher_than(2);
        assert!(l.is_empty());
    }

    #[test]
    fn upstream_test_remove_lower_than() {
        let mut l = SeqnoList::new(20, 0);
        add_all(&mut l, &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
        assert_eq!(l.size(), 10);
        l.remove_lower_than(0);
        assert_eq!(l.size(), 10);
        l.remove_lower_than(1);
        assert_eq!(l.size(), 10);
        l.remove_lower_than(2);
        assert_eq!(l.size(), 9);
        l.remove_lower_than(20);
        assert!(l.is_empty());
    }

    #[test]
    fn upstream_test_remove_lower_than2() {
        let mut l = SeqnoList::new(20, 0);
        l.remove_lower_than(5);
        l.add(1);
        assert_eq!(l.size(), 1);
        l.remove_lower_than(1);
        assert_eq!(l.size(), 1);
        l.remove_lower_than(5);
        assert!(l.is_empty());
    }

    #[test]
    fn upstream_test_remove_lower_than_with_offset() {
        let mut l = SeqnoList::new(20, 5);
        l.remove_lower_than(10);
        l.remove_lower_than(3);
        l.add(5);
        assert_eq!(l.size(), 1);
        l.remove_lower_than(5);
        assert_eq!(l.size(), 1);
        l.remove_lower_than(6);
        assert!(l.is_empty());

        add_all(&mut l, &[5, 6, 7]);
        l.remove_lower_than(6);
        assert_eq!(l.size(), 2);
    }

    #[test]
    fn upstream_test_iteration_family() {
        let mut l = SeqnoList::new(16, 0);
        l.add(1).add_range(5, 10).add(15);
        assert_eq!(l.to_vec(), vec![1, 5, 6, 7, 8, 9, 10, 15]);

        let mut l = SeqnoList::new(15, 0);
        l.add_range(5, 10);
        assert_eq!(l.to_vec(), vec![5, 6, 7, 8, 9, 10]);

        let mut l = SeqnoList::new(12, 0);
        l.add_range(5, 10).add(11);
        assert_eq!(l.to_vec(), vec![5, 6, 7, 8, 9, 10, 11]);

        let mut l = SeqnoList::new(11, 0);
        l.add(4).add_range(5, 10);
        assert_eq!(l.to_vec(), vec![4, 5, 6, 7, 8, 9, 10]);

        let mut l = SeqnoList::new(15, 0);
        l.add(3).add_range(5, 10).add(12);
        assert_eq!(l.to_vec(), vec![3, 5, 6, 7, 8, 9, 10, 12]);

        let mut l = SeqnoList::new(8, 0);
        l.add(3).add_range(5, 5).add(7);
        assert_eq!(l.to_vec(), vec![3, 5, 7]);
    }

    #[test]
    fn upstream_test_serialization() {
        let mut l = SeqnoList::new(1000, 0);
        add_all(&mut l, &[1, 10, 50]);
        l.add_range(100, 150).add_range(152, 153).add_range(200, 205);
        add_all(&mut l, &[300, 304, 306]);
        l.add_range(400, 450).add(500);
        for i in (502..550).step_by(2) {
            l.add(i);
        }
        let mut buf = Vec::new();
        l.write_to(&mut buf);
        assert_eq!(buf.len(), l.serialized_size());
        let l2 = SeqnoList::read_from(&mut WireReader::new(&buf));
        assert_eq!(l.size(), l2.size());
        assert_eq!(l.to_vec(), l2.to_vec());
    }

    #[test]
    fn upstream_test_serialization2() {
        let mut l = SeqnoList::new(8000, 0);
        for i in 0..8000 {
            l.add(i);
        }
        assert_eq!(l.size(), 8000);
        assert_eq!(l.serialized_size(), 4 + (8000 / 64 + 1) * 8);
    }

    #[test]
    fn upstream_test_serialization3() {
        let max_bundle_size = 64000usize;
        let n = (max_bundle_size - 50) * 8;
        let mut l = SeqnoList::new(n, 0);
        for i in 0..n as i64 {
            l.add(i);
        }
        assert_eq!(l.size(), n);
        assert!(l.serialized_size() <= max_bundle_size);
    }
}
