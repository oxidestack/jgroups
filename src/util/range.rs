//! `upstream/src/org/jgroups/util/Range.java`: a `[low, high]` seqno pair, used by NAKACK2/UNICAST3
//! callers that want a contiguous retransmission range rather than a `SeqnoList` bitset. Wire
//! format is `Bits.writeLongSequence(low, high)` — the same two-seqno compression `wire::view::Digest`
//! already uses per-entry, so this reuses `wire::bits`'s `write_long_sequence`/`read_long_sequence`/
//! `size_long_sequence` rather than re-deriving the compression scheme.

use crate::wire::bits::{size_long_sequence, try_read_long_sequence, write_long_sequence};
use crate::wire::stream::{WireReader, WireResult};

/// `Range`: the seqnos from `low` to `high`, both included.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Range {
    /// The first seqno.
    pub low: i64,
    /// The last seqno.
    pub high: i64,
}

impl Range {
    /// The range `[low, high]`.
    pub fn new(low: i64, high: i64) -> Range {
        Range { low, high }
    }

    /// `Range.compareTo`, `Range.java:30-34`: equal ranges compare 0, otherwise ordered by `low`
    /// alone (upstream deliberately ignores `high` once `low` differs).
    pub fn compare_to(&self, other: &Range) -> std::cmp::Ordering {
        if self == other {
            std::cmp::Ordering::Equal
        } else if self.low < other.low {
            std::cmp::Ordering::Less
        } else {
            std::cmp::Ordering::Greater
        }
    }

    /// `Range.writeTo`: both bounds as one compressed long sequence.
    pub fn write_to(&self, out: &mut Vec<u8>) {
        write_long_sequence(self.low, self.high, out);
    }

    /// `Range.readFrom`; panics on a malformed frame.
    pub fn read_from(inp: &mut WireReader<'_>) -> Range {
        Range::try_read_from(inp).unwrap_or_else(|e| panic!("{e}"))
    }

    /// [`Range::read_from`] for bytes from the network.
    pub fn try_read_from(inp: &mut WireReader<'_>) -> WireResult<Range> {
        let (low, high) = try_read_long_sequence(inp)?;
        Ok(Range { low, high })
    }

    /// `Range.serializedSize`.
    pub fn serialized_size(&self) -> usize {
        size_long_sequence(self.low, self.high)
    }
}

impl std::fmt::Display for Range {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{} : {}]", self.low, self.high)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compare_to_orders_by_low_only() {
        assert_eq!(Range::new(1, 5).compare_to(&Range::new(1, 5)), std::cmp::Ordering::Equal);
        assert_eq!(Range::new(1, 5).compare_to(&Range::new(2, 3)), std::cmp::Ordering::Less);
        assert_eq!(Range::new(2, 3).compare_to(&Range::new(1, 9)), std::cmp::Ordering::Greater);
        // Same low, different high: still "equal enough" to order as Greater per upstream's
        // `low < other.low? -1 : 1` fallthrough (only the exact-match branch returns 0).
        assert_eq!(Range::new(1, 5).compare_to(&Range::new(1, 9)), std::cmp::Ordering::Greater);
    }

    #[test]
    fn roundtrip() {
        for r in [Range::new(0, 0), Range::new(5, 5), Range::new(100, 260), Range::new(i64::MAX - 5, i64::MAX)] {
            let mut out = Vec::new();
            r.write_to(&mut out);
            assert_eq!(out.len(), r.serialized_size());
            let mut inp = WireReader::new(&out);
            assert_eq!(Range::read_from(&mut inp), r);
        }
    }

    #[test]
    fn display() {
        assert_eq!(Range::new(3, 7).to_string(), "[3 : 7]");
    }
}
