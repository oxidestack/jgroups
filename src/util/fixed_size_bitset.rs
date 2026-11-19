//! `upstream/src/org/jgroups/util/FixedSizeBitSet.java`: a non-growable bitset backed by `u64`
//! words, used by `util::seqno_list::SeqnoList` (a bitset of missing seqnos) and, indirectly, by
//! `util::table::Table` (not ported here — the gaps a `Table` reports are computed by scanning the
//! matrix directly, matching upstream's own `Table.Missing` visitor rather than a `FixedSizeBitSet`
//! of its own).
//!
//! Faithful bit-for-bit port of the Java version: 64-bit words, `wordIndex(i) = i >> 6`, the same
//! `WORD_MASK` shifting for range set/clear/flip. Rust's `<<`/`>>` on `u64` panic on a shift amount
//! `>= 64` (Java silently wraps to `amount & 63`), so every `WORD_MASK << n` / `WORD_MASK >>> -n`
//! translation below uses `checked_shl`/`checked_shr` with a `0`-on-overflow fallback — the same
//! result Java's wraparound produces for the shift amounts these methods ever pass (`from`/`to` are
//! always `< 64` after `% 64` via `wordIndex`/bit-within-word arithmetic; the only place a shift by
//! exactly 64 legitimately occurs is `WORD_MASK >>> -(to+1)` when `to+1` is a multiple of 64, which
//! must yield `0`, matching Java's `>>> 64 == >>> 0`... no: `>>> -0 == >>> 0 == no-op`. See the
//! inline comments on `set`/`clear` for the two shift idioms and why `checked_shl`/`_shr` reproduce
//! them exactly).

const BITS_PER_WORD: usize = 64;
const WORD_MASK: u64 = u64::MAX;

/// `FixedSizeBitSet`: a bitset of a size fixed at creation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FixedSizeBitSet {
    words: Vec<u64>,
    size: usize,
}

fn word_index(bit_index: usize) -> usize {
    bit_index / BITS_PER_WORD
}

/// `WORD_MASK << from`: Java's shift takes `from % 64` for a `long`; Rust panics on `from >= 64` in
/// debug builds, so mask the amount explicitly (`from` is always a bit-within-range index here, so
/// the wraparound is never actually exercised for `from >= 64` in practice, but masking keeps the
/// arithmetic identical to Java's `<<` semantics rather than relying on that).
fn mask_from(from: usize) -> u64 {
    WORD_MASK << (from % BITS_PER_WORD)
}

/// `WORD_MASK >>> -(to+1)`: Java's `>>>` on a negative shift amount takes `(-(to+1)) & 63`, i.e.
/// `(64 - ((to+1) % 64)) % 64`. Computed directly here instead of via a negated shift (Rust has no
/// unsigned-right-shift-by-negative operator).
fn mask_to(to: usize) -> u64 {
    let shift = ((BITS_PER_WORD - ((to + 1) % BITS_PER_WORD)) % BITS_PER_WORD) as u32;
    if shift == 0 { WORD_MASK } else { WORD_MASK >> shift }
}

impl FixedSizeBitSet {
    /// `new FixedSizeBitSet(size)`, `FixedSizeBitSet.java:34-39`.
    pub fn new(size: usize) -> FixedSizeBitSet {
        let words = if size == 0 { vec![0u64; 1] } else { vec![0u64; word_index(size - 1) + 1] };
        FixedSizeBitSet { words, size }
    }

    /// Only for `SeqnoList::read_from`, which recreates `words` itself from the wire.
    pub(crate) fn from_parts(size: usize, words: Vec<u64>) -> FixedSizeBitSet {
        FixedSizeBitSet { words, size }
    }

    pub(crate) fn words(&self) -> &[u64] {
        &self.words
    }

    /// The number of bits.
    pub fn size(&self) -> usize {
        self.size
    }

    fn check(&self, index: usize) {
        assert!(index < self.size, "index: {index}");
    }

    /// `set(int)`, `FixedSizeBitSet.java:50-58`. Returns `true` if the bit was previously clear.
    pub fn set(&mut self, index: usize) -> bool {
        self.check(index);
        let w = word_index(index);
        let bit = 1u64 << (index % BITS_PER_WORD);
        let already_set = self.words[w] & bit != 0;
        self.words[w] |= bit;
        !already_set
    }

    /// `set(int from, int to)`, `FixedSizeBitSet.java:69-94`: sets `[from, to]` inclusive.
    pub fn set_range(&mut self, from: usize, to: usize) {
        assert!(to >= from && to < self.size, "from={from}, to={to}");
        let start = word_index(from);
        let end = word_index(to);
        let first_mask = mask_from(from);
        let last_mask = mask_to(to);
        if start == end {
            self.words[start] |= first_mask & last_mask;
        } else {
            self.words[start] |= first_mask;
            for w in self.words.iter_mut().take(end).skip(start + 1) {
                *w = WORD_MASK;
            }
            self.words[end] |= last_mask;
        }
    }

    /// `clear(int)`, `FixedSizeBitSet.java:102-108`.
    pub fn clear(&mut self, index: usize) {
        self.check(index);
        let w = word_index(index);
        self.words[w] &= !(1u64 << (index % BITS_PER_WORD));
    }

    /// `clear(int from, int to)`, `FixedSizeBitSet.java:120-144`. Out-of-range is a silent no-op,
    /// matching upstream (it `return`s rather than throwing, unlike `set(from, to)`).
    pub fn clear_range(&mut self, from: usize, to: usize) {
        if to < from || to >= self.size {
            return;
        }
        let start = word_index(from);
        let end = word_index(to);
        let first_mask = mask_from(from);
        let last_mask = mask_to(to);
        if start == end {
            self.words[start] &= !(first_mask & last_mask);
        } else {
            self.words[start] &= !first_mask;
            for w in self.words.iter_mut().take(end).skip(start + 1) {
                *w = 0;
            }
            self.words[end] &= !last_mask;
        }
    }

    /// `get(int)`, `FixedSizeBitSet.java:155-161`.
    pub fn get(&self, index: usize) -> bool {
        self.check(index);
        let w = word_index(index);
        self.words[w] & (1u64 << (index % BITS_PER_WORD)) != 0
    }

    /// `nextSetBit(int)`, `FixedSizeBitSet.java:180-196`.
    pub fn next_set_bit(&self, from_index: usize) -> i64 {
        if from_index >= self.size {
            return -1;
        }
        let mut u = word_index(from_index);
        let mut word = self.words[u] & mask_from(from_index);
        loop {
            if word != 0 {
                return (u * BITS_PER_WORD + word.trailing_zeros() as usize) as i64;
            }
            u += 1;
            if u == self.words.len() {
                return -1;
            }
            word = self.words[u];
        }
    }

    /// `nextClearBit(int)`, `FixedSizeBitSet.java:205-226`.
    pub fn next_clear_bit(&self, from_index: usize) -> i64 {
        if from_index >= self.size {
            return -1;
        }
        let mut u = word_index(from_index);
        if u >= self.words.len() {
            return from_index as i64;
        }
        let mut word = !self.words[u] & mask_from(from_index);
        loop {
            if word != 0 {
                return (u * BITS_PER_WORD + word.trailing_zeros() as usize) as i64;
            }
            u += 1;
            if u == self.words.len() {
                return -1;
            }
            word = !self.words[u];
        }
    }

    /// `previousSetBit(int)`, `FixedSizeBitSet.java:238-253`.
    pub fn previous_set_bit(&self, from: usize) -> i64 {
        assert!(from < self.size, "index: {from}");
        let mut u = word_index(from);
        let mut word = self.words[u] & mask_to(from);
        loop {
            if word != 0 {
                return ((u + 1) * BITS_PER_WORD) as i64 - 1 - word.leading_zeros() as i64;
            }
            if u == 0 {
                return -1;
            }
            u -= 1;
            word = self.words[u];
        }
    }

    /// `cardinality()`, `FixedSizeBitSet.java:260-265`.
    pub fn cardinality(&self) -> usize {
        self.words.iter().map(|w| w.count_ones() as usize).sum()
    }

    /// `flip()`, `FixedSizeBitSet.java:275-300`: flips every bit `[0, size)`.
    pub fn flip(&mut self) {
        if self.size == 0 {
            return;
        }
        let start = word_index(0);
        let end = word_index(self.size);
        let first_mask = mask_from(0);
        // `WORD_MASK >>> -toIndex` where `toIndex=size` (not `size-1`): matches upstream's own
        // `flip()` call exactly (it re-derives the mask with `toIndex`, not `to`, unlike set/clear).
        let shift = ((BITS_PER_WORD - (self.size % BITS_PER_WORD)) % BITS_PER_WORD) as u32;
        let last_mask = if shift == 0 { WORD_MASK } else { WORD_MASK >> shift };
        if start == end {
            self.words[start] ^= first_mask & last_mask;
        } else {
            self.words[start] ^= first_mask;
            for w in self.words.iter_mut().take(end).skip(start + 1) {
                *w ^= WORD_MASK;
            }
            // Divergence: upstream's `words[endWordIndex] ^= lastWordMask` indexes
            // `wordIndex(size())`, which is one past `words.length` whenever `size` is an exact
            // multiple of 64 (e.g. size=128: words.length=2, endWordIndex=2) — an
            // ArrayIndexOutOfBoundsException upstream. `flip()` is unused by `SeqnoList`/`Table`,
            // the only two callers of this type in this crate's scope, so this edge case is dead
            // code either way; guarded here rather than reproducing the crash.
            if end < self.words.len() {
                self.words[end] ^= last_mask;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn set_get_clear_single_bit() {
        let mut b = FixedSizeBitSet::new(10);
        assert!(!b.get(3));
        assert!(b.set(3));
        assert!(b.get(3));
        assert!(!b.set(3)); // already set
        b.clear(3);
        assert!(!b.get(3));
    }

    #[test]
    fn set_range_within_one_word() {
        let mut b = FixedSizeBitSet::new(20);
        b.set_range(2, 5);
        for i in 0..20 {
            assert_eq!(b.get(i), (2..=5).contains(&i), "bit {i}");
        }
    }

    #[test]
    fn set_range_spanning_words() {
        let mut b = FixedSizeBitSet::new(200);
        b.set_range(60, 130);
        for i in 0..200 {
            assert_eq!(b.get(i), (60..=130).contains(&i), "bit {i}");
        }
        assert_eq!(b.cardinality(), 71);
    }

    #[test]
    fn clear_range_spanning_words() {
        let mut b = FixedSizeBitSet::new(200);
        b.set_range(0, 199);
        b.clear_range(60, 130);
        for i in 0..200 {
            assert_eq!(b.get(i), !(60..=130).contains(&i), "bit {i}");
        }
    }

    #[test]
    fn clear_range_out_of_bounds_is_a_noop() {
        let mut b = FixedSizeBitSet::new(10);
        b.set_range(0, 9);
        b.clear_range(5, 20); // to >= size: no-op per upstream
        assert_eq!(b.cardinality(), 10);
    }

    #[test]
    fn next_set_bit_across_word_boundary() {
        let mut b = FixedSizeBitSet::new(130);
        b.set(65);
        b.set(129);
        assert_eq!(b.next_set_bit(0), 65);
        assert_eq!(b.next_set_bit(66), 129);
        assert_eq!(b.next_set_bit(130), -1);
    }

    #[test]
    fn next_clear_bit_across_word_boundary() {
        let mut b = FixedSizeBitSet::new(130);
        b.set_range(0, 129);
        b.clear(70);
        assert_eq!(b.next_clear_bit(0), 70);
        // Faithful upstream quirk, not a bug in this port: `nextClearBit` never caps its result at
        // `size` (`FixedSizeBitSet.java:205-226` has no such check, unlike `nextSetBit`'s implicit
        // one via `fromIndex >= size`). The word array is sized to `wordIndex(size-1)+1`, so any
        // padding bits between `size` (130 here) and the next word boundary (192) are always 0 and
        // get reported as "clear" once every real bit is set. Only `toString()` calls this on
        // `SeqnoList`/`FixedSizeBitSet` in upstream, and this crate doesn't port that formatting —
        // see the module doc comment.
        assert_eq!(b.next_clear_bit(71), 130);
    }

    #[test]
    fn previous_set_bit() {
        let mut b = FixedSizeBitSet::new(130);
        b.set(5);
        b.set(70);
        assert_eq!(b.previous_set_bit(129), 70);
        assert_eq!(b.previous_set_bit(69), 5);
        assert_eq!(b.previous_set_bit(4), -1);
    }

    #[test]
    fn flip_all_bits() {
        let mut b = FixedSizeBitSet::new(70);
        b.set(0);
        b.set(69);
        b.flip();
        assert!(!b.get(0));
        assert!(!b.get(69));
        assert_eq!(b.cardinality(), 68);
    }

    #[test]
    fn cardinality_exact_word_multiple() {
        let mut b = FixedSizeBitSet::new(128);
        b.set_range(0, 127);
        assert_eq!(b.cardinality(), 128);
    }

    #[test]
    #[should_panic]
    fn get_out_of_range_panics() {
        let b = FixedSizeBitSet::new(5);
        b.get(5);
    }
}
