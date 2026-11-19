//! `upstream/src/org/jgroups/util/Table.java` (962 lines): the sliding-window buffer NAKACK2 and
//! UNICAST3 use for both sides of reliable delivery — the receive-side window that tracks which
//! seqnos have arrived and delivers them in order, and the send-side window a sender keeps of its
//! own sent messages so it can answer an `XMIT_REQ` (`Table.java`'s own doc comment: "used on
//! sender and receiver side").
//!
//! A matrix of rows (`num_rows` initially, each `elements_per_row` wide, `elements_per_row` a power
//! of 2 for `computeIndex`'s masking trick), mapping `seqno -> matrix[row][index]` via `offset`
//! (the seqno at `matrix[0][0]`). Three watermarks: `low` (everything at or below is purged), `hd`
//! (highest delivered/removed), `hr` (highest received, i.e. the highest seqno ever `add`ed).
//!
//! **Locking.** Upstream's `Table` owns a `ReentrantLock` internally. This port has none — every
//! caller (`protocols::nakack2`, `protocols::unicast3`) already holds its own `tokio::sync::Mutex`
//! around the whole per-sender/per-peer state the `Table` lives in (the same external-locking
//! pattern `protocols::gms::protocol::Gms` uses for its own state), so an internal lock here would
//! just be a second, redundant one. An implementation-structure difference, not a behavioural one.
//!
//! **Not ported**: `max_compaction_time`-driven *automatic* compaction inside `purge()` (an
//! optimization — `compact()` is still available to call directly, and every watermark/content
//! guarantee holds with or without it), `toString()`/`dump()` (debug formatting). `iterator()` and
//! `stream()` are both [`Table::iter`] (a Rust iterator is what a Java `Stream` over it is);
//! `Table.Visitor` is a closure passed to [`Table::for_each`]; the `MessageBatch` overloads of
//! `add` and the collector overload of `removeMany` take this crate's `wire::batch::MessageBatch`.

use std::collections::VecDeque;

use crate::util::seqno_list::SeqnoList;
use crate::wire::batch::MessageBatch;
use crate::wire::message::Message;

/// `findHighestSeqno(List)`, `Table.java:609-617`, and the `MessageBatch` overload
/// (`Table.java:619-629`, which skips seqnos `< 0` before comparing): `-1` when nothing qualifies.
fn find_highest_seqno(seqnos: impl Iterator<Item = i64>) -> i64 {
    let mut seqno = -1i64;
    for val in seqnos {
        if val.wrapping_sub(seqno) > 0 {
            seqno = val;
        }
    }
    seqno
}

const DEFAULT_RESIZE_FACTOR: f64 = 1.2;

fn next_power_of_two(n: usize) -> usize {
    n.next_power_of_two().max(1)
}

/// `org.jgroups.util.Table`: the receive window of `NAKACK2` and `UNICAST3`, a matrix of elements indexed by seqno that grows, compacts and purges as messages arrive and are delivered.
#[derive(Debug, Clone)]
pub struct Table<T> {
    num_rows: usize,
    elements_per_row: usize,
    resize_factor: f64,
    /// `None` row = not yet allocated (all elements implicitly missing), matching upstream's
    /// `matrix[i] == null`.
    matrix: Vec<Option<Vec<Option<T>>>>,
    offset: i64,
    size: usize,
    low: i64,
    hr: i64,
    hd: i64,
    num_compactions: u32,
    num_resizes: u32,
    num_moves: u32,
    num_purges: u32,
}

impl<T: Clone> Table<T> {
    /// An empty table whose first seqno is `offset + 1`, with upstream's default dimensions.
    pub fn new(offset: i64) -> Table<T> {
        Table::with_dimensions(5, 8192, offset, DEFAULT_RESIZE_FACTOR)
    }

    /// `Table(int num_rows, int elements_per_row, long offset, double resize_factor)`,
    /// `Table.java:115-124`.
    pub fn with_dimensions(num_rows: usize, elements_per_row: usize, offset: i64, resize_factor: f64) -> Table<T> {
        assert!(resize_factor > 1.0, "resize_factor needs to be > 1");
        let elements_per_row = next_power_of_two(elements_per_row);
        Table {
            num_rows,
            elements_per_row,
            resize_factor,
            matrix: (0..num_rows.max(1)).map(|_| None).collect(),
            offset,
            size: 0,
            low: offset,
            hr: offset,
            hd: offset,
            num_compactions: 0,
            num_resizes: 0,
            num_moves: 0,
            num_purges: 0,
        }
    }

    /// The seqno the first row starts at.
    pub fn offset(&self) -> i64 {
        self.offset
    }
    /// How many elements a row holds.
    pub fn elements_per_row(&self) -> usize {
        self.elements_per_row
    }
    /// `capacity()`, `Table.java:132`.
    pub fn capacity(&self) -> usize {
        self.matrix.len() * self.elements_per_row
    }
    /// The number of elements in the table.
    pub fn size(&self) -> usize {
        self.size
    }
    /// Whether the table holds no element.
    pub fn is_empty(&self) -> bool {
        self.size == 0
    }
    /// The seqno below which everything has been purged.
    pub fn low(&self) -> i64 {
        self.low
    }
    /// The highest delivered seqno.
    pub fn highest_delivered(&self) -> i64 {
        self.hd
    }
    /// The highest received seqno.
    pub fn highest_received(&self) -> i64 {
        self.hr
    }
    /// The number of rows allocated.
    pub fn num_rows(&self) -> usize {
        self.matrix.len()
    }
    /// How many times the matrix grew.
    pub fn num_resizes(&self) -> u32 {
        self.num_resizes
    }
    /// How many times rows were moved to the front.
    pub fn num_moves(&self) -> u32 {
        self.num_moves
    }
    /// How many times [`Self::purge`] ran.
    pub fn num_purges(&self) -> u32 {
        self.num_purges
    }
    /// How many times [`Self::compact`] ran.
    pub fn num_compactions(&self) -> u32 {
        self.num_compactions
    }

    /// `setHighestDelivered(long)`, `Table.java:186-195` — state-transfer only, per upstream's own
    /// doc comment.
    pub fn set_highest_delivered(&mut self, seqno: i64) {
        self.hd = seqno;
    }

    fn compute_row(&self, seqno: i64) -> i64 {
        seqno.wrapping_sub(self.offset).div_euclid(self.elements_per_row as i64)
    }

    fn compute_index(&self, seqno: i64) -> i64 {
        let diff = seqno.wrapping_sub(self.offset);
        if diff < 0 {
            diff
        } else {
            diff & (self.elements_per_row as i64 - 1)
        }
    }

    fn get_row(&mut self, index: usize) -> &mut Vec<Option<T>> {
        if self.matrix[index].is_none() {
            self.matrix[index] = Some(vec![None; self.elements_per_row]);
        }
        self.matrix[index].as_mut().unwrap()
    }

    /// `_add(long, T, boolean, Predicate)`, `Table.java:575-606`. `remove_filter` mirrors
    /// upstream's optional consecutive-removal filter (used by UNICAST3 to drop messages the
    /// application has already consumed via a different path) — `None` here for the common case.
    fn add_one(&mut self, seqno: i64, element: T, check_resize: bool, remove_filter: Option<&dyn Fn(&T) -> bool>) -> bool {
        if seqno.wrapping_sub(self.hd) <= 0 {
            return false;
        }
        let mut row_index = self.compute_row(seqno);
        if check_resize && row_index as usize >= self.matrix.len() {
            self.resize(seqno);
            row_index = self.compute_row(seqno);
        }
        if row_index < 0 || row_index as usize >= self.matrix.len() {
            return false; // seqno below the current window (already purged) — nothing to add
        }
        let index = self.compute_index(seqno);
        if index < 0 {
            return false;
        }
        let row = self.get_row(row_index as usize);
        if row[index as usize].is_some() {
            return false;
        }
        row[index as usize] = Some(element);
        self.size += 1;
        if seqno.wrapping_sub(self.hr) > 0 {
            self.hr = seqno;
        }
        if let Some(filter) = remove_filter
            && seqno.wrapping_sub(self.hd) > 0
        {
            loop {
                let next = self.hd.wrapping_add(1);
                if next.wrapping_sub(self.hr) > 0 {
                    break;
                }
                let row_index = self.compute_row(next);
                let index = self.compute_index(next);
                let keep_going = matches!(
                    self.matrix.get(row_index as usize).and_then(|r| r.as_ref()).and_then(|r| r[index as usize].as_ref()),
                    Some(msg) if filter(msg)
                );
                if !keep_going {
                    break;
                }
                self.hd = next;
                self.size = self.size.saturating_sub(1);
            }
        }
        true
    }

    /// `Table.add`: stores `element` at `seqno`; false if it was already delivered, already there or below the window.
    pub fn add(&mut self, seqno: i64, element: T) -> bool {
        self.add_one(seqno, element, true, None)
    }

    /// [`Self::add`], first dropping removed elements that `remove_filter` matches (`Table.add(seqno, element, filter)`).
    pub fn add_with_filter(&mut self, seqno: i64, element: T, remove_filter: &dyn Fn(&T) -> bool) -> bool {
        self.add_one(seqno, element, true, Some(remove_filter))
    }

    /// `add(List<LongTuple<T>>, boolean, T)`, `Table.java:260-285` (`add(list)` is
    /// `add_list(list, false, None)`): resizes at most once, for the highest seqno in the list, then
    /// adds every tuple without a per-element resize check. `const_value`, when given, is stored
    /// instead of each tuple's element. With `remove_added_elements`, tuples that could not be added
    /// are removed from `list`. True if at least one element was added.
    pub fn add_list(&mut self, list: &mut Vec<(i64, T)>, remove_added_elements: bool, const_value: Option<T>) -> bool {
        if list.is_empty() {
            return false;
        }
        let highest_seqno = find_highest_seqno(list.iter().map(|(seqno, _)| *seqno));
        if highest_seqno != -1 && self.compute_row(highest_seqno) >= self.matrix.len() as i64 {
            self.resize(highest_seqno);
        }
        let mut added = false;
        list.retain(|(seqno, element)| {
            let element = const_value.clone().unwrap_or_else(|| element.clone());
            if self.add_one(*seqno, element, false, None) {
                added = true;
                true
            } else {
                !remove_added_elements
            }
        });
        added
    }

    /// `removeMany(boolean, int, Predicate, Supplier<R>, BiConsumer<R,T>)`, `Table.java:436-447`
    /// with its `Remover` visitor (`Table.java:862-899`): drains consecutive present elements from
    /// `hd+1`; every drained element advances `hd` (and `low` when nulling), but only the ones
    /// `filter` accepts go into the result, and `max_results` (0 = unlimited) counts accepted ones.
    /// The result is created lazily by `result_creator` on the first accepted element, so `None` is
    /// upstream's `null` (nothing accepted).
    pub fn remove_many_into<R>(
        &mut self,
        nullify: bool,
        max_results: usize,
        filter: Option<&dyn Fn(&T) -> bool>,
        result_creator: impl FnOnce() -> R,
        mut accumulator: impl FnMut(&mut R, T),
    ) -> Option<R> {
        let mut result: Option<R> = None;
        let mut result_creator = Some(result_creator);
        let mut num_results = 0usize;
        loop {
            let next = self.hd.wrapping_add(1);
            if next.wrapping_sub(self.hr) > 0 {
                break;
            }
            let row_index = self.compute_row(next);
            let index = self.compute_index(next);
            if row_index < 0 || row_index as usize >= self.matrix.len() || index < 0 {
                break;
            }
            let Some(row) = self.matrix[row_index as usize].as_mut() else { break };
            let Some(element) = row[index as usize].clone() else { break };
            self.hd = next;
            self.size = self.size.saturating_sub(1);
            if nullify {
                row[index as usize] = None;
                if index as usize == self.elements_per_row - 1 {
                    self.matrix[row_index as usize] = None;
                }
                if next.wrapping_sub(self.low) > 0 {
                    self.low = next;
                }
            }
            if filter.is_none_or(|f| f(&element)) {
                let r = result.get_or_insert_with(|| (result_creator.take().expect("created once"))());
                accumulator(r, element);
                num_results += 1;
            }
            if max_results != 0 && num_results >= max_results {
                break;
            }
        }
        result
    }

    /// `get(long)`, `Table.java:338-355`.
    pub fn get(&self, seqno: i64) -> Option<&T> {
        if seqno.wrapping_sub(self.low) <= 0 || seqno.wrapping_sub(self.hr) > 0 {
            return None;
        }
        let row_index = self.compute_row(seqno);
        if row_index < 0 || row_index as usize >= self.matrix.len() {
            return None;
        }
        let index = self.compute_index(seqno);
        if index < 0 {
            return None;
        }
        self.matrix[row_index as usize].as_ref()?[index as usize].as_ref()
    }

    /// `remove(boolean)`, `Table.java:386-413`: removes and returns the next message after `hd`,
    /// only if it's present (no gap skipping).
    pub fn remove(&mut self, nullify: bool) -> Option<T> {
        let next = self.hd.wrapping_add(1);
        let row_index = self.compute_row(next);
        if row_index < 0 || row_index as usize >= self.matrix.len() {
            return None;
        }
        let index = self.compute_index(next);
        if index < 0 {
            return None;
        }
        let row = self.matrix[row_index as usize].as_mut()?;
        let existing = row[index as usize].clone();
        if existing.is_some() {
            self.hd = self.hd.wrapping_add(1);
            self.size = self.size.saturating_sub(1);
            if nullify {
                row[index as usize] = None;
                if self.hd.wrapping_sub(self.low) > 0 {
                    self.low = self.hd;
                }
            }
        }
        existing
    }

    /// `removeMany(boolean, int, Predicate)`, `Table.java:420-447`: drains consecutive deliverable
    /// (non-missing) elements starting at `hd+1`, up to `max_results` (0 = unlimited).
    /// Upstream's `null` (nothing accepted) is an empty `Vec` here; see [`Table::remove_many_into`].
    pub fn remove_many(&mut self, nullify: bool, max_results: usize, filter: Option<&dyn Fn(&T) -> bool>) -> Vec<T> {
        self.remove_many_into(nullify, max_results, filter, Vec::new, |v, e| v.push(e)).unwrap_or_default()
    }

    /// `purge(long, boolean)`, `Table.java:464-514`, minus the `max_compaction_time`-driven
    /// auto-compaction (see the module doc comment).
    pub fn purge(&mut self, mut seqno: i64, force: bool) {
        if seqno.wrapping_sub(self.low) <= 0 {
            return;
        }
        if force {
            if seqno.wrapping_sub(self.hr) > 0 {
                seqno = self.hr;
            }
        } else if seqno.wrapping_sub(self.hd) > 0 {
            seqno = self.hd;
        }

        let start_row = self.compute_row(self.low).max(0);
        let end_row = self.compute_row(seqno);
        if end_row < 0 {
            return;
        }
        for i in start_row..end_row {
            if let Some(row) = self.matrix.get_mut(i as usize) {
                *row = None;
            }
        }
        let index = self.compute_index(seqno);
        if let Some(Some(row)) = self.matrix.get_mut(end_row as usize) {
            for slot in row.iter_mut().take((index + 1).max(0) as usize) {
                *slot = None;
            }
        }
        if seqno.wrapping_sub(self.low) > 0 {
            self.low = seqno;
        }
        if force {
            if seqno.wrapping_sub(self.hd) > 0 {
                self.low = seqno;
                self.hd = seqno;
            }
            self.size = self.compute_size();
        }
        self.num_purges += 1;
    }

    /// `Table.compact`: moves the rows still in use to the front, shrinking the matrix.
    pub fn compact(&mut self) {
        let from = self.compute_row(self.low).max(0) as usize;
        let to = self.compute_row(self.hr).max(0) as usize;
        let range = to - from + 1;
        let mut new_size = ((range as f64) * self.resize_factor).max(range as f64 + 1.0) as usize;
        new_size = new_size.max(self.num_rows);
        if new_size < self.matrix.len() {
            let mut new_matrix: Vec<Option<Vec<Option<T>>>> = (0..new_size).map(|_| None).collect();
            for (dst, src) in new_matrix.iter_mut().zip(self.matrix[from..from + range].iter()) {
                *dst = src.clone();
            }
            self.matrix = new_matrix;
            self.offset = self.offset.wrapping_add(from as i64 * self.elements_per_row as i64);
            self.num_compactions += 1;
        }
    }

    /// `resize(long)`, `Table.java:636-654`: makes room for `seqno`, either by growing the matrix
    /// or by sliding already-purged rows out.
    fn resize(&mut self, seqno: i64) {
        let num_rows_to_purge = self.compute_row(self.low).max(0) as usize;
        let row_index = self.compute_row(seqno) - num_rows_to_purge as i64; // rows, not seqnos: no overflow
        if row_index < 0 {
            return;
        }
        let new_size = (row_index as usize + 1).max(self.matrix.len());
        if new_size > self.matrix.len() {
            let mut new_matrix: Vec<Option<Vec<Option<T>>>> = (0..new_size).map(|_| None).collect();
            for (dst, src) in new_matrix.iter_mut().zip(self.matrix[num_rows_to_purge..].iter()) {
                *dst = src.clone();
            }
            self.matrix = new_matrix;
            self.num_resizes += 1;
        } else if num_rows_to_purge > 0 {
            self.move_down(num_rows_to_purge);
        }
        self.offset = self.offset.wrapping_add(num_rows_to_purge as i64 * self.elements_per_row as i64);
    }

    /// `move(int)`, `Table.java:659-670`.
    fn move_down(&mut self, num_rows: usize) {
        if num_rows == 0 || num_rows > self.matrix.len() {
            return;
        }
        let len = self.matrix.len();
        for i in num_rows..len {
            self.matrix.swap(i - num_rows, i);
        }
        for row in self.matrix.iter_mut().skip(len - num_rows) {
            *row = None;
        }
        self.num_moves += 1;
    }

    /// `computeSize()`, `Table.java:696-700`: a full recount, used after a forced purge. Upstream
    /// counts the non-null elements of `stream()`, i.e. of `(hd, hr]`: delivered elements that were
    /// not nulled out (`remove_many(false, ..)`) are not part of the size.
    fn compute_size(&self) -> usize {
        self.entries_between(self.hd.wrapping_add(1), self.hr).filter(|(_, e)| e.is_some()).count()
    }

    /// `getNumMissing()`, `Table.java:704-712`.
    pub fn num_missing(&self) -> i64 {
        self.hr.wrapping_sub(self.hd).wrapping_sub(self.size as i64)
    }

    /// `getHighestDeliverable()`, `Table.java:153-166`: the highest seqno such that every seqno
    /// from `hd+1` up to it is present — may be `> hd` if elements were added but not yet removed.
    pub fn highest_deliverable(&self) -> i64 {
        let mut highest = self.hd;
        for (seqno, element) in self.entries_between(self.hd.wrapping_add(1), self.hr) {
            if element.is_none() {
                break;
            }
            highest = seqno;
        }
        highest
    }

    /// `getNumDeliverable()`, `Table.java:169-179`.
    pub fn num_deliverable(&self) -> usize {
        let mut n = 0;
        for (_, element) in self.entries_between(self.hd.wrapping_add(1), self.hr) {
            if element.is_none() {
                break;
            }
            n += 1;
        }
        n
    }

    /// `getMissing(int)`, `Table.java:728-746`: seqnos in `(highest_deliverable, hr]` with no
    /// element, oldest first, capped at `max_msgs` (0 = unlimited).
    pub fn missing(&self, max_msgs: usize) -> Option<SeqnoList> {
        if self.size == 0 {
            return None;
        }
        let start = self.highest_deliverable().wrapping_add(1);
        let capacity = self.hr.wrapping_sub(start).max(0) as usize;
        let max_size = if max_msgs > 0 { max_msgs.min(capacity) } else { capacity };
        if max_size == 0 {
            return None;
        }
        let to = start.wrapping_add(max_size as i64 - 1);
        let to = if to.wrapping_sub(self.hr.wrapping_sub(1)) > 0 { self.hr.wrapping_sub(1) } else { to };
        let to = if to.wrapping_sub(start.wrapping_sub(1)) < 0 { start.wrapping_sub(1) } else { to };
        let mut list = SeqnoList::new(max_size, start);
        let mut n = 0usize;
        for (seqno, element) in self.entries_between(start, to) {
            if element.is_none() {
                n += 1;
                if n > max_size {
                    break;
                }
                list.add(seqno);
            }
        }
        Some(list)
    }

    /// `getDigest()`, `Table.java:748-756`.
    pub fn digest(&self) -> (i64, i64) {
        (self.hd, self.hr)
    }

    /// Iterates `[from, to]` inclusive, yielding `(seqno, element)` — the Rust stand-in for
    /// upstream's `forEach(long, long, Visitor)` callback (`Table.java:535-555`); used internally
    /// by every read-only scan above, and by NAKACK2/UNICAST3 for draining deliverable messages.
    /// Collected into a `VecDeque` up front rather than a lazy borrow-checked iterator: every
    /// caller either consumes the whole range or breaks out early on the first gap, so there's no
    /// real cost to materializing it, and it sidesteps holding `&self` borrowed across the
    /// per-element row/column bookkeeping.
    pub fn entries_between(&self, from: i64, to: i64) -> impl Iterator<Item = (i64, Option<&T>)> {
        let mut out = VecDeque::new();
        self.for_each(from, to, |seqno, element, _, _| {
            out.push_back((seqno, element));
            true
        });
        out.into_iter()
    }

    /// `forEach(long, long, Visitor)`, `Table.java:535-555`: visits `[from, to]` inclusive with
    /// `(seqno, element, row, column)`; the visitor returns false to stop.
    pub fn for_each<'a>(&'a self, from: i64, to: i64, mut visitor: impl FnMut(i64, Option<&'a T>, i64, i64) -> bool) {
        if from.wrapping_sub(to) > 0 {
            return;
        }
        let mut row = self.compute_row(from);
        let mut column = self.compute_index(from);
        let mut seqno = from;
        let distance = to.wrapping_sub(from).wrapping_add(1);
        // `seqno`/`column`/`row` all advance in lockstep per iteration (mirroring upstream's
        // `forEach`, `Table.java:543-554`) — not a plain range clippy can rewrite into `(from..)`.
        #[allow(clippy::explicit_counter_loop)]
        for _ in 0..distance {
            let element = if row < 0 || row as usize >= self.matrix.len() {
                None
            } else {
                self.matrix[row as usize].as_ref().and_then(|r| r[column as usize].as_ref())
            };
            if !visitor(seqno, element, row, column) {
                break;
            }
            seqno = seqno.wrapping_add(1);
            column += 1;
            if column as usize >= self.elements_per_row {
                column = 0;
                row += 1;
            }
        }
    }

    /// `iterator()` / `stream()`, `Table.java:557-568` (`TableIterator`, `Table.java:824-858`):
    /// every slot of `[hd+1, hr]`, missing ones as `None` (upstream yields `null`).
    pub fn iter(&self) -> impl Iterator<Item = Option<&T>> {
        self.iter_range(self.hd.wrapping_add(1), self.hr)
    }

    /// `iterator(long, long)` / `stream(long, long)`, `Table.java:561-573`.
    pub fn iter_range(&self, from: i64, to: i64) -> impl Iterator<Item = Option<&T>> {
        self.entries_between(from, to).map(|(_, element)| element)
    }
}

impl Table<Message> {
    /// `add(MessageBatch, Function<T,Long>, boolean, T)`, `Table.java:302-331` (`add(batch,
    /// getter)` is `add_batch(batch, getter, false, None)`): resizes at most once for the highest
    /// seqno, skips messages whose seqno is `< 0`, and removes from `batch` every message that was
    /// not added, or every visited message when `remove_from_batch`. `const_value`, when given, is
    /// stored instead of each message. True if at least one message was added.
    pub fn add_batch(
        &mut self,
        batch: &mut MessageBatch,
        seqno_getter: &dyn Fn(&Message) -> i64,
        remove_from_batch: bool,
        const_value: Option<Message>,
    ) -> bool {
        if batch.is_empty() {
            return false;
        }
        let highest_seqno = find_highest_seqno(batch.iter().map(seqno_getter).filter(|seqno| *seqno >= 0));
        if highest_seqno != -1 && self.compute_row(highest_seqno) >= self.matrix.len() as i64 {
            self.resize(highest_seqno);
        }
        let mut retval = false;
        batch.remove_if(
            |msg| {
                let seqno = seqno_getter(msg);
                if seqno < 0 {
                    return false;
                }
                let element = const_value.clone().unwrap_or_else(|| msg.clone());
                let added = self.add_one(seqno, element, false, None);
                retval |= added;
                !added || remove_from_batch
            },
            true,
        );
        retval
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn small_table() -> Table<&'static str> {
        Table::with_dimensions(2, 4, 0, 1.5)
    }

    // Representative of TableTest's testAddition
    #[test]
    fn add_returns_true_for_a_new_seqno_and_false_for_a_duplicate() {
        let mut t = small_table();
        assert!(t.add(1, "a"));
        assert!(!t.add(1, "b"));
        assert_eq!(t.get(1), Some(&"a"));
        assert_eq!(t.size(), 1);
    }

    // Representative of TableTest's testAddition (below hd)
    #[test]
    fn add_below_hd_is_rejected() {
        let mut t = small_table();
        t.add(1, "a");
        t.remove(true);
        assert!(!t.add(1, "z")); // already delivered
        assert_eq!(t.highest_delivered(), 1);
    }

    #[test]
    fn get_outside_the_window_returns_none() {
        let mut t = small_table();
        t.add(5, "a");
        assert_eq!(t.get(0), None); // <= low
        assert_eq!(t.get(100), None); // > hr
    }

    // Representative of TableTest's testRemove
    #[test]
    fn remove_only_advances_on_a_present_contiguous_element() {
        let mut t = small_table();
        t.add(2, "b"); // gap at 1
        assert_eq!(t.remove(true), None); // hd+1 = 1, missing
        assert_eq!(t.highest_delivered(), 0);
        t.add(1, "a");
        assert_eq!(t.remove(true), Some("a"));
        assert_eq!(t.highest_delivered(), 1);
        assert_eq!(t.remove(true), Some("b"));
        assert_eq!(t.highest_delivered(), 2);
        assert_eq!(t.remove(true), None);
    }

    // Representative of TableTest's testRemoveMany
    #[test]
    fn remove_many_drains_the_deliverable_prefix_only() {
        let mut t = small_table();
        t.add(1, "a");
        t.add(2, "b");
        t.add(4, "d"); // gap at 3
        let drained = t.remove_many(true, 0, None);
        assert_eq!(drained, vec!["a", "b"]);
        assert_eq!(t.highest_delivered(), 2);
        assert_eq!(t.size(), 1); // "d" still buffered
    }

    #[test]
    fn remove_many_respects_max_results() {
        let mut t = Table::with_dimensions(2, 8, 0, 1.5);
        for i in 1..=5 {
            t.add(i, i);
        }
        let drained = t.remove_many(true, 3, None);
        assert_eq!(drained, vec![1, 2, 3]);
        assert_eq!(t.highest_delivered(), 3);
    }

    // Representative of TableTest's testPurge. Non-force purge is capped at `hd` (`Table.java:474-476`): with
    // nothing ever delivered, `hd` never moved off the initial offset, so `purge` without `force` is a
    // no-op here — exactly the "send-side" table shape (`protocols::nakack2`'s own `sent` table:
    // nothing ever calls `remove`/`remove_many` on it, only `add`; STABLE's digest-driven GC is
    // the only thing that ever advances its `low`, via `force=true`).
    #[test]
    fn purge_without_force_is_capped_at_hd() {
        let mut t = Table::with_dimensions(2, 4, 0, 1.5);
        for i in 1..=4 {
            t.add(i, i);
        }
        t.purge(2, false); // hd is still 0: no-op
        assert_eq!(t.low(), 0);
        assert_eq!(t.get(1), Some(&1));
    }

    #[test]
    fn purge_force_removes_everything_up_to_and_including_seqno() {
        let mut t = Table::with_dimensions(2, 4, 0, 1.5);
        for i in 1..=4 {
            t.add(i, i);
        }
        t.purge(2, true);
        assert_eq!(t.low(), 2);
        assert_eq!(t.highest_delivered(), 2);
        assert_eq!(t.get(1), None);
        assert_eq!(t.get(2), None); // purged too (<=)
        assert_eq!(t.get(3), Some(&3)); // still there, above the purge point
    }

    #[test]
    fn purge_force_moves_hd_and_low_to_seqno() {
        let mut t = small_table();
        t.add(1, "a");
        t.add(2, "b"); // not yet delivered
        t.purge(2, true);
        assert_eq!(t.highest_delivered(), 2);
        assert_eq!(t.low(), 2);
        assert_eq!(t.size(), 0);
    }

    // Representative of TableTest's testGetHighestDeliverable / testGetNumDeliverable
    #[test]
    fn highest_deliverable_and_num_deliverable_stop_at_the_first_gap() {
        let mut t = small_table();
        t.add(1, "a");
        t.add(2, "b");
        t.add(4, "d"); // gap at 3
        assert_eq!(t.highest_deliverable(), 2);
        assert_eq!(t.num_deliverable(), 2);
    }

    // Representative of TableTest's testGetMissing
    #[test]
    fn missing_lists_gaps_after_the_deliverable_prefix() {
        let mut t = Table::with_dimensions(2, 16, 0, 1.5);
        t.add(1, "a");
        t.add(3, "c");
        t.add(4, "d");
        t.add(7, "g");
        let missing = t.missing(0).unwrap();
        assert_eq!(missing.to_vec(), vec![2, 5, 6]);
    }

    #[test]
    fn missing_is_none_when_nothing_is_missing() {
        let mut t = small_table();
        t.add(1, "a");
        assert!(t.missing(0).is_none());
    }

    #[test]
    fn missing_respects_max_msgs() {
        let mut t = Table::with_dimensions(2, 16, 0, 1.5);
        t.add(1, "a");
        t.add(10, "j"); // gaps: 2..9
        let missing = t.missing(3).unwrap();
        assert_eq!(missing.to_vec().len(), 3);
        assert_eq!(missing.to_vec()[0], 2);
    }

    // Representative of TableTest's testResize (implicitly, via add() past the initial window)
    #[test]
    fn add_past_the_initial_window_resizes_the_matrix() {
        let mut t = Table::with_dimensions(1, 4, 0, 1.5);
        assert_eq!(t.capacity(), 4);
        for i in 1..=20 {
            assert!(t.add(i, i));
        }
        assert!(t.capacity() >= 20);
        assert_eq!(t.get(20), Some(&20));
    }

    // Representative of TableTest's testResizeWithPurge (resize after rows were purged: offset/low moves the window
    // forward instead of growing forever)
    #[test]
    fn resize_after_purge_reclaims_purged_rows_instead_of_growing_unbounded() {
        let mut t = Table::with_dimensions(2, 4, 0, 1.5);
        for i in 1..=8 {
            t.add(i, i);
        }
        t.remove_many(true, 0, None); // hd=8
        t.purge(8, false);
        let rows_before = t.num_rows();
        for i in 9..=16 {
            assert!(t.add(i, i));
        }
        // Either moved (no resize) or resized modestly — never grew as if nothing was ever purged.
        assert!(t.num_rows() <= rows_before + 2);
        assert_eq!(t.get(16), Some(&16));
    }

    #[test]
    fn compact_shrinks_the_matrix_after_a_purge() {
        let mut t = Table::with_dimensions(1, 4, 0, 1.5);
        for i in 1..=40 {
            t.add(i, i);
        }
        t.purge(35, true); // force: low=hd=35, rows below it are reclaimable
        let rows_before = t.num_rows();
        t.compact();
        assert!(t.num_rows() <= rows_before);
        assert_eq!(t.get(40), Some(&40)); // still above the purge point
        assert_eq!(t.get(20), None); // purged
    }

    #[test]
    fn digest_reports_hd_and_hr() {
        let mut t = small_table();
        t.add(1, "a");
        t.add(3, "c");
        assert_eq!(t.digest(), (0, 3));
    }

    #[test]
    fn num_missing_counts_the_gaps_between_hd_and_hr() {
        let mut t = small_table();
        t.add(2, "b");
        t.add(4, "d");
        assert_eq!(t.num_missing(), 2); // 1 and 3
    }

    #[test]
    fn add_with_filter_advances_hd_past_consecutive_filtered_messages() {
        let mut t = Table::with_dimensions(2, 8, 0, 1.5);
        let is_tombstone = |m: &&str| *m == "X";
        t.add_with_filter(1, "X", &is_tombstone);
        t.add_with_filter(2, "X", &is_tombstone);
        t.add_with_filter(3, "real", &is_tombstone);
        // Both tombstones auto-delivered (skipped) since they match the filter and are consecutive
        // from hd+1; "real" doesn't match, so hd stops there.
        assert_eq!(t.highest_delivered(), 2);
        assert_eq!(t.get(3), Some(&"real"));
    }

    // ============================== Additional TableTest ports ==============================

    // TableTest#testAddWithWrapAround: a small table (3 rows) cycles through several resize/purge
    // rounds, proving `low`/purge correctly null out everything at or below the purge point across
    // row boundaries, not just within the current row.
    #[test]
    fn add_with_wrap_around() {
        let mut buf: Table<i64> = Table::with_dimensions(3, 10, 5, DEFAULT_RESIZE_FACTOR);
        for i in 6..=15 {
            assert!(buf.add(i, i), "addition of seqno {i} failed");
        }
        for _ in 0..3 {
            assert!(buf.remove(false).is_some());
        }
        let low = buf.low();
        buf.purge(8, false);
        assert_eq!(buf.low(), 8);
        for i in low..=8 {
            assert_eq!(buf.get(i), None, "message with seqno={i} is not null");
        }

        for i in 16..=18 {
            assert!(buf.add(i, i));
        }
        while buf.remove(false).is_some() {}
        assert!(buf.is_empty());
        assert_eq!(buf.num_missing(), 0);
        let low = buf.low();
        buf.purge(18, false);
        assert_eq!(buf.low(), 18);
        for i in low..=18 {
            assert_eq!(buf.get(i), None, "message with seqno={i} is not null");
        }
    }

    // TableTest#testAddWithWrapAroundAndRemoveMany: same wraparound scenario, draining with
    // `remove_many` instead of one-at-a-time `remove`.
    #[test]
    fn add_with_wrap_around_and_remove_many() {
        let mut buf: Table<i64> = Table::with_dimensions(3, 10, 5, DEFAULT_RESIZE_FACTOR);
        for i in 6..=15 {
            assert!(buf.add(i, i), "addition of seqno {i} failed");
        }
        let removed = buf.remove_many(true, 3, None);
        for i in &removed {
            assert_eq!(buf.get(*i), None);
        }
        assert_eq!((buf.low(), buf.highest_delivered(), buf.highest_received()), (8, 8, 15));

        for i in 16..=18 {
            assert!(buf.add(i, i));
        }
        let removed = buf.remove_many(true, 0, None);
        assert_eq!(removed.len(), 10);
        for i in &removed {
            assert_eq!(buf.get(*i), None);
        }
        assert!(buf.is_empty());
        assert_eq!(buf.num_missing(), 0);
        assert_eq!((buf.low(), buf.highest_delivered(), buf.highest_received()), (18, 18, 18));
    }

    // TableTest#testAddMissing: a gap blocks `remove()` past it; filling the gap lets the whole
    // deliverable run (including what was already buffered past the gap) drain one at a time.
    #[test]
    fn add_missing() {
        let mut buf: Table<i64> = Table::with_dimensions(3, 10, 0, DEFAULT_RESIZE_FACTOR);
        for i in [1, 2, 4, 5, 6] {
            buf.add(i, i);
        }
        assert_eq!(buf.size(), 5);
        assert_eq!(buf.num_missing(), 1);

        assert_eq!(buf.remove(true), Some(1));
        assert_eq!(buf.remove(true), Some(2));
        assert_eq!(buf.remove(true), None); // blocked on the gap at 3

        buf.add(3, 3);
        assert_eq!(buf.size(), 4);
        assert_eq!(buf.num_missing(), 0);

        for i in 3..=6 {
            assert_eq!(buf.remove(true), Some(i));
        }
        assert_eq!(buf.remove(true), None);
    }

    // TableTest#testAddWithInvalidSeqno: a seqno at or below the window's offset is rejected.
    #[test]
    fn add_with_invalid_seqno_is_rejected() {
        let mut buf: Table<i64> = Table::with_dimensions(3, 10, 20, DEFAULT_RESIZE_FACTOR);
        assert!(!buf.add(10, 0));
        assert!(!buf.add(20, 0));
        assert!(buf.is_empty());
    }

    // TableTest#testGetHighestDeliverable2: removeMany draining past what's actually present still
    // reports the true highest deliverable/delivered, not the requested count.
    #[test]
    fn get_highest_deliverable_after_over_requesting_remove_many() {
        let mut table: Table<i64> = Table::with_dimensions(3, 10, 0, DEFAULT_RESIZE_FACTOR);
        for i in 1..=10 {
            table.add(i, i);
        }
        table.remove_many(true, 20, None);
        let highest_deliverable = table.highest_deliverable();
        assert_eq!(highest_deliverable, 10);
        assert_eq!(table.highest_delivered(), highest_deliverable);
    }

    // TableTest#testGetHighestDeliverable3: a removeMany call capped below the fully deliverable
    // count still leaves `highest_deliverable` correctly reporting the whole run's end.
    #[test]
    fn get_highest_deliverable_reports_the_full_deliverable_run_regardless_of_remove_many_count() {
        let mut table: Table<i64> = Table::with_dimensions(3, 10, 0, DEFAULT_RESIZE_FACTOR);
        for i in 1..=10 {
            table.add(i, i);
        }
        table.remove_many(true, 9, None);
        assert_eq!(table.highest_deliverable(), 10);
    }

    // TableTest#testPurgeLower: a force-purge to a lower seqno than the current low is a no-op,
    // whether the second purge call is itself forced or not.
    #[test]
    fn purge_lower_than_the_current_low_is_a_no_op() {
        let mut table: Table<i64> = Table::with_dimensions(3, 10, 0, DEFAULT_RESIZE_FACTOR);
        for i in 1..=30 {
            table.add(i, i);
        }
        table.purge(20, true);
        assert_eq!((table.low(), table.highest_delivered(), table.highest_received()), (20, 20, 30));
        table.purge(15, true);
        assert_eq!((table.low(), table.highest_delivered(), table.highest_received()), (20, 20, 30));

        let mut table: Table<i64> = Table::with_dimensions(3, 10, 0, DEFAULT_RESIZE_FACTOR);
        for i in 1..=30 {
            table.add(i, i);
        }
        table.purge(20, true);
        assert_eq!((table.low(), table.highest_delivered(), table.highest_received()), (20, 20, 30));
        table.purge(15, false);
        assert_eq!((table.low(), table.highest_delivered(), table.highest_received()), (20, 20, 30));
    }

    // TableTest#testResizeWithPurge2: purge followed by an addition that forces a resize must keep
    // every already-purged seqno gone (not resurrected by the resize) and every post-purge addition
    // correctly placed.
    #[test]
    fn resize_with_purge_keeps_purged_seqnos_gone_after_the_resize() {
        let mut table: Table<i64> = Table::with_dimensions(3, 10, 0, DEFAULT_RESIZE_FACTOR);
        for i in 1..=50 {
            table.add(i, i);
        }
        assert_eq!(table.size(), 50);
        table.remove_many(false, 43, None);
        assert_eq!((table.low(), table.highest_delivered(), table.highest_received()), (0, 43, 50));
        table.purge(43, false);
        assert_eq!((table.low(), table.highest_delivered(), table.highest_received()), (43, 43, 50));
        table.add(52, 52); // forces a resize

        assert_eq!(table.get(43), None);
        for i in 44..=50 {
            assert_eq!(table.get(i), Some(&i));
        }
        assert_eq!(table.get(51), None);
        assert_eq!(table.get(52), Some(&52));
        assert_eq!(table.get(53), None);
    }

    // ---- The TableTest methods that apply to this crate's narrowed `Table` ----

    /// `Table._get(long)`: the matrix cell, with none of `get`'s low/hr window checks.
    fn raw_get<T: Clone>(t: &Table<T>, seqno: i64) -> Option<T> {
        let row = t.compute_row(seqno);
        if row < 0 || row as usize >= t.matrix.len() {
            return None;
        }
        let idx = t.compute_index(seqno);
        if idx < 0 {
            return None;
        }
        t.matrix[row as usize].as_ref()?[idx as usize].clone()
    }

    /// `TableTest.assertIndices`.
    fn assert_indices<T: Clone>(t: &Table<T>, low: i64, hd: i64, hr: i64) {
        assert_eq!(t.low(), low, "low");
        assert_eq!(t.highest_delivered(), hd, "hd");
        assert_eq!(t.highest_received(), hr, "hr");
    }

    /// `TableTest.assertCapacity`.
    fn assert_capacity<T: Clone>(t: &Table<T>, elements_per_row: usize) {
        assert_eq!(t.capacity(), t.num_rows() * elements_per_row.next_power_of_two());
    }

    /// `TableTest.addAndGet` for one seqno.
    fn add_and_get(t: &mut Table<i64>, seqno: i64) {
        assert!(t.add(seqno, seqno), "seqno {seqno} not added");
        assert_eq!(t.get(seqno), Some(&seqno));
    }

    /// `TableTest.msg(num, dont_loopback)`: a message reduced to what the DONT_LOOPBACK filter reads.
    #[derive(Clone, Debug, PartialEq)]
    struct Msg {
        num: i64,
        dont_loopback: bool,
    }

    fn msg(num: i64, dont_loopback: bool) -> Msg {
        Msg { num, dont_loopback }
    }

    fn dont_loopback_filter(m: &Msg) -> bool {
        m.dont_loopback
    }

    fn table<T: Clone>(rows: usize, per_row: usize, offset: i64) -> Table<T> {
        Table::with_dimensions(rows, per_row, offset, DEFAULT_RESIZE_FACTOR)
    }

    // TableTest#testCreation
    #[test]
    fn creation_starts_empty_with_all_watermarks_at_the_offset() {
        let t: Table<i64> = table(3, 10, 0);
        assert_eq!(t.size(), 0);
        assert!(t.get(15).is_none());
        assert_indices(&t, 0, 0, 0);
    }

    // TableTest#testAdd
    #[test]
    fn add_counts_two_elements() {
        let mut t: Table<i64> = table(3, 10, 0);
        t.add(1, 322649);
        t.add(2, 100000);
        assert_eq!(t.size(), 2);
    }

    // TableTest#testAddAndRemove
    #[test]
    fn add_and_remove_with_dont_loopback_messages_advances_hd_over_them() {
        let mut t: Table<Msg> = table(3, 10, 0);
        t.add(1, msg(1, false));
        t.add(2, msg(2, false));
        assert_eq!(t.highest_deliverable(), 2);
        t.remove_many(true, 10, None);
        assert_eq!(t.highest_delivered(), 2);
        t.add(3, msg(3, false));
        assert_eq!(t.highest_received(), 3);

        t.add_with_filter(4, msg(4, true), &dont_loopback_filter);
        assert_eq!(t.highest_delivered(), 2);
        assert_eq!(t.highest_deliverable(), 4);
        t.remove_many(false, 10, None);
        assert_eq!(t.highest_delivered(), 4);

        t.add_with_filter(5, msg(5, true), &dont_loopback_filter);
        t.add_with_filter(6, msg(6, true), &dont_loopback_filter);
        assert_eq!(t.highest_delivered(), 6);
    }

    // TableTest#testAddAndRemove2
    #[test]
    fn add_and_remove_2_purge_after_filtered_adds_keeps_the_watermarks() {
        let mut t: Table<Msg> = table(3, 10, 0);
        for i in 1..=10 {
            t.add_with_filter(i, msg(i, true), &dont_loopback_filter);
        }
        assert_eq!(t.highest_delivered(), 10);
        assert_eq!(t.highest_received(), 10);
        assert_eq!(t.highest_deliverable(), 10);
        t.purge(10, false);
        assert_eq!(t.highest_delivered(), 10);
        assert_eq!(t.highest_received(), 10);
        assert_eq!(t.highest_deliverable(), 10);
        assert_eq!(t.low(), 10);
    }

    // TableTest#testAddAndRemove3
    #[test]
    fn add_and_remove_3_out_of_order_filtered_adds_all_deliver() {
        let mut t: Table<Msg> = table(3, 10, 3);
        t.add_with_filter(5, msg(5, true), &dont_loopback_filter);
        t.add_with_filter(6, msg(6, true), &dont_loopback_filter);
        t.add_with_filter(4, msg(4, true), &dont_loopback_filter);
        assert_eq!(t.highest_received(), 6);
        assert_eq!(t.highest_deliverable(), 6);
        assert_eq!(t.highest_delivered(), 6);
    }

    // TableTest#testAddAndRemove4
    #[test]
    fn add_and_remove_4_a_gap_stops_the_filtered_delivery() {
        let mut t: Table<Msg> = table(3, 10, 3);
        t.add_with_filter(7, msg(7, true), &dont_loopback_filter);
        t.add_with_filter(6, msg(6, true), &dont_loopback_filter);
        t.add_with_filter(4, msg(4, true), &dont_loopback_filter);
        assert_eq!(t.highest_received(), 7);
        assert_eq!(t.highest_deliverable(), 4);
        assert_eq!(t.highest_delivered(), 4);
    }

    // TableTest#testIndexWithRemoveMany
    #[test]
    fn index_with_remove_many_nulls_the_removed_cells() {
        let mut t: Table<i64> = table(3, 10, 5);
        assert_eq!(t.highest_delivered(), 5);
        assert_eq!(t.highest_received(), 5);
        t.add(6, 6);
        t.add(7, 7);
        let low = t.low();
        t.remove_many(true, 0, None);
        for i in low..=7 {
            assert!(raw_get(&t, i).is_none(), "message with seqno={i} is not null");
        }
        assert_indices(&t, 7, 7, 7);
    }

    // TableTest#testComputeSize
    #[test]
    fn compute_size_recounts_after_partial_removal() {
        let mut t: Table<i64> = table(3, 10, 0);
        for n in 1..=10 {
            t.add(n, n);
        }
        assert_eq!(t.compute_size(), 10);
        t.remove_many(false, 3, None);
        assert_eq!(t.compute_size(), 7);
        t.remove_many(true, 4, None);
        assert_eq!(t.compute_size(), t.size());
        assert_eq!(t.compute_size(), 3);
    }

    // TableTest#testRemove2
    #[test]
    fn remove_returns_the_next_element_whatever_the_nullify_flag() {
        let mut t: Table<i64> = table(3, 10, 0);
        for i in 1..=5 {
            t.add(i, i);
        }
        assert_indices(&t, 0, 0, 5);
        assert_eq!(t.remove(true), Some(1));
        assert_eq!(t.remove(false), Some(2));
        assert_eq!(t.remove(true), Some(3));
    }

    // TableTest#testRemoveMany2 (upstream's null result is an empty Vec here)
    #[test]
    fn remove_many_2_stops_at_a_gap_and_resumes_once_it_is_filled() {
        let mut t: Table<i64> = table(3, 10, 0);
        for i in [1, 2, 3, 4, 5, 6, 7, 9, 10] {
            t.add(i, i);
        }
        assert_eq!(t.remove_many(false, 3, None).len(), 3);
        assert_eq!(t.remove_many(false, 0, None).len(), 4);
        assert!(t.remove_many(false, 10, None).is_empty());
        t.add(8, 8);
        assert_eq!(t.remove_many(false, 0, None).len(), 3);
    }

    // TableTest#testRemoveManyWithNulling
    #[test]
    fn remove_many_with_nulling_clears_every_removed_cell() {
        let mut t: Table<i64> = table(3, 10, 0);
        for i in [1, 2, 3, 4, 5, 6, 7, 9, 10] {
            t.add(i, i);
        }
        let list = t.remove_many(true, 3, None);
        assert_eq!(list.len(), 3);
        for i in &list {
            assert!(raw_get(&t, *i).is_none());
        }
        let list = t.remove_many(true, 0, None);
        assert_eq!(list.len(), 4);
        for i in &list {
            assert!(raw_get(&t, *i).is_none());
        }
        assert!(t.remove_many(false, 10, None).is_empty());
        t.add(8, 8);
        let list = t.remove_many(true, 0, None);
        assert_eq!(list.len(), 3);
        for i in &list {
            assert!(raw_get(&t, *i).is_none());
        }
    }

    // TableTest#testRemoveMany3 (the collector overload, `remove_many_into`)
    #[test]
    fn remove_many_3_drains_everything_and_leaves_the_table_empty() {
        let mut t: Table<i64> = table(3, 10, 0);
        for i in 1..=10 {
            t.add(i, i);
        }
        let result = t.remove_many_into(true, 0, None, Vec::new, |v, e| v.push(e));
        assert_eq!(result.map(|r| r.len()), Some(10));
        assert!(t.is_empty());
    }

    // TableTest#testGetMissing4
    #[test]
    fn get_missing_4_lists_every_gap_up_to_hr() {
        let mut t: Table<i64> = table(3, 30, 0);
        for i in [2, 5, 10, 11, 12, 13, 15, 20, 28, 30] {
            t.add(i, i);
        }
        assert_eq!(t.num_missing(), 20);
        assert_eq!(t.missing(0).unwrap().size() as i64, 20);
    }

    // TableTest#testGetMissing5
    #[test]
    fn get_missing_5_size_matches_num_missing_for_each_shape() {
        let mut t: Table<i64> = table(3, 10, 0);
        t.add(1, 1);
        assert!(t.missing(0).is_none());
        assert_eq!(t.num_missing(), 0);

        let mut t: Table<i64> = table(3, 10, 0);
        t.add(10, 10);
        assert_eq!(t.num_missing(), t.missing(0).unwrap().size() as i64);

        let mut t: Table<i64> = table(3, 10, 0);
        t.add(5, 5);
        assert_eq!(t.num_missing(), t.missing(0).unwrap().size() as i64);

        let mut t: Table<i64> = table(3, 10, 0);
        t.add(5, 5);
        t.add(7, 7);
        let missing = t.missing(0).unwrap();
        assert_eq!(missing.size(), 5);
        assert_eq!(t.num_missing(), missing.size() as i64);
    }

    // TableTest#testGetMissingWithMaxSize
    #[test]
    fn get_missing_with_max_size_caps_the_list() {
        let mut t: Table<i64> = table(3, 10, 0);
        for i in (2..=50).step_by(2) {
            t.add(i, i);
        }
        assert_eq!(t.num_missing(), 25);
        assert_eq!(t.missing(0).unwrap().size(), 25);
        assert_eq!(t.missing(10).unwrap().size(), 5);
        assert_eq!(t.missing(200).unwrap().size(), 25);
    }

    // TableTest#testGetMissingWithMaxBundleSize
    #[test]
    fn get_missing_with_max_bundle_size_fits_one_bundle() {
        let max_bundle_size = 64000usize;
        let missing_msgs = 1_000_000i64;
        let max_xmit_req_size = (max_bundle_size - 50) * 8;
        let mut t: Table<i64> = table(10, 1000, 0);
        t.add(0, 0);
        t.add(missing_msgs, missing_msgs);
        let missing = t.missing(max_xmit_req_size).unwrap();
        assert!(
            missing.serialized_size() <= max_bundle_size,
            "serialized size of {} needs to be less than max_bundle_size of {max_bundle_size} bytes",
            missing.serialized_size()
        );
    }

    // TableTest#testGetMissingWithMaxBundleSize2
    #[test]
    fn get_missing_with_max_bundle_size_2_starts_after_the_deliverable_prefix() {
        let max_bundle_size = 64000usize;
        let missing_msgs = 2_000_000i64;
        let max_xmit_req_size = (max_bundle_size - 50) * 8;
        let mut t: Table<i64> = table(10, 1000, 0);
        for i in 0..missing_msgs / 2 {
            t.add(i, i);
        }
        t.add(missing_msgs, missing_msgs);
        let missing = t.missing(max_xmit_req_size).unwrap();
        assert!(missing.serialized_size() <= max_bundle_size);
        let limit = missing_msgs / 2 + max_xmit_req_size as i64;
        for l in missing.iter() {
            assert!(l >= missing_msgs / 2 && l < limit, "{l} outside [{}, {limit})", missing_msgs / 2);
        }
    }

    // TableTest#testResizeWithPurgeAndGetOfNonExistingElement
    #[test]
    fn resize_with_purge_and_get_of_non_existing_element() {
        let mut t: Table<i64> = table(3, 10, 0);
        for i in 1..=50 {
            add_and_get(&mut t, i);
        }
        assert_indices(&t, 0, 0, 50);
        assert!(t.size() == 50 && t.num_missing() == 0);

        for i in 1..=15 {
            assert_eq!(t.remove(false), Some(i));
        }
        assert_indices(&t, 0, 15, 50);
        assert!(t.size() == 35 && t.num_missing() == 0);

        t.purge(15, false);
        add_and_get(&mut t, 55); // triggers a resize()
        assert_indices(&t, 15, 15, 55);
        assert!(t.size() == 36 && t.num_missing() == 4);

        let mut expected: Vec<i64> = (16..50).collect();
        expected.push(55);
        let base = t.offset();
        for i in base..t.capacity() as i64 + base {
            if let Some(n) = raw_get(&t, i) {
                expected.retain(|x| *x != n);
            }
        }
        assert!(expected.is_empty(), "elements lost by the resize: {expected:?}");
    }

    // TableTest#testMove
    #[test]
    fn move_after_removing_everything_keeps_the_capacity_consistent() {
        let mut t: Table<i64> = table(3, 10, 0);
        for i in 1..50 {
            add_and_get(&mut t, i);
        }
        t.remove_many(true, 49, None);
        assert!(t.is_empty());
        add_and_get(&mut t, 50);
        assert_eq!(t.size(), 1);
        assert_capacity(&t, 10);
    }

    // TableTest#testMove2
    #[test]
    fn move_2_resize_after_removal_keeps_unremoved_elements() {
        let mut t: Table<i64> = table(3, 10, 0);
        for i in 1..30 {
            t.add(i, i);
        }
        t.remove_many(true, 23, None);
        t.add(35, 35); // triggers a resize() and a move()
        for i in 1..=23 {
            assert!(raw_get(&t, i).is_none(), "{i}");
        }
        for i in 24..30 {
            assert!(raw_get(&t, i).is_some(), "{i}");
        }
    }

    // TableTest#testMove3
    #[test]
    fn move_3_resize_by_adding_the_next_seqno_keeps_unremoved_elements() {
        let mut t: Table<i64> = table(3, 10, 0);
        for i in 1..30 {
            t.add(i, i);
        }
        t.remove_many(true, 23, None);
        t.add(30, 30);
        for i in 1..=23 {
            assert!(raw_get(&t, i).is_none(), "{i}");
        }
        for i in 24..30 {
            assert!(raw_get(&t, i).is_some(), "{i}");
        }
    }

    // TableTest#testPurge2
    #[test]
    fn purge_2_clears_everything_up_to_the_purge_seqno() {
        let mut t: Table<i64> = table(3, 10, 0);
        for i in 1..=7 {
            t.add(i, i);
            t.remove(false);
        }
        assert!(t.is_empty());
        let low = t.low();
        t.purge(3, false);
        assert_eq!(t.low(), 3);
        for i in low..=3 {
            assert!(raw_get(&t, i).is_none(), "message with seqno={i} is not null");
        }
        t.purge(6, false);
        assert!(raw_get(&t, 6).is_none());
        t.purge(7, false);
        assert!(raw_get(&t, 7).is_none());
        assert_eq!(t.low(), 7);
        assert!(t.is_empty());

        for i in 7..=14 {
            t.add(i, i);
            t.remove(false);
        }
        assert!(t.is_empty());
        let low = t.low();
        assert_eq!(low, 7);
        t.purge(12, false);
        assert_eq!(t.low(), 12);
        for i in low..=12 {
            assert!(raw_get(&t, i).is_none(), "message with seqno={i} is not null");
        }
    }

    // TableTest#testPurge3
    #[test]
    fn purge_3_remove_many_with_nulling_keeps_the_rest_readable() {
        let mut t: Table<i64> = table(3, 10, 0);
        for i in 1..=100 {
            t.add(i, i);
        }
        t.remove_many(true, 53, None);
        for i in 54..=100 {
            assert_eq!(t.get(i), Some(&i));
        }
    }

    // TableTest#testPurge4
    #[test]
    fn purge_4_remove_many_then_purge_keeps_the_rest_readable() {
        let mut t: Table<i64> = table(3, 10, 0);
        for i in 1..=100 {
            t.add(i, i);
        }
        t.remove_many(false, 53, None);
        t.purge(53, false);
        for i in 54..=100 {
            assert_eq!(t.get(i), Some(&i));
        }
    }

    // TableTest#testPurge5
    #[test]
    fn purge_5_successive_purges_clear_exactly_their_prefix() {
        let mut t: Table<i64> = table(3, 10, 0);
        for i in 1..=100 {
            t.add(i, i);
        }
        t.remove_many(false, 0, None);

        t.purge(10, false);
        for i in 1..=10 {
            assert!(raw_get(&t, i).is_none());
        }
        for i in 11..=100 {
            assert_eq!(t.get(i), Some(&i));
        }
        t.purge(10, false);
        for i in 11..=100 {
            assert_eq!(t.get(i), Some(&i));
        }
        t.purge(50, false);
        for i in 1..=50 {
            assert!(raw_get(&t, i).is_none());
        }
        for i in 51..=100 {
            assert_eq!(t.get(i), Some(&i));
        }
        t.purge(100, false);
        for i in 51..=100 {
            assert!(raw_get(&t, i).is_none());
        }
    }

    // TableTest#testPurgeForce
    #[test]
    fn purge_force_moves_all_watermarks_and_recounts_the_size() {
        let mut t: Table<i64> = table(3, 10, 0);
        for i in 1..=30 {
            t.add(i, i);
        }
        t.purge(15, true);
        assert_indices(&t, 15, 15, 30);
        for i in 1..=15 {
            assert!(raw_get(&t, i).is_none());
        }
        for i in 16..=30 {
            assert!(raw_get(&t, i).is_some());
        }
        assert!(t.get(5).is_none() && t.get(25).is_some());

        t.purge(30, true);
        assert_indices(&t, 30, 30, 30);
        assert!(t.is_empty());
        for i in 1..=30 {
            assert!(raw_get(&t, i).is_none());
        }

        for i in 31..=40 {
            t.add(i, i);
        }
        assert_eq!(t.size(), 10);
        assert_indices(&t, 30, 30, 40);

        t.purge(50, true);
        assert!(t.is_empty());
        assert_indices(&t, 40, 40, 40);
    }

    // TableTest#testCompact
    #[test]
    fn compact_after_a_purge_keeps_the_watermarks_and_the_size() {
        let mut t: Table<i64> = table(3, 10, 0);
        for i in 1..=80 {
            add_and_get(&mut t, i);
        }
        assert_eq!(t.size(), 80);
        assert_indices(&t, 0, 0, 80);
        let list = t.remove_many(false, 60, None);
        assert_eq!(list.len(), 60);
        assert!(list[0] == 1 && list[list.len() - 1] == 60);
        assert_indices(&t, 0, 60, 80);
        t.purge(60, false);
        assert_indices(&t, 60, 60, 80);
        assert_eq!(t.size(), 20);
        t.compact();
        assert_indices(&t, 60, 60, 80);
        assert_eq!(t.size(), 20);
        assert_capacity(&t, 10);
    }

    // TableTest#testCompact2 (upstream's remove() is remove(true), which also moves low)
    #[test]
    fn compact_2_after_removing_with_nulling_keeps_the_size() {
        let mut t: Table<i64> = table(3, 10, 0);
        for i in 1..=80 {
            add_and_get(&mut t, i);
        }
        assert_eq!(t.size(), 80);
        for _ in 1..=60 {
            t.remove(true);
        }
        assert_eq!(t.size(), 20);
        t.compact();
        assert_eq!(t.size(), 20);
        assert_capacity(&t, 10);
    }

    // TableTest#testSeqnoOverflow: seqnos that cross Long.MAX_VALUE, and ones that cross zero
    fn seqno_overflow(start: i64, delta: i64) {
        let mut seqno = start;
        let mut win: Table<Msg> = table(3, 10, seqno);
        for _ in 1..=delta {
            seqno = seqno.wrapping_add(1);
            win.add(seqno, msg(0, false));
        }
        assert_eq!(win.size(), delta as usize);
        assert_indices(&win, start, start, seqno);
        let msgs = win.remove_many(true, 200, None);
        assert_eq!(msgs.len(), delta as usize);
        assert!(win.is_empty());
        assert_indices(&win, seqno, seqno, seqno);
    }

    #[test]
    fn seqno_overflow_near_i64_max() {
        seqno_overflow(i64::MAX - 10, 20);
    }

    #[test]
    fn seqno_overflow_across_zero() {
        seqno_overflow(-10, 20);
    }

    // ---- The rest of TableTest, method by method ----
    //
    // With this block every one of TableTest's 85 test methods has a `// TableTest#<name>` port in
    // this module. Adaptations, none of which changes what a method checks:
    // - `List<LongTuple<T>>` is `Vec<(i64, T)>` (`Table::add_list`); `MessageBatch` is
    //   `wire::batch::MessageBatch` (`Table::add_batch`) with NAKACK2 message headers, as upstream's
    //   `createMessageBatch`.
    // - Upstream's `null` from `removeMany(..)` returning a `List` is an empty `Vec`; where a method
    //   asserts that `null` itself, the port calls `remove_many_into`, whose `None` is that `null`.
    // - `testConcurrentAdd`/`testConcurrentAddAndRemove`: `Table` has no internal lock (module doc
    //   comment), so the threads share it behind a `Mutex`, as NAKACK2/UNICAST3 do. Upstream's random
    //   10-500 ms adder sleeps and the remover's 2 s head start are dropped: the asserted end state does
    //   not depend on the interleaving (the remover always finds 1..5 present).
    // - `testIteration` and `testStream` both read `Table::iter` (a Rust iterator is the stream).

    use std::cell::Cell;
    use std::sync::{Arc, Barrier, Mutex};

    use crate::wire::header::{HeaderValue, NakAckHeader2};
    use crate::wire::ids::PROT_ID_NAKACK2;

    /// `TableTest.createList`.
    fn create_list(seqnos: &[i64]) -> Vec<(i64, i64)> {
        seqnos.iter().map(|s| (*s, *s)).collect()
    }

    /// `TableTest.createMessageBatch`: empty messages carrying a NAKACK2 message header.
    fn create_message_batch(seqnos: &[i64]) -> MessageBatch {
        let mut mb = MessageBatch::with_capacity(seqnos.len());
        for seqno in seqnos {
            let mut m = Message::empty(None);
            m.put_header(PROT_ID_NAKACK2, HeaderValue::NakAck(NakAckHeader2::create_message_header(*seqno)));
            mb.add(m);
        }
        mb
    }

    /// `TableTest.SEQNO_GETTER`.
    fn seqno_getter(m: &Message) -> i64 {
        match m.get_header(PROT_ID_NAKACK2) {
            Some(HeaderValue::NakAck(hdr)) => hdr.seqno,
            _ => -1,
        }
    }

    fn add_and_get_all(t: &mut Table<i64>, seqnos: &[i64]) {
        for seqno in seqnos {
            add_and_get(t, *seqno);
        }
    }

    // TableTest#testAddition
    #[test]
    fn addition() {
        let mut t: Table<i64> = table(3, 10, 0);
        assert!(!t.add(0, 0));
        add_and_get_all(&mut t, &[1, 5, 9, 10, 11, 19, 20, 29]);
        assert_eq!(t.size(), 8);
        assert_eq!(t.compute_size(), 8);
        assert_eq!(t.capacity(), 3 * 16);
        assert_indices(&t, 0, 0, 29);
    }

    // TableTest#testAdditionList
    #[test]
    fn addition_list() {
        let mut t: Table<i64> = table(3, 10, 0);
        assert!(!t.add_list(&mut create_list(&[0]), false, None));
        let seqnos = [1, 5, 9, 10, 11, 19, 20, 29];
        assert!(t.add_list(&mut create_list(&seqnos), false, None));
        for seqno in seqnos {
            assert_eq!(t.get(seqno), Some(&seqno));
        }
        assert_eq!(t.size(), 8);
        assert_eq!(t.compute_size(), 8);
        assert_eq!(t.capacity(), 3 * 16);
        assert_indices(&t, 0, 0, 29);
    }

    // TableTest#testAdditionMessageBatch
    #[test]
    fn addition_message_batch() {
        let mut t: Table<Message> = table(3, 10, 0);
        assert!(!t.add_batch(&mut create_message_batch(&[0]), &seqno_getter, false, None));
        let seqnos = [1, 5, 9, 10, 11, 19, 20, 29];
        assert!(t.add_batch(&mut create_message_batch(&seqnos), &seqno_getter, false, None));
        for seqno in seqnos {
            assert_eq!(t.get(seqno).map(seqno_getter), Some(seqno));
        }
        assert_eq!(t.size(), 8);
        assert_eq!(t.compute_size(), 8);
        assert_eq!(t.capacity(), 3 * 16);
        assert_indices(&t, 0, 0, 29);
    }

    // TableTest#testAdditionWithOffset
    #[test]
    fn addition_with_offset() {
        let mut t: Table<i64> = table(3, 10, 100);
        add_and_get_all(&mut t, &[101, 105, 109, 110, 111, 119, 120, 129]);
        assert_eq!(t.size(), 8);
        assert_eq!(t.capacity(), 3 * 16);
        assert_indices(&t, 100, 100, 129);
    }

    // TableTest#testAdditionListWithOffset
    #[test]
    fn addition_list_with_offset() {
        let mut t: Table<i64> = table(3, 10, 100);
        let seqnos = [101, 105, 109, 110, 111, 119, 120, 129];
        assert!(t.add_list(&mut create_list(&seqnos), false, None));
        assert_eq!(t.size(), 8);
        for seqno in seqnos {
            assert_eq!(t.get(seqno), Some(&seqno));
        }
        assert_eq!(t.capacity(), 3 * 16);
        assert_indices(&t, 100, 100, 129);
    }

    // TableTest#testAdditionMessageBatchWithOffset
    #[test]
    fn addition_message_batch_with_offset() {
        let mut t: Table<Message> = table(3, 10, 100);
        let seqnos = [101, 105, 109, 110, 111, 119, 120, 129];
        assert!(t.add_batch(&mut create_message_batch(&seqnos), &seqno_getter, false, None));
        assert_eq!(t.size(), 8);
        for seqno in seqnos {
            assert_eq!(t.get(seqno).map(seqno_getter), Some(seqno));
        }
        assert_eq!(t.capacity(), 3 * 16);
        assert_indices(&t, 100, 100, 129);
    }

    // TableTest#testAddList
    #[test]
    fn add_list() {
        let mut t: Table<i64> = table(3, 10, 0);
        assert!(t.add_list(&mut create_list(&[1, 2]), false, None));
        assert_eq!(t.size(), 2);
    }

    // TableTest#testAddMessageBatch
    #[test]
    fn add_message_batch() {
        let mut t: Table<Message> = table(3, 10, 0);
        assert!(t.add_batch(&mut create_message_batch(&[1, 2]), &seqno_getter, false, None));
        assert_eq!(t.size(), 2);
    }

    // TableTest#testAddListWithConstValue
    #[test]
    fn add_list_with_const_value() {
        let mut t: Table<i64> = table(3, 10, 0);
        const DUMMY: i64 = 0;
        assert!(t.add_list(&mut create_list(&[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]), false, Some(DUMMY)));
        assert_eq!(t.size(), 10);
        let list = t.remove_many(true, 0, Some(&|e: &i64| *e == DUMMY));
        assert_eq!(list, vec![DUMMY; 10]);
    }

    // TableTest#testAddMessageBatchWithConstValue
    #[test]
    fn add_message_batch_with_const_value() {
        let mut t: Table<Message> = table(3, 10, 0);
        let dummy = Message::empty(None);
        let mut mb = create_message_batch(&[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
        assert!(t.add_batch(&mut mb, &seqno_getter, false, Some(dummy.clone())));
        assert_eq!(t.size(), 10);
        let list = t.remove_many(true, 0, Some(&|m: &Message| *m == dummy));
        assert_eq!(list.len(), 10);
        assert!(list.iter().all(|m| *m == dummy));
    }

    // TableTest#testAddListWithRemoval
    #[test]
    fn add_list_with_removal() {
        let mut t: Table<i64> = table(3, 10, 0);
        let mut msgs = create_list(&[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
        assert!(t.add_list(&mut msgs, false, None));
        assert_eq!(msgs.len(), 10);

        let mut msgs = create_list(&[1, 3, 5, 7]);
        assert!(!t.add_list(&mut msgs, true, None));
        assert!(msgs.is_empty());

        let mut msgs = create_list(&[1, 3, 5, 7, 9, 10, 11, 12, 13, 14, 15]);
        assert!(t.add_list(&mut msgs, true, None));
        assert_eq!(msgs.len(), 5);
    }

    // TableTest#testAddMessageBatchWithRemoval
    #[test]
    fn add_message_batch_with_removal() {
        let mut t: Table<Message> = table(3, 10, 0);
        let mut mb = create_message_batch(&[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
        assert!(t.add_batch(&mut mb, &seqno_getter, false, None));
        assert_eq!(mb.size(), 10);

        let mut mb = create_message_batch(&[1, 3, 5, 7]);
        assert!(!t.add_batch(&mut mb, &seqno_getter, true, None));
        assert!(mb.is_empty());

        let mut mb = create_message_batch(&[1, 3, 5, 7, 9, 10, 11, 12, 13, 14, 15]);
        assert!(t.add_batch(&mut mb, &seqno_getter, true, None));
        assert!(mb.is_empty());
    }

    // TableTest#testAddListWithResizing
    #[test]
    fn add_list_with_resizing() {
        let mut t: Table<i64> = table(3, 5, 0);
        let mut msgs: Vec<(i64, i64)> = (1..100).map(|i| (i, i)).collect();
        t.add_list(&mut msgs, false, None);
        assert_eq!(t.num_resizes(), 1);
    }

    // TableTest#testAddMessageBatchWithResizing
    #[test]
    fn add_message_batch_with_resizing() {
        let mut t: Table<Message> = table(3, 5, 0);
        let seqnos: Vec<i64> = (1..=99).collect();
        t.add_batch(&mut create_message_batch(&seqnos), &seqno_getter, false, None);
        assert_eq!(t.num_resizes(), 1);
    }

    // TableTest#testAddListWithResizingNegativeSeqnos
    #[test]
    fn add_list_with_resizing_negative_seqnos() {
        let seqno = i64::MAX - 50;
        let mut t: Table<i64> = table(3, 5, seqno);
        let mut msgs: Vec<(i64, i64)> = (1..100i64).map(|i| (i.wrapping_add(seqno), i)).collect();
        t.add_list(&mut msgs, false, None);
        assert_eq!(t.num_resizes(), 1);
    }

    // TableTest#testAddListWithResizing2
    #[test]
    fn add_list_with_resizing_2() {
        let mut t: Table<i64> = table(3, 500, 0);
        let mut msgs: Vec<(i64, i64)> = (1..100).map(|i| (i, i)).collect();
        t.add_list(&mut msgs, false, None);
        assert_eq!(t.num_resizes(), 0);
    }

    // TableTest#testAdditionWithOffset2
    #[test]
    fn addition_with_offset_2() {
        let mut t: Table<i64> = table(3, 10, 2);
        add_and_get_all(&mut t, &[1000, 1001]);
        t.compact();
        add_and_get_all(&mut t, &[1005, 1009, 1010, 1011, 1019, 1020, 1029]);
        assert_eq!(t.size(), 9);
        assert_indices(&t, 2, 2, 1029);
    }

    // TableTest#testDuplicateAddition
    #[test]
    fn duplicate_addition() {
        let mut t: Table<i64> = table(3, 10, 0);
        add_and_get_all(&mut t, &[1, 5, 9, 10]);
        assert!(!t.add(5, 5));
        assert_eq!(t.get(5), Some(&5));
        assert_eq!(t.size(), 4);
        assert_indices(&t, 0, 0, 10);
    }

    // TableTest#testConcurrentAdd
    #[test]
    fn concurrent_add() {
        const NUM: i64 = 100;
        let buf: Arc<Mutex<Table<i64>>> = Arc::new(Mutex::new(table(3, 10, 0)));
        let latch = Arc::new(Barrier::new(NUM as usize + 1));
        let adders: Vec<_> = (1..=NUM)
            .map(|seqno| {
                let (buf, latch) = (buf.clone(), latch.clone());
                std::thread::spawn(move || {
                    latch.wait();
                    buf.lock().unwrap().add(seqno, seqno);
                })
            })
            .collect();
        latch.wait();
        for adder in adders {
            adder.join().unwrap();
        }
        assert_eq!(buf.lock().unwrap().size(), NUM as usize);
    }

    // TableTest#testConcurrentAddAndRemove
    #[test]
    fn concurrent_add_and_remove() {
        const NUM: i64 = 5;
        let buf: Arc<Mutex<Table<i64>>> = Arc::new(Mutex::new(table(3, 10, 0)));
        for i in 1..=10 {
            buf.lock().unwrap().add(i, i);
        }
        let latch = Arc::new(Barrier::new(NUM as usize + 1));
        let adders: Vec<_> = (0..NUM)
            .map(|i| {
                let (buf, latch) = (buf.clone(), latch.clone());
                std::thread::spawn(move || {
                    latch.wait();
                    buf.lock().unwrap().add(i + 11, i + 11);
                })
            })
            .collect();
        latch.wait();
        let remover = {
            let buf = buf.clone();
            std::thread::spawn(move || buf.lock().unwrap().remove_many(true, 5, None))
        };
        for adder in adders {
            adder.join().unwrap();
        }
        assert_eq!(remover.join().unwrap(), vec![1, 2, 3, 4, 5]);

        let mut buf = buf.lock().unwrap();
        assert_eq!(buf.size(), 10);
        assert_indices(&buf, 5, 5, 15);
        let list = buf.remove_many(true, 0, None);
        assert_eq!(list.len(), 10);
        for i in 6..=15 {
            assert!(list.contains(&i));
        }
        assert_indices(&buf, 15, 15, 15);
    }

    // TableTest#testIndex
    #[test]
    fn index() {
        let mut t: Table<i64> = table(3, 10, 5);
        assert_eq!(t.highest_delivered(), 5);
        assert_eq!(t.highest_received(), 5);
        t.add(6, 6);
        t.add(7, 7);
        t.remove(false);
        t.remove(false);
        let low = t.low();
        assert_eq!(low, 5);
        for seqno in 4..=7 {
            t.purge(seqno, false);
        }
        for i in low..=7 {
            assert_eq!(raw_get(&t, i), None, "message with seqno={i} is not null");
        }
    }

    // TableTest#testComputeSize2
    #[test]
    fn compute_size_2() {
        let mut t: Table<i64> = table(3, 10, 0);
        t.add(1, 1);
        assert_eq!(t.compute_size(), t.size());
        assert_eq!(t.compute_size(), 1);
        t.remove(false);
        assert_eq!(t.compute_size(), t.size());
        assert_eq!(t.compute_size(), 0);
    }

    // TableTest#testRemove
    #[test]
    fn remove_with_a_gap() {
        let mut t: Table<i64> = table(3, 10, 0);
        for i in 1..=9 {
            t.add(i, i);
        }
        t.add(20, 20);
        assert_eq!(t.size(), 10);
        assert_indices(&t, 0, 0, 20);
        assert_eq!(t.num_missing(), 10);
        for _ in 1..=10 {
            t.remove(true); // 10 is missing
        }
        assert_eq!(t.size(), 1);
        assert_indices(&t, 9, 9, 20);
        assert_eq!(t.num_missing(), 10);
    }

    // TableTest#testRemoveMany
    #[test]
    fn remove_many_up_to_max_results() {
        let mut t: Table<i64> = table(3, 10, 0);
        for seqno in [1, 2, 3, 4, 5, 7, 8, 9, 10] {
            t.add(seqno, seqno);
        }
        assert_indices(&t, 0, 0, 10);
        let list = t.remove_many(true, 4, None);
        assert!(t.size() == 5 && t.num_missing() == 1);
        assert_eq!(list, vec![1, 2, 3, 4]);
        assert_indices(&t, 4, 4, 10);
    }

    // TableTest#testRemoveManyWithWrapping
    #[test]
    fn remove_many_with_wrapping() {
        let mut t: Table<i64> = table(3, 10, 0);
        for seqno in [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 15, 16, 17, 18, 19, 20] {
            t.add(seqno, seqno);
        }
        assert_indices(&t, 0, 0, 20);
        assert!(t.size() == 18 && t.num_missing() == 2);
        assert_eq!(t.remove_many(true, 0, None).len(), 12);
        assert_indices(&t, 12, 12, 20);
        assert!(t.size() == 6 && t.num_missing() == 2);
        t.purge(12, false);
        assert_indices(&t, 12, 12, 20);
        assert!(t.size() == 6 && t.num_missing() == 2);
    }

    // TableTest#testRemoveManyWithWrapping2
    #[test]
    fn remove_many_with_wrapping_2() {
        let mut t: Table<i64> = table(3, 10, 0);
        for seqno in [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 15, 16, 17, 18, 19, 20] {
            t.add(seqno, seqno);
        }
        assert_indices(&t, 0, 0, 20);
        assert!(t.size() == 18 && t.num_missing() == 2);
        assert_eq!(t.remove_many(false, 0, None).len(), 12);
        assert_indices(&t, 0, 12, 20);
        assert!(t.size() == 6 && t.num_missing() == 2);
        t.purge(12, false);
        assert_indices(&t, 12, 12, 20);
        assert!(t.size() == 6 && t.num_missing() == 2);
    }

    fn one_to_ten() -> Table<i64> {
        let mut t = table(3, 10, 0);
        for i in 1..=10 {
            t.add(i, i);
        }
        t
    }

    // TableTest#testRemoveManyWithFilter
    #[test]
    fn remove_many_with_filter() {
        let mut t = one_to_ten();
        let list = t.remove_many(true, 0, Some(&|e: &i64| e % 2 == 0));
        assert_eq!(list, vec![2, 4, 6, 8, 10]);
        assert!(t.is_empty());
    }

    // TableTest#testRemoveManyWithFilterAcceptAll
    #[test]
    fn remove_many_with_filter_accept_all() {
        let mut t = one_to_ten();
        assert_eq!(t.remove_many(true, 0, Some(&|_: &i64| true)).len(), 10);
        assert!(t.is_empty());
    }

    // TableTest#testRemoveManyWithFilterAcceptNone (upstream's `list == null`: `None`)
    #[test]
    fn remove_many_with_filter_accept_none() {
        let mut t = one_to_ten();
        let list = t.remove_many_into(true, 0, Some(&|_: &i64| false), Vec::new, |v, e| v.push(e));
        assert_eq!(list, None);
        assert!(t.is_empty());
    }

    // TableTest#testRemoveManyWithFilterAcceptNone2: `max_results` counts accepted elements only, so
    // a filter that accepts 2 never reaches 3 and every element is drained.
    #[test]
    fn remove_many_with_filter_accept_none_2() {
        let mut t = one_to_ten();
        let cnt = Cell::new(0);
        let filter = |_: &i64| {
            cnt.set(cnt.get() + 1);
            cnt.get() <= 2
        };
        assert_eq!(t.remove_many(true, 3, Some(&filter)).len(), 2);
        assert!(t.is_empty());
    }

    // TableTest#testRemoveManyIntoMessageBatch
    #[test]
    fn remove_many_into_message_batch() {
        let mut t: Table<Message> = table(3, 10, 0);
        for i in 1..=10 {
            t.add(i, Message::bytes(None, b"hello".to_vec()));
        }
        let mut batch = MessageBatch::with_capacity(t.size());
        let batch_ptr: *const MessageBatch = &batch;
        let result = t.remove_many_into(true, 0, None, || &mut batch, |b, m| b.add(m));
        assert!(result.is_some_and(|r| std::ptr::eq(r, batch_ptr)));
        assert!(!batch.is_empty());
        assert!(t.is_empty());
        assert_eq!(batch.size(), 10);

        for seqno in 11..=15 {
            t.add(seqno, Message::bytes(None, b"test".to_vec()));
        }
        batch.clear();
        let result = t.remove_many_into(true, 0, None, || &mut batch, |b, m| b.add(m));
        assert!(result.is_some_and(|r| std::ptr::eq(r, batch_ptr)));
        assert!(!batch.is_empty());
        assert!(t.is_empty());
        assert_eq!(batch.size(), 5);

        assert!(t.remove_many_into(true, 0, None, || &mut batch, |b, m| b.add(m)).is_none());
    }

    // TableTest#testForEach
    #[test]
    fn for_each() {
        let mut t: Table<i64> = table(3, 10, 0);
        for i in 1..=20 {
            t.add(i, i);
        }
        let mut list = Vec::new();
        t.for_each(t.low() + 1, t.highest_received() - 1, |_, _, row, column| {
            list.push((row, column));
            true
        });
        let per_row = 10usize.next_power_of_two() as i64;
        for (count, (row, column)) in (1..).zip(list) {
            if count < per_row {
                assert_eq!((row, column), (0, count));
            } else {
                assert_eq!((row, column), (1, count - per_row));
            }
        }
    }

    // TableTest#testIteration
    #[test]
    fn iteration() {
        let mut t: Table<i64> = table(1, 10, 0);
        let list: Vec<i64> = (1..=20).collect();
        for i in &list {
            t.add(*i, *i);
        }
        let list2: Vec<i64> = t.iter().map(|e| *e.unwrap()).collect();
        assert_eq!(list2, list);
    }

    // TableTest#testStream
    #[test]
    fn stream() {
        let mut t: Table<i64> = table(1, 10, 0);
        let list: Vec<i64> = (1..=20).collect();
        for i in &list {
            t.add(*i, *i);
        }
        let list2: Vec<i64> = t.iter().flatten().copied().collect();
        assert_eq!(list2, list);
    }

    // TableTest#testGet
    #[test]
    fn get() {
        let mut t: Table<i64> = table(3, 10, 0);
        for i in 1..=5 {
            t.add(i, i);
        }
        assert_eq!(t.get(0), None);
        assert_eq!(t.get(1), Some(&1));
        assert_eq!(t.get(10), None);
        assert_eq!(t.get(5), Some(&5));
        assert_eq!(t.get(6), None);
    }

    // TableTest#testGetNullMessages
    #[test]
    fn get_null_messages() {
        let mut t: Table<i64> = table(3, 10, 0);
        t.add(1, 1);
        t.add(100, 100);
        assert_eq!(t.num_missing(), 98); // [2 .. 99]
        t.add(50, 50);
        assert_eq!(t.size(), 3);
        assert_eq!(t.num_missing(), 97);
    }

    // TableTest#testGetNullMessages2
    #[test]
    fn get_null_messages_2() {
        let mut t: Table<i64> = table(1, 10, 0);
        t.add(1, 1);
        t.add(5, 5);
        assert_eq!(t.num_missing(), 3); // [2 .. 4]
        t.add(10, 10);
        assert_eq!(t.size(), 3);
        assert_eq!(t.num_missing(), 7);
        t.add(14, 14);
        assert_eq!(t.size(), 4);
        assert_eq!(t.num_missing(), 10);
        while t.remove(true).is_some() {}
        assert_eq!(t.size(), 3);
        assert_eq!(t.num_missing(), 10);
    }

    fn assert_missing(t: &Table<i64>, expected: usize) {
        assert_eq!(t.missing(0).map(|m| m.size()), Some(expected));
        assert_eq!(t.num_missing(), expected as i64);
    }

    // TableTest#testGetMissing
    #[test]
    fn get_missing() {
        let mut t: Table<i64> = table(3, 10, 0);
        assert!(t.missing(0).is_none());
        for num in [2, 4, 6, 8] {
            t.add(num, num);
        }
        assert_missing(&t, 4);
    }

    // TableTest#testGetMissingWithOffset
    #[test]
    fn get_missing_with_offset() {
        let mut t: Table<i64> = table(3, 10, 300000);
        assert!(t.missing(0).is_none());
        for num in [300002, 300004, 300006, 300008] {
            t.add(num, num);
        }
        assert_missing(&t, 4);
        t.add(300001, 300001);
        t.remove_many(true, 2, None);
        assert_missing(&t, 3);
    }

    // TableTest#testGetMissing2
    #[test]
    fn get_missing_2() {
        let mut t: Table<i64> = table(3, 10, 0);
        for num in [3, 4, 5] {
            t.add(num, num);
        }
        assert_missing(&t, 2); // the range [1-2]
    }

    // TableTest#testGetMissing3
    #[test]
    fn get_missing_3() {
        let mut t: Table<i64> = table(3, 10, 0);
        t.add(8, 8);
        assert_missing(&t, 7);
    }

    // TableTest#testGetMissingLast
    #[test]
    fn get_missing_last() {
        let mut t: Table<i64> = table(3, 10, 0);
        for num in [1, 2, 3, 4, 5, 6, 8] {
            t.add(num, num);
        }
        assert_missing(&t, 1);
    }

    // TableTest#testGetMissingFirst
    #[test]
    fn get_missing_first() {
        let mut t: Table<i64> = table(3, 10, 0);
        for num in [2, 3, 4, 5] {
            t.add(num, num);
        }
        assert_missing(&t, 1);
    }

    // TableTest#testGetHighestDeliverable
    #[test]
    fn get_highest_deliverable() {
        let mut t: Table<i64> = table(3, 10, 0);
        let state = |t: &Table<i64>| (t.highest_delivered(), t.highest_deliverable(), t.num_deliverable());
        assert_eq!(state(&t), (0, 0, 0));
        for num in [1, 2, 3, 4, 5, 6, 8] {
            t.add(num, num);
        }
        assert_eq!(state(&t), (0, 6, 6));
        t.remove_many(true, 4, None);
        assert_eq!(state(&t), (4, 6, 2));
        t.remove_many(true, 100, None);
        assert_eq!(state(&t), (6, 6, 0));
        t.add(7, 7);
        assert_eq!(state(&t), (6, 8, 2));
        t.remove_many(true, 100, None);
        assert_eq!(state(&t), (8, 8, 0));
    }

    // TableTest#testMassAddition
    #[test]
    fn mass_addition() {
        let mut t: Table<i64> = table(3, 10, 0);
        const NUM_ELEMENTS: i64 = 10005;
        for i in 1..=NUM_ELEMENTS {
            t.add(i, i);
        }
        assert_eq!(t.size(), NUM_ELEMENTS as usize);
        assert_capacity(&t, 10);
        assert_indices(&t, 0, 0, NUM_ELEMENTS);
        assert_eq!(t.num_missing(), 0);
    }

    // TableTest#testResize
    #[test]
    fn resize() {
        let mut t: Table<i64> = table(3, 10, 0);
        assert_capacity(&t, 10);
        for seqno in [30, 35, 500, 515] {
            add_and_get(&mut t, seqno);
            assert_capacity(&t, 10);
        }
    }

    // TableTest#testResizeWithPurge
    #[test]
    fn resize_with_purge() {
        let mut t: Table<i64> = table(3, 10, 0);
        for i in 1..=100 {
            add_and_get(&mut t, i);
        }
        for i in 1..=60 {
            assert_eq!(t.remove(true), Some(i));
        }
        t.purge(50, false);
        add_and_get(&mut t, 120); // triggers a resize()
    }

    // TableTest#testPurge
    #[test]
    fn purge() {
        let mut t: Table<i64> = table(5, 10, 0);
        for seqno in 1..=25 {
            t.add(seqno, seqno);
        }
        let seqnos = [30, 31, 32, 37, 38, 39, 40, 41, 42, 47, 48, 49];
        for seqno in seqnos {
            t.add(seqno, seqno);
        }
        for _ in 1..=22 {
            t.remove(false);
        }
        t.purge(22, false);
        assert_eq!(t.size(), 3 + seqnos.len());
        assert_eq!(t.compute_size(), t.size());
    }
}
