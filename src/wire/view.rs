//! `View`, `Digest` and `JoinRsp`: the membership types GMS join marshals (`org.jgroups.View`,
//! `org.jgroups.util.Digest`, `org.jgroups.protocols.pbcast.JoinRsp`), plus `GMS`'s own
//! `marshal(View, Digest)`/`_readViewAndDigest` combined wire format for the `VIEW` message
//! payload (`protocols/pbcast/GMS.java:1097-1158, 1227-1251`).
//!
//! `MergeView` and `DeltaView` are both decoded (`read_view_and_digest_full`); a `DeltaView` is
//! applied against the current view as `GMS.createViewFromDeltaView` does. Every decoder here that
//! sees bytes from the network has a `try_` form returning an error where upstream throws. `Digest`'s
//! mutable variant (`MutableDigest`, used by `CoordGmsImpl` to build the join digest) is folded
//! into a plain constructor here (`Digest::zero`) and the free functions of `util::digest` rather
//! than a separate type.

use crate::wire::address::{self, Address};
use crate::wire::bits::{try_read_utf, size_long_sequence, size_utf, try_read_long_sequence, write_long_sequence, write_utf};
use crate::wire::header::ViewId;
use crate::wire::stream::{WireError, WireReader, WireResult, WireWrite};

// ------------------------------------- View -------------------------------------

/// `org.jgroups.View`: `view_id` (creator + Lamport id) plus an ordered member list, coordinator
/// first (`View.java:29-41`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct View {
    /// The view's id: its creator and sequence number.
    pub view_id: ViewId,
    /// The members, coordinator first, in join order.
    pub members: Vec<Address>,
}

impl View {
    /// `View.create(Address, long, Address...)`, `View.java:91-93`.
    pub fn create(coord: Address, id: i64, members: Vec<Address>) -> View {
        View { view_id: ViewId { creator: Some(coord), id }, members }
    }

    /// `View.getCreator`: the member that created the view, its coordinator at the time.
    pub fn creator(&self) -> Option<&Address> {
        self.view_id.creator.as_ref()
    }

    /// `View.getCoord()`, `View.java:120`: the first member, or `None` for an empty view.
    pub fn coord(&self) -> Option<&Address> {
        self.members.first()
    }

    /// The number of members.
    pub fn size(&self) -> usize {
        self.members.len()
    }

    /// Whether `mbr` is a member.
    pub fn contains_member(&self, mbr: &Address) -> bool {
        self.members.contains(mbr)
    }

    /// `View.containsMembers(Address...)`, `View.java:152`.
    pub fn contains_members(&self, mbrs: &[Address]) -> bool {
        mbrs.iter().all(|m| self.contains_member(m))
    }

    /// `View.diff(from, to)`, `View.java:247`: `(joined, left)`, each in the order of the view
    /// they are taken from (`to` for joiners, `from` for leavers). `from == None` is every member
    /// of `to` joining.
    pub fn diff(from: Option<&View>, to: &View) -> (Vec<Address>, Vec<Address>) {
        let Some(from) = from else {
            return (to.members.clone(), Vec::new());
        };
        let joined = to.members.iter().filter(|a| !from.contains_member(a)).cloned().collect();
        let left = from.members.iter().filter(|a| !to.contains_member(a)).cloned().collect();
        (joined, left)
    }

    /// `View.leftMembers(one, two)`, `View.java:224`: members of `one` that are not in `two`;
    /// `None` when either view is missing.
    pub fn left_members(one: Option<&View>, two: Option<&View>) -> Option<Vec<Address>> {
        let (one, two) = (one?, two?);
        Some(one.members.iter().filter(|a| !two.contains_member(a)).cloned().collect())
    }

    /// `View.newMembers(old, new_view)`, `View.java:232`.
    pub fn new_members(old: Option<&View>, new_view: Option<&View>) -> Option<Vec<Address>> {
        let new_view = new_view?;
        Some(new_view.members.iter().filter(|a| old.is_none_or(|o| !o.contains_member(a))).cloned().collect())
    }

    /// `View.sameViews(View...)`, `View.java:306`: every view carries the first one's `ViewId`.
    pub fn same_views(views: &[&View]) -> bool {
        let first = &views[0].view_id;
        views.iter().all(|v| &v.view_id == first)
    }

    /// `View.sameMembers(v1, v2)`, `View.java:317`: equal as sets, order ignored.
    pub fn same_members(v1: &View, v2: &View) -> bool {
        if std::ptr::eq(v1, v2) {
            return true;
        }
        if v1.size() != v2.size() {
            return false;
        }
        let (joined, left) = View::diff(Some(v1), v2);
        joined.is_empty() && left.is_empty()
    }

    /// `View.sameMembersOrdered(v1, v2)`, `View.java:328`: same members in the same order; the
    /// view ids are not compared.
    pub fn same_members_ordered(v1: &View, v2: &View) -> bool {
        v1.members == v2.members
    }

    /// `View.writeTo`, `View.java:200-203`.
    pub fn write_to(&self, out: &mut Vec<u8>) {
        self.view_id.write_to(out);
        address::write_addresses(&self.members, out);
    }

    /// `View.readFrom`, `View.java:206-210`.
    pub fn read_from(inp: &mut WireReader<'_>) -> View {
        View::try_read_from(inp).unwrap_or_else(|e| panic!("{e}"))
    }

    /// [`View::read_from`] for bytes from the network: a truncated frame or a null member list is
    /// an error (upstream throws `EOFException`/`NullPointerException`).
    pub fn try_read_from(inp: &mut WireReader<'_>) -> WireResult<View> {
        let view_id = ViewId::try_read_from(inp)?;
        let members = address::try_read_addresses(inp)?.ok_or_else(|| WireError::Invalid("View members are never null on the wire".into()))?;
        Ok(View { view_id, members })
    }

    /// `View.serializedSize`, `View.java:213-215`.
    pub fn serialized_size(&self) -> usize {
        self.view_id.serialized_size() + address::size_addresses(&self.members)
    }
}

// ------------------------------------- MergeView -------------------------------------

/// `org.jgroups.MergeView` (`MergeView.java`): a `View` plus the subgroup views that merged into
/// it. Delivered (instead of a plain view) to the application after a partition heals; the
/// receiving application tells a merge from an ordinary membership change by its type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MergeView {
    /// The merged view.
    pub view: View,
    /// The views of the subgroups that merged into it.
    pub subgroups: Vec<View>,
}

impl MergeView {
    /// A merge view of `view` from `subgroups`.
    pub fn new(view: View, subgroups: Vec<View>) -> MergeView {
        MergeView { view, subgroups }
    }

    fn index_of(&self, mbr: Option<&Address>) -> i16 {
        match mbr {
            Some(m) => self.view.members.iter().position(|x| x == m).map(|i| i as i16).unwrap_or(-1),
            None => -1,
        }
    }

    /// `MergeView.writeTo`, `MergeView.java:116-137`: the view, then each subgroup as indexes into
    /// the merged view's member list (an address is written only for a member not in it,
    /// JGRP-1707).
    pub fn write_to(&self, out: &mut Vec<u8>) {
        self.view.write_to(out);
        out.write_i16(self.subgroups.len() as i16);
        for v in &self.subgroups {
            let index = self.index_of(v.creator());
            out.write_i16(index);
            if index < 0 {
                address::write_address(v.creator(), out);
            }
            out.write_i64(v.view_id.id);
            out.write_i16(v.members.len() as i16);
            for mbr in &v.members {
                let index = self.index_of(Some(mbr));
                out.write_i16(index);
                if index < 0 {
                    address::write_address(Some(mbr), out);
                }
            }
        }
    }

    /// `MergeView.readFrom`, `MergeView.java:139-163`.
    pub fn read_from(inp: &mut WireReader<'_>) -> MergeView {
        MergeView::try_read_from(inp).unwrap_or_else(|e| panic!("{e}"))
    }

    /// [`MergeView::read_from`] for bytes from the network. An index past the merged view's member
    /// list is an error (upstream's `get(index)` throws `ArrayIndexOutOfBoundsException`).
    pub fn try_read_from(inp: &mut WireReader<'_>) -> WireResult<MergeView> {
        let view = View::try_read_from(inp)?;
        let len = inp.try_read_i16()?;
        let mut subgroups = Vec::new();
        for _ in 0..len.max(0) {
            let get = |inp: &mut WireReader<'_>, view: &View| -> WireResult<Option<Address>> {
                let index = inp.try_read_i16()?;
                if index >= 0 {
                    match view.members.get(index as usize) {
                        Some(a) => Ok(Some(a.clone())),
                        None => Err(WireError::Invalid(format!("MergeView member index {index} out of range ({} members)", view.members.len()))),
                    }
                } else {
                    address::try_read_address(inp)
                }
            };
            let creator = get(inp, &view)?;
            let id = inp.try_read_i64()?;
            let n = inp.try_read_i16()?;
            let mut members = Vec::new();
            for _ in 0..n.max(0) {
                members.extend(get(inp, &view)?);
            }
            subgroups.push(View { view_id: ViewId { creator, id }, members });
        }
        Ok(MergeView { view, subgroups })
    }

    /// `MergeView.serializedSize`, `MergeView.java:165-187`.
    pub fn serialized_size(&self) -> usize {
        let mut size = self.view.serialized_size() + 2;
        for v in &self.subgroups {
            size += 2 + 8 + 2;
            if self.index_of(v.creator()) < 0 {
                size += address::size(v.creator());
            }
            size += v.members.len() * 2;
            for m in &v.members {
                if self.index_of(Some(m)) < 0 {
                    size += address::size(Some(m));
                }
            }
        }
        size
    }
}

// ------------------------------------- DeltaView -------------------------------------

/// `org.jgroups.protocols.pbcast.DeltaView` (`DeltaView.java`): a `VIEW` message payload that
/// carries only the difference to the view `ref_view_id` identifies. A receiver that still holds
/// that view rebuilds the full one with `GMS.createViewFromDeltaView` ([`DeltaView::apply`]);
/// otherwise it asks the sender for the current view (`GET_CURRENT_VIEW`). Unlike a `View` it has
/// no member list of its own, so it is not a `View` here either.
///
/// `left_members`/`new_members` are `None` for a null array (`Util.readAddresses` returns null for
/// a `-1` count); upstream only fails on that when it applies the delta.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeltaView {
    /// The id of the new view.
    pub view_id: ViewId,
    /// The id of the view the delta is relative to.
    pub ref_view_id: ViewId,
    /// The members that left since the reference view.
    pub left_members: Option<Vec<Address>>,
    /// The members that joined since the reference view.
    pub new_members: Option<Vec<Address>>,
}

impl DeltaView {
    /// `new DeltaView(view_id, ref_view_id, left, new)`, `DeltaView.java:44-50`.
    pub fn new(view_id: ViewId, ref_view_id: ViewId, left_members: Vec<Address>, new_members: Vec<Address>) -> DeltaView {
        DeltaView { view_id, ref_view_id, left_members: Some(left_members), new_members: Some(new_members) }
    }

    /// `GMS.createDeltaView(current_view, next_view)`, `GMS.java:722-727`: left members are the
    /// current view's that `next_view` lacks, new members the other way round.
    pub fn create(current_view: &View, next_view: &View) -> DeltaView {
        let (joined, left) = View::diff(Some(current_view), next_view);
        DeltaView::new(next_view.view_id.clone(), current_view.view_id.clone(), left, joined)
    }

    /// `DeltaView.writeTo`, `DeltaView.java:63-68`. A null array is written as a `-1` count
    /// (`Util.writeAddresses`).
    pub fn write_to(&self, out: &mut Vec<u8>) {
        self.view_id.write_to(out);
        self.ref_view_id.write_to(out);
        for arr in [&self.left_members, &self.new_members] {
            match arr {
                Some(a) => address::write_addresses(a, out),
                None => out.write_i16(-1),
            }
        }
    }

    /// `DeltaView.readFrom`, `DeltaView.java:70-77`.
    pub fn try_read_from(inp: &mut WireReader<'_>) -> WireResult<DeltaView> {
        let view_id = ViewId::try_read_from(inp)?;
        let ref_view_id = ViewId::try_read_from(inp)?;
        let left_members = address::try_read_addresses(inp)?;
        let new_members = address::try_read_addresses(inp)?;
        Ok(DeltaView { view_id, ref_view_id, left_members, new_members })
    }

    /// `DeltaView.serializedSize`, `DeltaView.java:55-61` (`Util.size(Address[])` is 2 for null).
    pub fn serialized_size(&self) -> usize {
        let size = |a: &Option<Vec<Address>>| a.as_deref().map_or(2, address::size_addresses);
        self.view_id.serialized_size() + self.ref_view_id.serialized_size() + size(&self.left_members) + size(&self.new_members)
    }

    /// `GMS.createViewFromDeltaView(current_view, delta_view)`, `GMS.java:1102-1118`: errors where
    /// upstream throws `IllegalStateException` (no current view, or the delta refers to another
    /// view than `current_view`) or a `NullPointerException` (a null array); the caller then
    /// requests the full view.
    pub fn apply(&self, current_view: Option<&View>) -> Result<View, String> {
        let current = current_view.ok_or_else(|| format!("current view (None) or delta view ({self:?}) is null"))?;
        if current.view_id != self.ref_view_id {
            return Err(format!(
                "the view-id of the delta view ({:?}) doesn't match the current view-id ({:?}); discarding delta view {self:?}",
                self.ref_view_id, current.view_id
            ));
        }
        let (Some(left), Some(new)) = (&self.left_members, &self.new_members) else {
            return Err(format!("delta view {self:?} has a null member array"));
        };
        Ok(View { view_id: self.view_id.clone(), members: crate::util::membership::compute_new_membership(&current.members, new, left, &[]) })
    }
}

// ------------------------------------- Digest -------------------------------------

/// `org.jgroups.util.Digest`: for each member, the highest-delivered and highest-received seqno
/// (`util/Digest.java:18-38`). `members`/`seqnos` are kept as one `Vec<(Address, hd, hr)>` here
/// rather than upstream's parallel `Address[]`/`long[]` (an internal-layout detail with no bytes on
/// the wire — upstream's own doc comment calls the parallel-array split "reserved for Bela").
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Digest {
    /// `(member, highest delivered seqno, highest received seqno)`, one entry per member.
    pub entries: Vec<(Address, i64, i64)>,
}

impl Digest {
    /// `new Digest(Address, long, long)`, `Digest.java:71-74` — the singleton-coordinator digest
    /// `ClientGmsImpl.becomeSingletonMember` installs.
    pub fn one(mbr: Address, hd: i64, hr: i64) -> Digest {
        Digest { entries: vec![(mbr, hd, hr)] }
    }

    /// A digest with every member at seqno (0, 0) — the join digest of a stack without NAKACK2,
    /// which has no real seqnos to report.
    pub fn zero(members: &[Address]) -> Digest {
        Digest { entries: members.iter().map(|m| (m.clone(), 0, 0)).collect() }
    }

    /// The number of entries.
    pub fn capacity(&self) -> usize {
        self.entries.len()
    }

    /// `Digest.contains(Address)`, `Digest.java:100-105`.
    pub fn contains(&self, mbr: &Address) -> bool {
        self.entries.iter().any(|(m, _, _)| m == mbr)
    }

    /// The members the digest has an entry for, in order.
    pub fn members(&self) -> Vec<Address> {
        self.entries.iter().map(|(m, _, _)| m.clone()).collect()
    }

    /// `Digest.get(Address)`, `Digest.java:114-118`.
    pub fn get(&self, mbr: &Address) -> Option<(i64, i64)> {
        self.entries.iter().find(|(m, _, _)| m == mbr).map(|(_, hd, hr)| (*hd, *hr))
    }

    /// `Digest.writeTo(DataOutput, boolean)`, `Digest.java:145-151`.
    pub fn write_to(&self, write_addrs: bool, out: &mut Vec<u8>) {
        if write_addrs {
            let members: Vec<Address> = self.members();
            address::write_addresses(&members, out);
        } else {
            out.write_i16(self.entries.len() as i16);
        }
        for (_, hd, hr) in &self.entries {
            write_long_sequence(*hd, *hr, out);
        }
    }

    /// `Digest.readFrom(DataInput, boolean)`, `Digest.java:158-168`. When `read_addrs` is false,
    /// the caller (`JoinRsp.readFrom`, `GMS._readViewAndDigest`) supplies the member list the
    /// digest doesn't carry on the wire in that shape (it was already read as part of the `View`).
    pub fn read_from(read_addrs: bool, members_hint: Option<&[Address]>, inp: &mut WireReader<'_>) -> Digest {
        Digest::try_read_from(read_addrs, members_hint, inp).unwrap_or_else(|e| panic!("{e}"))
    }

    /// [`Digest::read_from`] for bytes from the network: truncation, a null member list, a missing
    /// hint or a count that disagrees with the view's members are errors.
    pub fn try_read_from(read_addrs: bool, members_hint: Option<&[Address]>, inp: &mut WireReader<'_>) -> WireResult<Digest> {
        let members = if read_addrs {
            address::try_read_addresses(inp)?.ok_or_else(|| WireError::Invalid("Digest members are never null on the wire".into()))?
        } else {
            let count = inp.try_read_i16()?;
            let hint = members_hint.ok_or_else(|| WireError::Invalid("digest without its own addresses needs a view to borrow members from".into()))?;
            if count < 0 || hint.len() != count as usize {
                return Err(WireError::Invalid(format!("digest member count {count} doesn't match the view it was read alongside ({})", hint.len())));
            }
            hint.to_vec()
        };
        let mut entries = Vec::with_capacity(members.len());
        for m in members {
            let (hd, hr) = try_read_long_sequence(inp)?;
            entries.push((m, hd, hr));
        }
        Ok(Digest { entries })
    }

    /// `Digest.serializedSize(boolean)`, `Digest.java:184-189`.
    pub fn serialized_size(&self, with_members: bool) -> usize {
        let mut retval = if with_members {
            let members: Vec<Address> = self.members();
            address::size_addresses(&members)
        } else {
            2
        };
        for (_, hd, hr) in &self.entries {
            retval += size_long_sequence(*hd, *hr);
        }
        retval
    }
}

// ------------------------------------- marshal(View, Digest) -------------------------------------

const VIEW_PRESENT: u16 = 1 << 0;
const DIGEST_PRESENT: u16 = 1 << 1;
const MERGE_VIEW: u16 = 1 << 2;
const DELTA_VIEW: u16 = 1 << 3;
const READ_ADDRS: u16 = 1 << 4;

/// `GMS.writeAddresses(View, Digest)`, `GMS.java:1121-1123`: read the digest's own addresses only
/// when they'd differ from the view's (an on-the-wire space saving; both sides always compute this
/// identically from the same `View`/`Digest` values, so there is nothing to disagree about).
fn write_addrs_flag(view: Option<&View>, digest: Option<&Digest>) -> bool {
    match (view, digest) {
        (Some(v), Some(d)) => v.members != d.members(),
        _ => true,
    }
}

/// `GMS.marshal(View, Digest)`, `GMS.java:1130-1148` — the `VIEW` message payload
/// `castViewChangeAndSendJoinRsps` sends (short flags, then the view, then the digest, each only if
/// present). Only ever called here with a plain `View` (never `MergeView`/`DeltaView` — see the
/// module doc comment), so `MERGE_VIEW`/`DELTA_VIEW` are never set.
pub fn marshal_view_digest(view: Option<&View>, digest: Option<&Digest>) -> Vec<u8> {
    marshal_view_digest_ext(view, None, digest)
}

/// `GMS.marshal(View, Digest)` with a `MergeView` (`GMS.determineFlags` sets `MERGE_VIEW` when the
/// view is one; `MergeView.writeTo` writes the subgroups after the view). `subgroups` is `Some`
/// exactly for a merge view.
pub fn marshal_view_digest_ext(view: Option<&View>, subgroups: Option<&[View]>, digest: Option<&Digest>) -> Vec<u8> {
    let mut out = Vec::new();
    let write_addrs = write_addrs_flag(view, digest);
    let mut flags = 0u16;
    if view.is_some() {
        flags |= VIEW_PRESENT;
        if subgroups.is_some() {
            flags |= MERGE_VIEW;
        }
    }
    if digest.is_some() {
        flags |= DIGEST_PRESENT;
    }
    if write_addrs {
        flags |= READ_ADDRS;
    }
    out.write_i16(flags as i16);
    if let Some(v) = view {
        match subgroups {
            Some(sub) => MergeView::new(v.clone(), sub.to_vec()).write_to(&mut out),
            None => v.write_to(&mut out),
        }
    }
    if let Some(d) = digest {
        d.write_to(write_addrs, &mut out);
    }
    out
}

/// `GMS.marshal(View, Digest)` for a `DeltaView` (`GMS.determineFlags` sets `DELTA_VIEW`). A delta
/// view has no member list, so `writeAddresses` is always true and a digest carries its own.
pub fn marshal_delta_view(delta: &DeltaView, digest: Option<&Digest>) -> Vec<u8> {
    let mut out = Vec::new();
    let mut flags = VIEW_PRESENT | DELTA_VIEW | READ_ADDRS;
    if digest.is_some() {
        flags |= DIGEST_PRESENT;
    }
    out.write_i16(flags as i16);
    delta.write_to(&mut out);
    if let Some(d) = digest {
        d.write_to(true, &mut out);
    }
    out
}

/// The view a `VIEW`/merge message carries, by kind (`GMS._readViewAndDigest`, `GMS.java:1234-1238`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReceivedView {
    /// A full view.
    Plain(View),
    /// A merge view.
    Merge(MergeView),
    /// A delta to a view the receiver should already hold.
    Delta(DeltaView),
}

/// `GMS._readViewAndDigest(byte[], int, int)`, `GMS.java:1227-1251`, all three view kinds. Any
/// malformed input is an `Err` (upstream throws; `readViewAndDigest` logs it and returns null).
pub fn read_view_and_digest_full(bytes: &[u8]) -> WireResult<(Option<ReceivedView>, Option<Digest>)> {
    let mut inp = WireReader::new(bytes);
    let flags = inp.try_read_i16()? as u16;

    let view = if flags & VIEW_PRESENT != 0 {
        Some(if flags & MERGE_VIEW != 0 {
            ReceivedView::Merge(MergeView::try_read_from(&mut inp)?)
        } else if flags & DELTA_VIEW != 0 {
            ReceivedView::Delta(DeltaView::try_read_from(&mut inp)?)
        } else {
            ReceivedView::Plain(View::try_read_from(&mut inp)?)
        })
    } else {
        None
    };

    let digest = if flags & DIGEST_PRESENT != 0 {
        if flags & READ_ADDRS != 0 {
            Some(Digest::try_read_from(true, None, &mut inp)?)
        } else {
            // upstream: `new Digest(tmp_view.getMembersRaw())`, a NullPointerException for no view
            // and a null member array for a delta view
            let hint = match &view {
                Some(ReceivedView::Plain(v)) => v.members.clone(),
                Some(ReceivedView::Merge(mv)) => mv.view.members.clone(),
                _ => return Err(WireError::Invalid("digest without its own addresses needs a view with members to borrow from".into())),
            };
            Some(Digest::try_read_from(false, Some(&hint), &mut inp)?)
        }
    } else {
        None
    };

    Ok((view, digest))
}

/// [`read_view_and_digest_full`] for a caller that has no use for a delta view (a merge message
/// never carries one): a `DeltaView` there is an error.
pub fn read_view_and_digest(bytes: &[u8]) -> WireResult<(Option<View>, Option<Digest>)> {
    let (view, _subgroups, digest) = read_view_and_digest_ext(bytes)?;
    Ok((view, digest))
}

/// A decoded full view, a merge view's subgroups, and the digest.
pub type ViewSubgroupsDigest = (Option<View>, Option<Vec<View>>, Option<Digest>);

/// [`read_view_and_digest`] that also returns the subgroups of a `MergeView` (`Some` exactly when
/// the `MERGE_VIEW` flag was set).
pub fn read_view_and_digest_ext(bytes: &[u8]) -> WireResult<ViewSubgroupsDigest> {
    let (view, digest) = read_view_and_digest_full(bytes)?;
    match view {
        None => Ok((None, None, digest)),
        Some(ReceivedView::Plain(v)) => Ok((Some(v), None, digest)),
        Some(ReceivedView::Merge(mv)) => Ok((Some(mv.view), Some(mv.subgroups), digest)),
        Some(ReceivedView::Delta(_)) => Err(WireError::Invalid("a DeltaView where a full view is required".into())),
    }
}

// ------------------------------------- JoinRsp -------------------------------------

const JOINRSP_VIEW_PRESENT: u8 = 1 << 0;
const JOINRSP_DIGEST_PRESENT: u8 = 1 << 1;
const JOINRSP_FAIL_REASON_PRESENT: u8 = 1 << 2;

/// `org.jgroups.protocols.pbcast.JoinRsp`: the coordinator's reply to a `JOIN_REQ`
/// (`protocols/pbcast/JoinRsp.java`). Its digest is always written/read with `write_addrs=false`
/// (`:59-60,78-80`: "don't write the membership; it is already present in the view"), so a
/// `JoinRsp` with a digest always has a view too.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinRsp {
    /// The view to install; `None` when the join failed.
    pub view: Option<View>,
    /// The digest of the members' seqnos, to start receiving from; `None` when the join failed.
    pub digest: Option<Digest>,
    /// Why the coordinator refused the join.
    pub fail_reason: Option<String>,
}

impl JoinRsp {
    /// A successful join reply.
    pub fn success(view: View, digest: Digest) -> JoinRsp {
        JoinRsp { view: Some(view), digest: Some(digest), fail_reason: None }
    }

    /// A refused join reply with `reason`.
    pub fn failure(reason: impl Into<String>) -> JoinRsp {
        JoinRsp { view: None, digest: None, fail_reason: Some(reason.into()) }
    }

    /// `JoinRsp.writeTo`, `JoinRsp.java:49-69`.
    pub fn write_to(&self, out: &mut Vec<u8>) {
        let mut flags = 0u8;
        if self.view.is_some() {
            flags |= JOINRSP_VIEW_PRESENT;
        }
        if self.digest.is_some() {
            flags |= JOINRSP_DIGEST_PRESENT;
        }
        if self.fail_reason.is_some() {
            flags |= JOINRSP_FAIL_REASON_PRESENT;
        }
        out.write_byte(flags);
        if let Some(v) = &self.view {
            v.write_to(out);
        }
        if let Some(d) = &self.digest {
            d.write_to(false, out);
        }
        if let Some(reason) = &self.fail_reason {
            write_utf(reason, out);
        }
    }

    /// `JoinRsp.readFrom`, `JoinRsp.java:72-89`.
    pub fn read_from(inp: &mut WireReader<'_>) -> JoinRsp {
        JoinRsp::try_read_from(inp).unwrap_or_else(|e| panic!("{e}"))
    }

    /// [`JoinRsp::read_from`] for bytes from the network.
    pub fn try_read_from(inp: &mut WireReader<'_>) -> WireResult<JoinRsp> {
        let flags = inp.try_read_byte()?;
        let view = if flags & JOINRSP_VIEW_PRESENT != 0 { Some(View::try_read_from(inp)?) } else { None };
        let digest = if flags & JOINRSP_DIGEST_PRESENT != 0 {
            let hint = view
                .as_ref()
                .ok_or_else(|| WireError::Invalid("JoinRsp digest needs a view to borrow members from".into()))?
                .members
                .clone();
            Some(Digest::try_read_from(false, Some(&hint), inp)?)
        } else {
            None
        };
        let fail_reason = if flags & JOINRSP_FAIL_REASON_PRESENT != 0 { Some(try_read_utf(inp)?) } else { None };
        Ok(JoinRsp { view, digest, fail_reason })
    }

    /// `JoinRsp.serializedSize`, `JoinRsp.java:92-102`.
    pub fn serialized_size(&self) -> usize {
        let mut retval = 1;
        if let Some(v) = &self.view {
            retval += v.serialized_size();
        }
        if let Some(d) = &self.digest {
            retval += d.serialized_size(false);
        }
        if let Some(reason) = &self.fail_reason {
            retval += size_utf(reason);
        }
        retval
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::vectors;

    fn u1() -> Address {
        Address::uuid(0x0102030405060708, 0x1112131415161718)
    }
    fn u2() -> Address {
        Address::uuid(0, 0)
    }

    #[test]
    fn view_roundtrips() {
        for v in [
            View::create(u1(), 0, vec![u1()]),
            View::create(u1(), 1, vec![u1(), u2()]),
            View { view_id: ViewId { creator: Some(u1()), id: 3 }, members: vec![] },
        ] {
            let mut out = Vec::new();
            v.write_to(&mut out);
            assert_eq!(out.len(), v.serialized_size());
            let mut r = WireReader::new(&out);
            assert_eq!(View::read_from(&mut r), v);
            assert!(r.is_empty());
        }
    }

    #[test]
    fn view_helpers() {
        let v = View::create(u1(), 1, vec![u1(), u2()]);
        assert_eq!(v.coord(), Some(&u1()));
        assert_eq!(v.size(), 2);
        assert!(v.contains_member(&u2()));
        assert!(!v.contains_member(&Address::uuid(9, 9)));
        assert_eq!(v.creator(), Some(&u1()));
    }

    #[test]
    fn digest_roundtrips_with_and_without_addresses() {
        let d = Digest { entries: vec![(u1(), 5, 10), (u2(), 0, 0)] };
        for write_addrs in [true, false] {
            let mut out = Vec::new();
            d.write_to(write_addrs, &mut out);
            assert_eq!(out.len(), d.serialized_size(write_addrs));
            let mut r = WireReader::new(&out);
            let hint = d.members();
            let back = Digest::read_from(write_addrs, if write_addrs { None } else { Some(&hint) }, &mut r);
            assert!(r.is_empty());
            assert_eq!(back, d);
        }
    }

    #[test]
    fn digest_zero_and_one() {
        let z = Digest::zero(&[u1(), u2()]);
        assert_eq!(z.get(&u1()), Some((0, 0)));
        assert_eq!(z.get(&u2()), Some((0, 0)));
        assert!(z.contains(&u1()));
        assert!(!z.contains(&Address::uuid(9, 9)));

        let one = Digest::one(u1(), 3, 7);
        assert_eq!(one.get(&u1()), Some((3, 7)));
        assert_eq!(one.capacity(), 1);
    }

    #[test]
    fn marshal_view_digest_roundtrips() {
        let view = View::create(u1(), 1, vec![u1(), u2()]);
        let same_members = Digest::zero(&view.members);
        let bytes = marshal_view_digest(Some(&view), Some(&same_members));
        let (v2, d2) = read_view_and_digest(&bytes).unwrap();
        assert_eq!(v2, Some(view.clone()));
        assert_eq!(d2, Some(same_members));

        let different_members = Digest::one(u1(), 0, 0);
        let bytes2 = marshal_view_digest(Some(&view), Some(&different_members));
        let (v3, d3) = read_view_and_digest(&bytes2).unwrap();
        assert_eq!(v3, Some(view));
        assert_eq!(d3, Some(different_members));

        let view_only = View::create(u1(), 0, vec![u1()]);
        let bytes3 = marshal_view_digest(Some(&view_only), None);
        let (v4, d4) = read_view_and_digest(&bytes3).unwrap();
        assert_eq!(v4, Some(view_only));
        assert_eq!(d4, None);
    }

    #[test]
    fn join_rsp_roundtrips() {
        let view = View::create(u1(), 1, vec![u1(), u2()]);
        let digest = Digest::zero(&view.members);
        let jr = JoinRsp::success(view, digest);
        let mut out = Vec::new();
        jr.write_to(&mut out);
        assert_eq!(out.len(), jr.serialized_size());
        let mut r = WireReader::new(&out);
        assert_eq!(JoinRsp::read_from(&mut r), jr);
        assert!(r.is_empty());

        let fail = JoinRsp::failure("auth failed");
        let mut out2 = Vec::new();
        fail.write_to(&mut out2);
        assert_eq!(out2.len(), fail.serialized_size());
        let mut r2 = WireReader::new(&out2);
        assert_eq!(JoinRsp::read_from(&mut r2), fail);
    }

    // Cross-checked against the real jgroups-5.2.28.Final jar (upstream-tools/vector-harness).
    #[test]
    fn matches_upstream_vectors() {
        let v = vectors::load();
        let u1 = u1();
        let u2 = u2();

        let view_singleton = View::create(u1.clone(), 0, vec![u1.clone()]);
        {
            let vec = vectors::get(&v, "view.singleton");
            let mut out = Vec::new();
            view_singleton.write_to(&mut out);
            assert_eq!(out, vec.bytes);
            assert_eq!(out.len() as i64, vec.size);
        }

        let view_two = View::create(u1.clone(), 1, vec![u1.clone(), u2.clone()]);
        {
            let vec = vectors::get(&v, "view.two");
            let mut out = Vec::new();
            view_two.write_to(&mut out);
            assert_eq!(out, vec.bytes);
            assert_eq!(out.len() as i64, vec.size);
        }

        let view_empty = View { view_id: ViewId { creator: Some(u1.clone()), id: 3 }, members: vec![] };
        {
            let vec = vectors::get(&v, "view.empty");
            let mut out = Vec::new();
            view_empty.write_to(&mut out);
            assert_eq!(out, vec.bytes);
        }

        let digest_singleton = Digest::one(u1.clone(), 0, 0);
        {
            let vec = vectors::get(&v, "digest.singleton.zero.withaddrs");
            let mut out = Vec::new();
            digest_singleton.write_to(true, &mut out);
            assert_eq!(out, vec.bytes);
            assert_eq!(out.len() as i64, vec.size);
        }
        {
            let vec = vectors::get(&v, "digest.singleton.zero.noaddrs");
            let mut out = Vec::new();
            digest_singleton.write_to(false, &mut out);
            assert_eq!(out, vec.bytes);
        }

        let digest_two = Digest { entries: vec![(u1.clone(), 5, 10), (u2.clone(), 0, 0)] };
        {
            let vec = vectors::get(&v, "digest.two.withaddrs");
            let mut out = Vec::new();
            digest_two.write_to(true, &mut out);
            assert_eq!(out, vec.bytes);
            assert_eq!(out.len() as i64, vec.size);
        }
        {
            let vec = vectors::get(&v, "digest.two.noaddrs");
            let mut out = Vec::new();
            digest_two.write_to(false, &mut out);
            assert_eq!(out, vec.bytes);
        }

        {
            let streamable = vectors::get(&v, "joinrsp.streamable.fail");
            assert_eq!(streamable.bytes[0], 1);
            assert_eq!(&streamable.bytes[1..], &vectors::get(&v, "joinrsp.fail").bytes[..]);
            let streamable = vectors::get(&v, "joinrsp.streamable.full");
            assert_eq!(&streamable.bytes[1..], &vectors::get(&v, "joinrsp.full").bytes[..]);
            let vec = vectors::get(&v, "joinrsp.full");
            let jr = JoinRsp::success(view_two.clone(), digest_two.clone());
            let mut out = Vec::new();
            jr.write_to(&mut out);
            assert_eq!(out, vec.bytes);
            assert_eq!(out.len() as i64, vec.size);
        }
        {
            let vec = vectors::get(&v, "joinrsp.fail");
            let jr = JoinRsp::failure("auth failed");
            let mut out = Vec::new();
            jr.write_to(&mut out);
            assert_eq!(out, vec.bytes);
            assert_eq!(out.len() as i64, vec.size);
        }

        {
            // members match -> write_addrs=false (READ_ADDRS unset)
            let vec = vectors::get(&v, "viewdigest.same_members");
            let bytes = marshal_view_digest(Some(&view_two), Some(&digest_two));
            assert_eq!(bytes, vec.bytes);
        }
        {
            // members differ -> write_addrs=true (READ_ADDRS set)
            let vec = vectors::get(&v, "viewdigest.different_members");
            let digest_subset = Digest::one(u1.clone(), 0, 0);
            let bytes = marshal_view_digest(Some(&view_two), Some(&digest_subset));
            assert_eq!(bytes, vec.bytes);
        }
        {
            let vec = vectors::get(&v, "viewdigest.view_only");
            let bytes = marshal_view_digest(Some(&view_singleton), None);
            assert_eq!(bytes, vec.bytes);
        }
    }

    /// `MergeView` and the `MERGE_VIEW`-flagged `VIEW` payload, byte for byte against the jar,
    /// including a subgroup member that is not in the merged view (written as an address).
    #[test]
    fn merge_view_matches_the_jar() {
        let v = vectors::load();
        let u1 = u1();
        let u2 = u2();
        let u3 = Address::uuid(0x4142434445464748, 0x5152535455565758);
        let merged = View::create(u1.clone(), 1, vec![u1.clone(), u2.clone()]);

        let mv = MergeView::new(merged.clone(), vec![View::create(u1.clone(), 0, vec![u1.clone()]), View::create(u2.clone(), 5, vec![u2.clone()])]);
        let vec = vectors::get(&v, "mergeview.two");
        let mut out = Vec::new();
        mv.write_to(&mut out);
        assert_eq!(out, vec.bytes);
        assert_eq!(out.len() as i64, vec.size);
        assert_eq!(mv.serialized_size() as i64, vec.size);
        assert_eq!(MergeView::read_from(&mut WireReader::new(&vec.bytes)), mv);

        let mvf = MergeView::new(
            merged.clone(),
            vec![View::create(u1.clone(), 3, vec![u1.clone(), u3.clone()]), View::create(u3.clone(), 4, vec![u3.clone()])],
        );
        let vec = vectors::get(&v, "mergeview.foreign");
        let mut out = Vec::new();
        mvf.write_to(&mut out);
        assert_eq!(out, vec.bytes);
        assert_eq!(mvf.serialized_size() as i64, vec.size);
        assert_eq!(MergeView::read_from(&mut WireReader::new(&vec.bytes)), mvf);

        let digest = Digest { entries: vec![(u1.clone(), 5, 10), (u2.clone(), 0, 0)] };
        let vec = vectors::get(&v, "viewdigest.merge");
        let bytes = marshal_view_digest_ext(Some(&merged), Some(&mv.subgroups), Some(&digest));
        assert_eq!(bytes, vec.bytes);
        let (view, subgroups, d) = read_view_and_digest_ext(&vec.bytes).unwrap();
        assert_eq!(view, Some(merged));
        assert_eq!(subgroups, Some(mv.subgroups));
        assert_eq!(d, Some(digest));
    }

    // ---- DeltaView (jgroups 5.2.28, DeltaViewTest / GMS.createViewFromDeltaView) ----

    fn u3() -> Address {
        Address::uuid(0x4142434445464748, 0x5152535455565758)
    }

    /// `DeltaView.writeTo`/`serializedSize` and the `DELTA_VIEW`-flagged `VIEW` payload, byte for
    /// byte against the jar.
    #[test]
    fn delta_view_matches_the_jar() {
        let v = vectors::load();
        let (u1, u2, u3) = (u1(), u2(), u3());
        let join = DeltaView::new(ViewId { creator: Some(u1.clone()), id: 2 }, ViewId { creator: Some(u1.clone()), id: 1 }, vec![], vec![u3.clone()]);
        let leave = DeltaView::new(ViewId { creator: Some(u1.clone()), id: 3 }, ViewId { creator: Some(u1.clone()), id: 2 }, vec![u2.clone()], vec![]);
        for (name, dv) in [("deltaview.join", &join), ("deltaview.leave", &leave)] {
            let vec = vectors::get(&v, name);
            let mut out = Vec::new();
            dv.write_to(&mut out);
            assert_eq!(out, vec.bytes, "{name}");
            assert_eq!(dv.serialized_size() as i64, vec.size, "{name}");
            assert_eq!(&DeltaView::try_read_from(&mut WireReader::new(&vec.bytes)).unwrap(), dv, "{name}");
        }

        let vec = vectors::get(&v, "viewdigest.delta");
        assert_eq!(marshal_delta_view(&join, None), vec.bytes);
        assert_eq!(read_view_and_digest_full(&vec.bytes).unwrap(), (Some(ReceivedView::Delta(join)), None));

        let digest = Digest { entries: vec![(u1.clone(), 5, 10), (u2.clone(), 0, 0)] };
        let vec = vectors::get(&v, "viewdigest.delta.digest");
        assert_eq!(marshal_delta_view(&leave, Some(&digest)), vec.bytes);
        assert_eq!(read_view_and_digest_full(&vec.bytes).unwrap(), (Some(ReceivedView::Delta(leave)), Some(digest)));
        // a caller that needs a full view gets an error, not a panic
        assert!(read_view_and_digest(&vec.bytes).is_err());
    }

    /// The `VIEW` payload our coordinator multicasts with `use_delta_views`
    /// (`DeltaView::create` + `marshal_delta_view`, the two calls `Gms::cast_view_change_and_send_join_rsps`
    /// makes) equals the real jar's `GMS.marshal(GMS.createDeltaView(current, next), digest)`, for a join and
    /// a leave; the coordinator's first view is the full one (`marshal_view_digest`).
    #[test]
    fn coordinator_delta_view_payload_matches_the_jar() {
        let v = vectors::load();
        let (u1, u2, u3) = (u1(), u2(), u3());
        let view = |id, members: &[&Address]| View { view_id: ViewId { creator: Some(u1.clone()), id }, members: members.iter().map(|a| (*a).clone()).collect() };
        let two = view(1, &[&u1, &u2]);
        let three = view(2, &[&u1, &u2, &u3]);
        let back = view(3, &[&u1, &u3]);
        let join_digest = Digest { entries: vec![(u1.clone(), 5, 10), (u2.clone(), 0, 0), (u3.clone(), 0, 0)] };
        let leave_digest = Digest { entries: vec![(u1.clone(), 0, 0), (u3.clone(), 0, 0)] };
        let join = marshal_delta_view(&DeltaView::create(&two, &three), Some(&join_digest));
        assert_eq!(join, vectors::get(&v, "gms.castview.delta.join").bytes);
        let leave = marshal_delta_view(&DeltaView::create(&three, &back), Some(&leave_digest));
        assert_eq!(leave, vectors::get(&v, "gms.castview.delta.leave").bytes);
        assert_eq!(marshal_view_digest(Some(&three), Some(&join_digest)), vectors::get(&v, "gms.castview.full.join").bytes);
        // and a member holding the reference view rebuilds the full one from what we sent
        let Ok((Some(ReceivedView::Delta(d)), Some(_))) = read_view_and_digest_full(&join) else { panic!("not a delta view") };
        assert_eq!(d.apply(Some(&two)).unwrap(), three);
    }

    /// `GMS.createDeltaView` then `GMS.createViewFromDeltaView` rebuild the next view, member order
    /// included (leavers removed, joiners appended).
    #[test]
    fn delta_view_rebuilds_the_next_view() {
        let (u1, u2, u3) = (u1(), u2(), u3());
        let current = View::create(u1.clone(), 1, vec![u1.clone(), u2.clone()]);
        let next = View::create(u1.clone(), 2, vec![u1.clone(), u3.clone()]);
        let delta = DeltaView::create(&current, &next);
        assert_eq!(delta.left_members, Some(vec![u2.clone()]));
        assert_eq!(delta.new_members, Some(vec![u3.clone()]));
        assert_eq!(delta.ref_view_id, current.view_id);
        assert_eq!(delta.apply(Some(&current)).unwrap(), next);

        // refers to another view than the one we hold -> error (the caller asks for the full view)
        let other = View::create(u1.clone(), 7, vec![u1.clone()]);
        assert!(delta.apply(Some(&other)).is_err());
        // no current view at all
        assert!(delta.apply(None).is_err());
        // a null array on the wire fails when applied, not when read
        let null_left = DeltaView { left_members: None, ..delta.clone() };
        let mut out = Vec::new();
        null_left.write_to(&mut out);
        let back = DeltaView::try_read_from(&mut WireReader::new(&out)).unwrap();
        assert_eq!(back, null_left);
        assert!(back.apply(Some(&current)).is_err());
    }

    /// Nothing a peer sends can panic the view codec: every truncation of every vector, and a
    /// handful of hostile shapes, come back as `Ok` or `Err`.
    #[test]
    fn malformed_view_payloads_are_errors_not_panics() {
        let v = vectors::load();
        for name in ["viewdigest.delta", "viewdigest.delta.digest", "viewdigest.merge", "viewdigest.same_members", "viewdigest.different_members", "viewdigest.view_only"] {
            let bytes = vectors::get(&v, name).bytes.clone();
            assert!(read_view_and_digest_full(&bytes).is_ok(), "{name}");
            for cut in 0..bytes.len() {
                let _ = read_view_and_digest_full(&bytes[..cut]);
            }
            for i in 0..bytes.len() {
                let mut flipped = bytes.clone();
                flipped[i] ^= 0xff;
                let _ = read_view_and_digest_full(&flipped);
            }
        }
        for name in ["joinrsp.full", "joinrsp.fail"] {
            let bytes = vectors::get(&v, name).bytes.clone();
            for cut in 0..bytes.len() {
                assert!(JoinRsp::try_read_from(&mut WireReader::new(&bytes[..cut])).is_err(), "{name} cut at {cut}");
            }
        }
        // delta flag, no view bytes
        assert!(read_view_and_digest_full(&[0x00, 0x09]).is_err());
        // digest that borrows its members from a delta view (a null member array upstream)
        let bytes = vectors::get(&v, "viewdigest.delta").bytes.clone();
        let mut no_addrs = bytes.clone();
        no_addrs[1] = (no_addrs[1] & !0x10) | 0x02;
        assert!(read_view_and_digest_full(&no_addrs).is_err());
        // a member count far beyond the bytes left (null creator, id 0, count 32767)
        assert!(read_view_and_digest_full(&[0x00, 0x01, 0x01, 0x00, 0x7f, 0xff]).is_err());
        // an unknown address kind
        assert!(read_view_and_digest_full(&[0x00, 0x01, 0x00, 0x7f, 0xff]).is_err());
        // a compressed-long length byte beyond 8
        assert!(read_view_and_digest_full(&[0x00, 0x01, 0x01, 0x0f, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]).is_err());
    }

    // ---- ViewTest (jgroups 5.2.28) ----

    fn addrs() -> Vec<Address> {
        (1..=9).map(|n| Address::uuid(n, n * 100)).collect()
    }

    fn big_view() -> (Vec<Address>, View) {
        let a = addrs();
        let view = View::create(a[0].clone(), 34, a[..8].to_vec());
        (a, view)
    }

    // ViewTest#testContainsMember
    #[test]
    fn contains_member_for_every_member_but_not_a_stranger() {
        let (a, view) = big_view();
        for m in &a[..6] {
            assert!(view.contains_member(m), "Member should be in view");
        }
        assert!(!view.contains_member(&a[8]), "Member should not be in view");
    }

    // ViewTest#testContainsMembers
    #[test]
    fn contains_members_ignores_order_and_requires_all() {
        let (a, view) = big_view();
        assert!(view.contains_members(&[a[1].clone(), a[0].clone(), a[3].clone(), a[2].clone()]));
        assert!(!view.contains_members(&[a[0].clone(), a[1].clone(), a[3].clone(), a[5].clone(), Address::uuid(99, 99)]));
        let v = View::create(a[0].clone(), 1, vec![a[0].clone(), a[1].clone(), a[2].clone()]);
        assert!(v.contains_members(&[a[0].clone(), a[1].clone()]));
        let v = View::create(a[0].clone(), 2, vec![a[0].clone(), a[1].clone()]);
        assert!(!v.contains_members(&[a[0].clone(), a[1].clone(), a[2].clone()]));
    }

    // ViewTest#testEqualsCreator
    #[test]
    fn creator_is_the_creating_member() {
        let (a, view) = big_view();
        assert_eq!(view.creator(), Some(&a[0]));
        assert_ne!(view.creator(), Some(&a[3]));
    }

    // ViewTest#testEquals, #testEquals2
    #[test]
    fn equality_compares_view_id_and_members() {
        let (a, view) = big_view();
        assert_eq!(view, view.clone());
        let v1 = View { view_id: ViewId { creator: Some(a[0].clone()), id: 12345 }, members: a[..8].to_vec() };
        let v2 = View::create(a[0].clone(), 12345, a[..8].to_vec());
        assert_eq!(v1, v2);
        assert_ne!(v1, View::create(a[0].clone(), 12543, a[..8].to_vec()));
    }

    // ViewTest#testSameViews
    #[test]
    fn same_views_compares_the_view_ids_only() {
        let a = addrs();
        let m = vec![a[0].clone(), a[1].clone(), a[2].clone()];
        let v1 = View::create(a[0].clone(), 1, m.clone());
        let v2 = View::create(a[0].clone(), 1, m.clone());
        let v3 = View::create(a[0].clone(), 1, m.clone());
        let v4 = View::create(a[1].clone(), 1, m);
        assert!(View::same_views(&[&v1, &v2, &v3]));
        assert!(!View::same_views(&[&v1, &v2, &v3, &v4]));
    }

    fn v(creator: &Address, id: i64, members: &[&Address]) -> View {
        View::create(creator.clone(), id, members.iter().map(|m| (*m).clone()).collect())
    }

    // ViewTest#testDiff
    #[test]
    fn diff_from_nothing_is_everyone_joining() {
        let a = addrs();
        let two = v(&a[0], 1, &[&a[0], &a[1], &a[2]]);
        let (joined, left) = View::diff(None, &two);
        assert_eq!(joined, vec![a[0].clone(), a[1].clone(), a[2].clone()]);
        assert!(left.is_empty());
    }

    // ViewTest#testDiff2
    #[test]
    fn diff_2_reports_joiners_in_the_new_views_order() {
        let a = addrs();
        let one = v(&a[0], 1, &[&a[0], &a[1], &a[2]]);
        let two = v(&a[0], 2, &[&a[0], &a[1], &a[2], &a[3], &a[4]]);
        let (joined, left) = View::diff(Some(&one), &two);
        assert_eq!(joined, vec![a[3].clone(), a[4].clone()]);
        assert!(left.is_empty());
    }

    // ViewTest#testDiff3
    #[test]
    fn diff_3_reports_leavers_in_the_old_views_order() {
        let a = addrs();
        let one = v(&a[0], 1, &[&a[0], &a[1], &a[2], &a[3], &a[4]]);
        let two = v(&a[0], 2, &[&a[0], &a[1], &a[2]]);
        let (joined, left) = View::diff(Some(&one), &two);
        assert!(joined.is_empty());
        assert_eq!(left, vec![a[3].clone(), a[4].clone()]);
    }

    // ViewTest#testDiff4
    #[test]
    fn diff_4_joiners_and_leavers_at_once() {
        let a = addrs();
        let one = v(&a[0], 1, &[&a[0], &a[1], &a[2], &a[3], &a[4], &a[5], &a[6]]);
        let two = v(&a[1], 2, &[&a[1], &a[2], &a[3], &a[6], &a[7], &a[8]]);
        let (joined, left) = View::diff(Some(&one), &two);
        assert_eq!(joined, vec![a[7].clone(), a[8].clone()]);
        assert_eq!(left, vec![a[0].clone(), a[4].clone(), a[5].clone()]);
    }

    // ViewTest#testDiffSameView
    #[test]
    fn diff_of_identical_views_is_empty() {
        let a = addrs();
        let ms: Vec<&Address> = a[..7].iter().collect();
        let (one, two) = (v(&a[0], 1, &ms), v(&a[0], 1, &ms));
        let (joined, left) = View::diff(Some(&one), &two);
        assert!(joined.is_empty() && left.is_empty());
    }

    // ViewTest#testSameMembers
    #[test]
    fn same_members_ignores_order_and_view_id() {
        let a = addrs();
        let one = v(&a[0], 1, &[&a[0], &a[1], &a[2], &a[3], &a[4]]);
        let two = v(&a[0], 2, &[&a[0], &a[1], &a[2]]);
        assert!(!View::same_members(&one, &two));

        let two = v(&a[0], 20, &[&a[0], &a[1], &a[2], &a[3], &a[4]]);
        assert!(View::same_members(&one, &two));
        assert!(View::same_members(&two, &one));
        assert!(View::same_members(&two, &two));

        let one = v(&a[1], 5, &[&a[0], &a[1], &a[2]]);
        let two = v(&a[2], 5, &[&a[1], &a[0], &a[2]]);
        assert!(View::same_members(&one, &two));

        let two = v(&a[2], 5, &[&a[1], &a[0], &a[2], &a[3]]);
        assert!(!View::same_members(&one, &two));

        let one = v(&a[1], 5, &[&a[3], &a[1], &a[2], &a[0]]);
        let two = v(&a[2], 5, &[&a[1], &a[0], &a[2]]);
        assert!(!View::same_members(&one, &two));
        assert!(!View::same_members(&two, &one));
    }

    // ViewTest#testSameMembersOrdered
    #[test]
    fn same_members_ordered_requires_the_same_order() {
        let a = addrs();
        let one = v(&a[0], 1, &[&a[0], &a[1], &a[2], &a[3], &a[4]]);
        let two = v(&a[0], 2, &[&a[0], &a[1], &a[2]]);
        assert!(!View::same_members_ordered(&one, &two));
        assert!(View::same_members_ordered(&one, &one));
        let two = v(&a[0], 5, &[&a[0], &a[1], &a[2], &a[3], &a[4]]);
        assert!(View::same_members_ordered(&one, &two)); // view id is not matched
        let swapped = v(&a[0], 5, &[&a[1], &a[0], &a[2], &a[3], &a[4]]);
        assert!(!View::same_members_ordered(&one, &swapped));
    }

    // ViewTest#testLeftMembers, #testLeftMembers2
    #[test]
    fn left_members_are_the_old_members_missing_from_the_new_view() {
        let a = addrs();
        let one = v(&a[0], 1, &[&a[0], &a[1], &a[2], &a[3]]);
        let two = v(&a[1], 2, &[&a[2], &a[3]]);
        let left = View::left_members(Some(&one), Some(&two)).unwrap();
        assert_eq!(left.len(), 2);
        assert!(left.contains(&a[0]) && left.contains(&a[1]));

        let two = v(&a[1], 2, &[&a[2], &a[3], &a[0], &a[1]]);
        assert!(View::left_members(Some(&one), Some(&two)).unwrap().is_empty());
        assert!(View::left_members(None, Some(&two)).is_none());
    }

    // ViewTest#testNewMembers, #testNewMembers2
    #[test]
    fn new_members_are_the_new_views_members_missing_from_the_old_one() {
        let a = addrs();
        let one = v(&a[0], 1, &[&a[0], &a[1]]);
        let two = v(&a[1], 2, &[&a[0], &a[1], &a[2], &a[3]]);
        let new_mbrs = View::new_members(Some(&one), Some(&two)).unwrap();
        assert_eq!(new_mbrs.len(), 2);
        assert!(new_mbrs.contains(&a[2]) && new_mbrs.contains(&a[3]));

        let two = v(&a[1], 2, &[&a[0], &a[1]]);
        let new_mbrs = View::new_members(None, Some(&two)).unwrap();
        assert_eq!(new_mbrs.len(), 2);
        assert!(new_mbrs.contains(&a[0]) && new_mbrs.contains(&a[1]));
    }

    // ViewTest#testIterator
    #[test]
    fn iterating_a_view_yields_its_members_in_order() {
        let (a, view) = big_view();
        let mut mbrs = Vec::new();
        for m in view.members.iter() {
            mbrs.push(m.clone());
        }
        assert_eq!(mbrs, a[..8].to_vec());
    }
}
