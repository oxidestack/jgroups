//! `Merger` (`protocols/pbcast/Merger.java`) as `impl Gms` (before it `Gms::merge` elected the
//! merge leader and had it install the merged view with zero digests directly in every member).
//!
//! The protocol, as upstream runs it:
//! 1. MERGE3 hands a map of the different views it found to GMS (`Event.MERGE`), which queues a `MERGE` request
//!    ([`Gms::merge`]). The request queue calls `Gms::merge_impl` (`Merger.merge`): nothing to do when the views are the
//!    same or a merge is already running; otherwise the smallest of the actual subview coordinators is the merge leader and
//!    only it goes on, in a task of its own (`MergeTask`).
//! 2. The leader makes a merge id (installing it suspends its request queue and starts the merge killer, which cancels the
//!    merge after `2 * merge_timeout`) and sends `MERGE_REQ` (carrying the members it expects) to every subgroup coordinator,
//!    itself included.
//! 3. A subgroup coordinator accepting the request (it adopts the merge id; a different merge in progress makes it
//!    ignore the request) fetches a digest from each member of its subgroup (`GET_DIGEST_REQ`/`GET_DIGEST_RSP`, waiting at
//!    most `merge_timeout / 2`) and answers with its view (the members that are also in its view) and the digest
//!    (`MERGE_RSP`). A failure cancels its side and rejects.
//! 4. The leader waits `merge_timeout` for the responses, drops the coordinators that did not answer, consolidates one
//!    `MergeView` and one digest ([`super::merger::consolidate_merge_data`]) and sends it to every subgroup coordinator as
//!    `INSTALL_MERGE_VIEW`, itself last. Any error sends `CANCEL_MERGE` to all of them.
//! 5. Each coordinator whose merge id matches casts the merged view to everybody (expecting `VIEW_ACK`s from its own
//!    subgroup only) and ends the merge: the queue resumes. The other members cancel their side when they install a
//!    `MergeView` they do not coordinate.
//!
//! What differs from upstream, all because of how this crate is built: a message addressed to this node is handled
//! directly instead of looping back through the transport; the handlers that wait for answers (`MERGE_REQ`,
//! `INSTALL_MERGE_VIEW`) run in tasks of their own, never on the receive task that delivers the answers; `GET_DIGEST_REQ` is
//! sent to the members of the subgroup one by one rather than multicast through NAKACK2, so it takes no multicast seqno;
//! a `MERGE_RSP` with a rejection has no payload and is dropped on arrival exactly as upstream drops it
//! (`GMS.handle`: `tuple == null`), leaving the leader to time out on that coordinator; FLUSH and `SUSPEND_STABLE` are
//! not ported (no MockServer stack has FLUSH; STABLE keeps running during a merge).

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::task::JoinHandle;

use super::merger::{
    consolidate_merge_data, determine_merge_coords_with_members, determine_merge_leader, sanitize_views, MergeData,
};
use super::protocol::Gms;
use crate::util::digest as digest_ops;
use crate::util::response_collector::ResponseCollector;
use crate::wire::address::{self, Address};
use crate::wire::header::{
    GmsHeader, HeaderValue, MergeId, GMS_CANCEL_MERGE, GMS_GET_DIGEST_REQ, GMS_GET_DIGEST_RSP, GMS_INSTALL_MERGE_VIEW, GMS_MERGE_REQ, GMS_MERGE_RSP,
};
use crate::wire::ids::PROT_ID_GMS;
use crate::wire::message::{self, Message};
use crate::wire::view::{marshal_view_digest, marshal_view_digest_ext, read_view_and_digest_ext, Digest, View};

/// `Merger`'s fields.
pub(super) struct MergeState {
    inner: Mutex<Inner>,
    next_id: AtomicI32,
    /// `Merger.merge_rsps`: `MERGE_RSP`s, keyed by the subgroup coordinator.
    merge_rsps: ResponseCollector<MergeData>,
    /// `Merger.digest_collector`: `GET_DIGEST_RSP`s, keyed by the subgroup member.
    digest_collector: ResponseCollector<Digest>,
}

struct Inner {
    /// `Merger.merge_id`
    merge_id: Option<MergeId>,
    /// `Merger.merge_id_history`, a `BoundedList(20)` (`null`s included, as `setMergeId` adds them)
    history: VecDeque<Option<MergeId>>,
    merge_killer: Option<JoinHandle<()>>,
    merge_task: Option<JoinHandle<()>>,
}

impl MergeState {
    pub(super) fn new() -> MergeState {
        MergeState {
            inner: Mutex::new(Inner { merge_id: None, history: VecDeque::new(), merge_killer: None, merge_task: None }),
            next_id: AtomicI32::new(1),
            merge_rsps: ResponseCollector::new(),
            digest_collector: ResponseCollector::new(),
        }
    }
}

impl Gms {
    /// `Merger.isMergeInProgress()`.
    pub fn is_merge_in_progress(&self) -> bool {
        self.merge.inner.lock().unwrap().merge_id.is_some()
    }

    /// `Merger.getMergeId()`: testing only.
    pub fn merge_id(&self) -> Option<MergeId> {
        self.merge.inner.lock().unwrap().merge_id.clone()
    }

    /// `Merger.matchMergeId(MergeId)`: `Util.match`, so `None` matches `None`.
    fn match_merge_id(&self, id: Option<&MergeId>) -> bool {
        self.merge.inner.lock().unwrap().merge_id.as_ref() == id
    }

    /// `Merger.setMergeId(expected, new_value)`, `Merger.java:56-72`: only when the current id is `expected`. Installing an
    /// id (not one already used) clears the request queue, discarding JOIN/LEAVE/MERGE requests until the merge is over, and
    /// starts the merge killer.
    pub(super) fn set_merge_id(&self, expected: Option<&MergeId>, new_value: Option<MergeId>) -> bool {
        let start_killer;
        {
            let mut inner = self.merge.inner.lock().unwrap();
            if inner.merge_id.as_ref() != expected {
                return false;
            }
            if let Some(n) = &new_value
                && inner.history.contains(&Some(n.clone()))
            {
                return false;
            }
            inner.history.push_back(new_value.clone());
            while inner.history.len() > 20 {
                inner.history.pop_front();
            }
            inner.merge_id = new_value.clone();
            start_killer = new_value.is_some();
        }
        if start_killer {
            self.requests.suspend();
            self.start_merge_killer(new_value.unwrap());
        }
        true
    }

    /// `Merger.startMergeKiller`: cancels the merge after `2 * merge_timeout`.
    fn start_merge_killer(&self, id: MergeId) {
        let Some(gms) = self.self_ref.get().and_then(std::sync::Weak::upgrade) else { return };
        let mut inner = self.merge.inner.lock().unwrap();
        if inner.merge_killer.as_ref().is_some_and(|h| !h.is_finished()) {
            return;
        }
        let timeout = self.config.merge_timeout * 2;
        inner.merge_killer = Some(tokio::spawn(async move {
            tokio::time::sleep(timeout).await;
            gms.cancel_merge(Some(&id)).await;
        }));
    }

    /// The synchronous part of `Merger.cancelMerge(MergeId)`, `Merger.java:446-455`.
    pub(super) fn cancel_merge_sync(&self, id: Option<&MergeId>) -> bool {
        if !self.set_merge_id(id, None) {
            return false;
        }
        let mut inner = self.merge.inner.lock().unwrap();
        if let Some(t) = inner.merge_task.take() {
            t.abort();
        }
        if let Some(k) = inner.merge_killer.take() {
            k.abort();
        }
        drop(inner);
        self.requests.resume();
        true
    }

    /// `Merger.cancelMerge(MergeId)`: ends the merge `id` (or the absence of one) if it is the current one: the merge task and
    /// killer stop, the collected responses are dropped and the request queue takes requests again.
    pub(super) async fn cancel_merge(&self, id: Option<&MergeId>) {
        if self.cancel_merge_sync(id) {
            self.merge.merge_rsps.reset(&[]).await;
        }
    }

    /// `Merger.forceCancelMerge()`.
    pub(super) async fn force_cancel_merge(&self) {
        let id = self.merge_id();
        if id.is_some() {
            self.cancel_merge(id.as_ref()).await;
        }
    }

    /// `GMS.up(Event.MERGE)`: queues a `MERGE` request with the different views MERGE3 found, keyed by the member that
    /// reported each. A merge needs a view to merge from; the request queue drops it while a merge is running.
    pub async fn merge(&self, views: HashMap<Address, View>) {
        self.add_requests([super::Request::Merge { views }]).await;
    }

    /// `ServerGmsImpl.merge(views)` -> `Merger.merge(views)`, `Merger.java:75-104`.
    pub(super) async fn merge_impl(&self, views: HashMap<Address, View>) {
        if self.state.lock().await.view.is_none() {
            return; // ClientGmsImpl: not a member of anything yet
        }
        if views.is_empty() {
            return; // the views passed with the MERGE event were empty
        }
        let refs: Vec<&View> = views.values().collect();
        if View::same_views(&refs) {
            return; // MERGE event is ignored because of identical views
        }
        if self.is_merge_in_progress() {
            return;
        }
        let Some(merge_leader) = determine_merge_leader(&views) else { return };
        if merge_leader == self.local_logical {
            self.start_merge_task(views);
        }
        // else: not the merge leader, waiting for the merge leader to start the merge
    }

    /// `MergeTask.start(views)`, `Merger.java:481-501`.
    fn start_merge_task(&self, views: HashMap<Address, View>) {
        let Some(gms) = self.self_ref.get().and_then(std::sync::Weak::upgrade) else { return };
        let mut inner = self.merge.inner.lock().unwrap();
        if inner.merge_task.as_ref().is_some_and(|h| !h.is_finished()) {
            return; // the merge thread is already running
        }
        // the subviews: the views as reported, equal ones once
        let mut subviews: Vec<View> = Vec::new();
        for v in views.values() {
            if !subviews.contains(v) {
                subviews.push(v.clone());
            }
        }
        // now remove all members which don't have us in their view (JGRP-1061)
        let mut sanitized = views;
        sanitize_views(&mut sanitized);
        let coords = determine_merge_coords_with_members(&sanitized);
        inner.merge_task = Some(tokio::spawn(async move { gms.merge_task_run(coords, subviews).await }));
    }

    /// `MergeTask.run()`, `Merger.java:513-540`.
    async fn merge_task_run(self: Arc<Gms>, coords: HashMap<Address, Vec<Address>>, subviews: Vec<View>) {
        // 1. generate the merge id
        let new_merge_id = MergeId { initiator: Some(self.local_logical.clone()), id: self.merge.next_id.fetch_add(1, Ordering::SeqCst) };
        let mut coords_copy: Vec<Address> = coords.keys().cloned().collect();
        if let Err(_reason) = self.merge_task_body(&new_merge_id, coords, subviews, &mut coords_copy).await {
            // the merge is cancelled
            self.send_merge_cancelled_message(&coords_copy, &new_merge_id).await;
            self.cancel_merge(Some(&new_merge_id)).await; // the message above cancels the merge, too: a 2nd line of defence
        }
    }

    /// `MergeTask._run`, `Merger.java:543-590`: the merge leader's side.
    async fn merge_task_body(
        &self, new_merge_id: &MergeId, mut coords: HashMap<Address, Vec<Address>>, subviews: Vec<View>, coords_copy: &mut Vec<Address>,
    ) -> Result<(), String> {
        if !self.set_merge_id(None, Some(new_merge_id.clone())) {
            return Err(format!("failed to set my own merge_id ({:?}) to {new_merge_id:?}", self.merge_id()));
        }

        // fetch the current views and digests from all subgroup coordinators into merge_rsps
        let all = self.get_merge_data_from_subgroup_coordinators(&coords, new_merge_id).await;
        if !all {
            let missing = self.merge.merge_rsps.get_missing().await;
            self.merge.merge_rsps.remove(&missing).await;
            // remove the coordinators that did not answer (so the new view goes only to those that accepted the merge)
            for m in &missing {
                coords.remove(m);
            }
            coords_copy.retain(|c| !missing.contains(c));
        }

        // removeRejectedMergeRequests
        let results = self.merge.merge_rsps.get_results().await;
        let mut rejected: Vec<Address> = Vec::new();
        for (sender, data) in &results {
            if data.merge_rejected {
                rejected.push(data.sender.clone().unwrap_or_else(|| sender.clone()));
            }
        }
        for r in &rejected {
            coords.remove(r);
        }
        self.merge.merge_rsps.remove(&rejected).await;

        let results = self.merge.merge_rsps.get_results().await;
        if results.is_empty() {
            return Err("did not get any merge responses from partition coordinators".into());
        }
        if !coords.contains_key(&self.local_logical) {
            return Err("merge leader rejected merge request".into()); // another member might have invoked a merge req on us
        }

        // combine all views and digests into 1 View/1 Digest
        let mut merge_data: Vec<MergeData> = results.into_values().collect();
        merge_data.sort_by(|a, b| match (&a.sender, &b.sender) {
            (Some(x), Some(y)) => x.compare_to(y),
            _ => std::cmp::Ordering::Equal,
        });
        let Some(combined) = consolidate_merge_data(&self.local_logical, &merge_data, subviews) else {
            return Err("could not consolidate merge".into());
        };

        // send the new view and digest to all coords (including myself): they install it in their subgroup members
        let coord_list: Vec<Address> = coords.keys().cloned().collect();
        self.send_merge_view(&coord_list, &combined, new_merge_id).await;
        Ok(())
    }

    /// `MergeTask.getMergeDataFromSubgroupCoordinators`, `Merger.java:600-627`: MERGE_REQ to every coordinator, then wait
    /// `merge_timeout` for the MERGE_RSPs. True when all answered.
    async fn get_merge_data_from_subgroup_coordinators(&self, coords: &HashMap<Address, Vec<Address>>, new_merge_id: &MergeId) -> bool {
        let targets: Vec<Address> = coords.keys().cloned().collect();
        self.merge.merge_rsps.reset(&targets).await;
        for (coord, mbrs) in coords {
            let mut payload = Vec::new();
            address::write_addresses(mbrs, &mut payload);
            if coord == &self.local_logical {
                // the leader is a subgroup coordinator too: its request is handled here, concurrently with the wait below
                if let Some(gms) = self.self_ref.get().and_then(std::sync::Weak::upgrade) {
                    let (id, me, mbrs) = (new_merge_id.clone(), self.local_logical.clone(), mbrs.clone());
                    tokio::spawn(async move { gms.handle_merge_request(me, id, mbrs).await });
                }
                continue;
            }
            let mut hdr = GmsHeader::with_member(GMS_MERGE_REQ, self.local_logical.clone(), false);
            hdr.merge_id = Some(new_merge_id.clone());
            self.send_gms_bytes(coord, hdr, payload, true).await;
        }
        self.merge.merge_rsps.wait_for_all_responses(self.config.merge_timeout).await
    }

    /// `Merger.sendMergeView(coords, combined_merge_data, merge_id)`, `Merger.java:289-344` (no flush): the consolidated view
    /// and digest to every subgroup coordinator, this node last, so that the other coordinators install it in parallel.
    async fn send_merge_view(&self, coords: &[Address], combined: &MergeData, merge_id: &MergeId) {
        let (Some(view), Some(digest)) = (&combined.view, &combined.digest) else { return };
        let mut coords_copy: Vec<Address> = coords.to_vec();
        if let Some(pos) = coords_copy.iter().position(|c| c == &self.local_logical) {
            let me = coords_copy.remove(pos);
            coords_copy.push(me);
        }
        let payload = marshal_view_digest_ext(Some(view), combined.subgroups.as_deref(), Some(digest));
        for coord in coords_copy {
            if coord == self.local_logical {
                if let Some(gms) = self.self_ref.get().and_then(std::sync::Weak::upgrade) {
                    let data = MergeData { sender: Some(self.local_logical.clone()), ..combined.clone() };
                    let id = merge_id.clone();
                    tokio::spawn(async move { gms.handle_merge_view(data, id).await });
                }
                continue;
            }
            let mut hdr = GmsHeader::new(GMS_INSTALL_MERGE_VIEW);
            hdr.merge_id = Some(merge_id.clone());
            self.send_gms_bytes(&coord, hdr, payload.clone(), false).await;
        }
    }

    /// `Merger.sendMergeCancelledMessage`.
    async fn send_merge_cancelled_message(&self, coords: &[Address], merge_id: &MergeId) {
        for coord in coords {
            if coord == &self.local_logical {
                continue; // cancelMerge(new_merge_id) right after takes care of this node
            }
            let mut hdr = GmsHeader::new(GMS_CANCEL_MERGE);
            hdr.merge_id = Some(merge_id.clone());
            self.send_gms_bytes(coord, hdr, Vec::new(), false).await;
        }
    }

    /// `ServerGmsImpl.handleMergeRequest` -> `Merger.handleMergeRequest`, `Merger.java:109-119`: a failure cancels this side
    /// and rejects.
    pub(super) async fn handle_merge_request(&self, sender: Address, merge_id: MergeId, mbrs: Vec<Address>) {
        if self.state.lock().await.view.is_none() {
            return;
        }
        if let Err(_e) = self.merge_request_inner(&sender, &merge_id, mbrs).await {
            self.cancel_merge(Some(&merge_id)).await;
            self.send_merge_rejected_response(&sender, &merge_id).await;
        }
    }

    /// `Merger._handleMergeRequest`, `Merger.java:260-300`.
    async fn merge_request_inner(&self, sender: &Address, merge_id: &MergeId, mbrs: Vec<Address>) -> Result<(), String> {
        let success = self.match_merge_id(Some(merge_id)) || self.set_merge_id(None, Some(merge_id.clone()));
        if !success {
            return Ok(()); // a merge is already in progress
        }

        // merge the membership of the current view with mbrs, removing members that are not in our current view
        let Some(current) = self.state.lock().await.view.clone() else { return Err("view is null".into()) };
        let mut members: Vec<Address> = Vec::new();
        for m in mbrs {
            if !members.contains(&m) && current.contains_member(&m) {
                members.push(m);
            }
        }
        if !members.contains(&self.local_logical) {
            members.push(self.local_logical.clone());
        }
        let view = View { view_id: current.view_id.clone(), members };

        // we still need to fetch digests from all members, and not just return our own (JGRP-948)
        let digest = self.fetch_digests_from_all_members_in_sub_partition(&view, Some(merge_id)).await;
        if digest.capacity() == 0 {
            return Err("failed fetching digests from subpartition members; dropping merge response".into());
        }
        if sender == &self.local_logical {
            // the leader answering its own MERGE_REQ: what the transport's loopback would deliver
            let data = MergeData::new(Some(self.local_logical.clone()), Some(view), Some(digest));
            self.handle_merge_response(data, Some(merge_id.clone())).await;
            return Ok(());
        }
        let payload = marshal_view_digest(Some(&view), Some(&digest));
        let mut hdr = GmsHeader::new(GMS_MERGE_RSP);
        hdr.merge_id = Some(merge_id.clone());
        self.send_gms_bytes(sender, hdr, payload, true).await;
        Ok(())
    }

    /// `Merger.fetchDigestsFromAllMembersInSubPartition`, `Merger.java:384-420`: one `GET_DIGEST_REQ` multicast to the
    /// members of the subgroup (OOB, `DONT_LOOPBACK`, through NAKACK2 like any message without a destination,
    /// [`send_multicast`](Gms::send_multicast)) and wait for the `GET_DIGEST_RSP`s or
    /// `merge_timeout / 2`; members that did not answer are left out. The own digest is read after the request went
    /// out, so it covers the request's own seqno (`Merger.java:400-401`).
    async fn fetch_digests_from_all_members_in_sub_partition(&self, view: &View, merge_id: Option<&MergeId>) -> Digest {
        let current_mbrs = &view.members;
        // optimisation: if we're the only member, there is nobody to ask
        if current_mbrs.len() == 1 && current_mbrs[0] == self.local_logical {
            let own = self.own_digest().await;
            let mut retval = digest_ops::unset(&view.members);
            digest_ops::set_from(&mut retval, &own);
            return retval;
        }

        let max_wait_time = self.config.merge_timeout / 2;
        self.merge.digest_collector.reset(current_mbrs).await;
        let mut hdr = GmsHeader::new(GMS_GET_DIGEST_REQ);
        hdr.merge_id = merge_id.cloned();
        let mut get_digest_req = Message::empty(None);
        get_digest_req.set_flag(message::FLAG_OOB);
        get_digest_req.transient_flags |= message::TRANSIENT_FLAG_DONT_LOOPBACK;
        get_digest_req.put_header(PROT_ID_GMS, HeaderValue::Gms(hdr));
        self.send_multicast(current_mbrs, get_digest_req).await;
        // add my own digest first: read after the request was sent, so that message is in it
        let own = self.own_digest().await;
        self.merge.digest_collector.add(self.local_logical.clone(), own).await;
        self.merge.digest_collector.wait_for_all_responses(max_wait_time).await;

        let missing = self.merge.digest_collector.get_missing().await;
        let valid: Vec<Address> = current_mbrs.iter().filter(|m| !missing.contains(m)).cloned().collect();
        let mut retval = digest_ops::unset(&valid);
        for d in self.merge.digest_collector.get_results().await.values() {
            digest_ops::set_from(&mut retval, d);
        }
        retval
    }

    /// `down(Event.GET_DIGEST, local_addr)`: this node's own digest entry.
    async fn own_digest(&self) -> Digest {
        match self.nakack2.get() {
            Some(n) => n.own_digest().await,
            None => Digest::one(self.local_logical.clone(), 0, 0),
        }
    }

    /// `GMS.handle(GET_DIGEST_REQ)`, `GMS.java:1043-1062`: only a member of the current view is answered; with a merge id
    /// the request also puts this node into the merge.
    pub(super) async fn handle_get_digest_req(&self, sender: &Address, merge_id: Option<&MergeId>) {
        let Some(view) = self.state.lock().await.view.clone() else { return };
        if !view.contains_member(sender) {
            return;
        }
        if let Some(id) = merge_id
            && !(self.match_merge_id(Some(id)) || self.set_merge_id(None, Some(id.clone())))
        {
            return;
        }
        // fetch only my own digest
        let digest = self.own_digest().await;
        let payload = marshal_view_digest(None, Some(&digest));
        self.send_gms_bytes(sender, GmsHeader::new(GMS_GET_DIGEST_RSP), payload, true).await;
    }

    /// `Merger.handleDigestResponse`.
    pub(super) async fn handle_digest_response(&self, sender: Address, digest: Digest) {
        self.merge.digest_collector.add(sender, digest).await;
    }

    /// `Merger.handleMergeResponse`, `Merger.java:121-128`.
    pub(super) async fn handle_merge_response(&self, data: MergeData, merge_id: Option<MergeId>) {
        if !self.match_merge_id(merge_id.as_ref()) {
            return; // a different merge id: discarded
        }
        if let Some(sender) = data.sender.clone() {
            self.merge.merge_rsps.add(sender, data).await;
        }
    }

    /// `Merger.handleMergeView`, `Merger.java:134-160` (no flush): casts the merged view and digest to everybody and ends
    /// the merge. VIEW_ACKs are expected from this node's *current* members only: another subgroup does not have us in its
    /// view yet and would discard the view without an ack.
    pub(super) async fn handle_merge_view(&self, data: MergeData, merge_id: MergeId) {
        if !self.match_merge_id(Some(&merge_id)) {
            return; // the merge ids don't match: the merge view is discarded
        }
        if let (Some(view), Some(digest)) = (data.view.clone(), data.digest.clone()) {
            let expected_acks: Vec<Address> = self.state.lock().await.view.as_ref().map(|v| v.members.clone()).unwrap_or_default();
            self.cast_view_change_and_send_join_rsps(view, data.subgroups.clone(), digest, &expected_acks, &[], None).await;
            self.wait_for_view_acks().await;
        }
        self.cancel_merge(Some(&merge_id)).await;
    }

    /// `Merger.handleMergeCancelled`.
    pub(super) async fn handle_merge_cancelled(&self, merge_id: Option<MergeId>) {
        self.cancel_merge(merge_id.as_ref()).await;
    }

    /// `Merger.sendMergeRejectedResponse`: an empty MERGE_RSP with `merge_rejected`, which the receiver drops (see the
    /// module doc comment).
    async fn send_merge_rejected_response(&self, sender: &Address, merge_id: &MergeId) {
        let mut hdr = GmsHeader::new(GMS_MERGE_RSP);
        hdr.merge_id = Some(merge_id.clone());
        hdr.merge_rejected = true;
        self.send_gms_bytes(sender, hdr, Vec::new(), true).await;
    }

    /// A GMS message with `payload`, OOB when `oob`, through UNICAST3. Addressed to this node it is not sent anywhere:
    /// everything that is sent to self here (the leader's MERGE_REQ/INSTALL_MERGE_VIEW) is handled by the caller directly.
    async fn send_gms_bytes(&self, dest: &Address, hdr: GmsHeader, payload: Vec<u8>, oob: bool) {
        if dest == &self.local_logical {
            return;
        }
        let Some(physical) = self.tcp.physical_for(dest).await else { return };
        let mut msg = Message::bytes(Some(dest.clone()), payload);
        msg.src = Some(self.local_logical.clone());
        if oob {
            msg.set_flag(message::FLAG_OOB);
        }
        msg.put_header(PROT_ID_GMS, HeaderValue::Gms(hdr));
        self.send_reliable(&physical, msg).await;
    }

    /// Decodes a `MERGE_RSP`/`INSTALL_MERGE_VIEW` payload (`readViewAndDigest`); `None` for an empty one.
    pub(super) fn read_merge_data(sender: Option<Address>, payload: &[u8], merge_rejected: bool) -> Option<MergeData> {
        if payload.is_empty() {
            return None;
        }
        // readViewAndDigest logs a failure and returns null
        let (view, subgroups, digest) = read_view_and_digest_ext(payload).ok()?;
        Some(MergeData { sender, view, subgroups, digest, merge_rejected })
    }

    /// Decodes a `MERGE_REQ` payload (`readMembers`).
    pub(super) fn read_merge_members(payload: &[u8]) -> Option<Vec<Address>> {
        if payload.is_empty() {
            return None;
        }
        address::try_read_addresses(&mut crate::wire::stream::WireReader::new(payload)).ok().flatten()
    }

    /// How long the merge leader waits for responses.
    pub fn merge_timeout(&self) -> Duration {
        self.config.merge_timeout
    }
}
