//! `MaxOneThreadPerSender` (`util/MaxOneThreadPerSender.java`, `SubmitToThreadPool.java`), TP's default message
//! processing policy (`TP.msg_processing_policy`, `TP.java:399-400`), applied where this crate hands a message to the
//! application.
//!
//! Upstream runs the whole up path of a message on the thread the policy picks: an OOB message on any pool thread
//! (`SubmitToThreadPool.process`), a regular one on at most one thread per sender, separately for multicasts and
//! unicasts (`MessageTable mcasts, ucasts`), queueing what arrives while that thread is busy. Here the protocols run on
//! the channel's single receive task, which never blocks on the application; only the delivery is dispatched: an OOB
//! message on a task of its own, a regular one into the ordered queue of its `(sender, multicast)` entry, drained by
//! one task. So a receiver blocked on a regular message from A holds back A's later regular messages of the same kind
//! and nothing else, as upstream (`OOBTest#testNonBlockingUnicastOOBMessage`). Loopback takes the same policy
//! (`MaxOneThreadPerSender.loopback`, `TP.loopback`, `TP.java:1116-1126`).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use tokio::sync::mpsc;

use crate::stack::BoxFuture;
use crate::wire::address::Address;
use crate::wire::message::{self, Message};

/// Delivers one message to the application (`JChannel.up(Message)`).
pub type Deliver = Arc<dyn Fn(Message) -> BoxFuture<'static, ()> + Send + Sync>;

/// `MaxOneThreadPerSender`: delivers one sender's messages in order, one at a time, while messages of different senders are delivered concurrently.
pub struct MaxOneThreadPerSender {
    deliver: Deliver,
    mcasts: Mutex<HashMap<Address, mpsc::UnboundedSender<Message>>>,
    ucasts: Mutex<HashMap<Address, mpsc::UnboundedSender<Message>>>,
}

impl MaxOneThreadPerSender {
    /// A policy that hands each message to `deliver`.
    pub fn new(deliver: Deliver) -> MaxOneThreadPerSender {
        MaxOneThreadPerSender { deliver, mcasts: Mutex::new(HashMap::new()), ucasts: Mutex::new(HashMap::new()) }
    }

    /// `process(Message, boolean oob)` and `loopback(Message, boolean oob)`, `MaxOneThreadPerSender.java:53-65`.
    pub fn process(&self, msg: Message) {
        if msg.is_flag_set(message::FLAG_OOB) {
            tokio::spawn((self.deliver)(msg)); // SubmitToThreadPool.process
            return;
        }
        let Some(sender) = msg.src.clone() else { return };
        let table = if msg.dest.is_none() { &self.mcasts } else { &self.ucasts };
        let mut map = table.lock().unwrap_or_else(|e| e.into_inner());
        let mut msg = Some(msg);
        if let Some(tx) = map.get(&sender) {
            match tx.send(msg.take().unwrap()) {
                Ok(()) => return,
                Err(mpsc::error::SendError(m)) => msg = Some(m), // its task is gone (runtime shutting down): replace it
            }
        }
        let (tx, mut rx) = mpsc::unbounded_channel::<Message>();
        let deliver = self.deliver.clone();
        tokio::spawn(async move {
            while let Some(m) = rx.recv().await {
                deliver(m).await;
            }
        });
        let _ = tx.send(msg.take().unwrap());
        map.insert(sender, tx);
    }

    /// `viewChange(List<Address>)`, `MaxOneThreadPerSender.java:74-77` (`MessageTable.viewChange`: `retainAll`). A
    /// removed entry's task still delivers what it had queued, then ends.
    pub fn view_change(&self, members: &[Address]) {
        for table in [&self.mcasts, &self.ucasts] {
            table.lock().unwrap_or_else(|e| e.into_inner()).retain(|k, _| members.contains(k));
        }
    }

    /// `destroy()`, `MaxOneThreadPerSender.java:48-51`.
    pub fn destroy(&self) {
        for table in [&self.mcasts, &self.ucasts] {
            table.lock().unwrap_or_else(|e| e.into_inner()).clear();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::sync::Notify;

    fn msg(src: &Address, dest: Option<Address>, n: u8, oob: bool) -> Message {
        let mut m = Message::bytes(dest, vec![n]);
        m.src = Some(src.clone());
        if oob {
            m.set_flag(message::FLAG_OOB);
        }
        m
    }

    fn body(m: &Message) -> u8 {
        match &m.payload {
            message::Payload::Bytes { array: Some(b), offset, .. } => b[*offset],
            _ => unreachable!(),
        }
    }

    /// A delivery that blocks on payload 1 until released, recording the order of everything delivered.
    fn recorder(gate: Arc<Notify>, out: Arc<Mutex<Vec<u8>>>) -> Deliver {
        Arc::new(move |m: Message| {
            let gate = gate.clone();
            let out = out.clone();
            Box::pin(async move {
                if body(&m) == 1 {
                    gate.notified().await;
                }
                out.lock().unwrap().push(body(&m));
            })
        })
    }

    #[tokio::test]
    async fn a_blocked_regular_message_holds_back_only_its_own_senders_regular_queue() {
        let (a, b) = (Address::random_uuid(), Address::random_uuid());
        let gate = Arc::new(Notify::new());
        let out = Arc::new(Mutex::new(Vec::new()));
        let p = MaxOneThreadPerSender::new(recorder(gate.clone(), out.clone()));
        p.process(msg(&a, None, 1, false)); // blocks A's multicast entry
        p.process(msg(&a, None, 2, false)); // queued behind 1
        p.process(msg(&a, None, 3, true)); // OOB: not held back
        p.process(msg(&a, Some(b.clone()), 4, false)); // A's unicasts are another entry
        p.process(msg(&b, None, 5, false)); // another sender
        tokio::time::sleep(Duration::from_millis(100)).await;
        let mut first = out.lock().unwrap().clone();
        first.sort();
        assert_eq!(first, vec![3, 4, 5]);
        gate.notify_one();
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(out.lock().unwrap()[3..], [1, 2]);
    }

    #[tokio::test]
    async fn view_change_drops_the_entries_of_departed_senders() {
        let (a, b) = (Address::random_uuid(), Address::random_uuid());
        let out = Arc::new(Mutex::new(Vec::new()));
        let p = MaxOneThreadPerSender::new(recorder(Arc::new(Notify::new()), out.clone()));
        p.process(msg(&a, None, 2, false));
        p.process(msg(&b, Some(a.clone()), 3, false));
        p.view_change(std::slice::from_ref(&a));
        assert!(p.mcasts.lock().unwrap().contains_key(&a));
        assert!(p.ucasts.lock().unwrap().is_empty());
        tokio::time::sleep(Duration::from_millis(50)).await;
        let mut got = out.lock().unwrap().clone();
        got.sort();
        assert_eq!(got, vec![2, 3]);
    }
}
