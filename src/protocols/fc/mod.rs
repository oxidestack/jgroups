//! Credit-based flow control: `UFC` (unicast) and `MFC` (multicast), sharing the bookkeeping and
//! wire behaviour `FlowControl.java` (the common abstract base upstream) defines. Rust has no
//! implementation inheritance, so that shared behaviour lives here as free functions/types
//! (`FlowControlConfig`, `ReceivedCredits`) composed into by `ufc::Ufc`/`mfc::Mfc` rather than a
//! base-class port.
//!
//! **What's ported**: `FlowControl.down(Message)`/`up(Message)`'s NO_FC bypass and
//! multicast/unicast routing (`handleMulticastMessage()`), `adjustCredit`/`handleCreditRequest`
//! (the receiver side, shared verbatim by both — [`ReceivedCredits`](flow_control::ReceivedCredits)), `sendCredit`/
//! `sendCreditRequest`'s message shape (`LongMessage` + `FcHeader`, `OOB|DONT_BUNDLE`), and
//! `handleViewChange`'s "add new members at full credits, drop departed ones" bookkeeping (driven
//! by [`crate::protocols::gms::ViewListener`] — see that trait's doc comment for why).
//!
//! **What's not ported**: `UFC_NB`/`MFC_NB` (the non-blocking, queueing variants — not used by
//! MockServer's stacks), the `up(MessageBatch)` overload (no `MessageBatch` delivery path exists in
//! this crate yet — every message here travels through `AsyncProtocol::up_message`/`down_message`
//! one at a time), and the `@ManagedAttribute`/JMX statistics beyond `num_blockings` (nothing in
//! this crate reads the rest).

pub mod flow_control;
pub mod mfc;
pub mod ufc;

pub use flow_control::FlowControlConfig;
pub use mfc::Mfc;
pub use ufc::Ufc;
