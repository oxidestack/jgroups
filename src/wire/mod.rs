//! The JGroups binary wire format: primitives, streams, addresses, messages, headers and the
//! protocol-id / header-magic-id tables.
//!
//! Scope: binary encoding only. No protocols, no networking, no
//! threading — those live in other modules.

pub mod address;
pub mod batch;
pub mod bits;
pub mod header;
pub mod ids;
pub mod message;
pub mod stream;
pub mod version;
pub mod view;

#[cfg(test)]
pub(crate) mod vectors;
