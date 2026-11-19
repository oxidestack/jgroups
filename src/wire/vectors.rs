//! Test-only loader for `tests/vectors/wire.vectors`: byte vectors captured from the real
//! `jgroups-5.2.28.Final` jar by the Java harness in `upstream-tools/vector-harness` (see
//! `upstream-tools/README.md`).
//!
//! Two line shapes:
//! - `name=size:hexbytes` — a wire vector. `size` is the value upstream's `serializedSize()` (or
//!   equivalent) call returned, `-1` when the vector has no single `serializedSize` counterpart
//!   (e.g. a full message frame). `hexbytes` is the exact byte sequence upstream's `writeTo`
//!   produced. [`get`] returns these.
//! - `name=text` — a scalar (used for `Version`, which is a `short` and some booleans/strings, not
//!   a byte sequence). [`get_raw`] returns these.
//!
//! A line is parsed as a wire vector only when the text right of `=` splits on the *first* `:`
//! into an integer and a valid same-length-parity hex string; everything else is a scalar.

use std::collections::HashMap;

const RAW: &str = include_str!("../../tests/vectors/wire.vectors");

pub struct Vector {
    pub size: i64,
    pub bytes: Vec<u8>,
}

pub enum Entry {
    Wire(Vector),
    Scalar(String),
}

pub fn load() -> HashMap<&'static str, Entry> {
    let mut map = HashMap::new();
    for line in RAW.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let (name, rest) = line.split_once('=').expect("vector line missing '='");
        let entry = match rest.split_once(':') {
            Some((size_str, hex)) if size_str.parse::<i64>().is_ok() && is_hex(hex) => {
                let size: i64 = size_str.parse().unwrap();
                Entry::Wire(Vector { size, bytes: decode_hex(hex) })
            }
            _ => Entry::Scalar(rest.to_string()),
        };
        map.insert(name, entry);
    }
    map
}

fn is_hex(s: &str) -> bool {
    s.len().is_multiple_of(2) && s.chars().all(|c| c.is_ascii_hexdigit())
}

fn decode_hex(hex: &str) -> Vec<u8> {
    (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).expect("invalid hex byte"))
        .collect()
}

/// Look up a wire vector by name, panicking with a helpful message if it is missing or is a
/// scalar line.
pub fn get<'a>(map: &'a HashMap<&'static str, Entry>, name: &str) -> &'a Vector {
    match map.get(name) {
        Some(Entry::Wire(v)) => v,
        Some(Entry::Scalar(_)) => panic!("vector {name} is a scalar, not a wire vector"),
        None => panic!("no such vector: {name}"),
    }
}

/// Look up a scalar line by name.
pub fn get_raw<'a>(map: &'a HashMap<&'static str, Entry>, name: &str) -> &'a str {
    match map.get(name) {
        Some(Entry::Scalar(s)) => s,
        Some(Entry::Wire(_)) => panic!("vector {name} is a wire vector, not a scalar"),
        None => panic!("no such vector: {name}"),
    }
}
