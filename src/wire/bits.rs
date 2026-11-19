//! Port of `org.jgroups.util.Bits` (upstream `util/Bits.java`): variable-length ("compressed")
//! encodings for `int`/`long`, the paired seqno encoding used by digests, and Java's
//! `DataOutput#writeUTF`/`readUTF` ("modified UTF-8") string format used by header fields.
//!
//! Only the subset JGroups' wire format actually uses is ported: the plain-value byte-array
//! writers (`writeLong`/`writeInt`, `upstream/util/Bits.java:63-231`) are not needed because every
//! wire consumer in this crate uses the compressed forms, and the `ByteBuffer` overloads are
//! folded into the `DataOutput`-style ones since `jgroups-rs` writes into a growable `Vec<u8>`
//! (see `stream.rs`) rather than a fixed `ByteBuffer`.

use crate::wire::stream::{WireError, WireResult};

/// Number of bytes needed to hold `n`'s significant bytes (upstream `bytesRequiredFor(long)`,
/// `Bits.java:706-715`). Negative numbers always need 8 bytes because the sign bit is significant
/// in two's complement.
fn bytes_required_for_long(n: i64) -> u8 {
    let u = n as u64;
    if u >> 56 != 0 {
        8
    } else if u >> 48 != 0 {
        7
    } else if u >> 40 != 0 {
        6
    } else if u >> 32 != 0 {
        5
    } else if u >> 24 != 0 {
        4
    } else if u >> 16 != 0 {
        3
    } else if u >> 8 != 0 {
        2
    } else {
        1
    }
}

fn bytes_required_for_int(n: i32) -> u8 {
    let u = n as u32;
    if u >> 24 != 0 {
        4
    } else if u >> 16 != 0 {
        3
    } else if u >> 8 != 0 {
        2
    } else {
        1
    }
}

fn byte_at_long(n: i64, index: u32) -> u8 {
    (n >> (index * 8)) as u8
}

fn byte_at_int(n: i32, index: u32) -> u8 {
    (n >> (index * 8)) as u8
}

/// `Bits.size(long)`, `Bits.java:296-298`.
pub fn size_long(n: i64) -> usize {
    if n == 0 {
        1
    } else {
        bytes_required_for_long(n) as usize + 1
    }
}

/// `Bits.size(int)`, `Bits.java:190-192`.
pub fn size_int(n: i32) -> usize {
    if n == 0 {
        1
    } else {
        bytes_required_for_int(n) as usize + 1
    }
}

/// `Bits.writeLongCompressed(long, DataOutput)`, `Bits.java:271-280`.
pub fn write_long_compressed(n: i64, out: &mut Vec<u8>) {
    if n == 0 {
        out.push(0);
        return;
    }
    let bytes_needed = bytes_required_for_long(n);
    out.push(bytes_needed);
    for i in 0..bytes_needed as u32 {
        out.push(byte_at_long(n, i));
    }
}

/// `Bits.readLongCompressed(DataInput)`, `Bits.java:282-287`.
pub fn read_long_compressed(inp: &mut crate::wire::stream::WireReader<'_>) -> i64 {
    try_read_long_compressed(inp).unwrap_or_else(|e| panic!("{e}"))
}

/// [`read_long_compressed`] that reports a truncated or impossible encoding as an error.
pub fn try_read_long_compressed(inp: &mut crate::wire::stream::WireReader<'_>) -> WireResult<i64> {
    let len = inp.try_read_byte()?;
    if len == 0 {
        return Ok(0);
    }
    try_make_long(inp, len as usize)
}

/// `Bits.writeIntCompressed(int, DataOutput)`, `Bits.java:140-149`.
pub fn write_int_compressed(n: i32, out: &mut Vec<u8>) {
    if n == 0 {
        out.push(0);
        return;
    }
    let bytes_needed = bytes_required_for_int(n);
    out.push(bytes_needed);
    for i in 0..bytes_needed as u32 {
        out.push(byte_at_int(n, i));
    }
}

/// `Bits.readIntCompressed(DataInput)`, `Bits.java:151-156`.
pub fn read_int_compressed(inp: &mut crate::wire::stream::WireReader<'_>) -> i32 {
    try_read_int_compressed(inp).unwrap_or_else(|e| panic!("{e}"))
}

/// [`read_int_compressed`] that reports a truncated or impossible encoding as an error.
pub fn try_read_int_compressed(inp: &mut crate::wire::stream::WireReader<'_>) -> WireResult<i32> {
    let len = inp.try_read_byte()?;
    if len == 0 {
        return Ok(0);
    }
    try_make_int(inp, len as usize)
}

/// `Bits.makeLong`: more than 8 bytes cannot be a long (upstream's `ArrayIndexOutOfBounds`-free
/// shift just discards them; here the count is rejected so a hostile length byte cannot spin).
fn try_make_long(inp: &mut crate::wire::stream::WireReader<'_>, bytes_to_read: usize) -> WireResult<i64> {
    if bytes_to_read > 8 {
        return Err(WireError::Invalid(format!("a long takes at most 8 bytes, got length {bytes_to_read}")));
    }
    let mut retval: i64 = 0;
    for i in 0..bytes_to_read {
        let b = inp.try_read_byte()?;
        retval |= (b as i64) << (i * 8);
    }
    Ok(retval)
}

fn try_make_int(inp: &mut crate::wire::stream::WireReader<'_>, bytes_to_read: usize) -> WireResult<i32> {
    if bytes_to_read > 4 {
        return Err(WireError::Invalid(format!("an int takes at most 4 bytes, got length {bytes_to_read}")));
    }
    let mut retval: i32 = 0;
    for i in 0..bytes_to_read {
        let b = inp.try_read_byte()?;
        retval |= (b as i32) << (i * 8);
    }
    Ok(retval)
}

fn encode_length(len1: u8, len2: u8) -> u8 {
    (len2 & 0x0f) | (len1 << 4)
}

fn first_nibble(len: u8) -> u8 {
    len >> 4
}

fn second_nibble(len: u8) -> u8 {
    len & !0xf0
}

/// `Bits.size(long, long)` (the paired-seqno size), `Bits.java:437-443`.
pub fn size_long_sequence(hd: i64, hr: i64) -> usize {
    if hd == 0 && hr == 0 {
        return 1;
    }
    let bytes_for_hd = bytes_required_for_long(hd) as usize;
    let bytes_for_delta = bytes_required_for_long(hr - hd) as usize;
    bytes_for_hd + bytes_for_delta + 1
}

/// `Bits.writeLongSequence(long, long, DataOutput)`, `Bits.java:375-396`. `hr` must be `>= hd`.
pub fn write_long_sequence(hd: i64, hr: i64, out: &mut Vec<u8>) {
    assert!(hr >= hd, "hr ({hr}) has to be >= hd ({hd})");
    if hd == 0 && hr == 0 {
        out.push(0);
        return;
    }
    let delta = hr - hd;
    let bytes_for_hd = bytes_required_for_long(hd);
    let bytes_for_delta = bytes_required_for_long(delta);
    out.push(encode_length(bytes_for_hd, bytes_for_delta));
    for i in 0..bytes_for_hd as u32 {
        out.push(byte_at_long(hd, i));
    }
    for i in 0..bytes_for_delta as u32 {
        out.push(byte_at_long(delta, i));
    }
}

/// `Bits.readLongSequence(DataInput, long[], int)`, `Bits.java:426-435`. Returns `(hd, hr)`.
pub fn read_long_sequence(inp: &mut crate::wire::stream::WireReader<'_>) -> (i64, i64) {
    try_read_long_sequence(inp).unwrap_or_else(|e| panic!("{e}"))
}

/// [`read_long_sequence`] that reports a truncated or impossible encoding as an error. The
/// delta is added with Java's wrapping `long` arithmetic.
pub fn try_read_long_sequence(inp: &mut crate::wire::stream::WireReader<'_>) -> WireResult<(i64, i64)> {
    let len = inp.try_read_byte()?;
    if len == 0 {
        return Ok((0, 0));
    }
    let len1 = first_nibble(len);
    let len2 = second_nibble(len);
    let hd = try_make_long(inp, len1 as usize)?;
    let hr = try_make_long(inp, len2 as usize)?.wrapping_add(hd);
    Ok((hd, hr))
}

// ------------------------- Strings (modified UTF-8) -------------------------

/// Encodes `s` the way `java.io.DataOutputStream#writeUTF` does: each UTF-16 code unit of the
/// string (not each Unicode scalar — astral characters are surrogate pairs and are encoded as
/// *two* 3-byte sequences, exactly like the JVM does) becomes 1, 2 or 3 bytes depending on its
/// value. (`Bits.java` doesn't reimplement this — it delegates to `DataOutput#writeUTF`, so this
/// is a port of that JDK behaviour, not of `Bits.java` itself.)
fn modified_utf8_bytes(s: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(s.len());
    for unit in s.encode_utf16() {
        let c = unit as u32;
        if (0x0001..=0x007f).contains(&c) {
            out.push(c as u8);
        } else if c > 0x07ff {
            out.push(0xe0 | ((c >> 12) as u8 & 0x0f));
            out.push(0x80 | ((c >> 6) as u8 & 0x3f));
            out.push(0x80 | (c as u8 & 0x3f));
        } else {
            out.push(0xc0 | ((c >> 6) as u8 & 0x1f));
            out.push(0x80 | (c as u8 & 0x3f));
        }
    }
    out
}

fn decode_modified_utf8(bytes: &[u8]) -> WireResult<String> {
    let bad = || WireError::Invalid("malformed modified UTF-8 input".into());
    let mut units: Vec<u16> = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        let a = bytes[i];
        if a & 0x80 == 0 {
            units.push(a as u16);
            i += 1;
        } else if a & 0xe0 == 0xc0 {
            let b = *bytes.get(i + 1).ok_or_else(bad)?;
            units.push((((a & 0x1f) as u16) << 6) | (b & 0x3f) as u16);
            i += 2;
        } else if a & 0xf0 == 0xe0 {
            let b = *bytes.get(i + 1).ok_or_else(bad)?;
            let c = *bytes.get(i + 2).ok_or_else(bad)?;
            units.push((((a & 0x0f) as u16) << 12) | (((b & 0x3f) as u16) << 6) | (c & 0x3f) as u16);
            i += 3;
        } else {
            return Err(bad());
        }
    }
    String::from_utf16(&units).map_err(|_| WireError::Invalid("invalid modified UTF-8 (unpaired surrogate)".into()))
}

/// `DataOutput#writeUTF(String)` directly, with no presence byte — what `JoinRsp.writeTo` calls
/// for `fail_reason` (`protocols/pbcast/JoinRsp.java:65-66`), guarded by its own flags byte rather
/// than `Bits.writeString`'s presence byte.
pub fn write_utf(s: &str, out: &mut Vec<u8>) {
    let bytes = modified_utf8_bytes(s);
    out.extend_from_slice(&(bytes.len() as u16).to_be_bytes());
    out.extend_from_slice(&bytes);
}

/// `DataInput#readUTF()`, the counterpart to [`write_utf`].
pub fn read_utf(inp: &mut crate::wire::stream::WireReader<'_>) -> String {
    try_read_utf(inp).unwrap_or_else(|e| panic!("{e}"))
}

/// [`read_utf`] for bytes from the network: truncation or malformed modified UTF-8 is an error
/// (upstream's `UTFDataFormatException`).
pub fn try_read_utf(inp: &mut crate::wire::stream::WireReader<'_>) -> WireResult<String> {
    let len = inp.try_read_u16_be()? as usize;
    let bytes = inp.try_read_bytes(len)?;
    decode_modified_utf8(bytes)
}

/// `utf16_len(s) + 2`, the size `writeUTF` needs for a string with no astral characters — used by
/// `JoinRsp.serializedSize` (`fail_reason.length() + 2`, its own approximation, same shape as
/// `PingData`'s logical-name sizing quirk documented in `ping_data.rs`).
pub fn size_utf(s: &str) -> usize {
    utf16_len(s) + 2
}

/// `Bits.writeString(String, DataOutput)`, `Bits.java:543-550`: presence byte, then
/// `DataOutput#writeUTF` (2-byte big-endian length + modified-UTF-8 bytes).
pub fn write_string(s: Option<&str>, out: &mut Vec<u8>) {
    match s {
        None => out.push(0),
        Some(s) => {
            out.push(1);
            let bytes = modified_utf8_bytes(s);
            let len = bytes.len() as u16;
            out.extend_from_slice(&len.to_be_bytes());
            out.extend_from_slice(&bytes);
        }
    }
}

/// `Bits.readString(DataInput)`, `Bits.java:577-582`.
pub fn read_string(inp: &mut crate::wire::stream::WireReader<'_>) -> Option<String> {
    try_read_string(inp).unwrap_or_else(|e| panic!("{e}"))
}

/// [`read_string`] for bytes from the network.
pub fn try_read_string(inp: &mut crate::wire::stream::WireReader<'_>) -> WireResult<Option<String>> {
    let presence = inp.try_read_byte()?;
    if presence != 1 {
        return Ok(None);
    }
    let len = inp.try_read_u16_be()? as usize;
    let bytes = inp.try_read_bytes(len)?;
    decode_modified_utf8(bytes).map(Some)
}

/// The number of UTF-16 code units in `s`, i.e. `java.lang.String#length()`. Header
/// `serializedSize()` methods (`PingHeader`, `MERGE3.MergeHeader`) use `logical_name.length() + 2`
/// as a cheap stand-in for the real `writeUTF` byte count — correct for the BMP/ASCII strings
/// MockServer actually sends, wrong in general (upstream's own approximation, ported faithfully).
pub fn utf16_len(s: &str) -> usize {
    s.encode_utf16().count()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::stream::WireReader;
    use crate::wire::vectors;

    fn roundtrip_long(v: i64) {
        let mut out = Vec::new();
        write_long_compressed(v, &mut out);
        assert_eq!(out.len(), size_long(v), "size_long({v})");
        let mut r = WireReader::new(&out);
        assert_eq!(read_long_compressed(&mut r), v);
        assert!(r.is_empty());
    }

    fn roundtrip_int(v: i32) {
        let mut out = Vec::new();
        write_int_compressed(v, &mut out);
        assert_eq!(out.len(), size_int(v), "size_int({v})");
        let mut r = WireReader::new(&out);
        assert_eq!(read_int_compressed(&mut r), v);
        assert!(r.is_empty());
    }

    // BitsTest#testWriteAndReadLong / testSizeLong
    #[test]
    fn write_and_read_long() {
        for &v in &[
            i64::MIN, -322649, -100, -1, 0, 1, 2, 4, 8, 9, 250, 260, i16::MAX as i64,
            i32::MIN as i64, 322649, i32::MAX as i64, i32::MAX as i64 + 100, i64::MAX - 10, i64::MAX,
        ] {
            roundtrip_long(v);
        }
    }

    // BitsTest#testWriteAndReadInt / testSizeInt
    #[test]
    fn write_and_read_int() {
        for &v in &[
            i32::MIN, -322649, -100, -1, 0, 1, 2, 4, 8, 9, 250, 260, i16::MAX as i32, 322649, i32::MAX,
        ] {
            roundtrip_int(v);
        }
    }

    #[test]
    fn size_long_boundaries() {
        // BitsTest#testSizeLong: num_bytes_required = shift/8 + 2
        assert_eq!(size_long(0), 1);
        for &shift in &[0u32, 1, 2, 4, 7, 8, 15, 16, 17, 23, 24, 25, 31, 32, 33, 39, 40, 41, 47, 48, 49, 55, 56] {
            let num: i64 = 1i64 << shift;
            assert_eq!(size_long(num), (shift / 8) as usize + 2, "shift={shift}");
        }
    }

    #[test]
    fn size_int_boundaries() {
        assert_eq!(size_int(0), 1);
        for &shift in &[0u32, 1, 2, 4, 7, 8, 15, 16, 17, 23, 24] {
            let num: i32 = 1i32 << shift;
            assert_eq!(size_int(num), (shift / 8) as usize + 2, "shift={shift}");
        }
    }

    // BitsTest#testWriteAndReadLongSequence
    #[test]
    fn write_and_read_long_sequence() {
        let numbers: &[i64] = &[
            0, 1, 50, 127, 128, 254, 255, 256,
            i16::MAX as i64, i16::MAX as i64 + 1, i16::MAX as i64 * 2, i16::MAX as i64 * 2 + 1,
            100000, 500000,
            i32::MAX as i64, i32::MAX as i64 + 1, i32::MAX as i64 * 2, i32::MAX as i64 + 10,
            i64::MAX / 10, i64::MAX - 1, i64::MAX,
        ];
        for &num in numbers {
            let mut out = Vec::new();
            write_long_sequence(num, num, &mut out);
            assert_eq!(out.len(), size_long_sequence(num, num));
            let mut r = WireReader::new(&out);
            assert_eq!(read_long_sequence(&mut r), (num, num));

            if num.checked_add(100).is_none() {
                continue;
            }
            let second = num + 100;
            let mut out = Vec::new();
            write_long_sequence(num, second, &mut out);
            assert_eq!(out.len(), size_long_sequence(num, second));
            let mut r = WireReader::new(&out);
            assert_eq!(read_long_sequence(&mut r), (num, second));
        }
    }

    #[test]
    fn write_and_read_string() {
        for s in [None, Some(""), Some("hello world"), Some("DemoChannel")] {
            let mut out = Vec::new();
            write_string(s, &mut out);
            let mut r = WireReader::new(&out);
            assert_eq!(read_string(&mut r), s.map(|s| s.to_string()));
            assert!(r.is_empty());
        }
    }

    // Cross-check against the real jgroups-5.2.28.Final jar (harness output), not hand-derived.
    #[test]
    fn matches_upstream_vectors() {
        let v = vectors::load();

        for &n in &[
            i64::MIN, -322649i64, -100, -1, 0, 1, 2, 4, 8, 9, 250, 260, i16::MAX as i64,
            i32::MIN as i64, 322649, i32::MAX as i64, i32::MAX as i64 + 100, i64::MAX - 10, i64::MAX,
        ] {
            let name = format!("bits.long.{n}");
            let vec = vectors::get(&v, &name);
            let mut out = Vec::new();
            write_long_compressed(n, &mut out);
            assert_eq!(out, vec.bytes, "{name}");
            assert_eq!(out.len() as i64, vec.size, "{name} size");
        }

        for &n in &[
            i32::MIN, -322649i32, -100, -1, 0, 1, 2, 4, 8, 9, 250, 260, i16::MAX as i32, 322649, i32::MAX,
        ] {
            let name = format!("bits.int.{n}");
            let vec = vectors::get(&v, &name);
            let mut out = Vec::new();
            write_int_compressed(n, &mut out);
            assert_eq!(out, vec.bytes, "{name}");
        }

        let seq_pairs: &[(i64, i64)] = &[
            (0, 0), (1, 1), (50, 150), (127, 127), (128, 128), (254, 354), (255, 255), (256, 256),
            (i16::MAX as i64, i16::MAX as i64 + 100), (100000, 100100),
            (i32::MAX as i64, i32::MAX as i64 + 100),
            (i64::MAX / 10, i64::MAX / 10 + 100), (i64::MAX - 1, i64::MAX),
        ];
        for &(hd, hr) in seq_pairs {
            let name = format!("bits.longseq.{hd}.{hr}");
            let vec = vectors::get(&v, &name);
            let mut out = Vec::new();
            write_long_sequence(hd, hr, &mut out);
            assert_eq!(out, vec.bytes, "{name}");
        }

        for (label, s) in [("null", None), ("empty", Some("")), ("ascii", Some("hello world")), ("cluster", Some("DemoChannel"))] {
            let name = format!("bits.string.{label}");
            let vec = vectors::get(&v, &name);
            let mut out = Vec::new();
            write_string(s, &mut out);
            assert_eq!(out, vec.bytes, "{name}");
        }
    }
}
