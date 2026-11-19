//! The two ends of the wire: a growable output buffer and a cursor over an input slice.
//!
//! Upstream JGroups writes to a `java.io.DataOutput` (usually `ByteArrayDataOutputStream`,
//! `upstream/src/org/jgroups/util/ByteArrayDataOutputStream.java`, a growable `byte[]` wrapped in
//! the `DataOutput` interface) and reads from a `java.io.DataInput`
//! (`ByteArrayDataInputStream.java`, a cursor over a `byte[]`). Rust has no equivalent interface
//! pair, and everything this crate writes ends up in a plain `Vec<u8>` regardless, so instead of
//! porting the two classes' full API (most of which — `readLine`, `readUTF` on arbitrary streams,
//! `skipBytes`, ...) nothing here ever calls, this ports only the primitive read/write operations
//! JGroups' headers and messages actually use, as a `WireWrite` extension trait on `Vec<u8>` (the
//! output side) and a `WireReader` cursor (the input side). Multi-byte integers are big-endian,
//! matching `DataOutputStream`/`DataInputStream`.

/// Output side: methods matching `java.io.DataOutput`'s primitive writers, implemented for
/// `Vec<u8>` so callers just build up the frame in an ordinary buffer.
pub trait WireWrite {
    /// `DataOutput.writeByte`.
    fn write_byte(&mut self, b: u8);
    /// `DataOutput.writeBoolean`: `1` or `0`.
    fn write_bool(&mut self, b: bool);
    /// `DataOutput.writeShort`, big-endian.
    fn write_i16(&mut self, v: i16);
    /// An unsigned 16-bit value, big-endian.
    fn write_u16(&mut self, v: u16);
    /// `DataOutput.writeInt`, big-endian.
    fn write_i32(&mut self, v: i32);
    /// `DataOutput.writeLong`, big-endian.
    fn write_i64(&mut self, v: i64);
    /// `DataOutput.writeChar`: one UTF-16 code unit (the first, for a character outside the BMP).
    fn write_char(&mut self, c: char);
    /// `DataOutput.writeFloat`: the IEEE 754 bits, big-endian.
    fn write_f32(&mut self, v: f32);
    /// `DataOutput.writeDouble`: the IEEE 754 bits, big-endian.
    fn write_f64(&mut self, v: f64);
}

impl WireWrite for Vec<u8> {
    fn write_byte(&mut self, b: u8) {
        self.push(b);
    }
    fn write_bool(&mut self, b: bool) {
        self.push(if b { 1 } else { 0 });
    }
    fn write_i16(&mut self, v: i16) {
        self.extend_from_slice(&v.to_be_bytes());
    }
    fn write_u16(&mut self, v: u16) {
        self.extend_from_slice(&v.to_be_bytes());
    }
    fn write_i32(&mut self, v: i32) {
        self.extend_from_slice(&v.to_be_bytes());
    }
    fn write_i64(&mut self, v: i64) {
        self.extend_from_slice(&v.to_be_bytes());
    }
    fn write_char(&mut self, c: char) {
        // Java `char` is a UTF-16 code unit; DataOutput#writeChar writes it as a plain u16 (not
        // modified UTF-8 — that's only for writeUTF/readUTF, see bits.rs).
        let units: Vec<u16> = c.encode_utf16(&mut [0u16; 2]).to_vec();
        self.write_u16(units[0]);
    }
    fn write_f32(&mut self, v: f32) {
        self.write_i32(v.to_bits() as i32);
    }
    fn write_f64(&mut self, v: f64) {
        self.write_i64(v.to_bits() as i64);
    }
}

/// A malformed or truncated frame: what upstream's `DataInput` reads surface as an
/// `EOFException`/`IOException` (or the `IllegalArgumentException` a codec throws for an
/// impossible value). Decoders that handle bytes from the network return this instead of panicking.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WireError {
    /// `EOFException`: `need` bytes were wanted, `remain` were left.
    Underflow {
        /// How many bytes the read wanted.
        need: usize,
        /// How many bytes were left.
        remain: usize,
    },
    /// Bytes that decode to a value the format does not allow.
    Invalid(String),
}

impl std::fmt::Display for WireError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WireError::Underflow { need, remain } => write!(f, "buffer underflow: need {need} bytes, {remain} remain"),
            WireError::Invalid(m) => f.write_str(m),
        }
    }
}

impl std::error::Error for WireError {}

/// The result of decoding bytes from the network.
pub type WireResult<T> = Result<T, WireError>;

/// Input side: a cursor over a borrowed byte slice, mirroring `ByteArrayDataInputStream`'s
/// primitive readers.
pub struct WireReader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> WireReader<'a> {
    /// A reader at the start of `buf`.
    pub fn new(buf: &'a [u8]) -> Self {
        WireReader { buf, pos: 0 }
    }

    /// How many bytes are left to read.
    pub fn remaining(&self) -> usize {
        self.buf.len() - self.pos
    }

    /// Whether every byte has been read.
    pub fn is_empty(&self) -> bool {
        self.remaining() == 0
    }

    /// The offset of the next byte to read.
    pub fn position(&self) -> usize {
        self.pos
    }

    /// The bytes consumed since `start` (a prior [`position`](Self::position)).
    pub fn slice_since(&self, start: usize) -> &'a [u8] {
        &self.buf[start..self.pos]
    }

    fn try_take(&mut self, n: usize) -> WireResult<&'a [u8]> {
        if n > self.remaining() {
            return Err(WireError::Underflow { need: n, remain: self.remaining() });
        }
        let slice = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(slice)
    }

    fn take(&mut self, n: usize) -> &'a [u8] {
        self.try_take(n).unwrap_or_else(|e| panic!("{e}"))
    }

    /// Reads a byte, or [`WireError::Underflow`] at the end.
    pub fn try_read_byte(&mut self) -> WireResult<u8> {
        Ok(self.try_take(1)?[0])
    }

    /// Reads a boolean: any non-zero byte is `true`.
    pub fn try_read_bool(&mut self) -> WireResult<bool> {
        Ok(self.try_read_byte()? != 0)
    }

    /// Reads a big-endian `i16`.
    pub fn try_read_i16(&mut self) -> WireResult<i16> {
        Ok(i16::from_be_bytes(self.try_take(2)?.try_into().unwrap()))
    }

    /// Reads a big-endian `u16`.
    pub fn try_read_u16_be(&mut self) -> WireResult<u16> {
        Ok(u16::from_be_bytes(self.try_take(2)?.try_into().unwrap()))
    }

    /// Reads a big-endian `i32`.
    pub fn try_read_i32(&mut self) -> WireResult<i32> {
        Ok(i32::from_be_bytes(self.try_take(4)?.try_into().unwrap()))
    }

    /// Reads a big-endian `i64`.
    pub fn try_read_i64(&mut self) -> WireResult<i64> {
        Ok(i64::from_be_bytes(self.try_take(8)?.try_into().unwrap()))
    }

    /// Borrows the next `n` bytes.
    pub fn try_read_bytes(&mut self, n: usize) -> WireResult<&'a [u8]> {
        self.try_take(n)
    }

    /// [`Self::try_read_byte`] for bytes known to be well formed; panics at the end.
    pub fn read_byte(&mut self) -> u8 {
        self.take(1)[0]
    }

    /// Reads a byte as a signed `i8`; panics at the end.
    pub fn read_i8(&mut self) -> i8 {
        self.read_byte() as i8
    }

    /// [`Self::try_read_bool`]; panics at the end.
    pub fn read_bool(&mut self) -> bool {
        self.read_byte() != 0
    }

    /// [`Self::try_read_i16`]; panics at the end.
    pub fn read_i16(&mut self) -> i16 {
        i16::from_be_bytes(self.take(2).try_into().unwrap())
    }

    /// [`Self::try_read_u16_be`]; panics at the end.
    pub fn read_u16_be(&mut self) -> u16 {
        u16::from_be_bytes(self.take(2).try_into().unwrap())
    }

    /// [`Self::try_read_i32`]; panics at the end.
    pub fn read_i32(&mut self) -> i32 {
        i32::from_be_bytes(self.take(4).try_into().unwrap())
    }

    /// [`Self::try_read_i64`]; panics at the end.
    pub fn read_i64(&mut self) -> i64 {
        i64::from_be_bytes(self.take(8).try_into().unwrap())
    }

    /// `DataInput.readChar`: one UTF-16 code unit; a lone surrogate reads as U+FFFD. Panics at the end.
    pub fn read_char(&mut self) -> char {
        let unit = self.read_u16_be();
        char::from_u32(unit as u32).unwrap_or(char::REPLACEMENT_CHARACTER)
    }

    /// `DataInput.readFloat`; panics at the end.
    pub fn read_f32(&mut self) -> f32 {
        f32::from_bits(self.read_i32() as u32)
    }

    /// `DataInput.readDouble`; panics at the end.
    pub fn read_f64(&mut self) -> f64 {
        f64::from_bits(self.read_i64() as u64)
    }

    /// [`Self::try_read_bytes`]; panics at the end.
    pub fn read_bytes(&mut self, n: usize) -> &'a [u8] {
        self.take(n)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ByteArrayDataInputOutputStreamTest: primitive round-trips this crate's wire format needs
    // (the header/message/address codecs use byte, bool, i16/u16, i32, i64 and the compressed
    // long/int forms in bits.rs — the upstream test file also exercises readLine/UTF-on-arbitrary-
    // streams/skipBytes, which nothing in this crate's scope calls, so those are not ported).
    #[test]
    fn byte_roundtrip() {
        let mut out = Vec::new();
        out.write_byte(0);
        out.write_byte(255);
        out.write_byte(127);
        let mut r = WireReader::new(&out);
        assert_eq!(r.read_byte(), 0);
        assert_eq!(r.read_byte(), 255);
        assert_eq!(r.read_byte(), 127);
        assert!(r.is_empty());
    }

    #[test]
    fn bool_roundtrip() {
        let mut out = Vec::new();
        out.write_bool(true);
        out.write_bool(false);
        let mut r = WireReader::new(&out);
        assert!(r.read_bool());
        assert!(!r.read_bool());
    }

    #[test]
    fn i16_roundtrip() {
        for v in [i16::MIN, -1, 0, 1, i16::MAX] {
            let mut out = Vec::new();
            out.write_i16(v);
            assert_eq!(out.len(), 2);
            let mut r = WireReader::new(&out);
            assert_eq!(r.read_i16(), v);
        }
    }

    #[test]
    fn i32_roundtrip() {
        for v in [i32::MIN, -1, 0, 1, i32::MAX] {
            let mut out = Vec::new();
            out.write_i32(v);
            assert_eq!(out.len(), 4);
            let mut r = WireReader::new(&out);
            assert_eq!(r.read_i32(), v);
        }
    }

    #[test]
    fn i64_roundtrip() {
        for v in [i64::MIN, -1, 0, 1, i64::MAX] {
            let mut out = Vec::new();
            out.write_i64(v);
            assert_eq!(out.len(), 8);
            let mut r = WireReader::new(&out);
            assert_eq!(r.read_i64(), v);
        }
    }

    #[test]
    fn f32_f64_roundtrip() {
        let mut out = Vec::new();
        out.write_f32(3.5f32);
        out.write_f64(-2.25f64);
        let mut r = WireReader::new(&out);
        assert_eq!(r.read_f32(), 3.5f32);
        assert_eq!(r.read_f64(), -2.25f64);
    }

    #[test]
    fn char_roundtrip() {
        let mut out = Vec::new();
        out.write_char('A');
        out.write_char('z');
        let mut r = WireReader::new(&out);
        assert_eq!(r.read_char(), 'A');
        assert_eq!(r.read_char(), 'z');
    }

    #[test]
    #[should_panic(expected = "buffer underflow")]
    fn underflow_panics() {
        let out: Vec<u8> = vec![1];
        let mut r = WireReader::new(&out);
        r.read_i32();
    }
}
