//! `PingData`: the discovery request/response payload TCPPING (and every other `Discovery`
//! subclass) marshals. Port of `upstream/src/org/jgroups/protocols/PingData.java`.
//!

use crate::wire::address::{self, Address};
use crate::wire::bits;
use crate::wire::stream::{WireError, WireReader, WireResult, WireWrite};

const IS_SERVER: u8 = 1;
const IS_COORD: u8 = 1 << 1;

/// `PingData.java:25-156`. `physical_addr` is typed `PhysicalAddress` upstream (an `IpAddress` in
/// every case this crate reaches); kept as a plain `Address` here since the wire dispatch
/// (`Util.writeAddress`/`readAddress`) doesn't distinguish the two.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PingData {
    /// The member the data describes.
    pub sender: Option<Address>,
    flags: u8,
    /// The sender's logical name.
    pub logical_name: Option<String>,
    /// The sender's physical address.
    pub physical_addr: Option<Address>,
    /// The members of the sender's view, when it sends them.
    pub mbrs: Option<Vec<Address>>,
}

impl PingData {
    /// `new PingData(sender, is_server)`.
    pub fn new(sender: Option<Address>, is_server: bool) -> PingData {
        let mut d = PingData { sender, flags: 0, logical_name: None, physical_addr: None, mbrs: None };
        d.server(is_server);
        d
    }

    /// `new PingData(sender, is_server, logical_name, physical_addr)`.
    pub fn full(sender: Option<Address>, is_server: bool, logical_name: Option<&str>, physical_addr: Option<Address>) -> PingData {
        let mut d = PingData::new(sender, is_server);
        d.logical_name = logical_name.map(str::to_string);
        d.physical_addr = physical_addr;
        d
    }

    /// Sets `mbrs`, builder style.
    pub fn mbrs(mut self, mbrs: Vec<Address>) -> PingData {
        self.mbrs = Some(mbrs);
        self
    }

    /// `PingData.coord(boolean)`: a coordinator is always a server too.
    pub fn coord(mut self, c: bool) -> PingData {
        if c {
            self.flags |= IS_COORD;
            self.flags |= IS_SERVER;
        } else {
            self.flags &= !IS_COORD;
        }
        self
    }

    /// `PingData.server(boolean)`: marks the sender as a member that has joined.
    pub fn server(&mut self, c: bool) -> &mut PingData {
        if c {
            self.flags |= IS_SERVER;
        } else {
            self.flags &= !IS_SERVER;
        }
        self
    }

    /// Whether the sender is a coordinator.
    pub fn is_coord(&self) -> bool {
        self.flags & IS_COORD != 0
    }
    /// Whether the sender has joined: a server or a coordinator.
    pub fn is_server(&self) -> bool {
        self.flags & IS_SERVER != 0 || self.is_coord()
    }

    /// `PingData.serializedSize()`, `:126-135`. Note upstream's own approximation for the
    /// logical-name contribution (`logical_name.length() + 2`, UTF-16 code units not encoded
    /// bytes — see `bits::utf16_len`'s doc comment); reproduced as-is, not "fixed".
    pub fn serialized_size(&self) -> usize {
        let mut size = 1; // the flags byte ("for is_server", PingData.java:127)
        size += address::size(self.sender.as_ref());
        size += 1; // presence byte for logical_name
        if let Some(name) = &self.logical_name {
            size += bits::utf16_len(name) + 2;
        }
        size += address::size(self.physical_addr.as_ref());
        size += mbrs_size(self.mbrs.as_deref());
        size
    }

    /// `PingData.writeTo(DataOutput)`, `:138-144`.
    pub fn write_to(&self, out: &mut Vec<u8>) {
        address::write_address(self.sender.as_ref(), out);
        out.write_byte(self.flags);
        bits::write_string(self.logical_name.as_deref(), out);
        address::write_address(self.physical_addr.as_ref(), out);
        write_mbrs(self.mbrs.as_deref(), out);
    }

    /// `Util.writeStreamable(obj, out)` (`Util.java:1873`): a presence byte (1) then `writeTo`. This is
    /// the form `Discovery.marshal` puts in a discovery message's array (`Discovery.java:614-621`);
    /// a bare `writeTo` is not what a JGroups peer expects on the wire.
    pub fn write_streamable(&self, out: &mut Vec<u8>) {
        out.write_byte(1);
        self.write_to(out);
    }

    /// `Util.readStreamable(PingData::new, in)` (`Discovery.readPingData`, `:623-631`): `None` for
    /// a zero presence byte (null object).
    pub fn read_streamable(inp: &mut WireReader<'_>) -> Option<PingData> {
        PingData::try_read_streamable(inp).unwrap_or_else(|e| panic!("{e}"))
    }

    /// [`PingData::read_streamable`] for bytes from the network.
    pub fn try_read_streamable(inp: &mut WireReader<'_>) -> WireResult<Option<PingData>> {
        if inp.try_read_byte()? == 0 { Ok(None) } else { PingData::try_read_from(inp).map(Some) }
    }

    /// `PingData.readFrom(DataInput)`, `:147-153`.
    pub fn read_from(inp: &mut WireReader<'_>) -> PingData {
        PingData::try_read_from(inp).unwrap_or_else(|e| panic!("{e}"))
    }

    /// [`PingData::read_from`] for bytes from the network.
    pub fn try_read_from(inp: &mut WireReader<'_>) -> WireResult<PingData> {
        let sender = address::try_read_address(inp)?;
        let flags = inp.try_read_byte()?;
        let logical_name = bits::try_read_string(inp)?;
        let physical_addr = address::try_read_address(inp)?;
        let mbrs = read_mbrs(inp)?;
        Ok(PingData { sender, flags, logical_name, physical_addr, mbrs })
    }
}

// `Util.writeAddresses`/`readAddresses` (`Util.java:1801-1846`): a `short` count (-1 for null),
// then each address through the flags-byte dispatch. NOTE: `Util.size(Collection<? extends
// Address>)` (`Util.java:1849-1862`) is *not* the sum of each element's real size — its own doc
// comment says "Assumes elements are of the same type!": it takes the first element's size and
// multiplies by the count. For a homogeneous `mbrs` list (the only case MockServer ever produces,
// since JGroups addresses in one cluster are always the same concrete type) this equals the real
// sum; for a mixed list (only ever seen in this crate's own byte-vector harness) it silently
// under- or over-counts. Reproduced exactly rather than "fixed", since `serializedSize()` (used to
// pre-size buffers, not to bound reads) being wrong for a mixed list is upstream's own bug, cross-
// checked against `pingdata.full`'s vector (`mbrs=[UUID, ExtendedUUID]`, real bytes 46, but
// upstream's own `serializedSize()` reports 36 there).
fn mbrs_size(mbrs: Option<&[Address]>) -> usize {
    match mbrs {
        None => 2,
        Some([]) => 2,
        Some(addrs) => 2 + address::size(Some(&addrs[0])) * addrs.len(),
    }
}

fn write_mbrs(mbrs: Option<&[Address]>, out: &mut Vec<u8>) {
    match mbrs {
        None => out.write_i16(-1),
        Some(addrs) => {
            out.write_i16(addrs.len() as i16);
            for a in addrs {
                address::write_address(Some(a), out);
            }
        }
    }
}

fn read_mbrs(inp: &mut WireReader<'_>) -> WireResult<Option<Vec<Address>>> {
    let len = inp.try_read_i16()?;
    if len < 0 {
        return Ok(None);
    }
    let mut v = Vec::with_capacity((len as usize).min(inp.remaining()));
    for _ in 0..len {
        v.push(address::try_read_address(inp)?.ok_or_else(|| WireError::Invalid("a null PingData member".into()))?);
    }
    Ok(Some(v))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::vectors;

    fn u1() -> Address {
        Address::uuid(0x0102030405060708, 0x1112131415161718)
    }
    fn eu1() -> Address {
        // Matches the vector harness's `eu1`: ExtendedUUID(u1's bits, no flags/entries set beyond
        // what its constructor default produces) — reconstructed here only for the mbrs list, not
        // asserted structurally (round-trip equality is enough since the wire codec is already
        // exhaustively tested in address.rs).
        address::read_address(&mut WireReader::new(&{
            let mut buf = Vec::new();
            address::write_address(
                Some(&Address::ExtendedUuid {
                    most_sig_bits: 0x0102030405060708,
                    least_sig_bits: 0x1112131415161718,
                    flags: 0,
                    entries: vec![(b"k1".to_vec(), b"v1".to_vec())],
                }),
                &mut buf,
            );
            buf
        }))
        .unwrap()
    }
    fn ip4() -> Address {
        Address::ip4(192, 168, 1, 5, 7800)
    }

    // `serialized_size()` is only checked against the real written length for a *homogeneous*
    // `mbrs` list — see `mbrs_size`'s doc comment: upstream's own `Util.size(Collection<Address>)`
    // undercounts (or overcounts) a mixed-type list by design ("assumes elements are of the same
    // type"), so `roundtrip_mixed_mbrs` below deliberately skips that assertion for the one shape
    // that hits it, and `cross_checked_against_real_jgroups_vectors` is what actually proves the
    // quirk is reproduced correctly (against real upstream output), not this helper.
    fn roundtrip(pd: &PingData) {
        let mut out = Vec::new();
        pd.write_to(&mut out);
        assert_eq!(out.len(), pd.serialized_size());
        let mut r = WireReader::new(&out);
        let back = PingData::read_from(&mut r);
        assert!(r.is_empty());
        assert_eq!(&back, pd);
    }

    fn roundtrip_ignoring_size_quirk(pd: &PingData) {
        let mut out = Vec::new();
        pd.write_to(&mut out);
        let mut r = WireReader::new(&out);
        let back = PingData::read_from(&mut r);
        assert!(r.is_empty());
        assert_eq!(&back, pd);
    }

    #[test]
    fn roundtrips_every_shape() {
        roundtrip(&PingData::new(Some(u1()), false));
        roundtrip(&PingData::new(Some(u1()), true).coord(true));
        roundtrip(&PingData::full(Some(u1()), true, Some("nodeB"), None));
        roundtrip(&PingData::full(Some(ip4()), true, Some("nodeC"), Some(Address::ip4(10, 0, 0, 1, 65535))));
        roundtrip(&PingData::full(Some(u1()), true, Some("nodeD"), Some(ip4())).mbrs(vec![u1(), u1()])); // homogeneous mbrs
    }

    #[test]
    fn roundtrip_mixed_type_mbrs_survives_the_upstream_size_quirk() {
        roundtrip_ignoring_size_quirk(
            &PingData::full(Some(u1()), true, Some("nodeA"), Some(ip4())).mbrs(vec![u1(), eu1()]),
        );
    }

    #[test]
    fn coord_implies_server() {
        let pd = PingData::new(Some(u1()), false).coord(true);
        assert!(pd.is_coord());
        assert!(pd.is_server());
    }

    #[test]
    fn streamable_form_matches_discovery_marshal_from_the_jar() {
        let map = vectors::load();
        let v = vectors::get(&map, "pingdata.streamable.coord");
        let pd = PingData::full(Some(u1()), true, Some("nodeA"), Some(ip4())).coord(true);
        let mut out = Vec::new();
        pd.write_streamable(&mut out);
        assert_eq!(out, v.bytes);
        assert_eq!(PingData::read_streamable(&mut WireReader::new(&v.bytes)), Some(pd));
        assert_eq!(PingData::read_streamable(&mut WireReader::new(&[0])), None);
    }

    #[test]
    fn cross_checked_against_real_jgroups_vectors() {
        let map = vectors::load();
        macro_rules! check {
            ($name:literal, $pd:expr) => {{
                let v = vectors::get(&map, $name);
                let pd = $pd;
                assert_eq!(pd.serialized_size() as i64, v.size, "{}: size", $name);
                let mut out = Vec::new();
                pd.write_to(&mut out);
                assert_eq!(out, v.bytes, "{}: bytes", $name);
            }};
        }
        check!("pingdata.full", PingData::full(Some(u1()), true, Some("nodeA"), Some(ip4())).mbrs(vec![u1(), eu1()]));
        check!("pingdata.minimal", PingData::new(Some(u1()), false));
        check!("pingdata.coord", PingData::new(Some(u1()), true).coord(true));
        check!("pingdata.no_physical_addr", PingData::full(Some(u1()), true, Some("nodeB"), None));
        check!(
            "pingdata.no_mbrs",
            PingData::full(Some(ip4()), true, Some("nodeC"), Some(Address::ip4(10, 0, 0, 1, 65535)))
        );
    }
}
