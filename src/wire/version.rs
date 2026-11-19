//! Port of `org.jgroups.Version` (`upstream/src/org/jgroups/Version.java`): the `short` wire
//! version and binary-compatibility rules.
//!
//! Upstream loads major/minor/micro from a `JGROUPS_VERSION.properties` resource baked into the
//! jar at build time and exposes two `System`-property toggles (`jgroups.version_check`,
//! `jgroups.version_check_micro`) read once into static state (`Version.java:88-96`). Neither fits
//! a library crate with no static init magic: [`VERSION`] is the jgroups-5.2.28.Final constant
//! (confirmed against the real jar via the harness vector `version.value`, not hand-derived), and
//! the two checks upstream reads from system properties are parameters here
//! (`is_binary_compatible`'s `version_check`/`version_check_micro`) rather than global mutable
//! config.

const MAJOR_SHIFT: u16 = 11;
const MINOR_SHIFT: u16 = 6;
const MAJOR_MASK: u16 = 0xf800; // 1111100000000000
const MINOR_MASK: u16 = 0x07c0; //      11111000000
const MICRO_MASK: u16 = 0x003f; //           111111

/// `Version.encode(5, 2, 28)` (`Version.java:142-144`) — jgroups-5.2.28.Final's wire version.
/// Cross-checked against the real jar: harness vector `version.value` = 10396.
pub const VERSION: u16 = 10396;

/// `Version.encode(int, int, int)`, `Version.java:142-144`.
pub fn encode(major: u16, minor: u16, micro: u16) -> u16 {
    (major << MAJOR_SHIFT)
        .wrapping_add(minor << MINOR_SHIFT)
        .wrapping_add(micro)
}

/// `Version.major`: the major part of an encoded version.
pub fn get_major(v: u16) -> u16 {
    (v & MAJOR_MASK) >> MAJOR_SHIFT
}
/// `Version.minor`: the minor part of an encoded version.
pub fn get_minor(v: u16) -> u16 {
    (v & MINOR_MASK) >> MINOR_SHIFT
}
/// `Version.micro`: the micro part of an encoded version.
pub fn get_micro(v: u16) -> u16 {
    v & MICRO_MASK
}

/// `Version.decode(short)`, `Version.java:168-173`. Returns `(major, minor, micro)`.
pub fn decode(v: u16) -> (u16, u16, u16) {
    (get_major(v), get_minor(v), get_micro(v))
}

/// `Version.print(short)`, `Version.java:147-152`.
pub fn print(v: u16) -> String {
    let (major, minor, micro) = decode(v);
    format!("{major}.{minor}.{micro}")
}

/// `Version.isSame(short)`, `Version.java:137-139`: compares against this crate's own [`VERSION`].
pub fn is_same(v: u16) -> bool {
    VERSION == v
}

/// `Version.isBinaryCompatible(short, short)`, `Version.java:194-205`. Major and minor must match;
/// micro may differ unless `version_check_micro` is set, in which case only an exact match counts
/// (and `version_check=false` short-circuits to "always compatible", upstream's own escape hatch).
pub fn is_binary_compatible(ver1: u16, ver2: u16, version_check: bool, version_check_micro: bool) -> bool {
    if !version_check || ver1 == ver2 {
        return true;
    }
    if version_check_micro {
        return false;
    }
    let (major1, minor1, _) = decode(ver1);
    let (major2, minor2, _) = decode(ver2);
    major1 == major2 && minor1 == minor2
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::vectors;

    // VersionTest#testSameVersion
    #[test]
    fn same_version() {
        fn matches(a: (u16, u16, u16), b: (u16, u16, u16)) -> bool {
            encode(a.0, a.1, a.2) == encode(b.0, b.1, b.2)
        }
        assert!(matches((0, 0, 1), (0, 0, 1)));
        assert!(matches((1, 0, 0), (1, 0, 0)));
        assert!(matches((10, 2, 60), (10, 2, 60)));
        assert!(!matches((1, 2, 3), (1, 2, 0)));
        assert!(!matches((0, 0, 0), (0, 0, 1)));
        assert!(!matches((2, 5, 0), (2, 5, 1)));
    }

    // VersionTest#testBinaryCompatibility (version_check=true, version_check_micro=false — the
    // upstream defaults)
    #[test]
    fn binary_compatibility_defaults() {
        let v = |a, b, c| encode(a, b, c);
        assert!(is_binary_compatible(v(0, 0, 0), v(0, 0, 0), true, false));
        assert!(is_binary_compatible(v(1, 2, 0), v(1, 2, 1), true, false));
        assert!(is_binary_compatible(v(1, 2, 0), v(1, 2, 60), true, false));
        assert!(!is_binary_compatible(v(2, 5, 0), v(2, 4, 1), true, false));
        assert!(!is_binary_compatible(v(2, 5, 0), v(2, 6, 0), true, false));
    }

    // VersionTest#testBinaryCompatibilityWithoutVersionCheck
    #[test]
    fn binary_compatibility_without_version_check() {
        let v = |a, b, c| encode(a, b, c);
        for (a, b) in [((0, 0, 0), (0, 0, 0)), ((1, 2, 0), (1, 2, 1)), ((1, 2, 0), (1, 2, 60)), ((2, 5, 0), (2, 4, 1)), ((2, 5, 0), (2, 6, 0))] {
            assert!(is_binary_compatible(v(a.0, a.1, a.2), v(b.0, b.1, b.2), false, false));
        }
    }

    // VersionTest#testBinaryCompatibilityWithVersionCheckMicro
    #[test]
    fn binary_compatibility_with_micro_check() {
        let v = |a, b, c| encode(a, b, c);
        assert!(is_binary_compatible(v(0, 0, 0), v(0, 0, 0), true, true));
        assert!(is_binary_compatible(v(5, 3, 1), v(5, 3, 1), true, true));
        assert!(!is_binary_compatible(v(1, 2, 0), v(1, 2, 1), true, true));
        assert!(!is_binary_compatible(v(1, 2, 0), v(1, 2, 60), true, true));
        assert!(!is_binary_compatible(v(2, 5, 0), v(2, 4, 1), true, true));
        assert!(!is_binary_compatible(v(2, 5, 0), v(2, 6, 0), true, true));
    }

    // VersionTest#testDifferentVersion / testNullVersion
    #[test]
    fn different_and_null_version() {
        assert_ne!(encode(2, 0, 7), encode(2, 0, 6));
        assert!(!is_same(0));
    }

    // Cross-check against the real jar: Version.version, Version.print(Version.version) and
    // Version.encode(5,2,28) as computed by the actual upstream class, not hand-derived.
    #[test]
    fn matches_upstream_vectors() {
        let v = vectors::load();
        let value: u16 = vectors::get_raw(&v, "version.value").parse().unwrap();
        assert_eq!(value, VERSION);

        let printed = vectors::get_raw(&v, "version.print");
        assert_eq!(printed, print(VERSION));

        let encoded_5_2_28: u16 = vectors::get_raw(&v, "version.encode.5.2.28").parse().unwrap();
        assert_eq!(encoded_5_2_28, encode(5, 2, 28));
        assert_eq!(encoded_5_2_28, VERSION);

        let encoded_5_2_0: u16 = vectors::get_raw(&v, "version.encode.5.2.0").parse().unwrap();
        assert_eq!(encoded_5_2_0, encode(5, 2, 0));

        let same_1: bool = vectors::get_raw(&v, "version.same.1").parse().unwrap();
        assert!(same_1);
        assert!(is_same(encode(5, 2, 28)));

        let same_0: bool = vectors::get_raw(&v, "version.same.0").parse().unwrap();
        assert!(!same_0);
        assert!(!is_same(0));
    }
}
