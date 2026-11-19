//! Property converters TCP/TCPPING's attributes need. Port of the relevant branches of
//! `upstream/src/org/jgroups/conf/PropertyConverters.java:160-216` (`Default.convert`) and
//! `util/Util.java:2576-2615` (`readBytes`/`readBytesInteger`/`readBytesLong`).
//!
//! Not ported: `float`/`double`/`byte`/enum/`InetAddress`-with-scope-fixup converters (nothing in
//! this crate's protocol set declares a field of those types) and every other `@Property`
//! converter class in the file (`NetworkInterfaceList`, `BindInterface`, etc. — used by protocols
//! this crate does not run).

/// A property value that does not convert to the type its protocol declares, with JGroups's message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConvertError(pub String);

impl std::fmt::Display for ConvertError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
impl std::error::Error for ConvertError {}

/// `Boolean.parseBoolean` — case-insensitive `"true"`/anything else is false, never an error
/// (matches the JDK method this stands in for, unlike Rust's `str::parse::<bool>`).
pub fn parse_bool(value: &str) -> bool {
    value.eq_ignore_ascii_case("true")
}

/// `Util.readBytesInteger`: a numeric literal (parsed as `f64`, so `"1.5k"` is valid, as upstream
/// allows) with an optional `k`/`kb`/`m`/`mb`/`g`/`gb` suffix (case-insensitive), k=1000,
/// m=1,000,000, g=1,000,000,000 — decimal, not binary (`Util.java:2617-2618`'s own comment on why).
pub fn read_bytes_i32(input: &str) -> Result<i32, ConvertError> {
    Ok(read_bytes_f64(input)? as i32)
}

/// `Util.readBytesLong`: [`read_bytes_i32`]'s syntax, as an `i64`.
pub fn read_bytes_i64(input: &str) -> Result<i64, ConvertError> {
    Ok(read_bytes_f64(input)? as i64)
}

fn read_bytes_f64(input: &str) -> Result<f64, ConvertError> {
    let lower = input.trim().to_lowercase();
    let (numeric_part, factor) = split_suffix(&lower);
    let num: f64 = numeric_part
        .parse()
        .map_err(|_| ConvertError(format!("not a byte-size value: {input:?}")))?;
    Ok(num * factor as f64)
}

/// `Util.readBytes`'s suffix search: it looks for the *first* matching character/substring in
/// scan order `k, kb, m, mb, g, gb`, cutting the numeric part there — note that because `k` is
/// tested before `kb`, `"1kb"` matches on the `k` at index 1 exactly the same as `"1k"` would,
/// so the trailing `b` is silently dropped from consideration; this reproduces that quirk rather
/// than "fixing" it, since upstream's actual behaviour is the oracle for this converter.
fn split_suffix(lower: &str) -> (&str, i64) {
    if let Some(idx) = lower.find('k') {
        return (&lower[..idx], 1_000);
    }
    if let Some(idx) = lower.find('m') {
        return (&lower[..idx], 1_000_000);
    }
    if let Some(idx) = lower.find('g') {
        return (&lower[..idx], 1_000_000_000);
    }
    (lower, 1)
}

/// `Util.parseCommaDelimitedStrings` as used by the comma-delimited-hosts and bind_addr
/// converters: trims each token, drops empty ones.
pub fn parse_comma_delimited(value: &str) -> Vec<String> {
    value.split(',').map(str::trim).filter(|s| !s.is_empty()).map(str::to_string).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bool_true_variants() {
        assert!(parse_bool("true"));
        assert!(parse_bool("TRUE"));
        assert!(parse_bool("True"));
    }

    #[test]
    fn bool_anything_else_is_false() {
        assert!(!parse_bool("false"));
        assert!(!parse_bool("yes"));
        assert!(!parse_bool(""));
    }

    #[test]
    fn plain_integer() {
        assert_eq!(read_bytes_i32("60000").unwrap(), 60000);
    }

    #[test]
    fn k_suffix_is_1000() {
        assert_eq!(read_bytes_i32("60k").unwrap(), 60000);
        assert_eq!(read_bytes_i32("60K").unwrap(), 60000);
    }

    #[test]
    fn m_suffix_is_1_000_000() {
        assert_eq!(read_bytes_i64("2m").unwrap(), 2_000_000);
        assert_eq!(read_bytes_i64("2M").unwrap(), 2_000_000);
    }

    #[test]
    fn fractional_numeric_part() {
        assert_eq!(read_bytes_i32("1.5k").unwrap(), 1500);
    }

    #[test]
    fn invalid_value_errors() {
        assert!(read_bytes_i32("not-a-number").is_err());
    }

    #[test]
    fn comma_delimited_trims_and_drops_empty() {
        assert_eq!(
            parse_comma_delimited(" a , b ,,c"),
            vec!["a".to_string(), "b".to_string(), "c".to_string()]
        );
    }
}
