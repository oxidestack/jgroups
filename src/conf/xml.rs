//! XML stack parsing: `<config>` with one child element per protocol, attributes are that
//! protocol's properties.
//!
//! Port of `upstream/src/org/jgroups/conf/XmlConfigurator.java:98-135` (`parseProtocols`) and the
//! protocol-name resolution `stack/Configurator.java` applies before instantiation
//! (`Global.java:74`: a bare name is prefixed `org.jgroups.protocols.`, a dotted name like
//! `pbcast.NAKACK2` becomes `org.jgroups.protocols.pbcast.NAKACK2`). Nested per-protocol subnodes
//! (`XmlConfigurator.java:117-130`, used by protocols with structured config, e.g. `SSL_KEY_EXCHANGE`)
//! are out of scope: none of the protocols this crate runs uses one.

use quick_xml::events::Event as XmlEvent;
use quick_xml::reader::Reader;

/// One `<Protocol attr="val" .../>` element: the protocol's short name as written in the XML
/// (before `org.jgroups.protocols.` resolution) and its attributes, in document order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProtocolConfig {
    /// The element's name as written, `TCP` or `pbcast.NAKACK2`.
    pub name: String,
    /// The element's attributes, name and value, in document order.
    pub attrs: Vec<(String, String)>,
}

impl ProtocolConfig {
    /// The value of the attribute `key`, if the element has it.
    pub fn get(&self, key: &str) -> Option<&str> {
        self.attrs.iter().find(|(k, _)| k == key).map(|(_, v)| v.as_str())
    }

    /// `stack/Configurator.java`'s class-name resolution (`Global.java:74`): a name containing a
    /// `.` is already namespaced under `org.jgroups.protocols` (e.g. `pbcast.NAKACK2` ->
    /// `org.jgroups.protocols.pbcast.NAKACK2`); a bare name gets `org.jgroups.protocols.` directly.
    /// This crate only ever needs to recognize the *last* segment (`TCP`, `TCPPING`) to pick a
    /// concrete Rust type, but the full class name is kept for diagnostics.
    pub fn fully_qualified_name(&self) -> String {
        format!("org.jgroups.protocols.{}", self.name)
    }

    /// The bare protocol identifier used to select an implementation, e.g. `"NAKACK2"` out of
    /// `"pbcast.NAKACK2"`.
    pub fn short_name(&self) -> &str {
        self.name.rsplit('.').next().unwrap_or(&self.name)
    }
}

/// A stack configuration that is not well-formed XML or not a JGroups stack (`<config>` of protocol elements).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct XmlParseError(pub String);

impl std::fmt::Display for XmlParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "XML stack parse error: {}", self.0)
    }
}
impl std::error::Error for XmlParseError {}

/// Parses a `<config>...</config>` document into an ordered list of protocol configurations.
/// `XmlConfigurator.parseProtocols`: the root element must (case-insensitively) be `config`; every
/// direct child is one protocol, its element name the protocol, its attributes the properties.
pub fn parse_stack(xml: &str) -> Result<Vec<ProtocolConfig>, XmlParseError> {
    let mut reader = Reader::from_str(xml);
    reader.config_mut().trim_text(true);

    let mut protocols = Vec::new();
    let mut seen_root = false;
    let mut depth: i32 = 0;

    loop {
        match reader.read_event() {
            Err(e) => return Err(XmlParseError(format!("{e}"))),
            Ok(XmlEvent::Eof) => break,
            Ok(XmlEvent::Start(tag)) => {
                depth += 1;
                let name = local_name(&tag);
                if !seen_root {
                    if name.to_lowercase() != "config" {
                        return Err(XmlParseError(format!(
                            "the configuration does not start with a <config> element: {name}"
                        )));
                    }
                    seen_root = true;
                    continue;
                }
                if depth == 2 {
                    protocols.push(protocol_config(&reader, &tag, name)?);
                }
                // depth > 2: a protocol's own nested subnodes, out of scope (see module doc).
            }
            Ok(XmlEvent::Empty(tag)) => {
                let name = local_name(&tag);
                if !seen_root {
                    return Err(XmlParseError(format!(
                        "the configuration does not start with a <config> element: {name}"
                    )));
                }
                if depth == 1 {
                    protocols.push(protocol_config(&reader, &tag, name)?);
                }
            }
            Ok(XmlEvent::End(_)) => {
                depth -= 1;
            }
            _ => {}
        }
    }

    if !seen_root {
        return Err(XmlParseError("empty document: no <config> element".into()));
    }
    Ok(protocols)
}

fn local_name(tag: &quick_xml::events::BytesStart<'_>) -> String {
    tag.local_name().as_ref().to_string()
}

fn protocol_config(
    _reader: &Reader<&[u8]>,
    tag: &quick_xml::events::BytesStart<'_>,
    name: String,
) -> Result<ProtocolConfig, XmlParseError> {
    let mut attrs: Vec<(String, String)> = Vec::new();
    for attr in tag.attributes() {
        // quick-xml's default (checked) attribute iterator already rejects a duplicate
        // attribute name on one element, which is a stricter but compatible reading of
        // `XmlConfigurator.parseProtocols` (`node.getAttributes()` builds a `Map`, so upstream
        // silently keeps only one value too — quick-xml surfaces it as an error instead).
        let attr = attr.map_err(|e| XmlParseError(format!("{e}")))?;
        let key = attr.key.local_name().as_ref().to_string();
        let value = attr
            .normalized_value(quick_xml::XmlVersion::Implicit1_0)
            .map_err(|e| XmlParseError(format!("{e}")))?
            .into_owned();
        attrs.push((key, value));
    }
    Ok(ProtocolConfig { name, attrs })
}

#[cfg(test)]
mod tests {
    use super::*;

    // tests/jgroups_kubernetes_test.rs and tests/shared_loopback_test.rs parse the real
    // `jgroups-kubernetes.xml`/`jgroups-loopback.xml`; these unit tests cover the parser itself with
    // a minimal TCP+TCPPING fragment.
    #[test]
    fn parses_minimal_tcp_tcpping_stack() {
        let xml = r#"<config>
            <TCP bind_addr="127.0.0.1" bind_port="7800" tcp_nodelay="true"/>
            <TCPPING initial_hosts="127.0.0.1[7800],127.0.0.1[7801]" port_range="1"/>
        </config>"#;
        let protocols = parse_stack(xml).unwrap();
        assert_eq!(protocols.len(), 2);
        assert_eq!(protocols[0].name, "TCP");
        assert_eq!(protocols[0].get("bind_addr"), Some("127.0.0.1"));
        assert_eq!(protocols[0].get("bind_port"), Some("7800"));
        assert_eq!(protocols[1].name, "TCPPING");
        assert_eq!(protocols[1].get("initial_hosts"), Some("127.0.0.1[7800],127.0.0.1[7801]"));
        assert_eq!(protocols[0].fully_qualified_name(), "org.jgroups.protocols.TCP");
    }

    #[test]
    fn dotted_name_short_name_resolves_last_segment() {
        let xml = r#"<config><pbcast.NAKACK2 use_mcast_xmit="false"/></config>"#;
        let protocols = parse_stack(xml).unwrap();
        assert_eq!(protocols[0].name, "pbcast.NAKACK2");
        assert_eq!(protocols[0].short_name(), "NAKACK2");
        assert_eq!(protocols[0].fully_qualified_name(), "org.jgroups.protocols.pbcast.NAKACK2");
    }

    #[test]
    fn rejects_non_config_root() {
        let err = parse_stack(r#"<stack><TCP/></stack>"#).unwrap_err();
        assert!(err.0.contains("does not start with a <config> element"), "{}", err.0);
    }

    #[test]
    fn empty_protocol_has_no_attrs() {
        let protocols = parse_stack(r#"<config><TCPPING/></config>"#).unwrap();
        assert_eq!(protocols[0].attrs.len(), 0);
    }

    #[test]
    fn duplicate_attribute_is_a_parse_error() {
        let err = parse_stack(r#"<config><TCP bind_port="7800" bind_port="7801"/></config>"#).unwrap_err();
        assert!(err.0.contains("duplicated attribute"), "{}", err.0);
    }
}
