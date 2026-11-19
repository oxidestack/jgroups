//! XML stack configuration: parsing (`xml.rs`), `${VAR:default}` substitution (`vars.rs`),
//! property converters (`convert.rs`) and `bind_addr` resolution (`bind_addr.rs`). Together these
//! port the subset of `upstream/src/org/jgroups/conf/` and `stack/Configurator.java` this crate's
//! protocols need — see each submodule's doc comment for exactly which upstream
//! methods and lines.

pub mod bind_addr;
pub mod convert;
pub mod vars;
pub mod xml;

pub use xml::{ProtocolConfig, XmlParseError};

/// An ordered protocol stack configuration, `${VAR:default}`-substituted, ready to hand to
/// `crate::stack::build_stack`.
pub type StackConfig = Vec<ProtocolConfig>;

/// Parses a stack XML document and substitutes `${VAR:default}` in every attribute value
/// (`XmlConfigurator.parse` + the variable substitution `Configurator` applies per-property before
/// handing values to `PropertyConverters`).
pub fn parse(xml: &str) -> Result<StackConfig, XmlParseError> {
    let mut protocols = xml::parse_stack(xml)?;
    for protocol in &mut protocols {
        for (_, value) in &mut protocol.attrs {
            *value = vars::substitute_variable(value);
        }
    }
    Ok(protocols)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn substitutes_variables_in_attributes() {
        unsafe { std::env::set_var("JGROUPS_RS_TEST_CONF_MOD_PORT", "7801") };
        let cfg = parse(r#"<config><TCP bind_port="${JGROUPS_RS_TEST_CONF_MOD_PORT}"/></config>"#).unwrap();
        assert_eq!(cfg[0].get("bind_port"), Some("7801"));
        unsafe { std::env::remove_var("JGROUPS_RS_TEST_CONF_MOD_PORT") };
    }

    #[test]
    fn unresolved_variable_without_default_stays_literal() {
        unsafe { std::env::remove_var("JGROUPS_RS_TEST_CONF_MOD_UNSET") };
        let cfg = parse(r#"<config><TCP bind_addr="${JGROUPS_RS_TEST_CONF_MOD_UNSET}"/></config>"#).unwrap();
        assert_eq!(cfg[0].get("bind_addr"), Some("${JGROUPS_RS_TEST_CONF_MOD_UNSET}"));
    }
}
