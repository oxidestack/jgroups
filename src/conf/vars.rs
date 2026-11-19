//! `${VAR:default}` substitution, port of `upstream/src/org/jgroups/util/Util.java:4846-4925`
//! (`substituteVariable`/`_substituteVar`/`getProperty`/`_getProperty`).
//!
//! Upstream resolves a variable against three tiers: an explicit `Properties` object (JGroups
//! passes the ones set on `JChannel`/system properties helpers), the JVM's System properties, then
//! the environment. This crate has neither a Properties layer nor JVM system properties, so only
//! the environment tier applies — the substitution algorithm (repeat until fixed point, `\${`
//! escapes, comma-delimited alternative names, unresolved stays literal) is otherwise identical.

/// `Util.substituteVariable`: repeatedly resolves the first `${...}` in `val` against the
/// environment until no more remain or a pass makes no further progress (an unresolved variable
/// leaves its `${...}` literally in the string, which would loop forever without the
/// no-progress check upstream also has).
pub fn substitute_variable(val: &str) -> String {
    let mut retval = val.to_string();
    loop {
        if !retval.contains("${") {
            break;
        }
        let next = substitute_one(&retval);
        if next == retval {
            break;
        }
        retval = next;
    }
    retval
}

fn substitute_one(val: &str) -> String {
    if val.contains("\\${") {
        // Matches upstream's blunt escape check: if the string contains an escaped marker
        // *anywhere*, no substitution happens at all on this pass (Util.java:4863).
        return val.to_string();
    }
    let Some(start) = val.find("${") else {
        return val.to_string();
    };
    let Some(rel_end) = val[start + 2..].find('}') else {
        // Util.java:4867 throws IllegalArgumentException("missing \"}\" in " + val); this crate
        // has no caller that wants a panic mid-parse, so leave the string untouched instead —
        // callers that need strict validation can check for a literal "${" surviving.
        return val.to_string();
    };
    let end = start + 2 + rel_end;
    let spec = &val[start + 2..end];
    let Some(resolved) = get_property(spec) else {
        return val.to_string();
    };
    format!("{}{}{}", &val[..start], resolved, &val[end + 1..])
}

/// `Util.getProperty`/`_getProperty`: `spec` is `NAME` or `NAME:default`, where `NAME` may itself
/// be a comma-delimited list of alternative environment variable names tried in order.
fn get_property(spec: &str) -> Option<String> {
    let (names, default) = match spec.find(':') {
        Some(idx) => {
            let default = spec[idx + 1..].trim();
            (&spec[..idx], if default.is_empty() { None } else { Some(default.to_string()) })
        }
        None => (spec, None),
    };
    for name in names.split(',').map(str::trim).filter(|s| !s.is_empty()) {
        if let Ok(v) = std::env::var(name) {
            return Some(v);
        }
    }
    default
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_variable_is_unchanged() {
        assert_eq!(substitute_variable("plain value"), "plain value");
    }

    #[test]
    fn resolves_from_environment() {
        // SAFETY: test-local env mutation, single-threaded per-test via serial execution is not
        // guaranteed by cargo, so use a name unlikely to collide.
        unsafe { std::env::set_var("JGROUPS_RS_TEST_VAR_142", "resolved") };
        assert_eq!(substitute_variable("${JGROUPS_RS_TEST_VAR_142}"), "resolved");
        unsafe { std::env::remove_var("JGROUPS_RS_TEST_VAR_142") };
    }

    #[test]
    fn falls_back_to_default_when_unset() {
        unsafe { std::env::remove_var("JGROUPS_RS_TEST_VAR_142_UNSET") };
        assert_eq!(
            substitute_variable("${JGROUPS_RS_TEST_VAR_142_UNSET:fallback}"),
            "fallback"
        );
    }

    #[test]
    fn unresolved_without_default_stays_literal() {
        unsafe { std::env::remove_var("JGROUPS_RS_TEST_VAR_142_UNSET2") };
        assert_eq!(
            substitute_variable("prefix-${JGROUPS_RS_TEST_VAR_142_UNSET2}-suffix"),
            "prefix-${JGROUPS_RS_TEST_VAR_142_UNSET2}-suffix"
        );
    }

    #[test]
    fn embedded_in_larger_string() {
        unsafe { std::env::set_var("JGROUPS_RS_TEST_VAR_142_HOST", "10.0.0.1") };
        assert_eq!(
            substitute_variable("${JGROUPS_RS_TEST_VAR_142_HOST}[7800]"),
            "10.0.0.1[7800]"
        );
        unsafe { std::env::remove_var("JGROUPS_RS_TEST_VAR_142_HOST") };
    }

    #[test]
    fn comma_delimited_alternatives_tried_in_order() {
        unsafe { std::env::remove_var("JGROUPS_RS_TEST_VAR_142_A") };
        unsafe { std::env::set_var("JGROUPS_RS_TEST_VAR_142_B", "second") };
        assert_eq!(
            substitute_variable("${JGROUPS_RS_TEST_VAR_142_A,JGROUPS_RS_TEST_VAR_142_B:def}"),
            "second"
        );
        unsafe { std::env::remove_var("JGROUPS_RS_TEST_VAR_142_B") };
    }
}
