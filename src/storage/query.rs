//! Query types for label-based sensor queries.
//!
//! This module provides a generic interface for querying sensors by labels,
//! inspired by Prometheus's label matching model but designed to be
//! storage-backend agnostic.

/// Type of matching operation for label filters.
///
/// This enum represents the four types of label matching operations
/// supported by the query interface, matching Prometheus's label matcher types.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MatcherType {
    /// Exact string equality (`=`)
    ///
    /// Matches when the label value exactly equals the specified value.
    Equal,

    /// Exact string inequality (`!=`)
    ///
    /// Matches when the label value does not equal the specified value,
    /// or when the label is not present.
    NotEqual,

    /// Regular expression match (`=~`)
    ///
    /// Matches when the label value matches the specified regex pattern.
    /// The regex syntax depends on the storage backend implementation.
    RegexMatch,

    /// Negated regular expression match (`!~`)
    ///
    /// Matches when the label value does not match the specified regex pattern,
    /// or when the label is not present.
    RegexNotMatch,
}

impl MatcherType {
    /// Returns true if this matcher uses regex matching.
    #[inline]
    #[allow(dead_code)] // Useful API method for future PromQL query support
    pub fn is_regex(&self) -> bool {
        matches!(self, Self::RegexMatch | Self::RegexNotMatch)
    }

    /// Returns true if this matcher is negated (NOT equal or NOT matching).
    #[inline]
    #[allow(dead_code)] // Useful API method for future PromQL query support
    pub fn is_negated(&self) -> bool {
        matches!(self, Self::NotEqual | Self::RegexNotMatch)
    }
}

/// A label matcher for filtering sensors by label values.
///
/// Label matchers are used to select sensors based on their labels.
/// Multiple matchers are combined with AND logic - a sensor must match
/// all matchers to be selected.
///
/// # Special Labels
///
/// The label name `__name__` is reserved for matching the metric/sensor name.
/// This follows the Prometheus convention.
///
/// # Examples
///
/// ```
/// use sensapp::storage::query::{LabelMatcher, MatcherType};
///
/// // Match sensors with name "cpu_usage"
/// let name_matcher = LabelMatcher::eq("__name__", "cpu_usage");
///
/// // Match sensors in the "production" environment
/// let env_matcher = LabelMatcher::eq("environment", "production");
///
/// // Match sensors with instance matching a pattern
/// let instance_matcher = LabelMatcher::regex("instance", "server-[0-9]+");
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LabelMatcher {
    /// Label name to match against.
    ///
    /// Use `__name__` to filter by metric/sensor name.
    pub name: String,

    /// Value to match (literal string or regex pattern depending on `matcher_type`).
    pub value: String,

    /// Type of matching operation to perform.
    pub matcher_type: MatcherType,
}

impl LabelMatcher {
    /// Creates a new label matcher.
    pub fn new(
        name: impl Into<String>,
        value: impl Into<String>,
        matcher_type: MatcherType,
    ) -> Self {
        Self {
            name: name.into(),
            value: value.into(),
            matcher_type,
        }
    }

    /// Creates an equality matcher (`=`).
    ///
    /// Matches sensors where the label exactly equals the value.
    pub fn eq(name: impl Into<String>, value: impl Into<String>) -> Self {
        Self::new(name, value, MatcherType::Equal)
    }

    /// Creates a not-equal matcher (`!=`).
    ///
    /// Matches sensors where the label does not equal the value.
    #[allow(dead_code)] // Used in integration tests
    pub fn neq(name: impl Into<String>, value: impl Into<String>) -> Self {
        Self::new(name, value, MatcherType::NotEqual)
    }

    /// Creates a regex match matcher (`=~`).
    ///
    /// Matches sensors where the label matches the regex pattern.
    #[allow(dead_code)] // Used in integration tests
    pub fn regex(name: impl Into<String>, pattern: impl Into<String>) -> Self {
        Self::new(name, pattern, MatcherType::RegexMatch)
    }

    /// Creates a negated regex matcher (`!~`).
    ///
    /// Matches sensors where the label does not match the regex pattern.
    #[allow(dead_code)] // Used in integration tests
    pub fn not_regex(name: impl Into<String>, pattern: impl Into<String>) -> Self {
        Self::new(name, pattern, MatcherType::RegexNotMatch)
    }

    /// Returns true if this matcher targets the metric name (`__name__`).
    #[inline]
    pub fn is_name_matcher(&self) -> bool {
        self.name == "__name__"
    }
}

impl std::fmt::Display for LabelMatcher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let op = match self.matcher_type {
            MatcherType::Equal => "=",
            MatcherType::NotEqual => "!=",
            MatcherType::RegexMatch => "=~",
            MatcherType::RegexNotMatch => "!~",
        };
        write!(f, "{}{}\"{}\"", self.name, op, self.value)
    }
}

// Conversion from Prometheus LabelMatcher
impl From<&crate::parsing::prometheus::remote_read_models::LabelMatcher> for LabelMatcher {
    fn from(m: &crate::parsing::prometheus::remote_read_models::LabelMatcher) -> Self {
        use crate::parsing::prometheus::remote_read_models::label_matcher::Type;

        let matcher_type = match Type::try_from(m.r#type) {
            Ok(Type::Eq) => MatcherType::Equal,
            Ok(Type::Neq) => MatcherType::NotEqual,
            Ok(Type::Re) => MatcherType::RegexMatch,
            Ok(Type::Nre) => MatcherType::RegexNotMatch,
            // Default to Equal for unknown types (defensive)
            Err(_) => MatcherType::Equal,
        };

        Self {
            name: m.name.clone(),
            value: m.value.clone(),
            matcher_type,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_matcher_type_is_regex() {
        assert!(!MatcherType::Equal.is_regex());
        assert!(!MatcherType::NotEqual.is_regex());
        assert!(MatcherType::RegexMatch.is_regex());
        assert!(MatcherType::RegexNotMatch.is_regex());
    }

    #[test]
    fn test_matcher_type_is_negated() {
        assert!(!MatcherType::Equal.is_negated());
        assert!(MatcherType::NotEqual.is_negated());
        assert!(!MatcherType::RegexMatch.is_negated());
        assert!(MatcherType::RegexNotMatch.is_negated());
    }

    #[test]
    fn test_label_matcher_constructors() {
        let eq = LabelMatcher::eq("foo", "bar");
        assert_eq!(eq.name, "foo");
        assert_eq!(eq.value, "bar");
        assert_eq!(eq.matcher_type, MatcherType::Equal);

        let neq = LabelMatcher::neq("foo", "bar");
        assert_eq!(neq.matcher_type, MatcherType::NotEqual);

        let regex = LabelMatcher::regex("foo", "bar.*");
        assert_eq!(regex.matcher_type, MatcherType::RegexMatch);

        let not_regex = LabelMatcher::not_regex("foo", "bar.*");
        assert_eq!(not_regex.matcher_type, MatcherType::RegexNotMatch);
    }

    #[test]
    fn test_label_matcher_is_name_matcher() {
        let name_matcher = LabelMatcher::eq("__name__", "cpu_usage");
        assert!(name_matcher.is_name_matcher());

        let label_matcher = LabelMatcher::eq("environment", "production");
        assert!(!label_matcher.is_name_matcher());
    }

    #[test]
    fn test_label_matcher_display() {
        assert_eq!(LabelMatcher::eq("foo", "bar").to_string(), "foo=\"bar\"");
        assert_eq!(LabelMatcher::neq("foo", "bar").to_string(), "foo!=\"bar\"");
        assert_eq!(
            LabelMatcher::regex("foo", "bar.*").to_string(),
            "foo=~\"bar.*\""
        );
        assert_eq!(
            LabelMatcher::not_regex("foo", "bar.*").to_string(),
            "foo!~\"bar.*\""
        );
    }

    #[test]
    fn test_label_matcher_new() {
        let matcher = LabelMatcher::new("test", "value", MatcherType::Equal);
        assert_eq!(matcher.name, "test");
        assert_eq!(matcher.value, "value");
        assert_eq!(matcher.matcher_type, MatcherType::Equal);
    }

    #[test]
    fn test_label_matcher_equality() {
        let m1 = LabelMatcher::eq("foo", "bar");
        let m2 = LabelMatcher::eq("foo", "bar");
        let m3 = LabelMatcher::eq("foo", "baz");

        assert_eq!(m1, m2);
        assert_ne!(m1, m3);
    }

    #[test]
    fn test_label_matcher_clone() {
        let original = LabelMatcher::regex("instance", "server-[0-9]+");
        let cloned = original.clone();

        assert_eq!(original, cloned);
    }

    #[test]
    fn test_from_prometheus_label_matcher() {
        use crate::parsing::prometheus::remote_read_models::LabelMatcher as PromLabelMatcher;
        use crate::parsing::prometheus::remote_read_models::label_matcher::Type as PromType;

        // Test EQ
        let prom_eq = PromLabelMatcher {
            r#type: PromType::Eq as i32,
            name: "__name__".to_string(),
            value: "cpu_usage".to_string(),
        };
        let matcher: LabelMatcher = (&prom_eq).into();
        assert_eq!(matcher.name, "__name__");
        assert_eq!(matcher.value, "cpu_usage");
        assert_eq!(matcher.matcher_type, MatcherType::Equal);

        // Test NEQ
        let prom_neq = PromLabelMatcher {
            r#type: PromType::Neq as i32,
            name: "env".to_string(),
            value: "test".to_string(),
        };
        let matcher: LabelMatcher = (&prom_neq).into();
        assert_eq!(matcher.matcher_type, MatcherType::NotEqual);

        // Test RE
        let prom_re = PromLabelMatcher {
            r#type: PromType::Re as i32,
            name: "instance".to_string(),
            value: "server-[0-9]+".to_string(),
        };
        let matcher: LabelMatcher = (&prom_re).into();
        assert_eq!(matcher.matcher_type, MatcherType::RegexMatch);

        // Test NRE
        let prom_nre = PromLabelMatcher {
            r#type: PromType::Nre as i32,
            name: "job".to_string(),
            value: "test.*".to_string(),
        };
        let matcher: LabelMatcher = (&prom_nre).into();
        assert_eq!(matcher.matcher_type, MatcherType::RegexNotMatch);
    }

    #[test]
    fn test_from_prometheus_label_matcher_unknown_type() {
        use crate::parsing::prometheus::remote_read_models::LabelMatcher as PromLabelMatcher;

        // Test unknown type defaults to Equal
        let prom_unknown = PromLabelMatcher {
            r#type: 999, // Invalid type
            name: "test".to_string(),
            value: "value".to_string(),
        };
        let matcher: LabelMatcher = (&prom_unknown).into();
        assert_eq!(matcher.matcher_type, MatcherType::Equal);
    }
}
