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
    pub fn is_regex(&self) -> bool {
        matches!(self, Self::RegexMatch | Self::RegexNotMatch)
    }

    /// Returns true if this matcher is negated (NOT equal or NOT matching).
    #[inline]
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
/// ```no_run,standalone_crate
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

    /// Value to match: a literal string, or for the regex types the pattern **fully anchored**
    /// (`^(?:pattern)$`), as `LabelMatcher::new` makes it. A regex matcher matches the whole
    /// value, never a part of it, like in Prometheus: `region=~"eu"` matches `eu`, not `eu-west`.
    pub value: String,

    /// Type of matching operation to perform.
    pub matcher_type: MatcherType,
}

/// The leading flag groups of a pattern, such as `(?i)`, and the rest.
fn split_leading_flags(pattern: &str) -> (&str, &str) {
    let mut flags_end = 0;
    let mut rest = pattern;
    while let Some(after) = rest.strip_prefix("(?") {
        match after.find(')') {
            Some(close)
                if close > 0
                    && after[..close]
                        .chars()
                        .all(|c| c.is_ascii_alphabetic() || c == '-') =>
            {
                flags_end += 2 + close + 1;
                rest = &after[close + 1..];
            }
            _ => break,
        }
    }
    pattern.split_at(flags_end)
}

/// The pattern of a regex matcher, anchored on both ends as Prometheus does: `(?s)^(?:pattern)$`.
/// `(?s)` makes `.` match a newline too, as in Prometheus (which anchors with `^(?s:pattern)$`)
/// and in PostgreSQL, where it is the default.
///
/// Leading flags such as `(?i)` are merged with it in front of the anchors: PostgreSQL only
/// accepts one group of embedded flags, at the very start of a regular expression. Flags other
/// than `i` and `s` are refused by [`validate_regex`]; without validation they are kept as they
/// were written.
pub fn anchor_regex(pattern: &str) -> String {
    let (flags, body) = split_leading_flags(pattern);
    let letters: String = flags
        .chars()
        .filter(|c| c.is_ascii_alphabetic() || *c == '-')
        .collect();
    if letters.chars().all(|c| c == 'i' || c == 's') {
        let case_insensitive = if letters.contains('i') { "i" } else { "" };
        format!("(?{case_insensitive}s)^(?:{body})$")
    } else {
        format!("{flags}^(?:{body})$")
    }
}

/// Check that the pattern of a regex matcher is RE2, the dialect of Prometheus, and means the same
/// on every backend: ClickHouse, DuckDB and BigQuery use RE2, SQLite and the in-memory matching use
/// the `regex` crate (RE2 syntax), and PostgreSQL its own engine. So it refuses:
///
/// - what is not RE2: unbalanced groups, back-references, lookarounds;
/// - the escapes PostgreSQL reads otherwise: `\b` is a backspace there, `\B` a backslash, `\x`
///   takes any number of hex digits, `\p{..}`, `\z`, `\Q` and `\<` do not exist, and RE2 has no
///   `\u`. Allowed are `\d \D \s \S \w \W \A \n \t \r \f \v \a` and the escaped punctuation;
/// - what the engines read differently: a `[` within a class (nested for the `regex` crate, a
///   literal for PostgreSQL and RE2, so the class ends elsewhere), the set operations `&&`, `--`
///   and `~~`, a negated POSIX class, a repetition bound above 255 (PostgreSQL's limit), and a
///   repetition of a repetition or of an assertion (`a**`, `^*`, `\A*`);
/// - groups other than `(...)` and `(?:...)`, and flags other than a leading `(?i)` or `(?s)`:
///   PostgreSQL has no named groups and no flags within the pattern, and `(?m)`, `(?x)`,
///   `(?q)` mean other things there.
pub fn validate_regex(pattern: &str) -> anyhow::Result<()> {
    let refuse = |why: &str| anyhow::anyhow!("The regex {pattern:?} is not supported: {why}");
    regex::Regex::new(pattern).map_err(|error| {
        let first_line = error.to_string();
        let first_line = first_line.lines().last().unwrap_or_default().to_string();
        refuse(&format!("it is not RE2 ({})", first_line.trim()))
    })?;

    let (flags, body) = split_leading_flags(pattern);
    if !flags
        .chars()
        .filter(|c| c.is_ascii_alphabetic() || *c == '-')
        .all(|c| c == 'i' || c == 's')
    {
        return Err(refuse("only the flags (?i) and (?s) are supported"));
    }

    // What the last token was, to refuse a repetition of a repetition or of an assertion
    #[derive(PartialEq)]
    enum Previous {
        Other,
        Quantifier,
        Assertion,
    }
    let mut previous = Previous::Other;
    let mut chars = body.chars().peekable();
    let mut in_class = false;
    while let Some(c) = chars.next() {
        let was = std::mem::replace(&mut previous, Previous::Other);
        match c {
            '*' | '+' | '?' if !in_class => {
                if was == Previous::Assertion || (was == Previous::Quantifier && c != '?') {
                    return Err(refuse(
                        "a repetition of a repetition or of an assertion is not supported",
                    ));
                }
                // A `?` after a quantifier makes it lazy
                if was != Previous::Quantifier {
                    previous = Previous::Quantifier;
                }
            }
            '{' if !in_class => {
                let mut lookahead = chars.clone();
                let content: String = lookahead.by_ref().take_while(|&c| c != '}').collect();
                let bounds: Vec<&str> = content.split(',').collect();
                let is_repetition = !content.is_empty()
                    && bounds.len() <= 2
                    && bounds
                        .iter()
                        .all(|bound| bound.chars().all(|c| c.is_ascii_digit()))
                    && !bounds[0].is_empty();
                if is_repetition {
                    // PostgreSQL stops at 255, RE2 at 1000
                    if bounds
                        .iter()
                        .any(|bound| bound.parse::<u32>().map_or(!bound.is_empty(), |n| n > 255))
                    {
                        return Err(refuse("a repetition bound above 255 is not supported"));
                    }
                    if was != Previous::Other {
                        return Err(refuse(
                            "a repetition of a repetition or of an assertion is not supported",
                        ));
                    }
                    chars = lookahead;
                    previous = Previous::Quantifier;
                }
            }
            '^' | '$' if !in_class => previous = Previous::Assertion,
            '&' | '-' | '~' if in_class && chars.peek() == Some(&c) => {
                return Err(refuse(
                    "the set operations of a character class (&&, --, ~~) are not supported",
                ));
            }
            // A `[` in a class is nested for the `regex` crate and a literal for PostgreSQL and
            // RE2, which end the class at the first `]`: the anchors would not be where they seem.
            // Only a POSIX class `[:alpha:]` may start there.
            '[' if in_class && chars.peek() != Some(&':') => {
                return Err(refuse("a character class within a character class"));
            }
            '\\' => {
                let Some(escaped) = chars.next() else { break };
                if escaped.is_ascii_alphanumeric() {
                    match escaped {
                        // An assertion like `^`: PostgreSQL refuses its repetition
                        'A' if !in_class => previous = Previous::Assertion,
                        'd' | 'D' | 's' | 'S' | 'w' | 'W' | 'A' | 'n' | 't' | 'r' | 'f' | 'v'
                        | 'a' => {}
                        _ => {
                            return Err(refuse(&format!(
                                "\\{escaped} means another thing in PostgreSQL, or nothing"
                            )));
                        }
                    }
                } else if escaped == '<' || escaped == '>' {
                    return Err(refuse("word boundaries are not supported"));
                }
            }
            '[' if !in_class => {
                in_class = true;
                // A `]` right after `[` or `[^` is a literal
                if chars.peek() == Some(&'^') {
                    chars.next();
                }
                if chars.peek() == Some(&']') {
                    chars.next();
                }
            }
            // A POSIX class within a class: `[[:digit:]]`
            '[' if chars.peek() == Some(&':') => {
                // `[:^digit:]` does not exist in PostgreSQL
                if chars.clone().nth(1) == Some('^') {
                    return Err(refuse("a negated POSIX class is not supported"));
                }
                for inner in chars.by_ref() {
                    if inner == ']' {
                        break;
                    }
                }
            }
            ']' if in_class => in_class = false,
            '(' if !in_class && chars.peek() == Some(&'?') => {
                chars.next();
                if chars.next() != Some(':') {
                    return Err(refuse(
                        "only (...) and (?:...) groups, and leading (?i) or (?s) flags, are supported",
                    ));
                }
            }
            _ => {}
        }
    }
    Ok(())
}

impl LabelMatcher {
    /// Creates a new label matcher. The pattern of a regex matcher is anchored, see
    /// [`anchor_regex`].
    pub fn new(
        name: impl Into<String>,
        value: impl Into<String>,
        matcher_type: MatcherType,
    ) -> Self {
        let value = value.into();
        let value = if matcher_type.is_regex() {
            anchor_regex(&value)
        } else {
            value
        };
        Self {
            name: name.into(),
            value,
            matcher_type,
        }
    }

    /// Creates a matcher from a request: a regex that is not RE2, or that a backend would read
    /// otherwise, is an error (see [`validate_regex`]).
    pub fn try_new(
        name: impl Into<String>,
        value: impl Into<String>,
        matcher_type: MatcherType,
    ) -> anyhow::Result<Self> {
        let value = value.into();
        if matcher_type.is_regex() {
            validate_regex(&value)?;
        }
        Ok(Self::new(name, value, matcher_type))
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
    pub fn neq(name: impl Into<String>, value: impl Into<String>) -> Self {
        Self::new(name, value, MatcherType::NotEqual)
    }

    /// Creates a regex match matcher (`=~`).
    ///
    /// Matches sensors where the label matches the regex pattern.
    pub fn regex(name: impl Into<String>, pattern: impl Into<String>) -> Self {
        Self::new(name, pattern, MatcherType::RegexMatch)
    }

    /// Creates a negated regex matcher (`!~`).
    ///
    /// Matches sensors where the label does not match the regex pattern.
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

// Conversion from Prometheus LabelMatcher. A type Prometheus does not have is an error, and not
// read as `=`: a reader would get another answer than the one it asked for.
impl TryFrom<&crate::parsing::prometheus::remote_read_models::LabelMatcher> for LabelMatcher {
    type Error = anyhow::Error;

    fn try_from(
        m: &crate::parsing::prometheus::remote_read_models::LabelMatcher,
    ) -> Result<Self, Self::Error> {
        use crate::parsing::prometheus::remote_read_models::label_matcher::Type;

        let matcher_type = match Type::try_from(m.r#type) {
            Ok(Type::Eq) => MatcherType::Equal,
            Ok(Type::Neq) => MatcherType::NotEqual,
            Ok(Type::Re) => MatcherType::RegexMatch,
            Ok(Type::Nre) => MatcherType::RegexNotMatch,
            Err(_) => anyhow::bail!("Unknown label matcher type {}", m.r#type),
        };

        Self::try_new(m.name.clone(), m.value.clone(), matcher_type)
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
        // The pattern shown is the one that is evaluated: anchored
        assert_eq!(
            LabelMatcher::regex("foo", "bar.*").to_string(),
            "foo=~\"(?s)^(?:bar.*)$\""
        );
        assert_eq!(
            LabelMatcher::not_regex("foo", "bar.*").to_string(),
            "foo!~\"(?s)^(?:bar.*)$\""
        );
    }

    #[test]
    fn regex_matchers_are_anchored_and_literals_are_not() {
        assert_eq!(LabelMatcher::regex("a", "b").value, "(?s)^(?:b)$");
        assert_eq!(LabelMatcher::not_regex("a", "b|c").value, "(?s)^(?:b|c)$");
        assert_eq!(LabelMatcher::eq("a", "b").value, "b");
        assert_eq!(LabelMatcher::neq("a", "b.*").value, "b.*");
        assert_eq!(LabelMatcher::regex("a", "").value, "(?s)^(?:)$");
    }

    #[test]
    fn leading_regex_flags_stay_in_front_of_the_anchors() {
        assert_eq!(anchor_regex("(?i)abc"), "(?is)^(?:abc)$");
        assert_eq!(anchor_regex("(?i)(?s)a.c"), "(?is)^(?:a.c)$");
        assert_eq!(anchor_regex("(?si)a.c"), "(?is)^(?:a.c)$");
        assert_eq!(anchor_regex("(?-i)abc"), "(?-i)^(?:abc)$");
        // Groups that are not flags are part of the pattern
        assert_eq!(anchor_regex("(?:ab)c"), "(?s)^(?:(?:ab)c)$");
        assert_eq!(anchor_regex("(ab)c"), "(?s)^(?:(ab)c)$");
        assert_eq!(anchor_regex("(?P<name>ab)"), "(?s)^(?:(?P<name>ab))$");
        // A flag group in the middle is left alone
        assert_eq!(anchor_regex("a(?i)b"), "(?s)^(?:a(?i)b)$");
        assert_eq!(anchor_regex("(?i"), "(?s)^(?:(?i)$");
    }

    #[test]
    fn regexes_are_checked_for_re2_and_postgresql() {
        for pattern in [
            "",
            "c7",
            "c7.*",
            "(?i)c7",
            "(?s)a.b",
            "(?is)a",
            "eu|us",
            "(a|b)c",
            "(?:a|b)c",
            r"\d+\.\d*",
            r"\w\s\S\W\D",
            r"\.\*\(\)\[\]\{\}\|\\",
            "[[:digit:]]+",
            "[]a]",
            "[^]a]",
            "[a-z(?]",
            r"[\d-]",
            "a{2,3}?",
            "a{255}",
            "a{1,255}",
            "a*?",
            r"\Aa*",
            "(a*)*",
            "[a-z0-9_.-]",
            "[a&b]",
            "[a~b]",
            "é",
        ] {
            assert!(
                validate_regex(pattern).is_ok(),
                "{pattern}: {:?}",
                validate_regex(pattern)
            );
        }
        for pattern in [
            "a)|(b",
            "(a",
            r"(a)\1",
            "(?=a)",
            "(?!a)",
            r"\bx",
            r"\Bx",
            r"\p{L}",
            r"\PL",
            r"\x41",
            r"\x{41}",
            r"\u{e9}",
            r"\z",
            r"\Qa\E",
            r"\<a",
            "(?-i)a",
            "(?m)a",
            "(?x)a # c",
            "(?U)a",
            "a(?i)b",
            "(?P<n>a)",
            "(?<n>a)",
            r"\C",
            // A `[` in a class: nested for the `regex` crate, a literal for PostgreSQL and RE2, so
            // they disagree on where the class ends and the anchors move
            "[a[b])|(c]]",
            "[a[bc]]",
            "[[a]b]",
            // Set operations of a class
            "[a&&b]",
            "[a-z--aeiou]",
            "[a~~b]",
            // Repetition bounds: PostgreSQL stops at 255, RE2 at 1000
            "a{256}",
            "a{1,256}",
            "a{1001}",
            // A repetition of a repetition or of an assertion
            "a**",
            "a+*",
            "a*{2}",
            "^*",
            "$+",
            r"\A*a",
            r"\A{2}a",
            // RE2 has no \u, and a negated POSIX class does not exist in PostgreSQL
            r"\u00e9",
            r"\U000000e9",
            "[[:^digit:]]",
        ] {
            assert!(validate_regex(pattern).is_err(), "{pattern}");
        }
    }

    #[test]
    fn remote_read_matchers_are_anchored_too() {
        use crate::parsing::prometheus::remote_read_models::{
            LabelMatcher as Remote, label_matcher::Type,
        };
        let matcher = LabelMatcher::try_from(&Remote {
            r#type: Type::Re as i32,
            name: "region".to_string(),
            value: "eu-.*".to_string(),
        })
        .unwrap();
        assert_eq!(matcher.matcher_type, MatcherType::RegexMatch);
        assert_eq!(matcher.value, "(?s)^(?:eu-.*)$");
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
        let matcher = LabelMatcher::try_from(&prom_eq).unwrap();
        assert_eq!(matcher.name, "__name__");
        assert_eq!(matcher.value, "cpu_usage");
        assert_eq!(matcher.matcher_type, MatcherType::Equal);

        // Test NEQ
        let prom_neq = PromLabelMatcher {
            r#type: PromType::Neq as i32,
            name: "env".to_string(),
            value: "test".to_string(),
        };
        let matcher = LabelMatcher::try_from(&prom_neq).unwrap();
        assert_eq!(matcher.matcher_type, MatcherType::NotEqual);

        // Test RE
        let prom_re = PromLabelMatcher {
            r#type: PromType::Re as i32,
            name: "instance".to_string(),
            value: "server-[0-9]+".to_string(),
        };
        let matcher = LabelMatcher::try_from(&prom_re).unwrap();
        assert_eq!(matcher.matcher_type, MatcherType::RegexMatch);

        // Test NRE
        let prom_nre = PromLabelMatcher {
            r#type: PromType::Nre as i32,
            name: "job".to_string(),
            value: "test.*".to_string(),
        };
        let matcher = LabelMatcher::try_from(&prom_nre).unwrap();
        assert_eq!(matcher.matcher_type, MatcherType::RegexNotMatch);
    }

    #[test]
    fn test_from_prometheus_label_matcher_unknown_type() {
        use crate::parsing::prometheus::remote_read_models::LabelMatcher as PromLabelMatcher;

        // An unknown type is refused
        let prom_unknown = PromLabelMatcher {
            r#type: 999, // Invalid type
            name: "test".to_string(),
            value: "value".to_string(),
        };
        let error = LabelMatcher::try_from(&prom_unknown).unwrap_err();
        assert!(error.to_string().contains("999"), "{error}");
    }
}
