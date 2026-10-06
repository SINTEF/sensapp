//! The file name of a download, and its `Content-Disposition` header (RFC 6266, RFC 8187).
//!
//! Names and labels come from stored data: they can hold anything (spaces, slashes, quotes, line
//! breaks, non-ASCII letters). The name is cleaned for the file systems, and the header carries it
//! percent-encoded, with an ASCII fallback, so that the header is printable ASCII whatever the name.

use crate::datamodel::{SensAppDateTime, Sensor};
use crate::storage::Aggregation;
use axum::http::HeaderValue;

/// The longest file name, extension excluded, in characters.
const MAX_STEM_CHARS: usize = 150;

/// `<name>[_<key>-<value>…][_<window>][_<step>-<aggregation>].<extension>`, the labels sorted by key.
///
/// The window is `<start>_<end>` in UTC (`20261006T090000Z`), `from-<start>` or `until-<end>` when one
/// bound is missing, nothing when both are.
pub fn file_name(
    sensor: &Sensor,
    start: Option<SensAppDateTime>,
    end: Option<SensAppDateTime>,
    bucket: Option<(&str, Aggregation)>,
    extension: &str,
) -> String {
    let mut parts = vec![sensor.name.clone()];

    let mut labels: Vec<&(String, String)> = sensor.labels.iter().collect();
    labels.sort();
    parts.extend(labels.iter().map(|(key, value)| format!("{key}-{value}")));

    match (start, end) {
        (Some(start), Some(end)) => {
            parts.push(compact_utc(start));
            parts.push(compact_utc(end));
        }
        (Some(start), None) => parts.push(format!("from-{}", compact_utc(start))),
        (None, Some(end)) => parts.push(format!("until-{}", compact_utc(end))),
        (None, None) => {}
    }

    if let Some((step, aggregation)) = bucket {
        parts.push(format!("{step}-{}", aggregation.name()));
    }

    let stem: String = parts
        .iter()
        .map(|part| clean(part))
        .collect::<Vec<_>>()
        .join("_")
        .chars()
        .take(MAX_STEM_CHARS)
        .collect();
    // A leading dot hides the file, a trailing dot or space is dropped by Windows
    let stem = stem.trim_matches(|c: char| c == '.' || c.is_whitespace());
    let stem = if stem.is_empty() { "series" } else { stem };

    format!("{stem}.{extension}")
}

/// `attachment; filename="<ASCII fallback>"; filename*=UTF-8''<percent-encoded name>`
pub fn content_disposition(file_name: &str) -> HeaderValue {
    let fallback: String = file_name
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '_') {
                c
            } else {
                '_'
            }
        })
        .collect();
    // `urlencoding` keeps `A-Za-z0-9-._~` and encodes the rest, a subset of the RFC 8187 attr-char
    let value = format!(
        "attachment; filename=\"{fallback}\"; filename*=UTF-8''{}",
        urlencoding::encode(file_name)
    );
    // Printable ASCII by construction: the fallback cannot happen
    HeaderValue::from_str(&value).unwrap_or_else(|_| HeaderValue::from_static("attachment"))
}

/// `20261006T090000Z`: an ISO 8601 basic format, without the colons that Windows refuses.
fn compact_utc(datetime: SensAppDateTime) -> String {
    let (year, month, day, hour, minute, second, _) = datetime.to_gregorian_utc();
    format!("{year:04}{month:02}{day:02}T{hour:02}{minute:02}{second:02}Z")
}

/// The characters that no file system takes (or that hide a path) become `_`.
fn clean(part: &str) -> String {
    part.chars()
        .map(|c| {
            if c.is_control() || matches!(c, '/' | '\\' | ':' | '*' | '?' | '"' | '<' | '>' | '|') {
                '_'
            } else {
                c
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datamodel::sensapp_vec::SensAppLabels;
    use crate::datamodel::{SensorType, unit::Unit};
    use uuid::Uuid;

    fn sensor(name: &str, labels: &[(&str, &str)]) -> Sensor {
        Sensor {
            uuid: Uuid::nil(),
            name: name.to_string(),
            sensor_type: SensorType::Float,
            unit: None::<Unit>,
            labels: labels
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect::<SensAppLabels>(),
        }
    }

    fn at(text: &str) -> SensAppDateTime {
        SensAppDateTime::from_gregorian_str(text).unwrap()
    }

    #[test]
    fn name_has_the_sorted_labels_the_window_and_the_bucket() {
        let sensor = sensor("temperature", &[("room", "kitchen"), ("floor", "1")]);
        let name = file_name(
            &sensor,
            Some(at("2026-10-06T09:00:00Z")),
            Some(at("2026-10-06T10:30:15.250Z")),
            Some(("5m", Aggregation::Avg)),
            "csv",
        );
        assert_eq!(
            name,
            "temperature_floor-1_room-kitchen_20261006T090000Z_20261006T103015Z_5m-avg.csv"
        );
    }

    #[test]
    fn name_says_which_bound_is_given() {
        let sensor = sensor("cpu", &[]);
        let start = Some(at("2026-01-02T03:04:05Z"));
        assert_eq!(
            file_name(&sensor, start, None, None, "jsonl"),
            "cpu_from-20260102T030405Z.jsonl"
        );
        assert_eq!(
            file_name(&sensor, None, start, None, "arrow"),
            "cpu_until-20260102T030405Z.arrow"
        );
        assert_eq!(file_name(&sensor, None, None, None, "json"), "cpu.json");
    }

    #[test]
    fn name_keeps_unicode_and_the_header_has_both_forms() {
        let name = file_name(&sensor("Sanitæranlegg", &[]), None, None, None, "csv");
        assert_eq!(name, "Sanitæranlegg.csv");
        assert_eq!(
            content_disposition(&name).to_str().unwrap(),
            "attachment; filename=\"Sanit_ranlegg.csv\"; filename*=UTF-8''Sanit%C3%A6ranlegg.csv"
        );
    }

    #[test]
    fn hostile_names_give_a_safe_file_name_and_a_valid_header() {
        let long = "é".repeat(500);
        for hostile in [
            "a\"b\r\nSet-Cookie: x",
            "../../etc/passwd",
            "..\\..\\windows",
            ".hidden",
            "",
            "   ",
            "...",
            "a;b, c=d",
            long.as_str(),
        ] {
            let name = file_name(
                &sensor(hostile, &[("k\n", "v/\"")]),
                None,
                None,
                None,
                "csv",
            );
            assert!(name.ends_with(".csv"), "{name}");
            assert!(!name.starts_with('.'), "{name}");
            assert!(!name.contains(['/', '\\', '"', '\r', '\n', ':']), "{name}");
            assert!(
                name.chars().count() <= MAX_STEM_CHARS + ".csv".len(),
                "{name}"
            );

            let header = content_disposition(&name);
            let text = header.to_str().expect("printable ASCII");
            assert!(text.starts_with("attachment; filename=\""), "{text}");
            assert!(!text.contains(['\r', '\n']), "{text}");
            // One quoted string: the fallback has no quote of its own
            assert_eq!(text.matches('"').count(), 2, "{text}");
        }
    }

    #[test]
    fn an_empty_name_is_series() {
        assert_eq!(
            file_name(&sensor("", &[]), None, None, None, "csv"),
            "series.csv"
        );
        assert_eq!(
            file_name(&sensor(" .. ", &[]), None, None, None, "csv"),
            "series.csv"
        );
    }

    #[test]
    fn a_long_name_is_cut_on_a_character() {
        let name = file_name(&sensor(&"é".repeat(500), &[]), None, None, None, "arrow");
        assert_eq!(name, format!("{}.arrow", "é".repeat(MAX_STEM_CHARS)));
    }
}
