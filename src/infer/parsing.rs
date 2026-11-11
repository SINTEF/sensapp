use std::{str::FromStr, sync::Arc};

use nom::{
    Err, IResult, Parser,
    branch::alt,
    bytes::complete::tag_no_case,
    character::complete::{i64, multispace0},
    combinator::{eof, map},
    number::complete::double,
    number::complete::recognize_float_or_exceptions,
    sequence::{delimited, terminated},
};
use rust_decimal::Decimal;

#[derive(Debug, Clone, PartialEq)]
pub enum InferedValue {
    Integer(i64),
    Numeric(rust_decimal::Decimal),
    Float(f64),
    String(String),
    Boolean(bool),
    Json(Arc<serde_json::Value>),
    DateTime(hifitime::Epoch),
    //Location,
    //Blob(Vec<u8>),
    // Future: Add support for boolean and additional date/timestamp formats
}

pub fn parse_integer(data: &str) -> IResult<&str, InferedValue> {
    map(i64, InferedValue::Integer).parse(data)
}

/// `inf` or `infinity` in any case, with an optional sign. `nom` reads them without a sign only.
fn parse_infinity(data: &str) -> Option<(&str, f64)> {
    let (sign, body) = match data.as_bytes().first() {
        Some(b'-') => (-1.0, &data[1..]),
        Some(b'+') => (1.0, &data[1..]),
        _ => (1.0, data),
    };
    ["infinity", "inf"].iter().find_map(|word| {
        let start = body.get(..word.len())?;
        start
            .eq_ignore_ascii_case(word)
            .then(|| (&body[word.len()..], sign * f64::INFINITY))
    })
}

pub fn parse_float(data: &str) -> IResult<&str, InferedValue> {
    // We use the "double" parser from nom, that returns a f64.
    // The parser named "float" from nom returns a f32.
    // `inf`, `+inf`, `-inf` and `Infinity` are infinities, which are values. NaN is a gap, not a
    // number: the CSV importer reads it as a missing value (see `is_missing_marker`) and it is
    // otherwise text.
    if let Some((rest, value)) = parse_infinity(data) {
        return Ok((rest, InferedValue::Float(value)));
    }
    let (rest, value) = double(data)?;
    if value.is_nan() {
        Err(Err::Error(nom::error::Error::new(
            data,
            nom::error::ErrorKind::Float,
        )))
    } else {
        Ok((rest, InferedValue::Float(value)))
    }
}

pub fn parse_numeric(data: &str) -> IResult<&str, InferedValue> {
    // We try to recognize the float using nom, but then we parse it with rust_decimal.
    let (i, s) = recognize_float_or_exceptions(data)?;
    match Decimal::from_str(s) {
        Ok(d) => Ok((i, InferedValue::Numeric(d))),
        Err(_) => Err(Err::Error(nom::error::Error::new(
            data,
            // If the number cannot be parsed, we return an error understandable by nom.
            nom::error::ErrorKind::Fail,
        ))),
    }
}

pub fn parse_string(data: &str) -> IResult<&str, InferedValue> {
    // placeholder, accepts anything
    Ok(("", InferedValue::String(data.to_string())))
}

pub fn parse_boolean(data: &str) -> IResult<&str, InferedValue> {
    // placeholder, accepts anything
    map(
        alt((tag_no_case("true"), tag_no_case("false"))),
        |s: &str| InferedValue::Boolean(s.to_lowercase() == "true"),
    )
    .parse(data)
}

fn is_likely_json(data: &str) -> bool {
    // Not done using nom because it's not the right tool.
    (data.starts_with('{') && data.ends_with('}')) || (data.starts_with('[') && data.ends_with(']'))
}

pub fn parse_json(data: &str) -> IResult<&str, InferedValue> {
    if is_likely_json(data) {
        serde_json::from_str(data)
            .map(|val| ("", InferedValue::Json(val)))
            .map_err(|_| Err::Error(nom::error::Error::new(data, nom::error::ErrorKind::Fail)))
    } else {
        Err(Err::Error(nom::error::Error::new(
            data,
            nom::error::ErrorKind::Fail,
        )))
    }
}

/// The time of a CSV cell, when the whole cell is an ISO 8601 calendar date or date and time:
/// `2024-06-01T12:00:00.123456+02:00`, with `T` or a space between the date and the time (pandas
/// writes a space), seconds and fraction optional, an offset `Z`, `+02:00` or `+0200`. Without an
/// offset the time is UTC, and a date alone is midnight UTC. Week and ordinal dates are not
/// recognised. The result is rounded down to the microsecond.
fn parse_datetime_cell(cell: &str) -> Option<hifitime::Epoch> {
    use crate::datamodel::sensapp_datetime::SensAppDateTimeExt;
    use chrono::{DateTime, NaiveDate, NaiveDateTime};

    let bytes = cell.as_bytes();
    if bytes.len() < 10 || !bytes[..4].iter().all(u8::is_ascii_digit) || bytes[4] != b'-' {
        return None;
    }
    let mut normalized = cell.to_string();
    if bytes.len() > 10 && bytes[10] == b' ' {
        normalized.replace_range(10..11, "T");
    }
    if normalized.ends_with(['Z', 'z']) {
        normalized.pop();
        normalized.push_str("+00:00");
    }

    let with_offset = ["%Y-%m-%dT%H:%M:%S%.f%z", "%Y-%m-%dT%H:%M%z"]
        .iter()
        .find_map(|format| DateTime::parse_from_str(&normalized, format).ok())
        .map(|datetime| datetime.timestamp_micros());
    let micros = with_offset
        .or_else(|| {
            ["%Y-%m-%dT%H:%M:%S%.f", "%Y-%m-%dT%H:%M"]
                .iter()
                .find_map(|format| NaiveDateTime::parse_from_str(&normalized, format).ok())
                .map(|datetime| datetime.and_utc().timestamp_micros())
        })
        .or_else(|| {
            NaiveDate::parse_from_str(&normalized, "%Y-%m-%d")
                .ok()
                .and_then(|date| date.and_hms_opt(0, 0, 0))
                .map(|datetime| datetime.and_utc().timestamp_micros())
        })?;
    Some(hifitime::Epoch::from_unix_microseconds_i64(micros))
}

pub fn parse_iso8601_datetime(data: &str) -> IResult<&str, InferedValue> {
    match parse_datetime_cell(data) {
        Some(epoch) => Ok(("", InferedValue::DateTime(epoch))),
        None => Err(Err::Error(nom::error::Error::new(
            data,
            nom::error::ErrorKind::Fail,
        ))),
    }
}

pub fn infer_type(data: &str) -> IResult<&str, InferedValue> {
    alt((
        terminated(parse_integer, eof),
        terminated(parse_float, eof),
        terminated(parse_boolean, eof),
        terminated(parse_iso8601_datetime, eof),
        terminated(parse_json, eof),
        terminated(parse_string, eof),
    ))
    .parse(data)
}

pub fn infer_type_with_trim(data: &str) -> IResult<&str, InferedValue> {
    alt((
        terminated(delimited(multispace0, parse_integer, multispace0), eof),
        terminated(delimited(multispace0, parse_float, multispace0), eof),
        terminated(delimited(multispace0, parse_boolean, multispace0), eof),
        terminated(
            delimited(multispace0, parse_iso8601_datetime, multispace0),
            eof,
        ),
        terminated(delimited(multispace0, parse_json, multispace0), eof),
        // We don't trim strings, as they can contain whitespace.
        terminated(parse_string, eof),
    ))
    .parse(data)
}

pub fn infer_type_with_numeric(data: &str) -> IResult<&str, InferedValue> {
    alt((
        terminated(parse_numeric, eof),
        terminated(parse_boolean, eof),
        terminated(parse_iso8601_datetime, eof),
        terminated(parse_json, eof),
        terminated(parse_string, eof),
    ))
    .parse(data)
}

pub fn infer_type_with_trim_and_numeric(data: &str) -> IResult<&str, InferedValue> {
    alt((
        terminated(delimited(multispace0, parse_numeric, multispace0), eof),
        terminated(delimited(multispace0, parse_boolean, multispace0), eof),
        terminated(
            delimited(multispace0, parse_iso8601_datetime, multispace0),
            eof,
        ),
        terminated(delimited(multispace0, parse_json, multispace0), eof),
        // We don't trim strings, as they can contain whitespace.
        terminated(parse_string, eof),
    ))
    .parse(data)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_integer() {
        assert_eq!(parse_integer("42"), Ok(("", InferedValue::Integer(42))));
        assert_eq!(parse_integer("-42"), Ok(("", InferedValue::Integer(-42))));
        assert_eq!(parse_integer("0"), Ok(("", InferedValue::Integer(0))));
        assert_eq!(
            parse_integer("123456789"),
            Ok(("", InferedValue::Integer(123456789)))
        );
        assert_eq!(
            parse_integer("123456789123456789123456789"),
            Err(Err::Error(nom::error::Error::new(
                "123456789123456789123456789",
                nom::error::ErrorKind::Digit
            )))
        );
    }

    #[test]
    fn infinities_are_floats_and_nan_is_not() {
        for (text, expected) in [
            ("inf", f64::INFINITY),
            ("+inf", f64::INFINITY),
            ("-inf", f64::NEG_INFINITY),
            ("Infinity", f64::INFINITY),
            ("-INFINITY", f64::NEG_INFINITY),
        ] {
            assert_eq!(
                parse_float(text),
                Ok(("", InferedValue::Float(expected))),
                "{text}"
            );
        }
        assert!(parse_float("NaN").is_err());
        assert!(parse_float("nan").is_err());
    }

    #[test]
    fn test_parse_float() {
        assert_eq!(parse_float("42"), Ok(("", InferedValue::Float(42.0))));
        assert_eq!(parse_float("-42"), Ok(("", InferedValue::Float(-42.0))));
        assert_eq!(parse_float("0"), Ok(("", InferedValue::Float(0.0))));
        assert_eq!(parse_float("42.0"), Ok(("", InferedValue::Float(42.0))));
        assert_eq!(parse_float("-42.0"), Ok(("", InferedValue::Float(-42.0))));
        assert_eq!(parse_float("0.0"), Ok(("", InferedValue::Float(0.0))));
        assert_eq!(parse_float("42.0\n"), Ok(("\n", InferedValue::Float(42.0))));

        // unprecise IEEE 754 floats
        assert_eq!(
            parse_float("12345678901.12345678901"),
            // Notice that it's not the same number.
            Ok(("", InferedValue::Float(12345678901.123457)))
        );

        assert_eq!(
            parse_integer("123456789123456789123456789.123456789"),
            Err(Err::Error(nom::error::Error::new(
                "123456789123456789123456789.123456789",
                nom::error::ErrorKind::Digit
            )))
        );
    }

    #[test]
    fn test_parse_numeric() {
        assert_eq!(
            parse_numeric("42"),
            Ok(("", InferedValue::Numeric(Decimal::new(42, 0))))
        );
        assert_eq!(
            parse_numeric("-42"),
            Ok(("", InferedValue::Numeric(Decimal::new(-42, 0))))
        );
        assert_eq!(
            parse_numeric("0"),
            Ok(("", InferedValue::Numeric(Decimal::new(0, 0))))
        );
        assert_eq!(
            parse_numeric("42.0"),
            Ok(("", InferedValue::Numeric(Decimal::new(42, 0))))
        );
        assert_eq!(
            parse_numeric("-42.0"),
            Ok(("", InferedValue::Numeric(Decimal::new(-42, 0))))
        );
        assert_eq!(
            parse_numeric("0.0"),
            Ok(("", InferedValue::Numeric(Decimal::new(0, 1))))
        );

        // unprecise IEEE 754 floats are gone
        assert_eq!(
            parse_numeric("12345678901.12345678901"),
            Ok((
                "",
                InferedValue::Numeric(Decimal::from_str("12345678901.12345678901").unwrap())
            ))
        );

        // Now large numbers are accepted
        assert_eq!(
            parse_numeric("123456789123456789123456789.123456789"),
            Ok((
                "",
                InferedValue::Numeric(
                    Decimal::from_str("123456789123456789123456789.123456789").unwrap()
                )
            ))
        );
        assert_eq!(
            parse_numeric("123456789123456789123456789"),
            Ok((
                "",
                InferedValue::Numeric(Decimal::from_str("123456789123456789123456789").unwrap())
            ))
        );
        assert_eq!(
            parse_numeric("123456789123456789123456789123456789"),
            Err(Err::Error(nom::error::Error::new(
                "123456789123456789123456789123456789",
                nom::error::ErrorKind::Fail
            )))
        );

        if let Ok((_, InferedValue::Numeric(d))) = parse_numeric("123456789123456789123456789") {
            assert_eq!(d.to_string(), "123456789123456789123456789");
        } else {
            panic!("Not a numeric");
        }
    }

    #[test]
    // Very simple test, as only the edge cases need to be tested.
    fn test_parse_string() {
        assert_eq!(
            parse_string(""),
            Ok(("", InferedValue::String("".to_string())))
        );
        assert_eq!(
            parse_string("abcd"),
            Ok(("", InferedValue::String("abcd".to_string())))
        );
        assert_eq!(
            parse_string("abcd\n"),
            Ok(("", InferedValue::String("abcd\n".to_string())))
        );
    }

    #[test]
    fn test_parse_boolean() {
        assert_eq!(parse_boolean("true"), Ok(("", InferedValue::Boolean(true))));
        assert_eq!(
            parse_boolean("false"),
            Ok(("", InferedValue::Boolean(false)))
        );
        assert_eq!(parse_boolean("TRUE"), Ok(("", InferedValue::Boolean(true))));
        assert_eq!(
            parse_boolean("FALSE"),
            Ok(("", InferedValue::Boolean(false)))
        );
        assert_eq!(parse_boolean("True"), Ok(("", InferedValue::Boolean(true))));
        assert_eq!(
            parse_boolean("False"),
            Ok(("", InferedValue::Boolean(false)))
        );
        assert_eq!(
            parse_boolean("abcd"),
            Err(Err::Error(nom::error::Error::new(
                "abcd",
                nom::error::ErrorKind::Tag
            )))
        );
    }

    #[test]
    fn test_is_like_json() {
        assert!(is_likely_json("{}"));
        assert!(is_likely_json("[]"));
        assert!(is_likely_json("{\n}"));
        assert!(is_likely_json("[{\"a\": 1}]"));
        assert!(is_likely_json("[{\"a\": 1}, {\"b\": 2}]"));
        assert!(!is_likely_json("[]\n"));
        assert!(!is_likely_json("42"));
        assert!(!is_likely_json("abcd"));
        assert!(!is_likely_json("\"abcd\""));
    }

    #[test]
    fn test_parse_json() {
        assert_eq!(
            parse_json("{}"),
            Ok(("", InferedValue::Json(Arc::new(serde_json::json!({})))))
        );
        assert_eq!(
            parse_json("[]"),
            Ok(("", InferedValue::Json(Arc::new(serde_json::json!([])))))
        );
        assert_eq!(
            parse_json("[{\"a\": 1}]"),
            Ok((
                "",
                InferedValue::Json(Arc::new(serde_json::json!([{"a": 1}])))
            ))
        );
        assert_eq!(
            parse_json("[{\"a\": 1}, {\"b\": 2}]"),
            Ok((
                "",
                InferedValue::Json(Arc::new(serde_json::json!([{"a": 1}, {"b": 2}])))
            ))
        );
        assert_eq!(
            parse_json("[{\"a\": 1}, {\"b\": 2}]\n\n"),
            Err(Err::Error(nom::error::Error::new(
                "[{\"a\": 1}, {\"b\": 2}]\n\n",
                nom::error::ErrorKind::Fail
            )))
        );
        assert_eq!(
            parse_json("abcd"),
            Err(Err::Error(nom::error::Error::new(
                "abcd",
                nom::error::ErrorKind::Fail
            )))
        );
    }

    #[test]
    fn test_infer_type() {
        assert_eq!(infer_type("42"), Ok(("", InferedValue::Integer(42))));
        assert_eq!(infer_type("-42"), Ok(("", InferedValue::Integer(-42))));
        assert_eq!(infer_type("0"), Ok(("", InferedValue::Integer(0))));
        assert_eq!(infer_type("42.0"), Ok(("", InferedValue::Float(42.0))));
        assert_eq!(infer_type("-42.0"), Ok(("", InferedValue::Float(-42.0))));
        assert_eq!(infer_type("0.0"), Ok(("", InferedValue::Float(0.0))));
        assert_eq!(
            infer_type("42.0\n"),
            Ok(("", InferedValue::String("42.0\n".to_string())))
        );
        assert_eq!(
            infer_type("12345678901.12345678901"),
            Ok(("", InferedValue::Float(12345678901.123457)))
        );
        assert_eq!(
            infer_type("abcd"),
            Ok(("", InferedValue::String("abcd".to_string())))
        );
        assert_eq!(
            infer_type("{}"),
            Ok(("", InferedValue::Json(Arc::new(serde_json::json!({})))))
        );
        assert_eq!(
            infer_type("[{\"a\": 1}]"),
            Ok((
                "",
                InferedValue::Json(Arc::new(serde_json::json!([{"a": 1}])))
            ))
        );
    }

    #[test]
    fn test_infer_type_with_trim() {
        assert_eq!(
            infer_type_with_trim(" 42 "),
            Ok(("", InferedValue::Integer(42)))
        );
        assert_eq!(
            infer_type_with_trim(" -42 "),
            Ok(("", InferedValue::Integer(-42)))
        );
        assert_eq!(
            infer_type_with_trim("-42.23"),
            Ok(("", InferedValue::Float(-42.23)))
        );
        // only whitespace
        assert_eq!(
            infer_type_with_trim(" \n"),
            Ok(("", InferedValue::String(" \n".to_string())))
        );
        // strings contain the whitespace
        assert_eq!(
            infer_type_with_trim(" abcd\n"),
            Ok(("", InferedValue::String(" abcd\n".to_string())))
        );
    }

    #[test]
    fn test_infer_type_with_numeric() {
        assert_eq!(
            infer_type_with_numeric("42"),
            Ok(("", InferedValue::Numeric(Decimal::new(42, 0))))
        );
        assert_eq!(
            infer_type_with_numeric("-123456789123456789123456789.123456789"),
            Ok((
                "",
                InferedValue::Numeric(
                    Decimal::from_str("-123456789123456789123456789.123456789").unwrap()
                )
            ))
        );
        assert_eq!(
            infer_type_with_numeric("abcd a\n\n "),
            Ok(("", InferedValue::String("abcd a\n\n ".to_string())))
        );
        assert_eq!(
            infer_type_with_numeric("FALSE"),
            Ok(("", InferedValue::Boolean(false)))
        );
        assert_eq!(
            infer_type_with_numeric("{}"),
            Ok(("", InferedValue::Json(Arc::new(serde_json::json!({})))))
        );
        assert_eq!(
            infer_type_with_numeric(" 42.12 "),
            Ok(("", InferedValue::String(" 42.12 ".to_string())))
        );
    }
    #[test]
    fn test_infer_type_with_trim_and_numeric() {
        assert_eq!(
            infer_type_with_trim_and_numeric(" 42 "),
            Ok(("", InferedValue::Numeric(Decimal::new(42, 0))))
        );
        assert_eq!(
            infer_type_with_trim_and_numeric(" -42 "),
            Ok(("", InferedValue::Numeric(Decimal::new(-42, 0))))
        );
        assert_eq!(
            infer_type_with_trim_and_numeric("-42.23"),
            Ok(("", InferedValue::Numeric(Decimal::new(-4223, 2))))
        );
        // only whitespace
        assert_eq!(
            infer_type_with_trim_and_numeric(" \n"),
            Ok(("", InferedValue::String(" \n".to_string())))
        );
        // strings contain the whitespace
        assert_eq!(
            infer_type_with_trim_and_numeric(" abcd\n"),
            Ok(("", InferedValue::String(" abcd\n".to_string())))
        );
        assert_eq!(
            infer_type_with_trim_and_numeric(" 42.12 "),
            Ok(("", InferedValue::Numeric(Decimal::new(4212, 2))))
        );
    }

    mod datetime {
        use super::*;

        /// The time of a cell, as Unix microseconds, or `None` when it is not a datetime.
        fn micros(cell: &str) -> Option<i64> {
            use crate::datamodel::sensapp_datetime::SensAppDateTimeExt;
            match parse_iso8601_datetime(cell) {
                Ok(("", InferedValue::DateTime(epoch))) => Some(epoch.to_unix_microseconds_i64()),
                _ => None,
            }
        }

        fn utc(y: i32, mo: u8, d: u8, h: u8, mi: u8, s: u8, us: u32) -> Option<i64> {
            use crate::datamodel::sensapp_datetime::SensAppDateTimeExt;
            Some(
                hifitime::Epoch::from_gregorian_utc(y, mo, d, h, mi, s, us * 1000)
                    .to_unix_microseconds_i64(),
            )
        }

        /// An offset is the local time minus UTC: it is subtracted. It was added.
        #[test]
        fn offsets_are_subtracted() {
            let noon_utc = utc(2024, 6, 1, 12, 0, 0, 0);
            assert_eq!(micros("2024-06-01T12:00:00Z"), noon_utc);
            assert_eq!(micros("2024-06-01T14:00:00+02:00"), noon_utc);
            assert_eq!(micros("2024-06-01T07:00:00-05:00"), noon_utc);
            assert_eq!(micros("2024-06-01T17:30:00+05:30"), noon_utc);
            assert_eq!(micros("2024-06-01T14:00:00+0200"), noon_utc);
            // An offset after a space was ignored; it is applied
            assert_eq!(micros("2024-06-01T14:00:00 +0200"), noon_utc);
            assert_eq!(
                micros("1969-12-24T00:00+01:00"),
                utc(1969, 12, 23, 23, 0, 0, 0)
            );
            assert_eq!(
                micros("1969-12-24T00:00-01:30"),
                utc(1969, 12, 24, 1, 30, 0, 0)
            );
        }

        /// No offset is UTC; a space separates the date and the time as well as `T` (pandas
        /// writes it); a date alone is midnight UTC.
        #[test]
        fn local_forms() {
            assert_eq!(micros("2020-01-01T00:00:00"), utc(2020, 1, 1, 0, 0, 0, 0));
            assert_eq!(micros("2018-12-24T00:00"), utc(2018, 12, 24, 0, 0, 0, 0));
            assert_eq!(micros("2024-06-01 12:00:00"), utc(2024, 6, 1, 12, 0, 0, 0));
            assert_eq!(
                micros("2024-06-01 14:00:00+02:00"),
                utc(2024, 6, 1, 12, 0, 0, 0)
            );
            assert_eq!(micros("2024-06-01"), utc(2024, 6, 1, 0, 0, 0, 0));
        }

        /// Fractions keep their microseconds (they were cut to milliseconds).
        #[test]
        fn fractions_keep_microseconds() {
            assert_eq!(
                micros("2024-06-01T12:00:00.123456Z"),
                utc(2024, 6, 1, 12, 0, 0, 123_456)
            );
            assert_eq!(
                micros("2024-06-01T12:00:00.5Z"),
                utc(2024, 6, 1, 12, 0, 0, 500_000)
            );
            // Below the microsecond, rounded down like every timestamp
            assert_eq!(
                micros("2024-06-01T12:00:00.123456789Z"),
                utc(2024, 6, 1, 12, 0, 0, 123_456)
            );
        }

        /// The whole cell must be a datetime: trailing text was ignored, an offset after a space
        /// was lost. Impossible dates are not datetimes.
        #[test]
        fn other_text_is_not_a_datetime() {
            for cell in [
                "2024-06-01T12:00:00Zjunk",
                "2024-07-32T00:00:00Z",
                "1969-02-29T00:00:00Z",
                "2024-06-01T25:00:00Z",
                "12:00:00",
                "2024",
                "hello",
                "",
            ] {
                assert_eq!(micros(cell), None, "{cell}");
            }
        }
    }
}
