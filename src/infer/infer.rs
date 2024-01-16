use std::{str::FromStr, sync::Arc};

use nom::{
    branch::alt,
    bytes::complete::tag_no_case,
    character::complete::{i64, multispace0},
    combinator::{eof, map},
    number::complete::double,
    number::complete::recognize_float_or_exceptions,
    sequence::{delimited, terminated},
    Err, IResult,
};
use rust_decimal::Decimal;

#[derive(Debug, Clone, PartialEq)]
pub enum InferedValue {
    Integer(i64),
    Numeric(rust_decimal::Decimal),
    Float(f64),
    String(String),
    Boolean(bool),
    JSON(Arc<serde_json::Value>),
    //Location,
    //Blob(Vec<u8>),
    // todo: booleans and dates and timestamps
}

pub fn parse_integer(data: &str) -> IResult<&str, InferedValue> {
    map(i64, |i| InferedValue::Integer(i))(data)
}

pub fn parse_float(data: &str) -> IResult<&str, InferedValue> {
    // We use the "double" parser from nom, that returns a f64.
    // The parser named "float" from nom returns a f32.
    map(double, |f| InferedValue::Float(f))(data)
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
    )(data)
}

fn is_likely_json(data: &str) -> bool {
    // Not done using nom because it's not the right tool.
    (data.starts_with('{') && data.ends_with('}')) || (data.starts_with('[') && data.ends_with(']'))
}

pub fn parse_json(data: &str) -> IResult<&str, InferedValue> {
    if is_likely_json(data) {
        serde_json::from_str(data)
            .map(|val| ("", InferedValue::JSON(val)))
            .map_err(|_| Err::Error(nom::error::Error::new(data, nom::error::ErrorKind::Fail)))
    } else {
        Err(Err::Error(nom::error::Error::new(
            data,
            nom::error::ErrorKind::Fail,
        )))
    }
}

pub fn infer_type(data: &str) -> IResult<&str, InferedValue> {
    alt((
        terminated(parse_integer, eof),
        terminated(parse_float, eof),
        terminated(parse_boolean, eof),
        terminated(parse_json, eof),
        terminated(parse_string, eof),
    ))(data)
}

pub fn infer_type_with_trim(data: &str) -> IResult<&str, InferedValue> {
    alt((
        terminated(delimited(multispace0, parse_integer, multispace0), eof),
        terminated(delimited(multispace0, parse_float, multispace0), eof),
        terminated(delimited(multispace0, parse_boolean, multispace0), eof),
        terminated(delimited(multispace0, parse_json, multispace0), eof),
        // We don't trim strings, as they can contain whitespace.
        terminated(parse_string, eof),
    ))(data)
}

pub fn infer_type_with_numeric(data: &str) -> IResult<&str, InferedValue> {
    alt((
        terminated(parse_numeric, eof),
        terminated(parse_boolean, eof),
        terminated(parse_json, eof),
        terminated(parse_string, eof),
    ))(data)
}

pub fn infer_type_with_trim_and_numeric(data: &str) -> IResult<&str, InferedValue> {
    alt((
        terminated(delimited(multispace0, parse_numeric, multispace0), eof),
        terminated(delimited(multispace0, parse_boolean, multispace0), eof),
        terminated(delimited(multispace0, parse_json, multispace0), eof),
        // We don't trim strings, as they can contain whitespace.
        terminated(parse_string, eof),
    ))(data)
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
            Ok(("", InferedValue::JSON(Arc::new(serde_json::json!({})))))
        );
        assert_eq!(
            parse_json("[]"),
            Ok(("", InferedValue::JSON(Arc::new(serde_json::json!([])))))
        );
        assert_eq!(
            parse_json("[{\"a\": 1}]"),
            Ok((
                "",
                InferedValue::JSON(Arc::new(serde_json::json!([{"a": 1}])))
            ))
        );
        assert_eq!(
            parse_json("[{\"a\": 1}, {\"b\": 2}]"),
            Ok((
                "",
                InferedValue::JSON(Arc::new(serde_json::json!([{"a": 1}, {"b": 2}])))
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
            Ok(("", InferedValue::JSON(Arc::new(serde_json::json!({})))))
        );
        assert_eq!(
            infer_type("[{\"a\": 1}]"),
            Ok((
                "",
                InferedValue::JSON(Arc::new(serde_json::json!([{"a": 1}])))
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
            Ok(("", InferedValue::JSON(Arc::new(serde_json::json!({})))))
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
}
