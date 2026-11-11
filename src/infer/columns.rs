use crate::datamodel::SensAppDateTime;

use super::parsing::*;
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq)]
pub enum InferedColumn {
    Integer(Vec<i64>),
    Numeric(Vec<rust_decimal::Decimal>),
    Float(Vec<f64>),
    String(Vec<String>),
    Boolean(Vec<bool>),
    DateTime(Vec<SensAppDateTime>),
    Json(Vec<Arc<serde_json::Value>>),
}

/// The type of a column of cells, and its values. A column with a cell of text, or with a mix
/// of types that have no common type, is text.
pub fn infer_column(column: Vec<String>, trim: bool, numeric: bool) -> InferedColumn {
    infer_typed_column(&column, trim, numeric).unwrap_or(InferedColumn::String(column))
}

/// The typed values of a column, or `None` when the column is text. The cells are borrowed, so
/// the caller keeps them when it needs the text.
pub fn infer_typed_column(column: &[String], trim: bool, numeric: bool) -> Option<InferedColumn> {
    // select the right infer method
    let infer_method = if trim {
        if numeric {
            infer_type_with_trim_and_numeric
        } else {
            infer_type_with_trim
        }
    } else if numeric {
        infer_type_with_numeric
    } else {
        infer_type
    };

    let mut has_integers = false;
    let mut has_numeric = false;
    let mut has_floats = false;
    let mut has_json = false;
    let mut has_boolean = false;
    let mut has_datetime = false;

    let mut infered_column = Vec::with_capacity(column.len());
    for value in column {
        let infered_value = match infer_method(value) {
            Ok((_, infered_value)) => infered_value,
            Err(error) => panic!("Failed to infer column: {error}"),
        };
        match &infered_value {
            InferedValue::Integer(_) => has_integers = true,
            InferedValue::Numeric(_) => has_numeric = true,
            InferedValue::Float(_) => has_floats = true,
            // If we have at least a string, everything is a string
            InferedValue::String(_) => return None,
            InferedValue::Json(_) => has_json = true,
            InferedValue::Boolean(_) => has_boolean = true,
            InferedValue::DateTime(_) => has_datetime = true,
        }
        infered_column.push(infered_value);
    }

    if has_json {
        return Some(InferedColumn::Json(
            infered_column
                .into_iter()
                .map(|value| match value {
                    InferedValue::Json(value) => value,
                    // Convert the other types to JSON, to be nice
                    InferedValue::Integer(value) => Arc::new(serde_json::Value::from(value)),
                    InferedValue::Float(value) => Arc::new(serde_json::Value::from(value)),
                    InferedValue::Boolean(value) => Arc::new(serde_json::Value::from(value)),
                    InferedValue::DateTime(value) => {
                        Arc::new(serde_json::Value::from(value.to_rfc3339()))
                    }
                    _ => unreachable!("We should have only JSON compatible types at this point"),
                })
                .collect::<Vec<_>>(),
        ));
    }

    // If we have booleans
    if has_boolean {
        // If we don't have only booleans, we use string instead
        if has_integers || has_numeric || has_floats || has_datetime {
            return None;
        }
        return Some(InferedColumn::Boolean(
            infered_column
                .into_iter()
                .map(|value| match value {
                    InferedValue::Boolean(value) => value,
                    _ => unreachable!("We should have only booleans at this point"),
                })
                .collect::<Vec<_>>(),
        ));
    }

    // If we have datetimes
    if has_datetime {
        // If we don't have only datetimes, we use string instead
        if has_integers || has_numeric || has_floats {
            return None;
        }
        return Some(InferedColumn::DateTime(
            infered_column
                .into_iter()
                .map(|value| match value {
                    InferedValue::DateTime(value) => value,
                    _ => unreachable!("We should have only datetimes at this point"),
                })
                .collect::<Vec<_>>(),
        ));
    }

    // If we have numerics
    if has_numeric {
        return Some(InferedColumn::Numeric(
            infered_column
                .into_iter()
                .map(|value| match value {
                    InferedValue::Numeric(value) => value,
                    _ => unreachable!("We should have only numerics"),
                })
                .collect::<Vec<_>>(),
        ));
    }

    // If we have floats, integers are also floats
    if has_floats {
        return Some(InferedColumn::Float(
            infered_column
                .into_iter()
                .map(|value| match value {
                    InferedValue::Float(value) => value,
                    InferedValue::Integer(value) => value as f64,
                    _ => unreachable!("We should have only floats and integers at this point"),
                })
                .collect::<Vec<_>>(),
        ));
    }

    // Only integers, or an empty column: Integer as a fallback
    Some(InferedColumn::Integer(
        infered_column
            .into_iter()
            .map(|value| match value {
                InferedValue::Integer(value) => value,
                _ => unreachable!("We should have only integers at this point"),
            })
            .collect::<Vec<_>>(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::{str::FromStr, sync::Arc};

    #[test]
    fn test_infer_column_integers() {
        let column = vec![
            "1".to_string(),
            "2".to_string(),
            "3".to_string(),
            "4".to_string(),
        ];
        let infered_column = infer_column(column, false, false);
        assert_eq!(infered_column, InferedColumn::Integer(vec![1, 2, 3, 4]));
    }

    #[test]
    fn test_infer_column_floats() {
        let column = vec![
            "1.1".to_string(),
            "2.2".to_string(),
            "3.3".to_string(),
            "4.4".to_string(),
        ];
        let infered_column = infer_column(column, false, false);
        assert_eq!(
            infered_column,
            InferedColumn::Float(vec![1.1, 2.2, 3.3, 4.4])
        );

        // now with a mix of integers and floats
        let column = vec![
            "1.1".to_string(),
            "2".to_string(),
            "3.3".to_string(),
            "4".to_string(),
        ];
        let infered_column = infer_column(column, false, false);
        assert_eq!(
            infered_column,
            InferedColumn::Float(vec![1.1, 2.0, 3.3, 4.0])
        );
    }

    #[test]
    fn test_infer_column_numeric() {
        let column = vec![
            "1".to_string(),
            "2.2".to_string(),
            "3.3".to_string(),
            "4.4".to_string(),
            "78953678389071".to_string(),
        ];
        let infered_column = infer_column(column, false, true);
        assert_eq!(
            infered_column,
            InferedColumn::Numeric(vec![
                rust_decimal::Decimal::from_str("1").unwrap(),
                rust_decimal::Decimal::from_str("2.2").unwrap(),
                rust_decimal::Decimal::from_str("3.3").unwrap(),
                rust_decimal::Decimal::from_str("4.4").unwrap(),
                rust_decimal::Decimal::from_str("78953678389071").unwrap(),
            ])
        );
    }

    #[test]
    fn test_infer_column_bool() {
        let column = vec![
            " true ".to_string(),
            "false".to_string(),
            "TRUE".to_string(),
            "FALSE\n".to_string(),
        ];
        let infered_column = infer_column(column, true, false);
        assert_eq!(
            infered_column,
            InferedColumn::Boolean(vec![true, false, true, false])
        );
    }

    #[test]
    fn test_boolean_fallback_to_string() {
        let column = vec![" true ".to_string(), "false".to_string(), "42".to_string()];
        let infered_column = infer_column(column, true, true);
        assert_eq!(
            infered_column,
            InferedColumn::String(vec![
                " true ".to_string(),
                "false".to_string(),
                "42".to_string(),
            ])
        );
    }

    #[test]
    fn test_infer_column_string() {
        let column = vec![
            "abcd".to_string(),
            "efgh".to_string(),
            " .  ".to_string(),
            "42".to_string(),
            "true".to_string(),
        ];

        let infered_column = infer_column(column, true, false);
        assert_eq!(
            infered_column,
            InferedColumn::String(vec![
                "abcd".to_string(),
                "efgh".to_string(),
                " .  ".to_string(),
                "42".to_string(),
                "true".to_string(),
            ])
        );
    }

    #[test]
    fn test_infer_column_json() {
        let column = vec![
            r#"{"a": 1}"#.to_string(),
            r#"[{"b": 2}]"#.to_string(),
            r#"{"c": true}"#.to_string(),
            r#"{"d": "{\"test\":true}"}"#.to_string(),
        ];

        let infered_column = infer_column(column, true, false);
        assert_eq!(
            infered_column,
            InferedColumn::Json(vec![
                Arc::new(json!({"a": 1})),
                Arc::new(json!([{"b": 2}])),
                Arc::new(json!({"c": true})),
                Arc::new(json!({"d": "{\"test\":true}"})),
            ])
        );
    }

    #[test]
    fn test_fallback_json() {
        let column = vec![
            r#"{"a": 1}"#.to_string(),
            r#"[{"b": 2}]"#.to_string(),
            "42".to_string(),
            "42.83".to_string(),
            "true".to_string(),
            "1951-10-26T00:00:00+02:00".to_string(),
        ];

        let infered_column = infer_column(column, true, false);
        assert_eq!(
            infered_column,
            InferedColumn::Json(vec![
                Arc::new(json!({"a": 1})),
                Arc::new(json!([{"b": 2}])),
                Arc::new(json!(42)),
                Arc::new(json!(42.83)),
                Arc::new(json!(true)),
                // Date is converted to UTC: 00:00 at +02:00 is 22:00 the day before
                Arc::new(json!("1951-10-25T22:00:00+00:00")),
            ])
        );
    }

    #[test]
    fn test_infer_column_empty() {
        let infered_column = infer_column(vec![], true, false);
        assert_eq!(infered_column, InferedColumn::Integer(vec![]));
    }

    #[test]
    fn test_datetime() {
        let column = vec![
            "2020-01-01T00:00:00Z".to_string(),
            "1969-12-24T14:21:32.0933+05:35".to_string(),
        ];

        let infered_column = infer_column(column, true, false);
        assert_eq!(
            infered_column,
            InferedColumn::DateTime(vec![
                hifitime::Epoch::from_gregorian_utc(2020, 1, 1, 0, 0, 0, 0),
                hifitime::Epoch::from_gregorian_utc(1969, 12, 24, 8, 46, 32, 93_300_000)
            ])
        );
    }
}
