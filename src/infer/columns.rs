use super::infer::*;
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq)]
pub enum InferedColumn {
    Integer(Vec<i64>),
    Numeric(Vec<rust_decimal::Decimal>),
    Float(Vec<f64>),
    String(Vec<String>),
    Boolean(Vec<bool>),
    JSON(Vec<Arc<serde_json::Value>>),
    //DateTime(Vec<chrono::NaiveDateTime>),
}

pub fn infer_column(column: Vec<String>, trim: bool, numeric: bool) -> InferedColumn {
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

    let infered_column = column
        .iter()
        .map(|value| infer_method(value))
        .collect::<Vec<_>>();

    let mut has_integers = false;
    let mut has_numeric = false;
    let mut has_floats = false;
    let mut has_string = false;
    let mut has_boolean = false;
    let mut has_json = false;

    for infered_value in infered_column.iter() {
        match infered_value {
            Ok((_, InferedValue::Integer(_))) => has_integers = true,
            Ok((_, InferedValue::Numeric(_))) => has_numeric = true,
            Ok((_, InferedValue::Float(_))) => has_floats = true,
            Ok((_, InferedValue::String(_))) => has_string = true,
            Ok((_, InferedValue::JSON(_))) => has_json = true,
            Ok((_, InferedValue::Boolean(_))) => has_boolean = true,
            _ => panic!("Failed to infer column"),
        }
    }

    // If we have at least a string, everything is a string
    if has_string {
        // We can return the column as is
        return InferedColumn::String(column);
    }

    if has_json {
        return InferedColumn::JSON(
            infered_column
                .iter()
                .map(|value| match value {
                    Ok((_, InferedValue::JSON(value))) => value.clone(),
                    // Convert the other types to JSON, to be nice
                    Ok((_, InferedValue::Integer(value))) => {
                        Arc::new(serde_json::Value::from(*value))
                    }
                    Ok((_, InferedValue::Float(value))) => {
                        Arc::new(serde_json::Value::from(*value))
                    }
                    Ok((_, InferedValue::Boolean(value))) => {
                        Arc::new(serde_json::Value::from(*value))
                    }
                    _ => unreachable!("We should have only JSON compatible types at this point"),
                })
                .collect::<Vec<_>>(),
        );
    }

    // If we have booleans
    if has_boolean {
        // If we don't have only booleans, we use string instead
        if has_integers || has_numeric || has_floats {
            return InferedColumn::String(column);
        }
        return InferedColumn::Boolean(
            infered_column
                .iter()
                .map(|value| match value {
                    Ok((_, InferedValue::Boolean(value))) => *value,
                    _ => unreachable!("We should have only booleans at this point"),
                })
                .collect::<Vec<_>>(),
        );
    }

    // If we have numerics
    if has_numeric {
        return InferedColumn::Numeric(
            infered_column
                .iter()
                .map(|value| match value {
                    Ok((_, InferedValue::Numeric(value))) => *value,
                    _ => unreachable!("We should have only numerics"),
                })
                .collect::<Vec<_>>(),
        );
    }

    // If we have floats, integers are also floats
    if has_floats {
        return InferedColumn::Float(
            infered_column
                .iter()
                .map(|value| match value {
                    Ok((_, InferedValue::Float(value))) => *value,
                    Ok((_, InferedValue::Integer(value))) => *value as f64,
                    _ => unreachable!("We should have only floats and integers at this point"),
                })
                .collect::<Vec<_>>(),
        );
    }

    // If we have only integers
    if has_integers {
        return InferedColumn::Integer(
            infered_column
                .iter()
                .map(|value| match value {
                    Ok((_, InferedValue::Integer(value))) => *value,
                    _ => unreachable!("We should have only integers at this point"),
                })
                .collect::<Vec<_>>(),
        );
    }

    unreachable!("failed to infer column");
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
            InferedColumn::JSON(vec![
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
        ];

        let infered_column = infer_column(column, true, false);
        assert_eq!(
            infered_column,
            InferedColumn::JSON(vec![
                Arc::new(json!({"a": 1})),
                Arc::new(json!([{"b": 2}])),
                Arc::new(json!(42)),
                Arc::new(json!(42.83)),
                Arc::new(json!(true)),
            ])
        );
    }
}
