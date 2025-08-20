use super::columns::InferedColumn;
use rust_decimal::Decimal;

pub fn is_i64_likely_timestamp(value: i64) -> bool {
    // Between 2000-01-01 and 2118-01-01
    // In 2118, I will very likely be long dead, so not my problem
    // if you somehow still use this guessing function.
    (946684800..=4670438400).contains(&value)
}

pub fn is_f64_likely_timestamp(value: f64) -> bool {
    (946684800.0..=4670438400.0).contains(&value)
}

pub fn is_decimal_likely_timestamp(value: Decimal) -> bool {
    let from = Decimal::from(946684800i64);
    let to = Decimal::from(4670438400i64);
    value >= from && value <= to
}

// This function takes a column and gives a score of
// how likely it is to be a datetime column.
// The numbers are completely arbitrary.
// This is more an helper function than something a production
// system should rely on. Of course production systems may
// rely on this, so the numbers should probably not be changed.
pub fn datetime_guesser(column_name: &str, column: &InferedColumn) -> isize {
    let lowercase_column_name = column_name.to_lowercase();
    let mut sum = 0_isize;
    sum += match lowercase_column_name.as_str() {
        "datetime" => 100,
        "timestamp" => 99,
        "date" => 50,
        "time" => 49,
        "created_at" | "createdat" => 38,
        "updated_at" | "updatedat " => 37,
        "recorded_at" | "recordedat" => 36,
        _ => 0,
    };
    if sum == 0 {
        // We have a few false positive like "outdated" or "candidate"
        // But this is fine.
        if lowercase_column_name.contains("date") {
            sum += 10;
        }
        // Also some false positive like "lifetime" or "downtime"
        if lowercase_column_name.contains("time") {
            sum += 9;
        }
        // Sometimes columns ends with "at", like CheckedInAt
        if lowercase_column_name.ends_with("at") {
            sum += 4;
        }
    }
    sum += match column {
        InferedColumn::DateTime(_) => 100,
        InferedColumn::Integer(values) => {
            // If all values are likely timestamps, it's likely a datetime column
            if values.iter().all(|value| is_i64_likely_timestamp(*value)) {
                42
            } else {
                2
            }
        }
        // Same with numeric more precise type
        InferedColumn::Numeric(values) => {
            if values
                .iter()
                .all(|value| is_decimal_likely_timestamp(*value))
            {
                42
            } else {
                2
            }
        }
        InferedColumn::Float(values) => {
            // It's slightly less likely that floats represent timestamps but still possible
            if values.iter().all(|value| is_f64_likely_timestamp(*value)) {
                32
            } else {
                1
            }
        }
        InferedColumn::Boolean(_) => -80,
        InferedColumn::String(_) => -100,
        InferedColumn::Json(_) => -128,
    };

    sum
}

pub fn likely_datetime_column(
    column_names: &[String],
    columns: &[InferedColumn],
) -> Option<String> {
    let best_candidate = column_names
        .iter()
        .zip(columns.iter())
        .map(|(column_name, column)| (column_name, datetime_guesser(column_name, column)))
        .filter(|(_, score)| *score >= 5) // Require minimum confidence threshold
        .max_by_key(|(_, score)| *score);

    match best_candidate {
        Some((column_name, _)) => Some(column_name.clone()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn test_is_i64_likely_timestamp() {
        assert!(!is_i64_likely_timestamp(0));
        // current timestamp
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        assert!(is_i64_likely_timestamp(now));
        assert!(!is_i64_likely_timestamp(-now));
        assert!(is_i64_likely_timestamp(946684801));
        assert!(is_i64_likely_timestamp(4670438400));
        assert!(!is_i64_likely_timestamp(2093009830983097));
    }

    #[test]
    fn test_is_f64_likely_timestamp() {
        assert!(!is_f64_likely_timestamp(0.0));
        // current timestamp
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as f64;
        assert!(is_f64_likely_timestamp(now));
        assert!(!is_f64_likely_timestamp(-now));
        assert!(is_f64_likely_timestamp(946684801.0));
        assert!(is_f64_likely_timestamp(4670438400.0));
        assert!(!is_f64_likely_timestamp(2093009830983097.0));
    }

    #[test]
    fn test_is_decimal_likely_timestamp() {
        assert!(!is_decimal_likely_timestamp(Decimal::new(0, 0)));
        // current timestamp
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        assert!(is_decimal_likely_timestamp(Decimal::new(now, 0)));
        assert!(!is_decimal_likely_timestamp(Decimal::new(-now, 0)));
        assert!(is_decimal_likely_timestamp(Decimal::new(946684801, 0)));
        assert!(is_decimal_likely_timestamp(Decimal::new(4670438400, 0)));
        assert!(!is_decimal_likely_timestamp(Decimal::new(
            2093009830983097,
            0
        )));
    }

    #[test]
    fn test_datetime_guesser() {
        assert_eq!(
            datetime_guesser("datetime", &InferedColumn::DateTime(vec![])),
            200
        );
        assert_eq!(
            datetime_guesser("timestamp", &InferedColumn::DateTime(vec![])),
            199
        );
        assert_eq!(
            datetime_guesser("date", &InferedColumn::Integer(vec![])),
            92
        );
        assert_eq!(
            datetime_guesser("time", &InferedColumn::Integer(vec![0, 946684801])),
            51
        );
        assert_eq!(
            datetime_guesser("created_at", &InferedColumn::Float(vec![0.0, 946684801.0])),
            39
        );
        assert_eq!(
            datetime_guesser("updated_at", &InferedColumn::Float(vec![946684801.0])),
            69
        );
        assert_eq!(
            datetime_guesser(
                "recorded_at",
                &InferedColumn::Numeric(vec![Decimal::new(946684801, 0)])
            ),
            78
        );
        assert_eq!(
            datetime_guesser(
                "date_of_creation",
                &InferedColumn::Numeric(vec![Decimal::new(2024, 0)])
            ),
            12
        );
        assert_eq!(
            datetime_guesser("sensor_time_ok", &InferedColumn::Boolean(vec![false])),
            -71
        );
        assert_eq!(
            datetime_guesser(
                "sensor_name",
                &InferedColumn::String(vec!["toto".to_string()])
            ),
            -100
        );
        assert_eq!(
            datetime_guesser(
                "sensor_format",
                &InferedColumn::Json(vec![Arc::new(serde_json::json!({"toto": true}))])
            ),
            -124
        );
    }

    #[test]
    fn test_likely_datetime_column() {
        assert_eq!(
            likely_datetime_column(
                &["timestamp".to_string(), "value".to_string()],
                &[
                    InferedColumn::DateTime(vec![
                        hifitime::Epoch::from_unix_seconds(0.0),
                        hifitime::Epoch::from_unix_seconds(1.0),
                        hifitime::Epoch::from_unix_seconds(2.0),
                    ]),
                    InferedColumn::Float(vec![0.0, 1.0, 2.0])
                ]
            ),
            Some("timestamp".to_string())
        );
        assert_eq!(
            likely_datetime_column(
                &["date".to_string(), "time".to_string()],
                &[
                    InferedColumn::Integer(vec![1, 2]),
                    InferedColumn::Integer(vec![946684801, 4670438400])
                ]
            ),
            Some("time".to_string())
        );
        assert_eq!(
            likely_datetime_column(
                &["created_at".to_string(), "content".to_string()],
                &[
                    InferedColumn::DateTime(vec![
                        hifitime::Epoch::from_unix_seconds(0.0),
                        hifitime::Epoch::from_unix_seconds(1.0),
                    ]),
                    InferedColumn::String(vec!["abc".to_string(), "def".to_string(),])
                ]
            ),
            Some("created_at".to_string())
        );
        // No columns
        assert_eq!(likely_datetime_column(&[], &[]), None,);

        // No datetime column
        assert_eq!(
            likely_datetime_column(
                &["name".to_string(), "content".to_string()],
                &[
                    InferedColumn::String(vec!["abc".to_string(), "def".to_string(),]),
                    InferedColumn::String(vec!["ghi".to_string(), "jkl".to_string(),])
                ]
            ),
            None
        );
    }

    #[test]
    fn test_likely_datetime_column_car1() {
        assert_eq!(
            likely_datetime_column(
                &[
                    "Time".to_string(),
                    "Lat".to_string(),
                    "Lon".to_string(),
                    "Bearing".to_string(),
                    "GPS Speed".to_string(),
                    "Revs(rpm)".to_string(),
                    "Speed(km/h)".to_string(),
                    "LPK(l/100km)".to_string(),
                    "CO₂(g/km)".to_string(),
                    "Coolant(°C)".to_string(),
                    "Baro(mb)".to_string(),
                    "GPS Height(m)".to_string(),
                ],
                &[
                    InferedColumn::Integer(vec![1344450050621, 1344450050774]),
                    InferedColumn::Float(vec![60.0, 60.0]),
                    InferedColumn::Float(vec![10.0, 10.0]),
                    InferedColumn::Float(vec![0.0, 0.0]),
                    InferedColumn::Float(vec![0.0, 0.0]),
                    InferedColumn::Float(vec![783.0, 783.0]),
                    InferedColumn::Float(vec![0.0, 0.0]),
                    InferedColumn::Float(vec![0.0, 0.0]),
                    InferedColumn::Float(vec![0.0, 0.0]),
                    InferedColumn::Float(vec![58.0, 58.0]),
                    InferedColumn::Float(vec![998.12, 998.02]),
                    InferedColumn::Float(vec![200.0, 200.0]),
                ]
            ),
            Some("Time".to_string())
        );
    }

    #[test]
    fn test_likely_datetime_column_ebike1() {
        assert_eq!(
            likely_datetime_column(
                &[
                    "time".to_string(),
                    "fix".to_string(),
                    "sats".to_string(),
                    "latitute".to_string(),
                    "longitude".to_string(),
                    "sonar".to_string(),
                    "alt".to_string(),
                    "gps_alt".to_string(),
                    "speed".to_string(),
                    "CRS".to_string(),
                    "roll".to_string(),
                    "pitch".to_string(),
                    "yaw".to_string(),
                ],
                &[
                    InferedColumn::Integer(vec![0, 100]),
                    InferedColumn::Integer(vec![1, 1]),
                    InferedColumn::Integer(vec![6, 6]),
                    InferedColumn::Float(vec![60.0, 60.0]),
                    InferedColumn::Float(vec![10.0, 10.0]),
                    InferedColumn::Integer(vec![0, 0]),
                    InferedColumn::Float(vec![207.95, 207.95]),
                    InferedColumn::Float(vec![204.53, 204.53]),
                    InferedColumn::Float(vec![0.02, 0.02]),
                    InferedColumn::Float(vec![111.97, 111.97]),
                    InferedColumn::Integer(vec![-953, -960]),
                    InferedColumn::Integer(vec![51, 50]),
                    InferedColumn::Integer(vec![17243, 17242]),
                ]
            ),
            Some("time".to_string())
        );
    }

    #[test]
    fn test_likely_datetime_column_ais() {
        assert_eq!(
            likely_datetime_column(
                &[
                    "mmsi".to_string(),
                    "imo_nr".to_string(),
                    "length".to_string(),
                    "date_time_utc".to_string(),
                    "lon".to_string(),
                    "lat".to_string(),
                    "sog".to_string(),
                    "cog".to_string(),
                    "true_heading".to_string(),
                    "nav_status".to_string(),
                    "message_nr".to_string(),
                ],
                &[
                    InferedColumn::Integer(vec![123456789, 123456789]),
                    InferedColumn::Integer(vec![9876543, 9876543]),
                    InferedColumn::Integer(vec![100, 100]),
                    InferedColumn::DateTime(vec![
                        hifitime::Epoch::from_unix_seconds(1420110601.0),
                        hifitime::Epoch::from_unix_seconds(1420110622.0),
                    ]),
                    InferedColumn::Float(vec![14.2535, 14.2549]),
                    InferedColumn::Float(vec![60.0, 60.0]),
                    InferedColumn::Float(vec![10.0, 10.0]),
                    InferedColumn::Float(vec![26.2, 28.0]),
                    InferedColumn::Integer(vec![27, 28]),
                    InferedColumn::Integer(vec![0, 0]),
                    InferedColumn::Integer(vec![1, 1]),
                ]
            ),
            Some("date_time_utc".to_string())
        );
    }
}
