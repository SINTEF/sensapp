use super::columns::InferedColumn;

pub fn is_i64_likely_timestamp(value: i64) -> bool {
    // Between 2000-01-01 and 2118-01-01
    // In 2118, I will very likely be long dead, so not my problem
    // if you somehow still use this guessing function.
    (946684800..=4670438400).contains(&value)
}

pub fn is_f64_likely_timestamp(value: f64) -> bool {
    (946684800.0..=4670438400.0).contains(&value)
}

pub fn is_decimal_likely_timestamp(value: rust_decimal::Decimal) -> bool {
    let from = rust_decimal::Decimal::from(946684800i64);
    let to = rust_decimal::Decimal::from(4670438400i64);
    value >= from && value <= to
}

// This function takes a column and gives a score of
// how likely it is to be a datetime column.
// The numbers are completely arbitrary.
// This is more an helper function than something a production
// system should rely on. Of course production systems may
// rely on this, so the numbers should probably not be changed.
fn datetime_guesser(column_name: &str, column: &InferedColumn) -> isize {
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
        InferedColumn::JSON(_) => -128,
    };

    sum
}

pub fn likely_datetime_column(
    column_names: &Vec<String>,
    columns: &Vec<InferedColumn>,
) -> Option<String> {
    let best_candidate = column_names
        .iter()
        .zip(columns.iter())
        .map(|(column_name, column)| (column_name, datetime_guesser(column_name, column)))
        .max_by_key(|(_, score)| *score);

    match best_candidate {
        Some((column_name, score)) if score > 0 => Some(column_name.clone()),
        _ => None,
    }
}
