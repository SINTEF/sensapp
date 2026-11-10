pub mod arrow;
pub mod csv;
pub mod jsonl;
pub mod senml;

pub use arrow::ArrowConverter;
pub use csv::CsvConverter;
pub use jsonl::JsonlConverter;
pub use senml::SenMLConverter;

/// A float as JSON. JSON has no NaN and no infinities, which `serde_json` writes as `null`: they
/// are written as the texts of Prometheus instead, `"NaN"`, `"+Inf"` and `"-Inf"`.
pub fn float_to_json(value: f64) -> serde_json::Value {
    if value.is_finite() {
        serde_json::Value::from(value)
    } else if value.is_nan() {
        serde_json::Value::from("NaN")
    } else if value > 0.0 {
        serde_json::Value::from("+Inf")
    } else {
        serde_json::Value::from("-Inf")
    }
}

#[cfg(test)]
mod tests {
    use super::float_to_json;

    #[test]
    fn non_finite_floats_are_texts_in_json() {
        assert_eq!(float_to_json(1.5), serde_json::json!(1.5));
        assert_eq!(float_to_json(f64::NAN), "NaN");
        assert_eq!(float_to_json(f64::INFINITY), "+Inf");
        assert_eq!(float_to_json(f64::NEG_INFINITY), "-Inf");
    }
}
