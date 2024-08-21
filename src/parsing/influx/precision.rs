use std::str::FromStr;

#[derive(Debug, Default, Clone, Copy, PartialEq)]
pub enum Precision {
    #[default]
    Nanoseconds,
    Microseconds,
    Milliseconds,
    Seconds,
}

impl FromStr for Precision {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "ns" => Ok(Precision::Nanoseconds),
            "us" => Ok(Precision::Microseconds),
            "ms" => Ok(Precision::Milliseconds),
            "s" => Ok(Precision::Seconds),
            _ => Err(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_precision_enum() {
        let result = Precision::from_str("ns").unwrap();
        assert_eq!(result, Precision::Nanoseconds);

        let result = Precision::from_str("us").unwrap();
        assert_eq!(result, Precision::Microseconds);

        let result = Precision::from_str("ms").unwrap();
        assert_eq!(result, Precision::Milliseconds);

        let result = Precision::from_str("s").unwrap();
        assert_eq!(result, Precision::Seconds);

        let result = Precision::from_str("wrong");
        assert!(result.is_err());

        let result = Precision::default();
        assert_eq!(result, Precision::Nanoseconds);
    }
}
