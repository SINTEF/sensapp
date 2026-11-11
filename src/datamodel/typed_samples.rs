use super::{Sample, SensAppDateTime, SensorType, sensapp_vec::SensAppVec};
use rust_decimal::{Decimal, prelude::ToPrimitive};
use smallvec::smallvec;
use std::str::FromStr;

/// Integers are exact in an `f64` up to 2^53
const EXACT_F64_INTEGER: i64 = 1 << 53;

/// Samples that cannot become the samples of another type without changing a value.
#[derive(Debug, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct TypeConversionError(pub String);

#[derive(Debug, PartialEq)]
pub enum TypedSamples {
    Integer(SensAppVec<Sample<i64>>),
    Numeric(SensAppVec<Sample<rust_decimal::Decimal>>),
    Float(SensAppVec<Sample<f64>>),
    String(SensAppVec<Sample<String>>),
    Boolean(SensAppVec<Sample<bool>>),
    Location(SensAppVec<Sample<geo::Point>>),
    Blob(SensAppVec<Sample<Vec<u8>>>),
    Json(SensAppVec<Sample<serde_json::Value>>),
}

impl TypedSamples {
    pub fn one_integer(value: i64, datetime: SensAppDateTime) -> Self {
        Self::Integer(smallvec![Sample { datetime, value }])
    }
    pub fn one_numeric(value: rust_decimal::Decimal, datetime: SensAppDateTime) -> Self {
        Self::Numeric(smallvec![Sample { datetime, value }])
    }
    pub fn one_float(value: f64, datetime: SensAppDateTime) -> Self {
        Self::Float(smallvec![Sample { datetime, value }])
    }
    pub fn one_string(value: String, datetime: SensAppDateTime) -> Self {
        Self::String(smallvec![Sample { datetime, value }])
    }
    pub fn one_boolean(value: bool, datetime: SensAppDateTime) -> Self {
        Self::Boolean(smallvec![Sample { datetime, value }])
    }
    pub fn one_location(value: geo::Point, datetime: SensAppDateTime) -> Self {
        Self::Location(smallvec![Sample { datetime, value }])
    }
    pub fn one_blob(value: Vec<u8>, datetime: SensAppDateTime) -> Self {
        Self::Blob(smallvec![Sample { datetime, value }])
    }
    pub fn one_json(value: serde_json::Value, datetime: SensAppDateTime) -> Self {
        Self::Json(smallvec![Sample { datetime, value }])
    }

    pub fn len(&self) -> usize {
        match self {
            TypedSamples::Integer(vec) => vec.len(),
            TypedSamples::Numeric(vec) => vec.len(),
            TypedSamples::Float(vec) => vec.len(),
            TypedSamples::String(vec) => vec.len(),
            TypedSamples::Boolean(vec) => vec.len(),
            TypedSamples::Location(vec) => vec.len(),
            TypedSamples::Blob(vec) => vec.len(),
            TypedSamples::Json(vec) => vec.len(),
        }
    }

    /// The times of the samples, in order.
    pub fn datetimes(&self) -> Box<dyn Iterator<Item = SensAppDateTime> + '_> {
        match self {
            TypedSamples::Integer(vec) => Box::new(vec.iter().map(|s| s.datetime)),
            TypedSamples::Numeric(vec) => Box::new(vec.iter().map(|s| s.datetime)),
            TypedSamples::Float(vec) => Box::new(vec.iter().map(|s| s.datetime)),
            TypedSamples::String(vec) => Box::new(vec.iter().map(|s| s.datetime)),
            TypedSamples::Boolean(vec) => Box::new(vec.iter().map(|s| s.datetime)),
            TypedSamples::Location(vec) => Box::new(vec.iter().map(|s| s.datetime)),
            TypedSamples::Blob(vec) => Box::new(vec.iter().map(|s| s.datetime)),
            TypedSamples::Json(vec) => Box::new(vec.iter().map(|s| s.datetime)),
        }
    }

    /// Keep the first `len` samples.
    pub fn truncate(&mut self, len: usize) {
        match self {
            TypedSamples::Integer(vec) => vec.truncate(len),
            TypedSamples::Numeric(vec) => vec.truncate(len),
            TypedSamples::Float(vec) => vec.truncate(len),
            TypedSamples::String(vec) => vec.truncate(len),
            TypedSamples::Boolean(vec) => vec.truncate(len),
            TypedSamples::Location(vec) => vec.truncate(len),
            TypedSamples::Blob(vec) => vec.truncate(len),
            TypedSamples::Json(vec) => vec.truncate(len),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The samples as samples of `target`, when every value converts exactly, and an error that
    /// names the first one that does not. Writers of one series do not always send the type it was
    /// created with: SenML only has floating point numbers, and pandas turns an integer column
    /// with gaps into `float64`. The rules:
    ///
    /// | From | To | When |
    /// | --- | --- | --- |
    /// | Float | Integer | the value is a whole number within the `i64` range |
    /// | Integer | Float | the value is within ±2^53 |
    /// | Integer | Numeric | always |
    /// | Float | Numeric | the shortest decimal form of the float, when it fits a `Decimal` |
    /// | Numeric | Integer | the value is a whole number within the `i64` range |
    /// | Numeric | Float | the float reads back as the same decimal |
    ///
    /// Anything else is refused.
    pub fn convert_to(self, target: SensorType) -> Result<TypedSamples, TypeConversionError> {
        let refuse = |reason: String| Err(TypeConversionError(reason));
        fn map<T, U>(
            samples: SensAppVec<Sample<T>>,
            convert: impl Fn(T) -> Result<U, String>,
        ) -> Result<SensAppVec<Sample<U>>, TypeConversionError> {
            samples
                .into_iter()
                .map(|sample| {
                    Ok(Sample {
                        datetime: sample.datetime,
                        value: convert(sample.value).map_err(TypeConversionError)?,
                    })
                })
                .collect()
        }
        let whole_i64 = |value: f64| -> Result<i64, String> {
            // 2^63 is the first float above `i64::MAX`
            if value.fract() == 0.0
                && (-9_223_372_036_854_775_808.0..9_223_372_036_854_775_808.0).contains(&value)
            {
                Ok(value as i64)
            } else {
                Err(format!("{value} is not a whole number of the 64-bit range"))
            }
        };
        Ok(match (self, target) {
            (samples @ TypedSamples::Integer(_), SensorType::Integer)
            | (samples @ TypedSamples::Float(_), SensorType::Float)
            | (samples @ TypedSamples::Numeric(_), SensorType::Numeric) => samples,
            (TypedSamples::Float(samples), SensorType::Integer) => {
                TypedSamples::Integer(map(samples, whole_i64)?)
            }
            (TypedSamples::Integer(samples), SensorType::Float) => {
                TypedSamples::Float(map(samples, |value| {
                    if (-EXACT_F64_INTEGER..=EXACT_F64_INTEGER).contains(&value) {
                        Ok(value as f64)
                    } else {
                        Err(format!(
                            "{value} is beyond 2^53, which a float holds exactly"
                        ))
                    }
                })?)
            }
            (TypedSamples::Integer(samples), SensorType::Numeric) => {
                TypedSamples::Numeric(map(samples, |value| Ok(Decimal::from(value)))?)
            }
            (TypedSamples::Float(samples), SensorType::Numeric) => {
                TypedSamples::Numeric(map(samples, |value| {
                    if !value.is_finite() {
                        return Err(format!("{value} is not a number"));
                    }
                    // `from_str` would round what is past the 28 decimals of a `Decimal`
                    Decimal::from_str_exact(&format!("{value}"))
                        .map_err(|_| format!("{value} does not fit a decimal"))
                })?)
            }
            (TypedSamples::Numeric(samples), SensorType::Integer) => {
                TypedSamples::Integer(map(samples, |value| {
                    if value.fract().is_zero() {
                        value
                            .to_i64()
                            .ok_or_else(|| format!("{value} is beyond the 64-bit range"))
                    } else {
                        Err(format!("{value} is not a whole number"))
                    }
                })?)
            }
            (TypedSamples::Numeric(samples), SensorType::Float) => {
                TypedSamples::Float(map(samples, |value| {
                    let float = value
                        .to_f64()
                        .ok_or_else(|| format!("{value} is not a float"))?;
                    // Exact when the shortest form of the float is the decimal
                    match Decimal::from_str(&format!("{float}")) {
                        Ok(back) if back == value => Ok(float),
                        _ => Err(format!("{value} is not exactly a float")),
                    }
                })?)
            }
            (samples, target) => {
                return refuse(format!(
                    "{} samples cannot become {target} samples",
                    samples.type_name()
                ));
            }
        })
    }

    fn type_name(&self) -> &'static str {
        match self {
            TypedSamples::Integer(_) => "Integer",
            TypedSamples::Numeric(_) => "Numeric",
            TypedSamples::Float(_) => "Float",
            TypedSamples::String(_) => "String",
            TypedSamples::Boolean(_) => "Boolean",
            TypedSamples::Location(_) => "Location",
            TypedSamples::Blob(_) => "Blob",
            TypedSamples::Json(_) => "JSON",
        }
    }

    pub fn clone_empty(&self) -> Self {
        match self {
            TypedSamples::Integer(_) => TypedSamples::Integer(smallvec![]),
            TypedSamples::Numeric(_) => TypedSamples::Numeric(smallvec![]),
            TypedSamples::Float(_) => TypedSamples::Float(smallvec![]),
            TypedSamples::String(_) => TypedSamples::String(smallvec![]),
            TypedSamples::Boolean(_) => TypedSamples::Boolean(smallvec![]),
            TypedSamples::Location(_) => TypedSamples::Location(smallvec![]),
            TypedSamples::Blob(_) => TypedSamples::Blob(smallvec![]),
            TypedSamples::Json(_) => TypedSamples::Json(smallvec![]),
        }
    }

    // The + Send is required and its absence would cause weird compilation errors in other parts of the code
    pub fn into_chunks(self, chunk_size: usize) -> Box<dyn Iterator<Item = TypedSamples> + Send> {
        if self.len() <= chunk_size {
            return Box::new(std::iter::once(self));
        }
        match self {
            TypedSamples::Integer(vec) => Box::new(
                ChunkIterator::new(vec.into_vec(), chunk_size)
                    .map(|v| TypedSamples::Integer(v.into())),
            ),
            TypedSamples::Numeric(vec) => Box::new(
                ChunkIterator::new(vec.into_vec(), chunk_size)
                    .map(|v| TypedSamples::Numeric(v.into())),
            ),
            TypedSamples::Float(vec) => Box::new(
                ChunkIterator::new(vec.into_vec(), chunk_size)
                    .map(|v| TypedSamples::Float(v.into())),
            ),
            TypedSamples::String(vec) => Box::new(
                ChunkIterator::new(vec.into_vec(), chunk_size)
                    .map(|v| TypedSamples::String(v.into())),
            ),
            TypedSamples::Boolean(vec) => Box::new(
                ChunkIterator::new(vec.into_vec(), chunk_size)
                    .map(|v| TypedSamples::Boolean(v.into())),
            ),
            TypedSamples::Location(vec) => Box::new(
                ChunkIterator::new(vec.into_vec(), chunk_size)
                    .map(|v| TypedSamples::Location(v.into())),
            ),
            TypedSamples::Blob(vec) => Box::new(
                ChunkIterator::new(vec.into_vec(), chunk_size)
                    .map(|v| TypedSamples::Blob(v.into())),
            ),
            TypedSamples::Json(vec) => Box::new(
                ChunkIterator::new(vec.into_vec(), chunk_size)
                    .map(|v| TypedSamples::Json(v.into())),
            ),
        }
    }
}

pub struct ChunkIterator<T> {
    inner: Vec<T>,
    chunk_size: usize,
}

impl<T> ChunkIterator<T> {
    fn new(inner: Vec<T>, chunk_size: usize) -> Self {
        Self { inner, chunk_size }
    }
}

impl<T> Iterator for ChunkIterator<T> {
    type Item = Vec<T>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.inner.is_empty() {
            None
        } else {
            let remaining = self
                .inner
                .split_off(std::cmp::min(self.chunk_size, self.inner.len()));
            Some(std::mem::replace(&mut self.inner, remaining))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_one_constructors() {
        let datetime = SensAppDateTime::from_unix_seconds(1.0);
        assert_eq!(
            TypedSamples::one_integer(1, datetime),
            TypedSamples::Integer(smallvec![Sample { datetime, value: 1 }])
        );
        assert_eq!(
            TypedSamples::one_numeric(rust_decimal::Decimal::new(1, 1), datetime),
            TypedSamples::Numeric(smallvec![Sample {
                datetime,
                value: rust_decimal::Decimal::new(1, 1)
            }])
        );
        assert_eq!(
            TypedSamples::one_float(1.0, datetime),
            TypedSamples::Float(smallvec![Sample {
                datetime,
                value: 1.0
            }])
        );
        assert_eq!(
            TypedSamples::one_string("1".to_string(), datetime),
            TypedSamples::String(smallvec![Sample {
                datetime,
                value: "1".to_string()
            }])
        );
        assert_eq!(
            TypedSamples::one_boolean(true, datetime),
            TypedSamples::Boolean(smallvec![Sample {
                datetime,
                value: true
            }])
        );
        assert_eq!(
            TypedSamples::one_location(geo::Point::new(1.0, 1.0), datetime),
            TypedSamples::Location(smallvec![Sample {
                datetime,
                value: geo::Point::new(1.0, 1.0)
            }])
        );
        assert_eq!(
            TypedSamples::one_blob(vec![1], datetime),
            TypedSamples::Blob(smallvec![Sample {
                datetime,
                value: vec![1]
            }])
        );
        assert_eq!(
            TypedSamples::one_json(serde_json::json!({}), datetime),
            TypedSamples::Json(smallvec![Sample {
                datetime,
                value: serde_json::json!({})
            }])
        );
    }

    #[test]
    fn integer_chunks() {
        let integers = TypedSamples::Integer(smallvec![
            Sample {
                datetime: SensAppDateTime::from_unix_seconds(1.0),
                value: 1,
            },
            Sample {
                datetime: SensAppDateTime::from_unix_seconds(2.0),
                value: 2,
            },
            Sample {
                datetime: SensAppDateTime::from_unix_seconds(3.0),
                value: 3,
            },
            Sample {
                datetime: SensAppDateTime::from_unix_seconds(4.0),
                value: 4,
            },
            Sample {
                datetime: SensAppDateTime::from_unix_seconds(5.0),
                value: 5,
            },
        ]);
        let mut chunks = integers.into_chunks(2);

        assert_eq!(
            chunks.next(),
            Some(TypedSamples::Integer(smallvec![
                Sample {
                    datetime: SensAppDateTime::from_unix_seconds(1.0),
                    value: 1,
                },
                Sample {
                    datetime: SensAppDateTime::from_unix_seconds(2.0),
                    value: 2,
                },
            ]))
        );
        assert_eq!(
            chunks.next(),
            Some(TypedSamples::Integer(smallvec![
                Sample {
                    datetime: SensAppDateTime::from_unix_seconds(3.0),
                    value: 3,
                },
                Sample {
                    datetime: SensAppDateTime::from_unix_seconds(4.0),
                    value: 4,
                },
            ]))
        );
        assert_eq!(
            chunks.next(),
            Some(TypedSamples::Integer(smallvec![Sample {
                datetime: SensAppDateTime::from_unix_seconds(5.0),
                value: 5,
            },]))
        );
        assert_eq!(chunks.next(), None);
    }

    #[test]
    fn test_len_and_chunks() {
        let integers = TypedSamples::Integer(smallvec![
            Sample {
                datetime: SensAppDateTime::from_unix_seconds(1.0),
                value: 1,
            },
            Sample {
                datetime: SensAppDateTime::from_unix_seconds(2.0),
                value: 2,
            },
            Sample {
                datetime: SensAppDateTime::from_unix_seconds(3.0),
                value: 3,
            },
        ]);
        assert_eq!(integers.len(), 3);
        // count is 2 and 1
        let mut chunks = integers.into_chunks(2);
        assert_eq!(chunks.next().unwrap().len(), 2);
        assert_eq!(chunks.next().unwrap().len(), 1);

        let numeric = TypedSamples::Numeric(smallvec![
            Sample {
                datetime: SensAppDateTime::from_unix_seconds(1.0),
                value: rust_decimal::Decimal::new(1, 1),
            },
            Sample {
                datetime: SensAppDateTime::from_unix_seconds(2.0),
                value: rust_decimal::Decimal::new(2, 2),
            },
            Sample {
                datetime: SensAppDateTime::from_unix_seconds(3.0),
                value: rust_decimal::Decimal::new(3, 3),
            },
        ]);
        assert_eq!(numeric.len(), 3);
        let mut chunks = numeric.into_chunks(4);
        assert_eq!(chunks.next().unwrap().len(), 3);

        let float = TypedSamples::Float(smallvec![
            Sample {
                datetime: SensAppDateTime::from_unix_seconds(1.0),
                value: 1.0,
            },
            Sample {
                datetime: SensAppDateTime::from_unix_seconds(2.0),
                value: 2.0,
            },
            Sample {
                datetime: SensAppDateTime::from_unix_seconds(3.0),
                value: 3.0,
            },
        ]);
        assert_eq!(float.len(), 3);
        let mut chunks = float.into_chunks(1);
        assert_eq!(chunks.next().unwrap().len(), 1);
        assert_eq!(chunks.next().unwrap().len(), 1);
        assert_eq!(chunks.next().unwrap().len(), 1);
        assert!(chunks.next().is_none());

        let string = TypedSamples::String(smallvec![
            Sample {
                datetime: SensAppDateTime::from_unix_seconds(1.0),
                value: "1".to_string(),
            },
            Sample {
                datetime: SensAppDateTime::from_unix_seconds(2.0),
                value: "2".to_string(),
            },
            Sample {
                datetime: SensAppDateTime::from_unix_seconds(3.0),
                value: "3".to_string(),
            },
        ]);
        assert_eq!(string.len(), 3);
        let mut chunks = string.into_chunks(2);
        assert_eq!(chunks.next().unwrap().len(), 2);
        assert_eq!(chunks.next().unwrap().len(), 1);

        let boolean = TypedSamples::Boolean(smallvec![
            Sample {
                datetime: SensAppDateTime::from_unix_seconds(1.0),
                value: true,
            },
            Sample {
                datetime: SensAppDateTime::from_unix_seconds(2.0),
                value: false,
            },
            Sample {
                datetime: SensAppDateTime::from_unix_seconds(3.0),
                value: true,
            },
        ]);
        assert_eq!(boolean.len(), 3);
        let mut chunks = boolean.into_chunks(3);
        assert_eq!(chunks.next().unwrap().len(), 3);

        let location = TypedSamples::Location(smallvec![
            Sample {
                datetime: SensAppDateTime::from_unix_seconds(1.0),
                value: geo::Point::new(1.0, 1.0),
            },
            Sample {
                datetime: SensAppDateTime::from_unix_seconds(2.0),
                value: geo::Point::new(2.0, 2.0),
            },
            Sample {
                datetime: SensAppDateTime::from_unix_seconds(3.0),
                value: geo::Point::new(3.0, 3.0),
            },
        ]);
        assert_eq!(location.len(), 3);
        let mut chunks = location.into_chunks(2);
        assert_eq!(chunks.next().unwrap().len(), 2);
        assert_eq!(chunks.next().unwrap().len(), 1);

        let blob = TypedSamples::Blob(smallvec![
            Sample {
                datetime: SensAppDateTime::from_unix_seconds(1.0),
                value: vec![1],
            },
            Sample {
                datetime: SensAppDateTime::from_unix_seconds(2.0),
                value: vec![2],
            },
            Sample {
                datetime: SensAppDateTime::from_unix_seconds(3.0),
                value: vec![3],
            },
        ]);
        assert_eq!(blob.len(), 3);
        let mut chunks = blob.into_chunks(2);
        assert_eq!(chunks.next().unwrap().len(), 2);

        let json = TypedSamples::Json(smallvec![
            Sample {
                datetime: SensAppDateTime::from_unix_seconds(1.0),
                value: serde_json::json!({}),
            },
            Sample {
                datetime: SensAppDateTime::from_unix_seconds(2.0),
                value: serde_json::json!({}),
            },
            Sample {
                datetime: SensAppDateTime::from_unix_seconds(3.0),
                value: serde_json::json!({}),
            },
        ]);
        assert_eq!(json.len(), 3);
        let mut chunks = json.into_chunks(2);
        assert_eq!(chunks.next().unwrap().len(), 2);
        assert_eq!(chunks.next().unwrap().len(), 1);
        assert!(chunks.next().is_none());
    }

    fn assert_send<T: Send>() {}

    #[test]
    fn test_send() {
        assert_send::<TypedSamples>();
        assert_send::<ChunkIterator<Sample<i64>>>();
    }
}

#[cfg(test)]
mod conversion_tests {
    use super::*;
    use crate::datamodel::sensapp_datetime::SensAppDateTimeExt;

    fn at(second: i64) -> SensAppDateTime {
        SensAppDateTime::from_unix_microseconds_i64(second * 1_000_000)
    }

    fn floats(values: &[f64]) -> TypedSamples {
        TypedSamples::Float(
            values
                .iter()
                .enumerate()
                .map(|(i, &value)| Sample {
                    datetime: at(i as i64),
                    value,
                })
                .collect(),
        )
    }

    fn integers(values: &[i64]) -> TypedSamples {
        TypedSamples::Integer(
            values
                .iter()
                .enumerate()
                .map(|(i, &value)| Sample {
                    datetime: at(i as i64),
                    value,
                })
                .collect(),
        )
    }

    fn numerics(values: &[&str]) -> TypedSamples {
        TypedSamples::Numeric(
            values
                .iter()
                .enumerate()
                .map(|(i, value)| Sample {
                    datetime: at(i as i64),
                    value: Decimal::from_str(value).unwrap(),
                })
                .collect(),
        )
    }

    #[test]
    fn float_to_integer_needs_whole_numbers_in_range() {
        assert_eq!(
            floats(&[1.0, -2.0, 0.0, 9_007_199_254_740_993.0]).convert_to(SensorType::Integer),
            Ok(integers(&[1, -2, 0, 9_007_199_254_740_992]))
        );
        for bad in [
            1.5,
            f64::NAN,
            f64::INFINITY,
            1e19,
            -1e19,
            9_223_372_036_854_775_808.0,
        ] {
            assert!(
                floats(&[1.0, bad]).convert_to(SensorType::Integer).is_err(),
                "{bad}"
            );
        }
        // The lowest `i64` is a float
        assert_eq!(
            floats(&[-9_223_372_036_854_775_808.0]).convert_to(SensorType::Integer),
            Ok(integers(&[i64::MIN]))
        );
    }

    #[test]
    fn integer_to_float_is_exact_up_to_two_to_the_53() {
        assert_eq!(
            integers(&[1, -9_007_199_254_740_992, 9_007_199_254_740_992])
                .convert_to(SensorType::Float),
            Ok(floats(&[
                1.0,
                -9_007_199_254_740_992.0,
                9_007_199_254_740_992.0
            ]))
        );
        assert!(
            integers(&[9_007_199_254_740_993])
                .convert_to(SensorType::Float)
                .is_err()
        );
        assert!(integers(&[i64::MAX]).convert_to(SensorType::Float).is_err());
    }

    #[test]
    fn to_numeric_goes_through_the_shortest_decimal_form() {
        assert_eq!(
            integers(&[i64::MIN, 7]).convert_to(SensorType::Numeric),
            Ok(numerics(&["-9223372036854775808", "7"]))
        );
        // 0.1 as a float is 0.1000000000000000055…: the shortest form is what was written
        assert_eq!(
            floats(&[0.1, 1.5e-3, 2.0]).convert_to(SensorType::Numeric),
            Ok(numerics(&["0.1", "0.0015", "2"]))
        );
        // 28 decimals fit
        assert_eq!(
            floats(&[1e-28]).convert_to(SensorType::Numeric),
            Ok(numerics(&["0.0000000000000000000000000001"]))
        );
        // Past the 28 decimals of a `Decimal` a value would be rounded, to 0 for the first two
        for bad in [
            f64::NAN,
            f64::INFINITY,
            1e300,
            1e-30,
            5e-324,
            1.2345678901234568e-15,
        ] {
            assert!(
                floats(&[bad]).convert_to(SensorType::Numeric).is_err(),
                "{bad}"
            );
        }
    }

    #[test]
    fn numeric_to_integer_and_float_when_exact() {
        assert_eq!(
            numerics(&["5", "-3.0"]).convert_to(SensorType::Integer),
            Ok(integers(&[5, -3]))
        );
        assert!(numerics(&["5.5"]).convert_to(SensorType::Integer).is_err());
        assert!(
            numerics(&["79228162514264337593543950335"])
                .convert_to(SensorType::Integer)
                .is_err()
        );
        assert_eq!(
            numerics(&["0.1", "2"]).convert_to(SensorType::Float),
            Ok(floats(&[0.1, 2.0]))
        );
        // More digits than a float keeps
        assert!(
            numerics(&["0.12345678901234567890123"])
                .convert_to(SensorType::Float)
                .is_err()
        );
    }

    #[test]
    fn the_same_type_stays_and_other_types_are_refused() {
        assert_eq!(
            floats(&[1.5]).convert_to(SensorType::Float),
            Ok(floats(&[1.5]))
        );
        let text = TypedSamples::String(smallvec![Sample {
            datetime: at(0),
            value: "1".to_string()
        }]);
        let error = text.convert_to(SensorType::Integer).unwrap_err();
        assert!(
            error.0.contains("String") && error.0.contains("Integer"),
            "{error}"
        );
        assert!(floats(&[1.0]).convert_to(SensorType::String).is_err());
        assert!(integers(&[1]).convert_to(SensorType::Boolean).is_err());
    }
}
