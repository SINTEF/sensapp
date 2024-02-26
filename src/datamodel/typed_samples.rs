use super::{sensapp_vec::SensAppVec, Sample, SensAppDateTime};
use smallvec::smallvec;

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
