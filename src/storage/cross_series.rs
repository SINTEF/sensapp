//! Query-time aggregation across several series.
//!
//! Series are grouped by a label subset (PromQL `by` / `without`), then bucketed in time. Each
//! (group, bucket) accumulates sum, count, min and max over the samples, so `avg` is a true
//! average of samples and not an average of per-series averages.
//!
//! There are two ways to get the accumulators:
//!
//! - [`read_cross_series`] asks the storage for per-series buckets (`sum`, `count`, `min` or `max`,
//!   and both `count` and `sum` for `avg`) and merges them. The database does the work on the raw
//!   samples, so the limits apply to buckets, not to samples. This is what the endpoint uses.
//! - [`aggregate_across_series`] accumulates raw samples already in memory. It is the reference
//!   the tests compare the first one with.

use crate::datamodel::sensapp_datetime::SensAppDateTimeExt;
use crate::datamodel::sensapp_vec::SensAppLabels;
use crate::datamodel::unit::Unit;
use crate::datamodel::{Sample, SensAppDateTime, Sensor, SensorData, SensorType, TypedSamples};
use crate::storage::common::{bucket_start, datetime_to_micros};
use crate::storage::{
    Aggregation, LabelMatcher, SelectorLimitExceeded, SensorDataQueryOptions, StorageInstance,
};
use anyhow::{Result, anyhow};
use rust_decimal::prelude::ToPrimitive;
use smallvec::SmallVec;
use std::collections::{BTreeMap, BTreeSet};

/// Which labels identify an output series.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Grouping {
    /// Keep only these labels
    By(Vec<String>),
    /// Keep every label except these
    Without(Vec<String>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct CrossSeriesQuery {
    pub aggregation: Aggregation,
    /// `None` collapses every input series into a single output series.
    pub grouping: Option<Grouping>,
    /// Bucket width in microseconds. `None` puts the whole window in a single bucket.
    pub step_us: Option<i64>,
    /// Bucket origin, normally the window start.
    pub origin_us: i64,
}

impl CrossSeriesQuery {
    pub fn validate(&self) -> Result<()> {
        if matches!(
            self.aggregation,
            Aggregation::First | Aggregation::Last | Aggregation::Latest
        ) {
            return Err(anyhow!(
                "'first' and 'last' are not defined across series; use sum, avg, min, max or count"
            ));
        }
        if self.step_us.is_some_and(|step| step <= 0) {
            return Err(anyhow!("'step' must be greater than zero"));
        }
        if self.step_us.is_some_and(|step| step % 1000 != 0) {
            return Err(anyhow!("'step' must be a whole number of milliseconds"));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
struct Accumulator {
    sum: f64,
    count: i64,
    min: f64,
    max: f64,
}

impl Accumulator {
    fn new(value: f64) -> Self {
        Self {
            sum: value,
            count: 1,
            min: value,
            max: value,
        }
    }

    fn add(&mut self, value: f64) {
        self.sum += value;
        self.count += 1;
        self.min = self.min.min(value);
        self.max = self.max.max(value);
    }

    /// What the database computed for one series and one bucket with `aggregation`: only that
    /// field is known, the others are what merging leaves unchanged.
    fn partial(aggregation: Aggregation, value: f64) -> Self {
        let mut accumulator = Self {
            sum: 0.0,
            count: 0,
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
        };
        match aggregation {
            Aggregation::Sum => accumulator.sum = value,
            Aggregation::Count => accumulator.count = value as i64,
            Aggregation::Min => accumulator.min = value,
            Aggregation::Max => accumulator.max = value,
            Aggregation::Avg | Aggregation::First | Aggregation::Last | Aggregation::Latest => {
                unreachable!("not mergeable, rejected before")
            }
        }
        accumulator
    }

    fn merge(&mut self, other: Self) {
        self.sum += other.sum;
        self.count += other.count;
        self.min = self.min.min(other.min);
        self.max = self.max.max(other.max);
    }
}

struct Group {
    names: BTreeSet<String>,
    units: Vec<Option<Unit>>,
    buckets: BTreeMap<i64, Accumulator>,
}

fn group_labels(labels: &SensAppLabels, grouping: &Option<Grouping>) -> SensAppLabels {
    match grouping {
        None => SmallVec::new(),
        Some(Grouping::By(keep)) => labels
            .iter()
            .filter(|(name, _)| keep.contains(name))
            .cloned()
            .collect(),
        Some(Grouping::Without(drop)) => labels
            .iter()
            .filter(|(name, _)| !drop.contains(name))
            .cloned()
            .collect(),
    }
}

fn add_samples<V>(
    group: &mut Group,
    samples: &[Sample<V>],
    query: &CrossSeriesQuery,
    to_f64: impl Fn(&V) -> Option<f64>,
) -> Result<()> {
    for sample in samples {
        let value = to_f64(&sample.value)
            .ok_or_else(|| anyhow!("a sample value cannot be represented as a float"))?;
        let timestamp_us = datetime_to_micros(&sample.datetime);
        let bucket = match query.step_us {
            Some(step_us) => bucket_start(timestamp_us, query.origin_us, step_us),
            None => query.origin_us,
        };
        group
            .buckets
            .entry(bucket)
            .and_modify(|accumulator| accumulator.add(value))
            .or_insert_with(|| Accumulator::new(value));
    }
    Ok(())
}

/// Aggregate the samples of `series` across series, returning one series per group.
///
/// Only numeric series are supported. Output series are ordered by their labels, their samples
/// by time. Buckets without any sample are omitted.
pub fn aggregate_across_series(
    series: Vec<SensorData>,
    query: &CrossSeriesQuery,
) -> Result<Vec<SensorData>> {
    query.validate()?;

    let mut groups: BTreeMap<SensAppLabels, Group> = BTreeMap::new();
    for data in series {
        let group = group_for(&mut groups, &data.sensor, &query.grouping);

        match &data.samples {
            TypedSamples::Float(samples) => {
                add_samples(group, samples, query, |v| Some(*v))?;
            }
            TypedSamples::Integer(samples) => {
                add_samples(group, samples, query, |v| Some(*v as f64))?;
            }
            TypedSamples::Numeric(samples) => {
                add_samples(group, samples, query, |v| v.to_f64())?;
            }
            _ => {
                return Err(anyhow!(
                    "aggregation across series is only supported for numeric series, '{}' is not numeric",
                    data.sensor.name
                ));
            }
        }
    }

    finish(groups, query.aggregation)
}

/// The group a series belongs to, which learns the name and the unit of the series.
fn group_for<'a>(
    groups: &'a mut BTreeMap<SensAppLabels, Group>,
    sensor: &Sensor,
    grouping: &Option<Grouping>,
) -> &'a mut Group {
    let group = groups
        .entry(group_labels(&sensor.labels, grouping))
        .or_insert_with(|| Group {
            names: BTreeSet::new(),
            units: Vec::new(),
            buckets: BTreeMap::new(),
        });
    group.names.insert(sensor.name.clone());
    group.units.push(sensor.unit.clone());
    group
}

fn finish(
    groups: BTreeMap<SensAppLabels, Group>,
    aggregation: Aggregation,
) -> Result<Vec<SensorData>> {
    groups
        .into_iter()
        .filter(|(_, group)| !group.buckets.is_empty())
        .map(|(labels, group)| build_output(labels, group, aggregation))
        .collect()
}

/// The window of a single bucket when there is no usable step: a thousand years.
const WHOLE_WINDOW_STEP_MS: i64 = 1000 * 366 * 24 * 3600 * 1000;

/// How much a cross-series read of buckets looked at.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CrossSeriesStats {
    /// Input series that had at least one sample in the window
    pub series: usize,
    /// Buckets read from the storage, all series together
    pub buckets: usize,
}

/// The result of [`read_cross_series`], or the limit that was exceeded.
pub type CrossSeriesRead =
    std::result::Result<(Vec<SensorData>, CrossSeriesStats), SelectorLimitExceeded>;

fn numeric_values(samples: &TypedSamples) -> Result<Vec<(i64, f64)>> {
    fn collect<V>(
        samples: &[Sample<V>],
        to_f64: impl Fn(&V) -> Option<f64>,
    ) -> Result<Vec<(i64, f64)>> {
        samples
            .iter()
            .map(|sample| {
                let value = to_f64(&sample.value)
                    .ok_or_else(|| anyhow!("a value cannot be represented as a float"))?;
                Ok((datetime_to_micros(&sample.datetime), value))
            })
            .collect()
    }
    match samples {
        TypedSamples::Float(samples) => collect(samples, |v| Some(*v)),
        TypedSamples::Integer(samples) => collect(samples, |v| Some(*v as f64)),
        TypedSamples::Numeric(samples) => collect(samples, |v| v.to_f64()),
        _ => Err(anyhow!("aggregation across series needs numeric series")),
    }
}

/// Aggregate the numeric series matching `matchers` across series, from buckets computed by the
/// storage: `max_series` series and `max_buckets` buckets in total (the buckets of every
/// per-series read, so a `step` that makes fewer buckets raises what a query can cover).
///
/// `query.origin_us` must be the start of the window, which is where the storage starts its
/// buckets. Without a `step` the whole window is one bucket. Series that have no sample in the
/// window are left out, so they do not take part in the name of the output series.
///
/// `avg` needs two reads, `count` and `sum`, since per-series averages cannot be merged. They run
/// at the same time. A sample written between the two can make a bucket slightly off, and a
/// bucket seen by only one of them is left out.
pub async fn read_cross_series<S: StorageInstance + ?Sized>(
    storage: &S,
    matchers: &[LabelMatcher],
    start_time: Option<SensAppDateTime>,
    end_time: Option<SensAppDateTime>,
    query: &CrossSeriesQuery,
    max_series: usize,
    max_buckets: usize,
) -> Result<CrossSeriesRead> {
    query.validate()?;
    let origin_us = start_time.as_ref().map(datetime_to_micros).unwrap_or(0);
    if query.origin_us != origin_us {
        return Err(anyhow!(
            "the origin of the buckets must be the start of the window"
        ));
    }
    let step_ms = match (query.step_us, &start_time, &end_time) {
        (Some(step_us), _, _) => step_us / 1000,
        // One bucket for the whole window, closed at both ends: one millisecond more than its length
        (None, Some(start), Some(end)) => {
            let window_us = datetime_to_micros(end) - datetime_to_micros(start);
            (window_us / 1000 + 1).max(1)
        }
        (None, _, _) => WHOLE_WINDOW_STEP_MS,
    };

    let reads: &[Aggregation] = match query.aggregation {
        Aggregation::Avg => &[Aggregation::Count, Aggregation::Sum],
        Aggregation::Sum => &[Aggregation::Sum],
        Aggregation::Count => &[Aggregation::Count],
        Aggregation::Min => &[Aggregation::Min],
        Aggregation::Max => &[Aggregation::Max],
        Aggregation::First | Aggregation::Last | Aggregation::Latest => {
            unreachable!("rejected by validate")
        }
    };

    // The reads do not depend on each other: `avg` waits for the slower of its two, not for both
    let reads_done = futures::future::try_join_all(reads.iter().map(|aggregation| {
        let options = SensorDataQueryOptions {
            start_time,
            end_time,
            limit: None,
            step_ms: Some(step_ms),
            aggregation: Some(*aggregation),
            simplify: None,
        };
        async move {
            storage
                .query_selector_aggregated(matchers, &options, max_series, max_buckets)
                .await
        }
    }))
    .await?;

    // Per input series, in the order the storage gave them: its sensor and its merged buckets
    let mut series: Vec<(Sensor, BTreeMap<i64, Accumulator>)> = Vec::new();
    let mut positions: std::collections::HashMap<uuid::Uuid, usize> = Default::default();
    let mut buckets_read = 0usize;
    for (aggregation, read) in reads.iter().zip(reads_done) {
        let read = match read {
            Ok(read) => read,
            Err(exceeded) => return Ok(Err(exceeded)),
        };
        for data in read {
            let values = numeric_values(&data.samples)?;
            buckets_read += values.len();
            let uuid = data.sensor.uuid;
            let position = match positions.get(&uuid) {
                Some(position) => *position,
                None => {
                    series.push((data.sensor, BTreeMap::new()));
                    positions.insert(uuid, series.len() - 1);
                    series.len() - 1
                }
            };
            for (bucket, value) in values {
                series[position]
                    .1
                    .entry(bucket)
                    .or_insert_with(|| Accumulator::partial(Aggregation::Sum, 0.0))
                    .merge(Accumulator::partial(*aggregation, value));
            }
        }
    }

    let stats = CrossSeriesStats {
        series: series.len(),
        buckets: buckets_read,
    };
    let mut groups: BTreeMap<SensAppLabels, Group> = BTreeMap::new();
    for (sensor, mut buckets) in series {
        if query.aggregation == Aggregation::Avg {
            buckets.retain(|_, accumulator| accumulator.count > 0);
        }
        if buckets.is_empty() {
            continue;
        }
        let group = group_for(&mut groups, &sensor, &query.grouping);
        for (bucket, accumulator) in buckets {
            group
                .buckets
                .entry(bucket)
                .and_modify(|merged| merged.merge(accumulator))
                .or_insert(accumulator);
        }
    }
    Ok(Ok((finish(groups, query.aggregation)?, stats)))
}

fn build_output(
    labels: SensAppLabels,
    group: Group,
    aggregation: Aggregation,
) -> Result<SensorData> {
    let mut names = group.names.into_iter();
    let name = match (names.next(), names.next()) {
        (Some(only), None) => format!("{}({})", aggregation.name(), only),
        _ => aggregation.name().to_string(),
    };

    let unit = if aggregation.output_is_count() {
        None
    } else {
        match group.units.split_first() {
            Some((first, rest)) if rest.iter().all(|unit| unit == first) => first.clone(),
            _ => None,
        }
    };

    let samples = if aggregation.output_is_count() {
        TypedSamples::Integer(
            group
                .buckets
                .iter()
                .map(|(bucket, acc)| Sample {
                    datetime: SensAppDateTime::from_unix_microseconds_i64(*bucket),
                    value: acc.count,
                })
                .collect(),
        )
    } else {
        TypedSamples::Float(
            group
                .buckets
                .iter()
                .map(|(bucket, acc)| Sample {
                    datetime: SensAppDateTime::from_unix_microseconds_i64(*bucket),
                    value: match aggregation {
                        Aggregation::Sum => acc.sum,
                        Aggregation::Avg => acc.sum / acc.count as f64,
                        Aggregation::Min => acc.min,
                        Aggregation::Max => acc.max,
                        Aggregation::Count
                        | Aggregation::First
                        | Aggregation::Last
                        | Aggregation::Latest => {
                            unreachable!("handled or rejected before")
                        }
                    },
                })
                .collect(),
        )
    };

    let sensor_type = if aggregation.output_is_count() {
        SensorType::Integer
    } else {
        SensorType::Float
    };
    let sensor = Sensor::new_without_uuid(name, sensor_type, unit, Some(labels))?;

    Ok(SensorData::new(sensor, samples))
}

#[cfg(test)]
mod tests {
    use super::*;
    use smallvec::smallvec;
    use uuid::Uuid;

    fn sensor(name: &str, sensor_type: SensorType, labels: &[(&str, &str)]) -> Sensor {
        Sensor::new(
            Uuid::new_v4(),
            name.to_string(),
            sensor_type,
            None,
            Some(
                labels
                    .iter()
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect(),
            ),
        )
    }

    fn floats(points: &[(i64, f64)]) -> TypedSamples {
        TypedSamples::Float(
            points
                .iter()
                .map(|(seconds, value)| Sample {
                    datetime: SensAppDateTime::from_unix_microseconds_i64(seconds * 1_000_000),
                    value: *value,
                })
                .collect(),
        )
    }

    fn query(
        aggregation: Aggregation,
        grouping: Option<Grouping>,
        step_s: Option<i64>,
    ) -> CrossSeriesQuery {
        CrossSeriesQuery {
            aggregation,
            grouping,
            step_us: step_s.map(|s| s * 1_000_000),
            origin_us: 0,
        }
    }

    fn run(series: Vec<SensorData>, query: &CrossSeriesQuery) -> Result<Vec<SensorData>> {
        // Output sensors get a deterministic uuid, which needs the configuration
        _ = crate::config::load_configuration_for_tests();
        aggregate_across_series(series, query)
    }

    fn values(data: &SensorData) -> Vec<(i64, f64)> {
        match &data.samples {
            TypedSamples::Float(samples) => samples
                .iter()
                .map(|s| (datetime_to_micros(&s.datetime) / 1_000_000, s.value))
                .collect(),
            TypedSamples::Integer(samples) => samples
                .iter()
                .map(|s| (datetime_to_micros(&s.datetime) / 1_000_000, s.value as f64))
                .collect(),
            _ => panic!("unexpected sample type"),
        }
    }

    #[test]
    fn avg_is_over_samples_not_over_series_averages() {
        // Series a: 3 samples of 10. Series b: 1 sample of 30.
        // Avg of samples = 15. Avg of series averages would be 20.
        let series = vec![
            SensorData::new(
                sensor("temp", SensorType::Float, &[]),
                floats(&[(0, 10.0), (1, 10.0), (2, 10.0)]),
            ),
            SensorData::new(sensor("temp", SensorType::Float, &[]), floats(&[(3, 30.0)])),
        ];
        let out = run(series, &query(Aggregation::Avg, None, None)).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].sensor.name, "avg(temp)");
        assert_eq!(values(&out[0]), vec![(0, 15.0)]);
    }

    #[test]
    fn step_buckets_align_to_origin_and_skip_empty_buckets() {
        let series = vec![
            SensorData::new(
                sensor("temp", SensorType::Float, &[]),
                floats(&[(1, 1.0), (12, 2.0)]),
            ),
            SensorData::new(
                sensor("temp", SensorType::Float, &[]),
                floats(&[(2, 3.0), (45, 4.0)]),
            ),
        ];
        let out = run(series, &query(Aggregation::Sum, None, Some(10))).unwrap();
        // Buckets [0,10) [10,20) [40,50); [20,40) is empty and omitted
        assert_eq!(values(&out[0]), vec![(0, 4.0), (10, 2.0), (40, 4.0)]);
    }

    #[test]
    fn by_splits_groups_and_keeps_only_listed_labels() {
        let series = vec![
            SensorData::new(
                sensor("temp", SensorType::Float, &[("room", "a"), ("id", "1")]),
                floats(&[(0, 10.0)]),
            ),
            SensorData::new(
                sensor("temp", SensorType::Float, &[("room", "a"), ("id", "2")]),
                floats(&[(0, 20.0)]),
            ),
            SensorData::new(
                sensor("temp", SensorType::Float, &[("room", "b"), ("id", "3")]),
                floats(&[(0, 5.0)]),
            ),
        ];
        let out = run(
            series,
            &query(
                Aggregation::Avg,
                Some(Grouping::By(vec!["room".into()])),
                None,
            ),
        )
        .unwrap();
        assert_eq!(out.len(), 2);
        assert_eq!(
            out[0].sensor.labels.as_slice(),
            &[("room".to_string(), "a".to_string())]
        );
        assert_eq!(values(&out[0]), vec![(0, 15.0)]);
        assert_eq!(values(&out[1]), vec![(0, 5.0)]);
    }

    #[test]
    fn without_drops_only_listed_labels() {
        let series = vec![
            SensorData::new(
                sensor("temp", SensorType::Float, &[("room", "a"), ("id", "1")]),
                floats(&[(0, 1.0)]),
            ),
            SensorData::new(
                sensor("temp", SensorType::Float, &[("room", "a"), ("id", "2")]),
                floats(&[(0, 3.0)]),
            ),
        ];
        let out = run(
            series,
            &query(
                Aggregation::Max,
                Some(Grouping::Without(vec!["id".into()])),
                None,
            ),
        )
        .unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(
            out[0].sensor.labels.as_slice(),
            &[("room".to_string(), "a".to_string())]
        );
        assert_eq!(values(&out[0]), vec![(0, 3.0)]);
    }

    #[test]
    fn count_is_integer_and_mixed_numeric_types_are_combined() {
        let integers = || {
            TypedSamples::Integer(smallvec![
                Sample {
                    datetime: SensAppDateTime::from_unix_microseconds_i64(0),
                    value: 2,
                },
                Sample {
                    datetime: SensAppDateTime::from_unix_microseconds_i64(1_000_000),
                    value: 4,
                },
            ])
        };
        let series = || {
            vec![
                SensorData::new(sensor("temp", SensorType::Integer, &[]), integers()),
                SensorData::new(sensor("temp", SensorType::Float, &[]), floats(&[(2, 6.0)])),
            ]
        };
        let counted = run(series(), &query(Aggregation::Count, None, None)).unwrap();
        assert_eq!(counted[0].sensor.sensor_type, SensorType::Integer);
        assert_eq!(values(&counted[0]), vec![(0, 3.0)]);

        let averaged = run(series(), &query(Aggregation::Avg, None, None)).unwrap();
        assert_eq!(values(&averaged[0]), vec![(0, 4.0)]);
    }

    #[test]
    fn non_numeric_series_and_first_last_are_rejected() {
        let strings = TypedSamples::String(smallvec![Sample {
            datetime: SensAppDateTime::from_unix_microseconds_i64(0),
            value: "on".to_string(),
        }]);
        let series = vec![SensorData::new(
            sensor("state", SensorType::String, &[]),
            strings,
        )];
        assert!(run(series, &query(Aggregation::Sum, None, None)).is_err());

        assert!(run(vec![], &query(Aggregation::First, None, None)).is_err());
        assert!(run(vec![], &query(Aggregation::Last, None, None)).is_err());
    }

    #[test]
    fn no_series_gives_no_output() {
        let out = run(vec![], &query(Aggregation::Sum, None, None)).unwrap();
        assert!(out.is_empty());
    }
}
