use crate::datamodel::SensAppDateTime;
use anyhow::{Result, anyhow};
use std::str::FromStr;

/// The longest step, in milliseconds: the years 1 to 9999 that can be stored. A step multiplied into
/// microseconds by the SQL of the buckets then still fits in an `i64`, with room for the
/// arithmetic around it.
pub const MAX_STEP_MS: i64 = (crate::datamodel::sensapp_datetime::LATEST_UNIX_US
    - crate::datamodel::sensapp_datetime::EARLIEST_UNIX_US)
    / 1000
    + 1;

/// How the samples of a bucket are reduced to one value.
///
/// `First`, `Last` and `Latest` among samples of a series at the same timestamp (written twice,
/// without deduplication): SQLite and DuckDB keep the order of the writes, the first one written
/// for `First` and the last one for `Last` and `Latest`. The other backends keep either of them.
/// Deduplicating at ingestion or with the vacuum leaves one sample per timestamp and value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Aggregation {
    Avg,
    Min,
    Max,
    Sum,
    Count,
    First,
    Last,
    /// The value of the last sample of the bucket, stamped with the timestamp of that sample
    /// instead of the start of the bucket (`Last`). For the readers that look for the latest
    /// sample before a time, such as a Prometheus instant selector. Internal: not parsed from
    /// a request.
    Latest,
}

impl Aggregation {
    pub fn name(self) -> &'static str {
        match self {
            Self::Avg => "avg",
            Self::Min => "min",
            Self::Max => "max",
            Self::Sum => "sum",
            Self::Count => "count",
            Self::First => "first",
            Self::Last => "last",
            Self::Latest => "latest",
        }
    }

    pub fn output_is_count(self) -> bool {
        matches!(self, Self::Count)
    }
}

impl FromStr for Aggregation {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self> {
        match value.to_ascii_lowercase().as_str() {
            "avg" => Ok(Self::Avg),
            "min" => Ok(Self::Min),
            "max" => Ok(Self::Max),
            "sum" => Ok(Self::Sum),
            "count" => Ok(Self::Count),
            "first" => Ok(Self::First),
            "last" => Ok(Self::Last),
            _ => Err(anyhow!(
                "Unsupported aggregation '{}'. Supported values: avg, min, max, sum, count, first, last",
                value
            )),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SimplifyOptions {
    pub tolerance: f64,
    pub high_quality: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SensorDataQueryOptions {
    pub start_time: Option<SensAppDateTime>,
    pub end_time: Option<SensAppDateTime>,
    pub limit: Option<usize>,
    pub step_ms: Option<i64>,
    pub aggregation: Option<Aggregation>,
    pub simplify: Option<SimplifyOptions>,
}

impl SensorDataQueryOptions {
    pub fn validate(&self) -> Result<()> {
        if self.step_ms.is_some() != self.aggregation.is_some() {
            return Err(anyhow!(
                "'step' and 'aggregation' must be provided together"
            ));
        }

        if let Some(step_ms) = self.step_ms
            && step_ms <= 0
        {
            return Err(anyhow!("'step' must be greater than zero"));
        }
        if let Some(step_ms) = self.step_ms
            && step_ms > MAX_STEP_MS
        {
            return Err(anyhow!(
                "'step' must not be longer than the years 1 to 9999 that can be stored"
            ));
        }

        if let Some(simplify) = self.simplify
            && (!simplify.tolerance.is_finite() || simplify.tolerance <= 0.0)
        {
            return Err(anyhow!(
                "'simplify_tolerance' must be a finite number greater than zero"
            ));
        }

        Ok(())
    }
}
