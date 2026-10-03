//! Turning the samples of a batch into the updates RRDtool accepts.
//!
//! An RRD file only takes strictly increasing times, in whole seconds, and refuses the update of a
//! time that is not after the last one stored. A batch can hold samples in any order, twice, or
//! that were stored by a previous request that was retried, so they are sorted and deduplicated
//! here, and the refusals of the daemon for old times are not failures (see [`stale_updates`]).

use rrdcached_client::{
    batch_update::BatchUpdate,
    errors::RRDCachedClientError::{self, BatchUpdateErrorResponse},
};
use std::collections::BTreeMap;
use uuid::Uuid;

/// What the daemon says when a time is not after the last update of the file.
const STALE_UPDATE_MESSAGE: &str = "illegal attempt to update using time";

/// The samples of one series, ready for an update: strictly increasing times in seconds since
/// the Unix epoch.
#[derive(Debug, Clone, PartialEq)]
pub struct SeriesPoints {
    pub uuid: Uuid,
    pub points: Vec<(u64, f64)>,
}

impl SeriesPoints {
    /// The first time of the series, the start of its file if it has to be created.
    pub fn first_time(&self) -> Option<u64> {
        self.points.first().map(|(time, _)| *time)
    }
}

/// Counts of what was left out of a batch.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Skipped {
    /// Samples before 1970, which an RRD cannot hold
    pub before_epoch: usize,
    /// Samples at the same second as another of the series: only the last one is kept
    pub same_second: usize,
}

/// Sort the samples of every series, keep one per second (the last one given) and merge the
/// entries of the same series. A series without a sample left is not returned.
pub fn prepare_series(
    series: impl IntoIterator<Item = (Uuid, Vec<(i64, f64)>)>,
) -> (Vec<SeriesPoints>, Skipped) {
    let mut skipped = Skipped::default();
    // Ordered by UUID so that the same batch always gives the same updates
    let mut by_series: BTreeMap<Uuid, Vec<(i64, f64)>> = BTreeMap::new();
    for (uuid, points) in series {
        by_series.entry(uuid).or_default().extend(points);
    }

    let prepared = by_series
        .into_iter()
        .filter_map(|(uuid, mut points)| {
            let count = points.len();
            points.retain(|(time, _)| *time >= 0);
            skipped.before_epoch += count - points.len();

            // A stable sort keeps the given order of the samples of one second
            points.sort_by_key(|(time, _)| *time);
            let mut unique: Vec<(u64, f64)> = Vec::with_capacity(points.len());
            for (time, value) in points {
                let time = time as u64;
                match unique.last_mut() {
                    Some(last) if last.0 == time => {
                        last.1 = value;
                        skipped.same_second += 1;
                    }
                    _ => unique.push((time, value)),
                }
            }
            (!unique.is_empty()).then_some(SeriesPoints {
                uuid,
                points: unique,
            })
        })
        .collect();
    (prepared, skipped)
}

/// The `UPDATE` commands of a batch, one per sample.
pub fn batch_updates(series: &[SeriesPoints]) -> Result<Vec<BatchUpdate>, RRDCachedClientError> {
    let mut updates = Vec::with_capacity(series.iter().map(|s| s.points.len()).sum());
    for series in series {
        let path = series.uuid.to_string();
        for (time, value) in &series.points {
            updates.push(BatchUpdate::new(&path, Some(*time as usize), vec![*value])?);
        }
    }
    Ok(updates)
}

/// How many updates of a rejected batch the daemon refused because their time is not after the
/// last update of the file: samples that are stored already (a retried request) or that arrive
/// too late. The daemon applies the other lines of the batch, so these are not a failure of the
/// write. `None` when the error is anything else.
pub fn stale_updates(error: &RRDCachedClientError) -> Option<usize> {
    match error {
        BatchUpdateErrorResponse(_, lines)
            if !lines.is_empty()
                && lines.iter().all(|line| line.contains(STALE_UPDATE_MESSAGE)) =>
        {
            Some(lines.len())
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(n: u128) -> Uuid {
        Uuid::from_u128(n)
    }

    #[test]
    fn samples_are_sorted_and_deduplicated_per_series() {
        let (series, skipped) = prepare_series([(
            id(1),
            vec![(30, 3.0), (10, 1.0), (20, 2.0), (20, 2.5), (10, 1.5)],
        )]);
        assert_eq!(
            series,
            vec![SeriesPoints {
                uuid: id(1),
                points: vec![(10, 1.5), (20, 2.5), (30, 3.0)],
            }]
        );
        assert_eq!(skipped.same_second, 2);
    }

    #[test]
    fn the_last_given_sample_of_a_second_wins() {
        let (series, _) = prepare_series([(id(1), vec![(10, 1.0), (10, 2.0), (10, 3.0)])]);
        assert_eq!(series[0].points, vec![(10, 3.0)]);
    }

    #[test]
    fn entries_of_the_same_series_are_merged() {
        let (series, _) = prepare_series([
            (id(1), vec![(20, 2.0)]),
            (id(2), vec![(5, 5.0)]),
            (id(1), vec![(10, 1.0)]),
        ]);
        assert_eq!(series.len(), 2);
        assert_eq!(series[0].uuid, id(1));
        assert_eq!(series[0].points, vec![(10, 1.0), (20, 2.0)]);
        assert_eq!(series[1].uuid, id(2));
    }

    #[test]
    fn samples_before_the_epoch_are_dropped_and_empty_series_disappear() {
        let (series, skipped) = prepare_series([
            (id(1), vec![(-5, 1.0), (0, 2.0)]),
            (id(2), vec![(-1, 1.0)]),
            (id(3), vec![]),
        ]);
        assert_eq!(series.len(), 1);
        assert_eq!(series[0].points, vec![(0, 2.0)]);
        assert_eq!(skipped.before_epoch, 2);
    }

    #[test]
    fn updates_are_one_command_per_sample() {
        let (series, _) = prepare_series([(id(1), vec![(10, 1.5), (20, f64::NAN)])]);
        let commands: Vec<String> = batch_updates(&series)
            .unwrap()
            .iter()
            .map(|update| update.to_command_string().unwrap())
            .collect();
        let name = id(1).to_string();
        assert_eq!(commands[0], format!("UPDATE {name}.rrd 10:1.5\n"));
        assert_eq!(commands[1], format!("UPDATE {name}.rrd 20:NaN\n"));
    }

    fn rejected(lines: &[&str]) -> RRDCachedClientError {
        BatchUpdateErrorResponse(
            "errors".to_string(),
            lines.iter().map(|line| line.to_string()).collect(),
        )
    }

    #[test]
    fn stale_updates_are_counted_when_they_are_all_there_is() {
        let error = rejected(&[
            "4 illegal attempt to update using time 1704067320.000000 when last update time is 1704067320.000000 (minimum one second step)\n",
            "5 illegal attempt to update using time 1704067300.000000 when last update time is 1704067320.000000 (minimum one second step)\n",
        ]);
        assert_eq!(stale_updates(&error), Some(2));
    }

    #[test]
    fn another_refusal_is_a_failure() {
        let error = rejected(&[
            "1 illegal attempt to update using time 5 when last update time is 6\n",
            "2 /var/lib/rrdcached/db/x.rrd: No such file or directory\n",
        ]);
        assert_eq!(stale_updates(&error), None);
        assert_eq!(
            stale_updates(&RRDCachedClientError::Parsing("x".to_string())),
            None
        );
        assert_eq!(stale_updates(&rejected(&[])), None);
    }
}
