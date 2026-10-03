//! The shapes of the RRD files SensApp creates.

use anyhow::{Result, bail};
use rrdcached_client::{
    consolidation_function::ConsolidationFunction, create::CreateRoundRobinArchive,
};

/// The base step of every file: the time between two rows of the finest archive of a preset
/// that stores every step. A sample is part of the row of its step.
pub const STEP_SECONDS: u64 = 10;

/// How long a gap between two samples may be before the values in it are unknown.
pub const DEFAULT_HEARTBEAT_SECONDS: i64 = 3600;

#[derive(Debug, Clone, PartialEq)]
pub enum Preset {
    Munin,
    Hoarder,
}

impl Preset {
    /// The step of the rows of the finest archive, in seconds: the resolution of the freshest data.
    pub fn finest_step_seconds(&self) -> i64 {
        self.get_round_robin_archives()
            .iter()
            .map(|archive| archive.steps)
            .min()
            .unwrap_or(1)
            * STEP_SECONDS as i64
    }

    /// The time covered by the longest archive, in seconds.
    pub fn retention_seconds(&self) -> i64 {
        self.get_round_robin_archives()
            .iter()
            .map(|archive| archive.steps * archive.rows)
            .max()
            .unwrap_or(0)
            * STEP_SECONDS as i64
    }

    pub fn get_round_robin_archives(&self) -> Vec<CreateRoundRobinArchive> {
        match self {
            Preset::Munin => vec![
                // Every 5 minutes for 600 entries
                CreateRoundRobinArchive {
                    consolidation_function: ConsolidationFunction::Average,
                    xfiles_factor: 0.5,
                    steps: 30,
                    rows: 600,
                },
                // Every 30 minutes for 700 entries
                CreateRoundRobinArchive {
                    consolidation_function: ConsolidationFunction::Average,
                    xfiles_factor: 0.5,
                    steps: 180,
                    rows: 700,
                },
                // Every 2 hours for 775 entries
                CreateRoundRobinArchive {
                    consolidation_function: ConsolidationFunction::Average,
                    xfiles_factor: 0.5,
                    steps: 720,
                    rows: 775,
                },
                // Every day for 797 entries
                CreateRoundRobinArchive {
                    consolidation_function: ConsolidationFunction::Average,
                    xfiles_factor: 0.5,
                    steps: 8640,
                    rows: 797,
                },
            ],
            Preset::Hoarder => vec![
                // Every 10 seconds for 1 day
                CreateRoundRobinArchive {
                    consolidation_function: ConsolidationFunction::Average,
                    xfiles_factor: 0.5,
                    steps: 1,
                    rows: 8640,
                },
                // Every minute for 2 days
                CreateRoundRobinArchive {
                    consolidation_function: ConsolidationFunction::Average,
                    xfiles_factor: 0.5,
                    steps: 6,
                    rows: 2880,
                },
                // Every 10 minutes for 7 days
                CreateRoundRobinArchive {
                    consolidation_function: ConsolidationFunction::Average,
                    xfiles_factor: 0.5,
                    steps: 60,
                    rows: 1008,
                },
                // Every hour for 1 year
                CreateRoundRobinArchive {
                    consolidation_function: ConsolidationFunction::Average,
                    xfiles_factor: 0.5,
                    steps: 360,
                    rows: 8760,
                },
                // Every day for 10 years
                CreateRoundRobinArchive {
                    consolidation_function: ConsolidationFunction::Average,
                    xfiles_factor: 0.5,
                    steps: 8640,
                    rows: 3650,
                },
            ],
        }
    }
}

impl std::str::FromStr for Preset {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "munin" => Ok(Preset::Munin),
            "hoarder" => Ok(Preset::Hoarder),
            _ => bail!("Invalid preset: {}", s),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn presets_parse_case_insensitively() {
        assert_eq!("Munin".parse::<Preset>().unwrap(), Preset::Munin);
        assert_eq!("HOARDER".parse::<Preset>().unwrap(), Preset::Hoarder);
        assert!("other".parse::<Preset>().is_err());
    }

    #[test]
    fn finest_step_and_retention() {
        // Hoarder keeps every 10 seconds, for a day; the longest archive is 10 years of days
        assert_eq!(Preset::Hoarder.finest_step_seconds(), 10);
        assert_eq!(Preset::Hoarder.retention_seconds(), 3650 * 86400);
        // Munin has no archive finer than 5 minutes
        assert_eq!(Preset::Munin.finest_step_seconds(), 300);
        assert_eq!(Preset::Munin.retention_seconds(), 797 * 86400);
    }

    #[test]
    fn every_step_divides_the_next_one() {
        // The freshest rows are fetched from a finer archive after the last row of a coarser
        // one, which is only seamless if the coarser steps are multiples of the finer ones
        for preset in [Preset::Munin, Preset::Hoarder] {
            let mut steps: Vec<i64> = preset
                .get_round_robin_archives()
                .iter()
                .map(|archive| archive.steps)
                .collect();
            steps.sort_unstable();
            for pair in steps.windows(2) {
                assert_eq!(pair[1] % pair[0], 0, "{preset:?}: {pair:?}");
            }
        }
    }
}
