pub mod arrow;
pub mod csv;
pub mod senml;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct IngestionStats {
    pub series: usize,
    pub samples: usize,
    /// Cells that held no reading (a null, a NaN, an empty or `NA` CSV cell) and were skipped:
    /// they are not in `samples`
    pub skipped: usize,
}

impl IngestionStats {
    pub const fn new(series: usize, samples: usize) -> Self {
        Self {
            series,
            samples,
            skipped: 0,
        }
    }

    pub const fn with_skipped(mut self, skipped: usize) -> Self {
        self.skipped = skipped;
        self
    }
}
