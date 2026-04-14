pub mod arrow;
pub mod csv;
pub mod senml;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct IngestionStats {
    pub series: usize,
    pub samples: usize,
}

impl IngestionStats {
    pub const fn new(series: usize, samples: usize) -> Self {
        Self { series, samples }
    }
}
