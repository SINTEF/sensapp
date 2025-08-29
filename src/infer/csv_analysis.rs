use super::columns::InferedColumn;
use super::geo_guesser::LatLonColumnNames;

/// Represents the detected structure of a CSV file
#[derive(Debug, Clone, PartialEq)]
pub enum CsvStructure {
    /// Wide format: each column represents a different sensor
    /// Example: timestamp, sensor1, sensor2, sensor3
    Wide,
    /// Long format: rows contain sensor identifier and single value
    /// Example: timestamp, sensor_id, value
    Long,
    /// Single sensor format: timestamp and value columns only
    /// Example: timestamp, value
    SingleSensor,
}

/// Information about detected CSV headers
#[derive(Debug, Clone, PartialEq)]
pub struct HeaderInfo {
    /// Whether headers were detected
    pub has_headers: bool,
    /// The detected header row (if any)
    pub headers: Vec<String>,
    /// Confidence score for header detection (0.0-1.0)
    pub confidence: f32,
}

/// Complete analysis of CSV structure and content
#[derive(Debug, Clone)]
pub struct CsvAnalysis {
    /// CSV structure type
    pub structure: CsvStructure,
    /// Column type inference results
    pub inferred_columns: Vec<InferedColumn>,
    /// Detected geographic coordinate columns
    pub geo_columns: Option<LatLonColumnNames>,
    /// Likely datetime column index
    pub datetime_column: Option<usize>,
    /// Likely sensor ID column (for long format)
    pub sensor_id_column: Option<usize>,
    /// Likely value column (for long format)
    pub value_column: Option<usize>,
    /// Likely unit column (for long format)
    pub unit_column: Option<usize>,
}

impl CsvAnalysis {
    // Implementation methods are in create_analysis_with_existing_headers function in csv.rs
}
