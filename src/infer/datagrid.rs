use anyhow::{Error, bail};

pub struct StringDataGrid {
    pub column_names: Vec<String>,
    pub rows: Vec<Vec<String>>,
}

impl StringDataGrid {
    pub fn new(column_names: Vec<String>, rows: Vec<Vec<String>>) -> Result<Self, Error> {
        // Check that all rows have the same length
        let row_length = column_names.len();
        if !rows.iter().all(|row| row.len() == row_length) {
            bail!("All rows must have the same length");
        }
        Ok(Self { column_names, rows })
    }

    /// Check if the data grid is empty (no columns or no rows)
    pub fn is_empty(&self) -> bool {
        self.column_names.is_empty() || self.rows.is_empty()
    }

    // Additional methods available but unused in current implementation
}
