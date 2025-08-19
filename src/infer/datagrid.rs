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

    pub fn detect_header(rows: Vec<Vec<String>>) -> Result<Self, Error> {
        let column_names = rows
            .first()
            .ok_or_else(|| anyhow::anyhow!("Cannot detect header: CSV data grid contains no rows"))?
            .clone();
        let rows = rows[1..].to_vec();
        Self::new(column_names, rows)
    }
}
