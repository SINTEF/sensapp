//! The connection string: `bigquery://[key.json]?project_id=P&dataset_id=D[&location=L][&max_bytes_billed=N]`.
//!
//! Without a key file the client uses the Application Default Credentials: the
//! `GOOGLE_APPLICATION_CREDENTIALS` file, what `gcloud auth application-default login` stored, or the
//! metadata server of Google Cloud.

use crate::storage::StorageError;
use anyhow::{Result, bail};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectionInfo {
    /// Service account key file, `None` for the Application Default Credentials
    pub credentials_file: Option<String>,
    pub project_id: String,
    pub dataset_id: String,
    /// Where the dataset is created, and where the queries run when it is given
    pub location: Option<String>,
    /// Queries that would bill more bytes than this fail without being charged
    pub max_bytes_billed: Option<i64>,
}

/// Project ids are lowercase letters, digits and hyphens (domain-scoped ones also have `.` and `:`).
/// What matters here is that the id can sit inside backticks in a statement, so no quote, backtick,
/// space or other punctuation.
pub fn validate_project_id(project_id: &str) -> Result<()> {
    let valid = !project_id.is_empty()
        && project_id.len() <= 128
        && project_id
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.' | ':'));
    if !valid {
        return Err(StorageError::Configuration(format!(
            "invalid BigQuery project_id '{project_id}': letters, digits, '-', '_', '.' and ':' only"
        ))
        .into());
    }
    Ok(())
}

/// Dataset ids are letters, digits and underscores, up to 1024 characters.
pub fn validate_dataset_id(dataset_id: &str) -> Result<()> {
    let valid = !dataset_id.is_empty()
        && dataset_id.len() <= 1024
        && dataset_id
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_');
    if !valid {
        return Err(StorageError::Configuration(format!(
            "invalid BigQuery dataset_id '{dataset_id}': letters, digits and '_' only"
        ))
        .into());
    }
    Ok(())
}

pub fn parse_connection_string(connection_string: &str) -> Result<ConnectionInfo> {
    let Some(rest) = connection_string.strip_prefix("bigquery:") else {
        bail!("Invalid scheme in connection string, expected bigquery://");
    };
    // `bigquery://key.json`, `bigquery:///absolute/key.json`, `bigquery://?...` (no key file)
    let rest = rest.strip_prefix("//").unwrap_or(rest);
    let (path, query) = rest.split_once('?').unwrap_or((rest, ""));

    let credentials_file = if path.is_empty() {
        None
    } else {
        Some(urlencoding::decode(path)?.into_owned())
    };

    let mut project_id = None;
    let mut dataset_id = None;
    let mut location = None;
    let mut max_bytes_billed = None;
    for (key, value) in url::form_urlencoded::parse(query.as_bytes()) {
        match key.as_ref() {
            "project_id" => project_id = Some(value.into_owned()),
            "dataset_id" => dataset_id = Some(value.into_owned()),
            "location" => location = Some(value.into_owned()),
            "max_bytes_billed" => {
                let bytes: i64 =
                    value
                        .parse()
                        .ok()
                        .filter(|bytes| *bytes > 0)
                        .ok_or_else(|| {
                            StorageError::Configuration(format!(
                                "invalid max_bytes_billed '{value}': a number of bytes above 0"
                            ))
                        })?;
                max_bytes_billed = Some(bytes);
            }
            other => bail!(
                "Unknown parameter '{other}' in the BigQuery connection string \
                 (known: project_id, dataset_id, location, max_bytes_billed)"
            ),
        }
    }

    let project_id = project_id
        .filter(|id| !id.is_empty())
        .ok_or_else(|| StorageError::Configuration("project_id is required".to_string()))?;
    let dataset_id = dataset_id
        .filter(|id| !id.is_empty())
        .ok_or_else(|| StorageError::Configuration("dataset_id is required".to_string()))?;
    validate_project_id(&project_id)?;
    validate_dataset_id(&dataset_id)?;
    if let Some(location) = &location
        && (location.is_empty()
            || !location
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '-'))
    {
        return Err(StorageError::Configuration(format!("invalid location '{location}'")).into());
    }

    Ok(ConnectionInfo {
        credentials_file,
        project_id,
        dataset_id,
        location,
        max_bytes_billed,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_a_key_file_and_the_ids() {
        let info = parse_connection_string(
            "bigquery://key.json?project_id=my-project-123&dataset_id=sensapp_test",
        )
        .unwrap();
        assert_eq!(info.credentials_file.as_deref(), Some("key.json"));
        assert_eq!(info.project_id, "my-project-123");
        assert_eq!(info.dataset_id, "sensapp_test");
        assert_eq!(info.location, None);
        assert_eq!(info.max_bytes_billed, None);
    }

    #[test]
    fn absolute_and_encoded_key_paths() {
        let info =
            parse_connection_string("bigquery:///home/me/my%20key.json?project_id=p&dataset_id=d")
                .unwrap();
        assert_eq!(
            info.credentials_file.as_deref(),
            Some("/home/me/my key.json")
        );
    }

    #[test]
    fn no_key_file_means_application_default_credentials() {
        for url in [
            "bigquery://?project_id=p&dataset_id=d",
            "bigquery:?project_id=p&dataset_id=d",
        ] {
            assert_eq!(parse_connection_string(url).unwrap().credentials_file, None);
        }
    }

    #[test]
    fn location_and_cost_cap() {
        let info = parse_connection_string(
            "bigquery://?project_id=p&dataset_id=d&location=europe-north1&max_bytes_billed=1000000000",
        )
        .unwrap();
        assert_eq!(info.location.as_deref(), Some("europe-north1"));
        assert_eq!(info.max_bytes_billed, Some(1_000_000_000));
    }

    #[test]
    fn missing_or_unknown_parameters_are_refused() {
        assert!(parse_connection_string("bigquery://key.json?dataset_id=d").is_err());
        assert!(parse_connection_string("bigquery://key.json?project_id=p").is_err());
        assert!(parse_connection_string("bigquery://?project_id=p&dataset_id=d&datset=x").is_err());
        assert!(parse_connection_string("postgres://?project_id=p&dataset_id=d").is_err());
        assert!(
            parse_connection_string("bigquery://?project_id=p&dataset_id=d&max_bytes_billed=0")
                .is_err()
        );
    }

    #[test]
    fn identifiers_that_could_leave_the_backticks_are_refused() {
        for bad in ["p`; DROP", "p q", "p'", "", "p/../x"] {
            assert!(validate_project_id(bad).is_err(), "{bad}");
        }
        for bad in ["d`x", "d-x", "d.x", "", "d x"] {
            assert!(validate_dataset_id(bad).is_err(), "{bad}");
        }
        assert!(validate_project_id("example.com:my-project").is_ok());
        assert!(validate_dataset_id("sensapp_2026").is_ok());
        assert!(
            parse_connection_string("bigquery://?project_id=p&dataset_id=d%60%3B").is_err(),
            "the encoded backtick is decoded before the check"
        );
    }
}
