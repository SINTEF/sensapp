use anyhow::{Context, Result, bail};
use url::Url;

/// What a `clickhouse://` or `clickhouses://` connection string says.
///
/// The string is a regular URL: `clickhouse[s]://[user[:password]@]host[:port][/database]`.
/// The user, the password and the database are percent-decoded, so a generated password
/// containing `@`, `/`, `:`, `?` or `#` must be percent-encoded (`@` is `%40`, `/` is `%2F`).
#[derive(Debug, PartialEq, Eq)]
pub(super) struct ConnectionParams {
    /// `http://host:port` or `https://host:port`
    pub endpoint_url: String,
    pub user: String,
    pub password: Option<String>,
    pub database: Option<String>,
}

impl ConnectionParams {
    pub fn parse(connection_string: &str) -> Result<Self> {
        let secure = if connection_string.starts_with("clickhouses://") {
            true
        } else if connection_string.starts_with("clickhouse://") {
            false
        } else {
            bail!(
                "ClickHouse connection string must start with 'clickhouse://' or 'clickhouses://'"
            );
        };

        // The error of the url crate never contains the input, so no password can leak.
        let url = Url::parse(connection_string)
            .context("Invalid ClickHouse connection string (is the password percent-encoded?)")?;
        let host = url
            .host_str()
            .filter(|host| !host.is_empty())
            .context("ClickHouse connection string has no host")?;
        let port = url.port().unwrap_or(if secure { 8443 } else { 8123 });

        let decode = |value: &str| -> Result<String> {
            Ok(urlencoding::decode(value)
                .context("ClickHouse connection string is not valid UTF-8 once decoded")?
                .into_owned())
        };

        let user = match url.username() {
            "" => "default".to_string(),
            user => decode(user)?,
        };
        let password = url.password().map(decode).transpose()?;
        let database = match url.path().trim_start_matches('/') {
            "" => None,
            database => Some(decode(database)?),
        };

        Ok(Self {
            endpoint_url: format!("{}://{host}:{port}", if secure { "https" } else { "http" }),
            user,
            password,
            database,
        })
    }
}

/// Quote a database name for use in a statement: names such as `sensapp-prod` are not valid
/// bare identifiers.
pub(super) fn quote_identifier(name: &str) -> String {
    format!("`{}`", name.replace('\\', "\\\\").replace('`', "\\`"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(connection_string: &str) -> ConnectionParams {
        ConnectionParams::parse(connection_string).unwrap()
    }

    #[test]
    fn plain_and_secure_defaults() {
        let plain = parse("clickhouse://reader:secret@localhost/sensapp");
        assert_eq!(plain.endpoint_url, "http://localhost:8123");
        assert_eq!(plain.user, "reader");
        assert_eq!(plain.password.as_deref(), Some("secret"));
        assert_eq!(plain.database.as_deref(), Some("sensapp"));

        let secure = parse("clickhouses://reader:secret@example.com/sensapp");
        assert_eq!(secure.endpoint_url, "https://example.com:8443");
        assert_eq!(
            parse("clickhouses://reader:secret@example.com:9443/sensapp").endpoint_url,
            "https://example.com:9443"
        );
    }

    #[test]
    fn credentials_and_database_are_optional() {
        let bare = parse("clickhouse://localhost:8123");
        assert_eq!(bare.user, "default");
        assert_eq!(bare.password, None);
        assert_eq!(bare.database, None);
        assert_eq!(parse("clickhouse://localhost/").database, None);
        assert_eq!(parse("clickhouse://alice@localhost/db").password, None);
    }

    #[test]
    fn credentials_are_percent_decoded() {
        let params = parse("clickhouse://us%40er:p%40ss%3Aw%2Frd%23%3F@localhost/db");
        assert_eq!(params.user, "us@er");
        assert_eq!(params.password.as_deref(), Some("p@ss:w/rd#?"));
    }

    #[test]
    fn unencoded_at_and_colon_in_the_password_still_work() {
        let params = parse("clickhouse://alice:p@ss:word@localhost/db");
        assert_eq!(params.user, "alice");
        assert_eq!(params.password.as_deref(), Some("p@ss:word"));
        assert_eq!(params.endpoint_url, "http://localhost:8123");
    }

    #[test]
    fn database_names_may_contain_hyphens_and_encoded_characters() {
        assert_eq!(
            parse("clickhouse://localhost/sensapp-prod")
                .database
                .as_deref(),
            Some("sensapp-prod")
        );
        assert_eq!(
            parse("clickhouse://localhost/my%20db").database.as_deref(),
            Some("my db")
        );
    }

    #[test]
    fn ipv6_hosts_keep_their_brackets() {
        assert_eq!(
            parse("clickhouse://[::1]:8123/db").endpoint_url,
            "http://[::1]:8123"
        );
    }

    #[test]
    fn invalid_strings_are_rejected_without_leaking_the_password() {
        for bad in [
            "postgres://localhost/db",
            "clickhouse://",
            "clickhouse://user:s3cr3t-password@localhost:notaport/db",
        ] {
            let error = format!("{:#}", ConnectionParams::parse(bad).unwrap_err());
            assert!(!error.contains("s3cr3t-password"), "{error}");
        }
    }

    #[test]
    fn identifiers_are_quoted() {
        assert_eq!(quote_identifier("sensapp-prod"), "`sensapp-prod`");
        assert_eq!(quote_identifier("a`b"), "`a\\`b`");
        assert_eq!(quote_identifier("a\\b"), "`a\\\\b`");
    }
}
