/**
 * A list cursor is a string that contains two values:
 *  - the last created_at value,
 *  - the last uuid value.
 *
 * It is used to paginate the list of sensors.
 *
 * It is NOT a secret. But it doesn't have to be human readable.
 *
 * For simplicity, we use use a string serialisation, and the
 * ASCII UNIT SEPARATOR (US) character to separate the two values.
 * The string is then rot13 and base64+url encoded. Rot13 for fun.
 */
use anyhow::{bail, Result};

#[derive(Debug)]
pub struct ListCursor {
    pub next_created_at: String,
    pub next_uuid: String,
}

impl ListCursor {
    pub fn new(next_created_at: String, next_uuid: String) -> Self {
        Self {
            next_created_at,
            next_uuid,
        }
    }

    /// Parse a list cursor from a string.
    ///
    /// The string must be in the format:
    ///  - last_created_at
    ///  - last_uuid
    ///
    /// The string is then rot13 and base64+url decoded.
    pub fn parse(cursor: &str) -> Result<Self> {
        use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
        let base64_data = rot13::rot13(cursor);
        let data = URL_SAFE_NO_PAD.decode(base64_data)?;
        let data_string = String::from_utf8(data)?;
        // split on the separator
        let parts: Vec<&str> = data_string.split('\u{001F}').collect();
        if parts.len() != 2 {
            bail!("Invalid cursor: must contain two parts");
        }
        Ok(Self {
            next_created_at: parts[0].to_string(),
            next_uuid: parts[1].to_string(),
        })
    }

    /// Convert the list cursor to a string.
    ///
    /// The string is then rot13 and base64+url encoded.
    #[allow(clippy::inherent_to_string)]
    pub fn to_string(&self) -> String {
        use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
        let data_string = format!("{}\u{001F}{}", self.next_created_at, self.next_uuid);
        let data = data_string.as_bytes();
        let base64_data = URL_SAFE_NO_PAD.encode(data);
        rot13::rot13(&base64_data)
    }
}

impl Default for ListCursor {
    fn default() -> Self {
        Self::new(
            "-1".to_string(),
            "00000000-0000-0000-0000-000000000000".to_string(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_list_cursor() {
        let cursor = ListCursor::new(
            "2023-01-01T00:00:00Z".to_string(),
            "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa".to_string(),
        );
        let string = cursor.to_string();

        assert_eq!(
            string,
            "ZwNlZl0jZF0jZIDjZQbjZQbjZSbsLJSuLJSuLJRgLJSuLF1uLJSuYJSuLJRgLJSuLJSuLJSuLJSu"
        );

        let parsed = ListCursor::parse(&string).unwrap();
        assert_eq!(parsed.next_created_at, "2023-01-01T00:00:00Z");
        assert_eq!(parsed.next_uuid, "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa");
    }

    #[test]
    fn test_list_cursor_default() {
        let cursor = ListCursor::default();
        assert_eq!(cursor.next_created_at, "-1");
        assert_eq!(cursor.next_uuid, "00000000-0000-0000-0000-000000000000");
    }

    #[test]
    fn test_parsing_failures() {
        assert!(ListCursor::parse("").is_err());
        assert!(
            ListCursor::parse("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\u{001F}")
                .is_err()
        );
        assert!(ListCursor::parse("aaa\u{001F}aa\u{001F}aa").is_err());
    }
}
