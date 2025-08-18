use nom::AsChar;
use nom::{
    IResult, Parser,
    character::complete::{char, satisfy},
    combinator::{map, map_res},
    multi::count,
};
use uuid::Uuid;

#[inline]
fn hex_digit_char(c: char) -> bool {
    c.is_hex_digit()
}

fn parse_hex_char(data: &str) -> IResult<&str, u8> {
    map_res(satisfy(hex_digit_char), |s: char| match s.to_digit(16) {
        Some(d) => Ok(d as u8),
        None => Err("Invalid hex digit"),
    })
    .parse(data)
}

pub fn parse_uuid(data: &str) -> IResult<&str, uuid::Uuid> {
    map(
        (
            count(parse_hex_char, 8),
            char('-'),
            count(parse_hex_char, 4),
            char('-'),
            count(parse_hex_char, 4),
            char('-'),
            count(parse_hex_char, 4),
            char('-'),
            count(parse_hex_char, 12),
        ),
        |(a, _, b, _, c, _, d, _, e)| {
            let bytes = [
                (a[0] << 4) | a[1],
                (a[2] << 4) | a[3],
                (a[4] << 4) | a[5],
                (a[6] << 4) | a[7],
                (b[0] << 4) | b[1],
                (b[2] << 4) | b[3],
                (c[0] << 4) | c[1],
                (c[2] << 4) | c[3],
                (d[0] << 4) | d[1],
                (d[2] << 4) | d[3],
                (e[0] << 4) | e[1],
                (e[2] << 4) | e[3],
                (e[4] << 4) | e[5],
                (e[6] << 4) | e[7],
                (e[8] << 4) | e[9],
                (e[10] << 4) | e[11],
            ];
            Uuid::from_bytes(bytes)
        },
    )
    .parse(data)
}

pub fn attempt_uuid_parsing(s: &str) -> Option<uuid::Uuid> {
    match parse_uuid(s) {
        Ok((_, uuid)) => Some(uuid),
        Err(_) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_uuid() {
        let uuid = parse_uuid("01234567-89ab-cdef-0123-456789abcdef")
            .unwrap()
            .1;
        assert_eq!(uuid.to_string(), "01234567-89ab-cdef-0123-456789abcdef");
    }

    #[test]
    fn test_attempt_uuid_parsing() {
        let uuid = attempt_uuid_parsing("01234567-89ab-cdef-0123-456789abcdef").unwrap();
        assert_eq!(uuid.to_string(), "01234567-89ab-cdef-0123-456789abcdef");
        let uuid = attempt_uuid_parsing("01234567-89AB-CDEF-0123-456789ABCDEF").unwrap();
        assert_eq!(uuid.to_string(), "01234567-89ab-cdef-0123-456789abcdef");
        let uuid = attempt_uuid_parsing("aa6e8b8f-5b0b-5b7a-8c4d-2b9f1c1b1b1b").unwrap();
        assert_eq!(uuid.to_string(), "aa6e8b8f-5b0b-5b7a-8c4d-2b9f1c1b1b1b");
    }

    #[test]
    fn test_attempt_uuid_parsing_invalid() {
        let uuid = attempt_uuid_parsing("01234567-89ab-cdef-0123-456789abcde");
        assert!(uuid.is_none());
        let uuid = attempt_uuid_parsing("01234567-89ab-cdef-0123-456789abcdeg");
        assert!(uuid.is_none());
        let uuid = attempt_uuid_parsing("");
        assert!(uuid.is_none());
        let uuid = attempt_uuid_parsing("auniestau");
        assert!(uuid.is_none());
    }
}
