use super::parsing::{infer_type, InferedValue};

pub fn is_header(cells: &[String]) -> bool {
    if cells.is_empty() {
        return false;
    }
    // Infer the type of every cell
    cells.iter().map(|cell| infer_type(cell)).all(|result| {
        if let Ok((_, infered_value)) = result {
            // matches!(infered_value, InferedValue::String(_))
            match infered_value {
                InferedValue::String(string_value) => !string_value.is_empty(),
                _ => false,
            }
        } else {
            unreachable!("Error while inferring type");
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_header() {
        assert!(is_header(&["name".to_string(), "value".to_string()]));
        assert!(!is_header(&[
            "name".to_string(),
            "value".to_string(),
            "3".to_string()
        ]));
        assert!(!is_header(&[
            "name".to_string(),
            "value".to_string(),
            "".to_string()
        ]));

        assert!(!is_header(&[]));
    }
}
