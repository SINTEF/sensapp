use std::fmt;

#[derive(Debug, Clone)]
pub struct Unit {
    pub name: String,
    pub description: Option<String>,
}

impl Unit {
    pub fn new(name: String, description: Option<String>) -> Self {
        Unit { name, description }
    }
}

// Implement display for Unit
impl fmt::Display for Unit {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_unit_display() {
        let unit = Unit {
            name: "test".to_string(),
            description: None,
        };
        assert_eq!(format!("{}", unit), "test");
    }
}
