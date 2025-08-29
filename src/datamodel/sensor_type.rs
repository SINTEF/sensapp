use serde::{Deserialize, Serialize};
use std::{
    fmt,
    hash::Hash,
    io::{self, Write},
    str::FromStr,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SensorType {
    Integer = 1,
    Numeric = 20,
    Float = 30,
    String = 40,
    Boolean = 50,
    Location = 60,
    Json = 70,
    Blob = 80,
}

// Implement Display for SensorType (preferred over ToString)
impl fmt::Display for SensorType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            SensorType::Integer => "Integer",
            SensorType::Numeric => "Numeric",
            SensorType::Float => "Float",
            SensorType::String => "String",
            SensorType::Boolean => "Boolean",
            SensorType::Location => "Location",
            SensorType::Json => "JSON",
            SensorType::Blob => "Blob",
        };
        write!(f, "{}", s)
    }
}

impl SensorType {
    fn to_u8(self) -> u8 {
        self as u8
    }

    pub fn write_to<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        let v = self.to_u8().to_le_bytes();
        writer.write_all(&v)
    }
}

impl FromStr for SensorType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "integer" => Ok(SensorType::Integer),
            "numeric" => Ok(SensorType::Numeric),
            "float" => Ok(SensorType::Float),
            "string" => Ok(SensorType::String),
            "boolean" => Ok(SensorType::Boolean),
            "location" => Ok(SensorType::Location),
            "json" => Ok(SensorType::Json),
            "blob" => Ok(SensorType::Blob),
            _ => Err(format!("Unknown sensor type: {}", s)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sensor_type_to_string() {
        assert_eq!(SensorType::Integer.to_string(), "Integer");
        assert_eq!(SensorType::Numeric.to_string(), "Numeric");
        assert_eq!(SensorType::Float.to_string(), "Float");
        assert_eq!(SensorType::String.to_string(), "String");
        assert_eq!(SensorType::Boolean.to_string(), "Boolean");
        assert_eq!(SensorType::Location.to_string(), "Location");
        assert_eq!(SensorType::Json.to_string(), "JSON");
        assert_eq!(SensorType::Blob.to_string(), "Blob");
    }

    #[test]
    fn test_sensor_type_to_u8() {
        assert_eq!(SensorType::Integer.to_u8(), 1);
        assert_eq!(SensorType::Numeric.to_u8(), 20);
        assert_eq!(SensorType::Float.to_u8(), 30);
        assert_eq!(SensorType::String.to_u8(), 40);
        assert_eq!(SensorType::Boolean.to_u8(), 50);
        assert_eq!(SensorType::Location.to_u8(), 60);
        assert_eq!(SensorType::Json.to_u8(), 70);
        assert_eq!(SensorType::Blob.to_u8(), 80);
    }

    #[test]
    fn test_sensor_type_write_to() {
        let mut buf = Vec::new();
        SensorType::Integer.write_to(&mut buf).unwrap();
        assert_eq!(buf, [1]);
        buf.clear();
    }
}
