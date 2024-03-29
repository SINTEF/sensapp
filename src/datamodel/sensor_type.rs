use std::{
    hash::Hash,
    io::{self, Write},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
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

// Implement to_string() for SensorType
impl ToString for SensorType {
    fn to_string(&self) -> String {
        match self {
            SensorType::Integer => "Integer".to_string(),
            SensorType::Numeric => "Numeric".to_string(),
            SensorType::Float => "Float".to_string(),
            SensorType::String => "String".to_string(),
            SensorType::Boolean => "Boolean".to_string(),
            SensorType::Location => "Location".to_string(),
            SensorType::Json => "JSON".to_string(),
            SensorType::Blob => "Blob".to_string(),
        }
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
