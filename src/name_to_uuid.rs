use crate::config;
use anyhow::Error;
use cached::proc_macro::cached;
use once_cell::sync::OnceCell;
use std::sync::Arc;
use uuid::Uuid;

type NameToUuidKey = [u8; 32];
static UUID_KEY: OnceCell<Arc<NameToUuidKey>> = OnceCell::new();

fn initialise_uuid_key() -> Result<Arc<[u8; 32]>, Error> {
    const KEY_CONTEXT: &str = "SENSAPP uuid key 2024-01-19 strings to unique ids";
    let salt = config::get()?.sensor_salt.clone();
    let key = blake3::derive_key(KEY_CONTEXT, salt.as_bytes());

    Ok(Arc::new(key))
}

#[cached(
    sync_writes = true,
    size = 1024,
    result = true,
    key = "Vec<u8>",
    convert = r#"{ uuid_buffer.clone() }"#
)]
pub fn uuid_v8_blake3(name: &str, uuid_buffer: Vec<u8>) -> Result<Uuid, Error> {
    // Using a UUID v5 (SHA1) or v3 (MD5) is too easy to implement.
    // It's friday, let's take terrible decisions and use Blake3 instead.

    let key = UUID_KEY.get_or_try_init(initialise_uuid_key)?;

    // Hash the sensor name only to get a 32-bits beginning
    let mut hash_name_output = [0; 4];
    let mut hasher_name = blake3::Hasher::new_keyed(key);
    hasher_name.update(name.as_bytes());
    hasher_name.finalize_xof().fill(&mut hash_name_output);

    let mut hash_everything_output = [0; 12];
    let mut hasher_everything = blake3::Hasher::new_keyed(key);
    hasher_everything.update(&uuid_buffer);
    hasher_everything
        .finalize_xof()
        .fill(&mut hash_everything_output);

    // Create a buffer with the name hash and the uuid buffer
    let mut uuid_bytes = [0; 16];
    uuid_bytes[..4].copy_from_slice(&hash_name_output);
    uuid_bytes[4..].copy_from_slice(&hash_everything_output);

    Ok(uuid::Builder::from_custom_bytes(uuid_bytes).into_uuid())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::load_configuration;

    #[test]
    fn test_initialise_uuid_key() {
        _ = load_configuration();
        let result = initialise_uuid_key();
        assert!(result.is_ok());
    }

    #[test]
    fn test_uuid_v8_blake3() {
        _ = load_configuration();
        let name = "TestSensor";
        let uuid_buffer = Vec::from("test");
        let uuid1 = uuid_v8_blake3(name, uuid_buffer.clone()).unwrap();
        let uuid2 = uuid_v8_blake3(name, uuid_buffer).unwrap();
        assert_eq!(uuid1, uuid2); // Should be the same for the same input

        let uuid_buffer = Vec::from("another test");
        let different_uuid = uuid_v8_blake3(name, uuid_buffer).unwrap();
        assert_ne!(uuid1, different_uuid); // Different input should produce different UUID
    }

    #[test]
    fn test_uuid_v8_blake3_more() {
        _ = load_configuration();

        let uuid_buffer_a = Vec::from("test _a");
        let uuid_buffer_b = Vec::from("test very different");

        let uuid = uuid_v8_blake3("test", uuid_buffer_a).unwrap();
        assert_eq!(uuid.to_string(), "b46bd9dc-588e-83a1-8f85-b7a09d8f033b");
        let uuid = uuid_v8_blake3("test", uuid_buffer_b).unwrap();
        assert_eq!(uuid.to_string(), "b46bd9dc-cef5-8df8-8416-0fe6c1c9e5e1");
        // starts with b46bd9dc in both cases

        let uuid = uuid_v8_blake3("not a test", Vec::from("not a test _a")).unwrap();
        assert_eq!(uuid.to_string(), "ac8e016e-79f8-8cd4-bc0e-093e4f7b9f18");
        // does not start with b46bd9dc
    }
}
