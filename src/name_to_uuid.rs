use crate::infer::uuid::attempt_uuid_parsing;
use anyhow::{anyhow, Error};
use once_cell::sync::OnceCell;
use std::sync::Arc;
use uuid::Uuid;

type NameToUuidKey = [u8; 32];
static UUID_HASH_MAC: OnceCell<Arc<NameToUuidKey>> = OnceCell::new();

pub fn initialise_uuid_hash_mac(salt: &str) -> Result<(), Error> {
    const KEY_CONTEXT: &str = "SENSAPP uuid hash mac 2024-01-19 strings to unique ids";
    let key = blake3::derive_key(KEY_CONTEXT, salt.as_bytes());

    match UUID_HASH_MAC.set(Arc::new(key)) {
        Ok(_) => Ok(()),
        Err(e) => Err(anyhow!("Failed to set UUID_HASH_MAC: {:?}", e)),
    }
}

pub fn uuid_v8_blake3(name: &str) -> Result<Uuid, Error> {
    // uuid::Uuid::from_bytes(uuid::v5::NAMESPACE_DNS, name.as_bytes())
    // Using a UUID v5 (SHA1) or v3 (MD5) is too easy to implement.
    // It's friday, let's take terrible decisions and use Blake3 instead.

    let key = UUID_HASH_MAC.get().ok_or_else(|| {
        anyhow!("UUID_HASH_MAC not initialised. Please call initialise_uuid_hash_mac() before using name_to_uuid()")
    })?;

    // Create the random bytes
    let mut hash_output = [0; 16];
    let mut hasher = blake3::Hasher::new_keyed(key);
    hasher.update(name.as_bytes());
    hasher.finalize_xof().fill(&mut hash_output);

    Ok(uuid::Builder::from_custom_bytes(hash_output).into_uuid())
}

pub fn name_to_uuid(name: &str) -> Result<Uuid, Error> {
    match attempt_uuid_parsing(name) {
        Some(uuid) => Ok(uuid),
        None => uuid_v8_blake3(name),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_uuid_v8_blake3() {
        let _ = initialise_uuid_hash_mac("sensapp tests");

        let uuid = uuid_v8_blake3("test").unwrap();
        assert_eq!(uuid.to_string(), "a2794553-385f-8d6c-9d2f-843cf728307a");

        let uuid = uuid_v8_blake3("test2").unwrap();
        assert_eq!(uuid.to_string(), "daa4b5f3-70b5-820f-819b-787344e7a4c7");

        // This is case sensitive
        let uuid = uuid_v8_blake3("TEST").unwrap();
        assert_eq!(uuid.to_string(), "6aa50a6c-9f4f-899f-9f24-93efacb0c9e5");

        let uuid = uuid_v8_blake3("").unwrap();
        assert_eq!(uuid.to_string(), "58748fa2-0c24-86b3-925b-59e65e916af0");

        // Giving an UUID will return another UUID
        let uuid = uuid_v8_blake3("aa6e8b8f0-5b0b-5b7a-8c4d-2b9f1c1b1b1b").unwrap();
        assert_eq!(uuid.to_string(), "d90a33ab-0e7e-8e19-99ab-847c5399884a");

        // Already initialised
        let is_err = initialise_uuid_hash_mac("sensapp tests 2");
        assert!(is_err.is_err());
    }

    #[test]
    fn test_name_to_uuid() {
        let _ = initialise_uuid_hash_mac("sensapp tests");
        let uuid = name_to_uuid("test").unwrap();
        assert_eq!(uuid.to_string(), "a2794553-385f-8d6c-9d2f-843cf728307a");

        let uuid = name_to_uuid("test2").unwrap();
        assert_eq!(uuid.to_string(), "daa4b5f3-70b5-820f-819b-787344e7a4c7");

        let uuid = name_to_uuid("").unwrap();
        assert_eq!(uuid.to_string(), "58748fa2-0c24-86b3-925b-59e65e916af0");

        // Giving an UUID will return the same UUID
        let uuid = name_to_uuid("aa6e8b8f-5b0b-5b7a-8c4d-2b9f1c1b1b1b").unwrap();
        assert_eq!(uuid.to_string(), "aa6e8b8f-5b0b-5b7a-8c4d-2b9f1c1b1b1b");

        // This is not case sensitive
        let uuid = name_to_uuid("AA6E8B8F-5b0b-5B7A-8c4d-2B9F1C1B1B1B").unwrap();
        assert_eq!(uuid.to_string(), "aa6e8b8f-5b0b-5b7a-8c4d-2b9f1c1b1b1b");

        // If it's not a valid UUID, even it's almost, it will return a new UUID
        // This may be a bit confusing to the users
        // But I'm not sure that trying to detect almost UUIDs is sound.
        let uuid = name_to_uuid("aa6e8b8f0-5b0b-5b7a-8c4d-2b9f1c1b1b1G").unwrap();
        assert_eq!(uuid.to_string(), "48dfa368-01bd-8d5a-892e-1e009653c92b");
    }
}
