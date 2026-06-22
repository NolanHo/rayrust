//! Serialization bridge between Rust and Ray C++ SDK.
//!
//! Ray C++ SDK uses msgpack for serialization. We use `rmp-serde` which
//! produces compatible msgpack output.
//!
//! Note: The C++ SDK's `Serializer::Serialize<T>` calls `msgpack::pack(buffer, t)`
//! which serializes T directly (no wrapping). We mirror this with `rmp-serde`
//! using the same direct serialization.

use serde::Serialize;

use crate::error::RayError;

/// Serialize a value to msgpack bytes.
pub fn serialize<T: Serialize>(value: &T) -> Result<Vec<u8>, RayError> {
    let mut buf = Vec::new();
    value.serialize(&mut rmp_serde::Serializer::new(&mut buf))?;
    Ok(buf)
}

/// Deserialize a value from msgpack bytes.
pub fn deserialize<T: serde::de::DeserializeOwned>(data: &[u8]) -> Result<T, RayError> {
    let mut de = rmp_serde::Deserializer::from_read_ref(data);
    T::deserialize(&mut de).map_err(RayError::from)
}
