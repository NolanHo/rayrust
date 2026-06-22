//! Serialization bridge between Rust and Ray C++ SDK.
//!
//! Ray C++ SDK uses msgpack for serialization. We use `rmp-serde` which
//! produces compatible msgpack output.
//!
//! For cross-language (Python) calls, Ray wraps data with a 9-byte XLANG
//! header: [msgpack_int(data_len)] [zero_padding to 9 bytes] [raw data].
//! We need to strip this header before deserializing.

use serde::Serialize;

use crate::error::RayError;

/// XLANG header length (matches Ray's XLANG_HEADER_LEN).
const XLANG_HEADER_LEN: usize = 9;

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

/// Deserialize a value from cross-language (Python) result bytes.
///
/// Python task results are wrapped with a 9-byte XLANG header:
///   [msgpack_int(data_len)] [zero_padding to 9 bytes] [raw msgpack data]
///
/// This function strips the header and deserializes the raw msgpack data.
pub fn deserialize_xlang<T: serde::de::DeserializeOwned>(data: &[u8]) -> Result<T, RayError> {
    if data.len() <= XLANG_HEADER_LEN {
        return Err(RayError::Serialization(format!(
            "xlang result too short: {} bytes (need > {})",
            data.len(),
            XLANG_HEADER_LEN
        )));
    }

    // The first bytes are a msgpack-encoded integer (the data length),
    // followed by zero-padding to reach XLANG_HEADER_LEN.
    // The actual msgpack data starts at offset XLANG_HEADER_LEN.
    let raw_data = &data[XLANG_HEADER_LEN..];
    deserialize(raw_data)
}
