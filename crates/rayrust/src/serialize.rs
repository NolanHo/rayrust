//! Serialization bridge between Rust and Ray C++ SDK.
//!
//! Ray C++ SDK uses msgpack for serialization. We use `rmp-serde` which
//! produces compatible msgpack output.
//!
//! For cross-language (Python) calls, Ray wraps data with a 9-byte XLANG
//! header: [msgpack_int(data_len)] [zero_padding to 9 bytes] [raw data].
//! We need to strip this header before deserializing.
//!
//! For dynamic/generic deserialization (when the Rust caller doesn't know the
//! exact Python return type), use `rmpv::Value` which is msgpack's equivalent
//! of `serde_json::Value`.

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

// ─── Cross-language (XLANG) support ────────────────────────────

/// Build the 9-byte XLANG header for the given payload length.
///
/// Format: [msgpack_int(data_len)] [zero_padding to 9 bytes]
fn build_xlang_header(data_len: usize) -> [u8; XLANG_HEADER_LEN] {
    let mut header = [0u8; XLANG_HEADER_LEN];
    let mut buf = Vec::new();
    // Encode data_len as a msgpack integer.
    if data_len <= 0x7f {
        buf.push(data_len as u8); // positive fixint
    } else if data_len <= 0xff {
        buf.push(0xcc); // uint8
        buf.push(data_len as u8);
    } else if data_len <= 0xffff {
        buf.push(0xcd); // uint16
        buf.extend_from_slice(&(data_len as u16).to_be_bytes());
    } else {
        buf.push(0xce); // uint32
        buf.extend_from_slice(&(data_len as u32).to_be_bytes());
    }
    let n = buf.len().min(XLANG_HEADER_LEN);
    header[..n].copy_from_slice(&buf[..n]);
    header
}

/// Serialize a value and wrap it with the XLANG header.
///
/// Use this when Rust needs to produce data that Python will read via
/// Ray's cross-language deserialization path.
pub fn serialize_xlang<T: Serialize>(value: &T) -> Result<Vec<u8>, RayError> {
    let payload = serialize(value)?;
    let header = build_xlang_header(payload.len());
    let mut buf = Vec::with_capacity(XLANG_HEADER_LEN + payload.len());
    buf.extend_from_slice(&header);
    buf.extend_from_slice(&payload);
    Ok(buf)
}

/// Check if data starts with a valid XLANG header.
///
/// The XLANG header is: [msgpack_int(payload_len)] [padding/metadata to 9 bytes]
/// We verify by:
/// 1. Decoding the first bytes as a msgpack integer
/// 2. Checking the decoded length matches the remaining data after 9-byte header
///
/// Note: In Ray 2.51.1+, the header padding may contain metadata (not all zeros).
/// So we only check the length match, not zero-padding.
fn has_xlang_header(data: &[u8]) -> bool {
    if data.len() <= XLANG_HEADER_LEN {
        return false;
    }
    let header = &data[..XLANG_HEADER_LEN];
    let payload = &data[XLANG_HEADER_LEN..];

    // Try to decode the first bytes as a msgpack integer (the payload length)
    let payload_len = match header[0] {
        // Positive fixint (0-127)
        v if v <= 0x7f => v as usize,
        // uint8
        0xcc if header.len() >= 2 => header[1] as usize,
        // uint16
        0xcd if header.len() >= 3 => {
            u16::from_be_bytes([header[1], header[2]]) as usize
        }
        // uint32
        0xce if header.len() >= 5 => {
            u32::from_be_bytes([header[1], header[2], header[3], header[4]]) as usize
        }
        _ => return false,
    };

    // The decoded length must match the actual payload length
    payload_len == payload.len()
}

/// Deserialize a value from cross-language (Python) result bytes.
///
/// Python task results are wrapped with a 9-byte XLANG header:
///   [msgpack_int(data_len)] [zero_padding to 9 bytes] [raw msgpack data]
///
/// This function auto-detects whether the XLANG header is present.
/// If present, it strips the header and deserializes the raw msgpack data.
/// If not present (e.g. in local mode), it deserializes directly.
pub fn deserialize_xlang<T: serde::de::DeserializeOwned>(data: &[u8]) -> Result<T, RayError> {
    if has_xlang_header(data) {
        let raw_data = &data[XLANG_HEADER_LEN..];
        deserialize(raw_data)
    } else {
        // No XLANG header — deserialize directly (e.g. local mode)
        deserialize(data)
    }
}

/// Deserialize cross-language (Python) result into a generic msgpack Value.
///
/// This is useful when the caller does not know the exact type of the
/// Python return value at compile time. The returned `rmpv::Value` can
/// hold any msgpack type: nil, bool, int, float, str, bin, array, or map.
///
/// # Example
/// ```ignore
/// let obj_ref = rayrust::task_call_python("mymod", "complex_func", &args)?;
/// let val: rmpv::Value = obj_ref.get_value()?;
/// match val {
///     rmpv::Value::Array(items) => { /* Python returned a list */ }
///     rmpv::Value::Map(entries) => { /* Python returned a dict */ }
///     _ => {}
/// }
/// ```
pub fn deserialize_xlang_value(data: &[u8]) -> Result<rmpv::Value, RayError> {
    if has_xlang_header(data) {
        let mut reader = &data[XLANG_HEADER_LEN..];
        rmpv::decode::read_value(&mut reader)
            .map_err(|e| RayError::Serialization(format!("msgpack value decode error: {}", e)))
    } else {
        // No XLANG header — deserialize directly (e.g. local mode)
        let mut reader = data;
        rmpv::decode::read_value(&mut reader)
            .map_err(|e| RayError::Serialization(format!("msgpack value decode error: {}", e)))
    }
}

/// Deserialize raw msgpack bytes into a generic msgpack Value (no XLANG header).
pub fn deserialize_value(data: &[u8]) -> Result<rmpv::Value, RayError> {
    let mut reader = data;
    rmpv::decode::read_value(&mut reader)
        .map_err(|e| RayError::Serialization(format!("msgpack value decode error: {}", e)))
}

// ─── Tests ─────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    // ── Basic round-trip ──────────────────────────────────────

    #[test]
    fn test_serialize_deserialize_roundtrip() {
        let val: i64 = 42;
        let bytes = serialize(&val).unwrap();
        let back: i64 = deserialize(&bytes).unwrap();
        assert_eq!(val, back);
    }

    // ── XLANG header construction & detection ──────────────────

    #[test]
    fn test_serialize_xlang_header_format() {
        // Small payload (len <= 0x7f): first byte is the length as positive fixint
        let header = build_xlang_header(5);
        assert_eq!(header[0], 5);
        assert_eq!(header[1..], [0u8; 8]);

        // Medium payload (len <= 0xff): uint8 format
        let header = build_xlang_header(200);
        assert_eq!(header[0], 0xcc);
        assert_eq!(header[1], 200);
        assert_eq!(header[2..], [0u8; 7]);

        // Larger payload (len <= 0xffff): uint16 format
        let header = build_xlang_header(1000);
        assert_eq!(header[0], 0xcd);
        assert_eq!(header[1..3], [0x03, 0xe8]);
    }

    #[test]
    fn test_has_xlang_header() {
        // No header — raw msgpack
        let raw = serialize(&42i64).unwrap();
        assert!(!has_xlang_header(&raw));

        // With header
        let xlang = serialize_xlang(&42i64).unwrap();
        assert!(has_xlang_header(&xlang));

        // With header — larger data
        let big_data = vec![0u8; 200];
        let xlang_big = serialize_xlang(&big_data).unwrap();
        assert!(has_xlang_header(&xlang_big));

        // Empty-ish data
        assert!(!has_xlang_header(&[]));
        assert!(!has_xlang_header(&[0u8; 5]));
    }

    #[test]
    fn test_has_xlang_header_non_zero_padding() {
        // Simulate Ray 2.51.1+ header format where padding contains metadata.
        // Format: [msgpack_int(payload_len)] [arbitrary metadata bytes to fill 9] [payload]
        let payload = serialize(&42i64).unwrap(); // single byte: 0x2a
        let mut data = vec![payload.len() as u8]; // fixint = 1
        // Fill remaining 8 header bytes with non-zero "metadata" (like Ray does)
        data.extend_from_slice(&[0x81, 0x04, 0x64, 0x28, 0x7f, 0x00, 0x00, 0x40]);
        data.extend_from_slice(&payload);
        assert_eq!(data.len(), 10);
        assert!(has_xlang_header(&data));

        // Should correctly strip and deserialize
        let back: i64 = deserialize_xlang(&data).unwrap();
        assert_eq!(back, 42);
    }

    #[test]
    fn test_has_xlang_header_uint8_len() {
        // Payload > 127 bytes: header uses uint8 format (0xcc + 1 byte)
        // Use a string of 200 bytes: str8 header (2 bytes) + 200 = 202 bytes payload
        let payload_str = "x".repeat(200);
        let payload = serialize(&payload_str).unwrap();
        assert!(payload.len() > 127 && payload.len() <= 255,
            "payload len = {}, expected 128-255", payload.len());
        let xlang = serialize_xlang(&payload_str).unwrap();
        assert!(has_xlang_header(&xlang));
        let back: String = deserialize_xlang(&xlang).unwrap();
        assert_eq!(back.len(), 200);
    }

    #[test]
    fn test_has_xlang_header_uint16_len() {
        // Payload > 255 bytes: header uses uint16 format (0xcd + 2 bytes)
        // Use a string of 1000 bytes: str16 header (3 bytes) + 1000 = 1003 bytes payload
        let payload_str = "x".repeat(1000);
        let payload = serialize(&payload_str).unwrap();
        assert!(payload.len() > 255,
            "payload len = {}, expected > 255", payload.len());
        let xlang = serialize_xlang(&payload_str).unwrap();
        assert!(has_xlang_header(&xlang));
        let back: String = deserialize_xlang(&xlang).unwrap();
        assert_eq!(back.len(), 1000);
    }

    #[test]
    fn test_has_xlang_header_uint32_len() {
        // Payload > 65535 bytes: header uses uint32 format (0xce + 4 bytes)
        let big_string = "x".repeat(70000);
        let payload = serialize(&big_string).unwrap();
        assert!(payload.len() > 65535);
        let xlang = serialize_xlang(&big_string).unwrap();
        assert!(has_xlang_header(&xlang));
        let back: String = deserialize_xlang(&xlang).unwrap();
        assert_eq!(back.len(), 70000);
    }

    // ── XLANG round-trip: all primitive types ──────────────────

    #[test]
    fn test_serialize_xlang_roundtrip() {
        let val = vec![1i64, 2, 3, 4, 5];
        let xlang_bytes = serialize_xlang(&val).unwrap();
        let payload_len = serialize(&val).unwrap().len();
        assert_eq!(xlang_bytes.len(), XLANG_HEADER_LEN + payload_len);
        let back: Vec<i64> = deserialize_xlang(&xlang_bytes).unwrap();
        assert_eq!(val, back);
    }

    #[test]
    fn test_serialize_xlang_string() {
        let val = "hello".to_string();
        let xlang_bytes = serialize_xlang(&val).unwrap();
        let back: String = deserialize_xlang(&xlang_bytes).unwrap();
        assert_eq!(val, back);
    }

    #[test]
    fn test_serialize_xlang_bool() {
        let xlang = serialize_xlang(&true).unwrap();
        let back: bool = deserialize_xlang(&xlang).unwrap();
        assert!(back);

        let xlang = serialize_xlang(&false).unwrap();
        let back: bool = deserialize_xlang(&xlang).unwrap();
        assert!(!back);
    }

    #[test]
    fn test_serialize_xlang_f64() {
        let val: f64 = 3.141592653589793;
        let xlang = serialize_xlang(&val).unwrap();
        let back: f64 = deserialize_xlang(&xlang).unwrap();
        assert_eq!(val, back);
    }

    #[test]
    fn test_serialize_xlang_none() {
        let val: Option<i64> = None;
        let xlang = serialize_xlang(&val).unwrap();
        let back: Option<i64> = deserialize_xlang(&xlang).unwrap();
        assert_eq!(val, back);
    }

    #[test]
    fn test_serialize_xlang_i32() {
        let val: i32 = -12345;
        let xlang = serialize_xlang(&val).unwrap();
        let back: i32 = deserialize_xlang(&xlang).unwrap();
        assert_eq!(val, back);
    }

    // ── XLANG round-trip: complex types ────────────────────────

    #[test]
    fn test_serialize_xlang_empty_list() {
        let val: Vec<i64> = vec![];
        let xlang = serialize_xlang(&val).unwrap();
        let back: Vec<i64> = deserialize_xlang(&xlang).unwrap();
        assert_eq!(val, back);
        assert!(back.is_empty());
    }

    #[test]
    fn test_serialize_xlang_empty_map() {
        let val: HashMap<String, i64> = HashMap::new();
        let xlang = serialize_xlang(&val).unwrap();
        let back: HashMap<String, i64> = deserialize_xlang(&xlang).unwrap();
        assert_eq!(val, back);
        assert!(back.is_empty());
    }

    #[test]
    fn test_serialize_xlang_nested_vec() {
        let val: Vec<Vec<i64>> = vec![vec![1, 2], vec![3, 4, 5], vec![]];
        let xlang = serialize_xlang(&val).unwrap();
        let back: Vec<Vec<i64>> = deserialize_xlang(&xlang).unwrap();
        assert_eq!(val, back);
    }

    #[test]
    fn test_serialize_xlang_tuple() {
        let val = (42i64, "hello".to_string(), true);
        let xlang = serialize_xlang(&val).unwrap();
        let back: (i64, String, bool) = deserialize_xlang(&xlang).unwrap();
        assert_eq!(val, back);
    }

    #[test]
    fn test_serialize_xlang_large_list() {
        let val: Vec<i64> = (0..10000).collect();
        let xlang = serialize_xlang(&val).unwrap();
        let back: Vec<i64> = deserialize_xlang(&xlang).unwrap();
        assert_eq!(val, back);
    }

    // ── Auto-detect: with and without header ──────────────────

    #[test]
    fn test_deserialize_xlang_auto_detect_no_header() {
        let val = vec![1i64, 2, 3, 4, 5];
        let raw_bytes = serialize(&val).unwrap();
        let back: Vec<i64> = deserialize_xlang(&raw_bytes).unwrap();
        assert_eq!(val, back);
    }

    #[test]
    fn test_deserialize_xlang_auto_detect_with_header() {
        let val = vec![1i64, 2, 3, 4, 5];
        let xlang_bytes = serialize_xlang(&val).unwrap();
        let back: Vec<i64> = deserialize_xlang(&xlang_bytes).unwrap();
        assert_eq!(val, back);
    }

    #[test]
    fn test_deserialize_xlang_auto_detect_non_zero_padding() {
        // Simulate Ray 2.51.1+ where header has metadata in padding
        let payload = serialize(&vec![1i64, 2, 3]).unwrap();
        let mut data = vec![payload.len() as u8];
        data.extend_from_slice(&[0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff]);
        data.extend_from_slice(&payload);
        let back: Vec<i64> = deserialize_xlang(&data).unwrap();
        assert_eq!(back, vec![1, 2, 3]);
    }

    // ── rmpv::Value deserialization ────────────────────────────

    #[test]
    fn test_deserialize_xlang_value_list() {
        let val = vec![10i64, 20, 30];
        let xlang_bytes = serialize_xlang(&val).unwrap();
        let value = deserialize_xlang_value(&xlang_bytes).unwrap();
        assert!(value.is_array());
        let arr = value.as_array().unwrap();
        assert_eq!(arr.len(), 3);
        assert_eq!(arr[0].as_i64().unwrap(), 10);
        assert_eq!(arr[1].as_i64().unwrap(), 20);
        assert_eq!(arr[2].as_i64().unwrap(), 30);
    }

    #[test]
    fn test_deserialize_xlang_value_dict() {
        let map_value = rmpv::Value::Map(vec![
            (rmpv::Value::String("name".into()), rmpv::Value::String("alice".into())),
            (rmpv::Value::String("age".into()), rmpv::Value::Integer(30.into())),
        ]);
        let xlang_bytes = serialize_xlang(&map_value).unwrap();
        let value = deserialize_xlang_value(&xlang_bytes).unwrap();
        assert!(value.is_map());

        let entries = value.as_map().unwrap();
        let name_val = entries.iter()
            .find(|(k, _)| k.as_str() == Some("name"))
            .map(|(_, v)| v)
            .unwrap();
        assert_eq!(name_val.as_str().unwrap(), "alice");

        let age_val = entries.iter()
            .find(|(k, _)| k.as_str() == Some("age"))
            .map(|(_, v)| v)
            .unwrap();
        assert_eq!(age_val.as_i64().unwrap(), 30);
    }

    #[test]
    fn test_deserialize_xlang_value_none() {
        let val: Option<i64> = None;
        let xlang_bytes = serialize_xlang(&val).unwrap();
        let value = deserialize_xlang_value(&xlang_bytes).unwrap();
        assert!(value.is_nil());
    }

    #[test]
    fn test_deserialize_xlang_value_nested() {
        let mut inner1 = HashMap::new();
        inner1.insert("id".to_string(), 1i64);
        let mut inner2 = HashMap::new();
        inner2.insert("id".to_string(), 2i64);
        let mut outer = HashMap::new();
        outer.insert("items".to_string(), vec![inner1, inner2]);

        let xlang_bytes = serialize_xlang(&outer).unwrap();
        let value = deserialize_xlang_value(&xlang_bytes).unwrap();
        assert!(value.is_map());

        let entries = value.as_map().unwrap();
        let items = entries.iter()
            .find(|(k, _)| k.as_str() == Some("items"))
            .map(|(_, v)| v)
            .unwrap();
        assert!(items.is_array());
        assert_eq!(items.as_array().unwrap().len(), 2);
    }

    #[test]
    fn test_deserialize_xlang_value_mixed_array() {
        // Simulate Python's [42, "hello", True, None, 3.14]
        let mixed = rmpv::Value::Array(vec![
            rmpv::Value::Integer(42.into()),
            rmpv::Value::String("hello".into()),
            rmpv::Value::Boolean(true),
            rmpv::Value::Nil,
            rmpv::Value::F32(3.14),
        ]);
        let xlang = serialize_xlang(&mixed).unwrap();
        let val = deserialize_xlang_value(&xlang).unwrap();
        assert!(val.is_array());
        let arr = val.as_array().unwrap();
        assert_eq!(arr.len(), 5);
        assert_eq!(arr[0].as_i64().unwrap(), 42);
        assert_eq!(arr[1].as_str().unwrap(), "hello");
        assert_eq!(arr[2].as_bool().unwrap(), true);
        assert!(arr[3].is_nil());
    }

    #[test]
    fn test_deserialize_xlang_value_empty_list() {
        let val: Vec<i64> = vec![];
        let xlang = serialize_xlang(&val).unwrap();
        let value = deserialize_xlang_value(&xlang).unwrap();
        assert!(value.is_array());
        assert_eq!(value.as_array().unwrap().len(), 0);
    }

    #[test]
    fn test_deserialize_value_string() {
        let val = "hello world".to_string();
        let bytes = serialize(&val).unwrap();
        let value = deserialize_value(&bytes).unwrap();
        assert_eq!(value.as_str().unwrap(), "hello world");
    }

    #[test]
    fn test_deserialize_value_bool() {
        let bytes = serialize(&true).unwrap();
        let value = deserialize_value(&bytes).unwrap();
        assert_eq!(value.as_bool().unwrap(), true);
    }

    // ── False-positive guard: raw msgpack that looks like XLANG ──

    #[test]
    fn test_has_xlang_header_false_positive_guard() {
        // A raw msgpack positive fixint of value 0 (1 byte total).
        // has_xlang_header requires data.len() > 9, so this is rejected.
        let raw = serialize(&0i64).unwrap();
        assert!(!has_xlang_header(&raw));

        // A raw msgpack array of 9 bytes (fixarray of 9 elements would be > 9 bytes).
        // But fixarray marker 0x09 followed by 9 single-byte values = 10 bytes total.
        // The first byte 0x09 <= 0x7f, so it's interpreted as fixint=9.
        // Payload after 9-byte header = 1 byte. 9 != 1, so NOT detected as XLANG.
        let raw = serialize(&vec![1i64, 2, 3, 4, 5, 6, 7, 8, 9]).unwrap();
        // This should NOT be detected as XLANG header (payload len won't match)
        // unless by coincidence — let's verify it round-trips through deserialize_xlang
        let back: Vec<i64> = deserialize_xlang(&raw).unwrap();
        assert_eq!(back, vec![1, 2, 3, 4, 5, 6, 7, 8, 9]);
    }

    #[test]
    fn test_serialize_xlang_then_serialize_again() {
        // Double-wrapping should not happen: serialize_xlang produces data that
        // has_xlang_header detects, but deserialize_xlang strips it correctly.
        let val = 42i64;
        let xlang1 = serialize_xlang(&val).unwrap();
        // If someone accidentally calls serialize_xlang on already-wrapped data:
        let xlang2 = serialize_xlang(&xlang1).unwrap();
        // has_xlang_header should detect the outer header
        assert!(has_xlang_header(&xlang2));
        // First strip gives us the inner xlang data
        let inner: Vec<u8> = deserialize_xlang(&xlang2).unwrap();
        // has_xlang_header should detect the inner header too
        assert!(has_xlang_header(&inner));
        // Second strip gives us the original value
        let val_back: i64 = deserialize_xlang(&inner).unwrap();
        assert_eq!(val_back, 42);
    }
}
