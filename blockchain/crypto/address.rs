use bech32::{FromBase32, ToBase32, Variant};
use std::fmt;

pub const KUMQUAT_ADDRESS_HRP: &str = "kmq";
pub const KUMQUAT_ADDRESS_LEN: usize = 32;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AddressCodecError {
    InvalidLength(usize),
    InvalidHrp(String),
    InvalidEncoding(String),
}

impl fmt::Display for AddressCodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AddressCodecError::InvalidLength(len) => {
                write!(
                    f,
                    "address must decode to {} bytes, got {}",
                    KUMQUAT_ADDRESS_LEN, len
                )
            }
            AddressCodecError::InvalidHrp(hrp) => {
                write!(
                    f,
                    "address must use the '{}' prefix, got '{}'",
                    KUMQUAT_ADDRESS_HRP, hrp
                )
            }
            AddressCodecError::InvalidEncoding(message) => {
                write!(f, "invalid address: {}", message)
            }
        }
    }
}

impl std::error::Error for AddressCodecError {}

pub fn encode_address(address: &[u8; KUMQUAT_ADDRESS_LEN]) -> String {
    bech32::encode(KUMQUAT_ADDRESS_HRP, address.to_base32(), Variant::Bech32m)
        .expect("failed to encode Kumquat address")
}

pub fn decode_address(value: &str) -> Result<[u8; KUMQUAT_ADDRESS_LEN], AddressCodecError> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(AddressCodecError::InvalidEncoding(
            "address is empty".to_string(),
        ));
    }

    if let Ok(bytes) = hex::decode(trimmed) {
        return bytes_to_address(&bytes);
    }

    let (hrp, data, variant) = bech32::decode(trimmed)
        .map_err(|err| AddressCodecError::InvalidEncoding(err.to_string()))?;

    if hrp != KUMQUAT_ADDRESS_HRP {
        return Err(AddressCodecError::InvalidHrp(hrp));
    }

    if variant != Variant::Bech32m {
        return Err(AddressCodecError::InvalidEncoding(
            "address must use Bech32m encoding".to_string(),
        ));
    }

    let bytes = Vec::<u8>::from_base32(&data)
        .map_err(|err| AddressCodecError::InvalidEncoding(err.to_string()))?;

    bytes_to_address(&bytes)
}

pub fn normalize_address(value: &str) -> Result<String, AddressCodecError> {
    decode_address(value).map(|address| encode_address(&address))
}

fn bytes_to_address(bytes: &[u8]) -> Result<[u8; KUMQUAT_ADDRESS_LEN], AddressCodecError> {
    if bytes.len() != KUMQUAT_ADDRESS_LEN {
        return Err(AddressCodecError::InvalidLength(bytes.len()));
    }

    let mut address = [0u8; KUMQUAT_ADDRESS_LEN];
    address.copy_from_slice(bytes);
    Ok(address)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_decode_round_trip() {
        let address = [7u8; KUMQUAT_ADDRESS_LEN];
        let encoded = encode_address(&address);
        assert!(encoded.starts_with("kmq1"));
        assert_eq!(decode_address(&encoded).unwrap(), address);
    }

    #[test]
    fn test_decode_legacy_hex() {
        let address = [9u8; KUMQUAT_ADDRESS_LEN];
        let hex_value = hex::encode(address);
        assert_eq!(decode_address(&hex_value).unwrap(), address);
        assert_eq!(
            normalize_address(&hex_value).unwrap(),
            encode_address(&address)
        );
    }

    #[test]
    fn test_reject_wrong_hrp() {
        let address = [3u8; KUMQUAT_ADDRESS_LEN];
        let wrong = bech32::encode("btc", address.to_base32(), Variant::Bech32m).unwrap();
        assert!(matches!(
            decode_address(&wrong),
            Err(AddressCodecError::InvalidHrp(_))
        ));
    }
}
