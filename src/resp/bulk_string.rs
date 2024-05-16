use std::ops::Deref;

use bytes::{Buf, BytesMut};
use enum_dispatch::enum_dispatch;

use crate::{RespDecode, RespEncode, RespError};

use super::{extract_fixed_data, parse_length, CRLF_LEN};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd)]
pub struct BulkString(pub(crate) Vec<u8>);

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd)]
pub struct NullBulkString;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd)]
#[enum_dispatch(RespEncode, RespDecode)]
pub enum RespBulkString {
    BulkString(BulkString),
    NullBulkString(NullBulkString),
}

impl RespBulkString {
    pub fn new(s: impl Into<Vec<u8>>) -> Self {
        match s.into() {
            s if s.is_empty() => RespBulkString::NullBulkString(NullBulkString),
            s => RespBulkString::BulkString(BulkString::new(s)),
        }
    }
}

impl RespEncode for BulkString {
    fn encode(self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(self.len() + 16);
        buf.extend_from_slice(&format!("${}\r\n", self.len()).into_bytes());
        buf.extend_from_slice(&self);
        buf.extend_from_slice(b"\r\n");
        buf
    }
}

impl RespDecode for BulkString {
    const PREFIX: &'static str = "$";
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        let (end, len) = parse_length(buf, Self::PREFIX)?;
        let remained = &buf[end + CRLF_LEN..];
        if remained.len() < len + CRLF_LEN {
            return Err(RespError::NotComplete);
        }

        buf.advance(end + CRLF_LEN);

        let data = buf.split_to(len + CRLF_LEN);
        Ok(BulkString::new(data[..len].to_vec()))
    }

    fn expect_length(buf: &[u8]) -> Result<usize, RespError> {
        let (end, len) = parse_length(buf, Self::PREFIX)?;
        Ok(end + CRLF_LEN + len + CRLF_LEN)
    }
}

impl RespEncode for NullBulkString {
    fn encode(self) -> Vec<u8> {
        b"$-1\r\n".to_vec()
    }
}

impl RespDecode for NullBulkString {
    const PREFIX: &'static str = "$";
    fn decode(buf: &mut BytesMut) -> Result<Self, RespError> {
        extract_fixed_data(buf, "$-1\r\n", "NullBulkString")?;
        Ok(NullBulkString)
    }

    fn expect_length(_buf: &[u8]) -> Result<usize, RespError> {
        Ok(5)
    }
}

impl BulkString {
    pub fn new(s: impl Into<Vec<u8>>) -> Self {
        BulkString(s.into())
    }
}

impl AsRef<[u8]> for BulkString {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl Deref for BulkString {
    type Target = Vec<u8>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<&str> for BulkString {
    fn from(s: &str) -> Self {
        BulkString(s.as_bytes().to_vec())
    }
}

impl From<String> for BulkString {
    fn from(s: String) -> Self {
        BulkString(s.into_bytes())
    }
}

impl From<&[u8]> for BulkString {
    fn from(s: &[u8]) -> Self {
        BulkString(s.to_vec())
    }
}

impl<const N: usize> From<&[u8; N]> for BulkString {
    fn from(s: &[u8; N]) -> Self {
        BulkString(s.to_vec())
    }
}

impl From<&str> for RespBulkString {
    fn from(s: &str) -> Self {
        match s {
            "" => RespBulkString::NullBulkString(NullBulkString),
            s => RespBulkString::BulkString(BulkString::new(s)),
        }
    }
}

impl From<&[u8]> for RespBulkString {
    fn from(s: &[u8]) -> Self {
        match s {
            [] => RespBulkString::NullBulkString(NullBulkString),
            s => RespBulkString::BulkString(BulkString::new(s)),
        }
    }
}

impl From<String> for RespBulkString {
    fn from(s: String) -> Self {
        match s.as_str() {
            "" => RespBulkString::NullBulkString(NullBulkString),
            s => RespBulkString::BulkString(BulkString::new(s)),
        }
    }
}

impl<const N: usize> From<&[u8; N]> for RespBulkString {
    fn from(s: &[u8; N]) -> Self {
        if s.is_empty() {
            RespBulkString::NullBulkString(NullBulkString)
        } else {
            RespBulkString::BulkString(BulkString::new(s))
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::RespFrame;

    use super::*;
    use anyhow::Result;

    #[test]
    fn test_bulk_string_encode() {
        let frame: RespFrame = RespBulkString::new(b"hello".to_vec()).into();
        assert_eq!(frame.encode(), b"$5\r\nhello\r\n");
    }

    #[test]
    fn test_null_bulk_string_encode() {
        let frame: RespFrame = RespBulkString::new("").into();
        assert_eq!(frame.encode(), b"$-1\r\n");
    }

    #[test]
    fn test_bulk_string_decode() -> Result<()> {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(b"$5\r\nhello\r\n");

        let frame = BulkString::decode(&mut buf)?;
        assert_eq!(frame, BulkString::new(b"hello"));

        buf.extend_from_slice(b"$5\r\nhello");
        let ret = BulkString::decode(&mut buf);
        assert_eq!(ret.unwrap_err(), RespError::NotComplete);

        buf.extend_from_slice(b"\r\n");
        let frame = BulkString::decode(&mut buf)?;
        assert_eq!(frame, BulkString::new(b"hello"));

        Ok(())
    }

    #[test]
    fn test_null_bulk_string_decode() -> Result<()> {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(b"$-1\r\n");

        let frame = NullBulkString::decode(&mut buf)?;
        assert_eq!(frame, NullBulkString);

        Ok(())
    }
}
