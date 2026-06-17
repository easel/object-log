//! On-disk segment format (little-endian, version 1).
//!
//! ```text
//! header:  MAGIC "OLOG" (4) | version u16 | topic_len u16 | topic bytes
//!          | partition u32 | base_offset u64 | epoch u64 | record_count u32
//! record:  offset_delta u32 | timestamp_ms i64 | key_len i32 (-1 = none)
//!          | value_len u32 | header_count u16 | [key] | value
//!          | header*( name_len u16 | value_len u32 | name | value )
//! trailer: SHA-256 (32) over all preceding bytes
//! ```
//!
//! Records are offset-contiguous from `base_offset`. Decoding verifies the
//! trailer checksum, the magic/version, contiguity, and that no trailing bytes
//! remain before the checksum.

use crate::{AppendedRecord, ObjectLogError, PartitionId, RecordHeader, TopicName, TopicPartition};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use sha2::{Digest, Sha256};

const MAGIC: &[u8; 4] = b"OLOG";
const VERSION: u16 = 1;
const CHECKSUM_LEN: usize = 32;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Segment {
    pub topic_partition: TopicPartition,
    pub base_offset: u64,
    pub epoch: u64,
    pub records: Vec<AppendedRecord>,
}

impl Segment {
    pub fn last_offset(&self) -> u64 {
        self.base_offset + self.records.len() as u64 - 1
    }
}

pub(crate) fn encode_segment(segment: &Segment) -> Result<Bytes, ObjectLogError> {
    if segment.records.is_empty() {
        return Err(ObjectLogError::InvalidBatch);
    }
    validate_contiguous(segment)?;

    let topic = segment.topic_partition.topic.as_str().as_bytes();
    let topic_len = u16::try_from(topic.len())
        .map_err(|_| ObjectLogError::Serialization("topic too long".to_string()))?;
    let record_count = u32::try_from(segment.records.len())
        .map_err(|_| ObjectLogError::Serialization("too many records".to_string()))?;

    let mut buf = BytesMut::new();
    buf.put_slice(MAGIC);
    buf.put_u16_le(VERSION);
    buf.put_u16_le(topic_len);
    buf.put_slice(topic);
    buf.put_u32_le(segment.topic_partition.partition.0);
    buf.put_u64_le(segment.base_offset);
    buf.put_u64_le(segment.epoch);
    buf.put_u32_le(record_count);

    for record in &segment.records {
        let delta = u32::try_from(record.offset - segment.base_offset)
            .map_err(|_| ObjectLogError::Serialization("offset delta too large".to_string()))?;
        buf.put_u32_le(delta);
        buf.put_i64_le(record.timestamp_ms);
        match &record.key {
            Some(key) => {
                let len = i32::try_from(key.len())
                    .map_err(|_| ObjectLogError::Serialization("key too large".to_string()))?;
                buf.put_i32_le(len);
            }
            None => buf.put_i32_le(-1),
        }
        let value_len = u32::try_from(record.value.len())
            .map_err(|_| ObjectLogError::Serialization("value too large".to_string()))?;
        let header_count = u16::try_from(record.headers.len())
            .map_err(|_| ObjectLogError::Serialization("too many headers".to_string()))?;
        buf.put_u32_le(value_len);
        buf.put_u16_le(header_count);
        if let Some(key) = &record.key {
            buf.put_slice(key);
        }
        buf.put_slice(&record.value);
        for header in &record.headers {
            let name = header.name.as_bytes();
            let name_len = u16::try_from(name.len())
                .map_err(|_| ObjectLogError::Serialization("header name too large".to_string()))?;
            let value_len = u32::try_from(header.value.len())
                .map_err(|_| ObjectLogError::Serialization("header value too large".to_string()))?;
            buf.put_u16_le(name_len);
            buf.put_u32_le(value_len);
            buf.put_slice(name);
            buf.put_slice(&header.value);
        }
    }

    let checksum = Sha256::digest(&buf);
    buf.put_slice(&checksum);
    Ok(buf.freeze())
}

pub(crate) fn decode_segment(bytes: &[u8]) -> Result<Segment, ObjectLogError> {
    if bytes.len() < MAGIC.len() + 2 + CHECKSUM_LEN {
        return Err(ObjectLogError::CorruptSegment(
            "segment too short".to_string(),
        ));
    }
    let payload_len = bytes.len() - CHECKSUM_LEN;
    let expected = &bytes[payload_len..];
    let actual = Sha256::digest(&bytes[..payload_len]);
    if actual.as_slice() != expected {
        return Err(ObjectLogError::CorruptSegment(
            "segment checksum mismatch".to_string(),
        ));
    }

    let mut buf = &bytes[..payload_len];
    if buf.remaining() < 4 || &buf[..4] != MAGIC {
        return Err(ObjectLogError::CorruptSegment("bad magic".to_string()));
    }
    buf.advance(4);
    let version = read_u16(&mut buf)?;
    if version != VERSION {
        return Err(ObjectLogError::UnsupportedSegmentVersion(version));
    }
    let topic_len = read_u16(&mut buf)? as usize;
    let topic_bytes = read_bytes(&mut buf, topic_len)?;
    let topic = String::from_utf8(topic_bytes.to_vec())
        .map_err(|err| ObjectLogError::CorruptSegment(err.to_string()))?;
    let topic = TopicName::new(topic)?;
    let partition = PartitionId(read_u32(&mut buf)?);
    let base_offset = read_u64(&mut buf)?;
    let epoch = read_u64(&mut buf)?;
    let record_count = read_u32(&mut buf)? as usize;
    let topic_partition = TopicPartition { topic, partition };
    let mut records = Vec::with_capacity(record_count);

    for index in 0..record_count {
        let delta = read_u32(&mut buf)? as u64;
        let timestamp_ms = read_i64(&mut buf)?;
        let key_len = read_i32(&mut buf)?;
        let value_len = read_u32(&mut buf)? as usize;
        let header_count = read_u16(&mut buf)? as usize;
        let key = if key_len < 0 {
            None
        } else {
            Some(Bytes::copy_from_slice(read_bytes(
                &mut buf,
                key_len as usize,
            )?))
        };
        let value = Bytes::copy_from_slice(read_bytes(&mut buf, value_len)?);
        let mut headers = Vec::with_capacity(header_count);
        for _ in 0..header_count {
            let name_len = read_u16(&mut buf)? as usize;
            let header_value_len = read_u32(&mut buf)? as usize;
            let name = String::from_utf8(read_bytes(&mut buf, name_len)?.to_vec())
                .map_err(|err| ObjectLogError::CorruptSegment(err.to_string()))?;
            let value = Bytes::copy_from_slice(read_bytes(&mut buf, header_value_len)?);
            headers.push(RecordHeader { name, value });
        }
        let offset = base_offset + delta;
        let expected = base_offset + index as u64;
        if offset != expected {
            return Err(ObjectLogError::OffsetDiscontinuity {
                expected,
                actual: offset,
            });
        }
        records.push(AppendedRecord {
            offset,
            key,
            value,
            headers,
            timestamp_ms,
        });
    }

    if buf.has_remaining() {
        return Err(ObjectLogError::CorruptSegment(
            "trailing bytes before checksum".to_string(),
        ));
    }

    let segment = Segment {
        topic_partition,
        base_offset,
        epoch,
        records,
    };
    validate_contiguous(&segment)?;
    Ok(segment)
}

fn validate_contiguous(segment: &Segment) -> Result<(), ObjectLogError> {
    for (index, record) in segment.records.iter().enumerate() {
        let expected = segment.base_offset + index as u64;
        if record.offset != expected {
            return Err(ObjectLogError::OffsetDiscontinuity {
                expected,
                actual: record.offset,
            });
        }
    }
    Ok(())
}

fn read_bytes<'a>(buf: &mut &'a [u8], len: usize) -> Result<&'a [u8], ObjectLogError> {
    if buf.remaining() < len {
        return Err(ObjectLogError::CorruptSegment("unexpected eof".to_string()));
    }
    let (head, tail) = buf.split_at(len);
    *buf = tail;
    Ok(head)
}

fn read_u16(buf: &mut &[u8]) -> Result<u16, ObjectLogError> {
    if buf.remaining() < 2 {
        return Err(ObjectLogError::CorruptSegment("unexpected eof".to_string()));
    }
    Ok(buf.get_u16_le())
}

fn read_u32(buf: &mut &[u8]) -> Result<u32, ObjectLogError> {
    if buf.remaining() < 4 {
        return Err(ObjectLogError::CorruptSegment("unexpected eof".to_string()));
    }
    Ok(buf.get_u32_le())
}

fn read_u64(buf: &mut &[u8]) -> Result<u64, ObjectLogError> {
    if buf.remaining() < 8 {
        return Err(ObjectLogError::CorruptSegment("unexpected eof".to_string()));
    }
    Ok(buf.get_u64_le())
}

fn read_i32(buf: &mut &[u8]) -> Result<i32, ObjectLogError> {
    if buf.remaining() < 4 {
        return Err(ObjectLogError::CorruptSegment("unexpected eof".to_string()));
    }
    Ok(buf.get_i32_le())
}

fn read_i64(buf: &mut &[u8]) -> Result<i64, ObjectLogError> {
    if buf.remaining() < 8 {
        return Err(ObjectLogError::CorruptSegment("unexpected eof".to_string()));
    }
    Ok(buf.get_i64_le())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tp() -> TopicPartition {
        TopicPartition::new(TopicName::new("events").unwrap(), PartitionId(0))
    }

    #[test]
    fn segment_round_trips() {
        let segment = Segment {
            topic_partition: tp(),
            base_offset: 10,
            epoch: 2,
            records: vec![AppendedRecord {
                offset: 10,
                key: Some(Bytes::from_static(b"k")),
                value: Bytes::from_static(b"v"),
                headers: vec![RecordHeader::new("h", Bytes::from_static(b"x"))],
                timestamp_ms: 123,
            }],
        };
        let encoded = encode_segment(&segment).unwrap();
        let decoded = decode_segment(&encoded).unwrap();
        assert_eq!(decoded, segment);
    }

    #[test]
    fn checksum_mismatch_is_rejected() {
        let segment = Segment {
            topic_partition: tp(),
            base_offset: 0,
            epoch: 0,
            records: vec![AppendedRecord {
                offset: 0,
                key: None,
                value: Bytes::from_static(b"v"),
                headers: vec![],
                timestamp_ms: 1,
            }],
        };
        let mut encoded = encode_segment(&segment).unwrap().to_vec();
        encoded[12] ^= 0xff;
        assert!(matches!(
            decode_segment(&encoded),
            Err(ObjectLogError::CorruptSegment(_))
        ));
    }
}
