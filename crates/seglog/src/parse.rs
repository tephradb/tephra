use crate::{
    CONTROL_FLAG, CRC32C_SIZE, LEN_SIZE, LENGTH_MASK, RECORD_HEAD_SIZE, calculate_crc32c,
    has_unknown_flags,
    read::{ReadError, is_truncation_marker},
};

/// Parses a record from a byte slice starting at the given offset.
///
/// This function extracts and validates a record from raw bytes without performing any I/O.
/// It validates the CRC32C checksum.
///
/// # Arguments
///
/// * `bytes` - The byte buffer containing the record
/// * `offset` - The starting position within the buffer to parse from
///
/// # Returns
///
/// Returns a tuple of `([u8; H], Vec<u8>, usize)` where:
/// - The first element is the header portion (H bytes)
/// - The second element is the record data
/// - The third element is the total bytes consumed from the buffer
///
/// # Errors
///
/// - `ReadError::OutOfBounds` - If the buffer doesn't contain enough bytes
/// - `ReadError::TruncationMarker` - If a truncation marker (all zeros) is encountered
/// - `ReadError::Crc32cMismatch` - If the CRC32C checksum validation fails
pub fn parse_record<const H: usize>(
    bytes: &[u8],
    offset: usize,
) -> Result<([u8; H], Vec<u8>, usize), ReadError> {
    let (header, data, total) = parse_record_parts::<H>(bytes, offset)?;
    let header: [u8; H] = header.try_into().unwrap();
    Ok((header, data.to_vec(), total))
}

/// Like [`parse_record`], but **borrows** the record data from `bytes` rather than copying it.
///
/// Returns the data slice (the payload after the `H`-byte header) and the total number of bytes
/// the record occupies. The caller keeps `bytes` alive for as long as it holds the returned
/// slice. This is the zero-copy counterpart the reverse scan uses to slice records out of a
/// window buffer it already owns; validation and errors are identical to [`parse_record`].
pub fn parse_record_ref<const H: usize>(
    bytes: &[u8],
    offset: usize,
) -> Result<(&[u8], usize), ReadError> {
    let (_header, data, total) = parse_record_parts::<H>(bytes, offset)?;
    Ok((data, total))
}

/// Validates the record framing at `offset` and returns `(header, data, total_len)`, all
/// borrowing `bytes`. The single definition of the framing and CRC logic, shared by
/// [`parse_record`] (which copies the data out) and [`parse_record_ref`] (which borrows it), so
/// the two can never drift.
fn parse_record_parts<const H: usize>(
    bytes: &[u8],
    offset: usize,
) -> Result<(&[u8], &[u8], usize), ReadError> {
    // Check if we have enough bytes for the record header
    if offset + RECORD_HEAD_SIZE > bytes.len() {
        return Err(ReadError::OutOfBounds {
            offset: offset as u64,
            length: RECORD_HEAD_SIZE,
            flushed_offset: bytes.len() as u64,
        });
    }

    let record_header_buf = &bytes[offset..offset + RECORD_HEAD_SIZE];

    // Check for truncation marker
    if is_truncation_marker(record_header_buf) {
        return Err(ReadError::TruncationMarker {
            offset: offset as u64,
        });
    }

    // Parse record header
    let length_bytes: [u8; LEN_SIZE] = record_header_buf[..LEN_SIZE].try_into().unwrap();
    let length_with_flag = u32::from_le_bytes(length_bytes);
    if has_unknown_flags(length_with_flag) {
        return Err(ReadError::Corrupt {
            offset: offset as u64,
        });
    }
    let is_control = length_with_flag & CONTROL_FLAG != 0;
    let payload_len = (length_with_flag & LENGTH_MASK) as usize; // H + data_len
    let crc = u32::from_le_bytes(
        record_header_buf[LEN_SIZE..LEN_SIZE + CRC32C_SIZE]
            .try_into()
            .unwrap(),
    );

    let payload_offset = offset + RECORD_HEAD_SIZE;

    // Check if we have enough bytes for the payload
    if payload_offset + payload_len > bytes.len() {
        return Err(ReadError::OutOfBounds {
            offset: offset as u64,
            length: RECORD_HEAD_SIZE + payload_len,
            flushed_offset: bytes.len() as u64,
        });
    }

    let payload = &bytes[payload_offset..payload_offset + payload_len];

    if is_control {
        // Control records carry no caller header; the CRC covers the whole payload.
        if calculate_crc32c(&length_bytes, &[], payload) != crc {
            return Err(ReadError::Crc32cMismatch {
                offset: offset as u64,
            });
        }
        return Err(ReadError::ControlRecord {
            offset: offset as u64,
            len: RECORD_HEAD_SIZE + payload_len,
        });
    }

    let header = &payload[..H];
    let data = &payload[H..];

    // Validate CRC over header + data
    let calculated_crc = calculate_crc32c(&length_bytes, header, data);
    if crc != calculated_crc {
        return Err(ReadError::Crc32cMismatch {
            offset: offset as u64,
        });
    }

    Ok((header, data, RECORD_HEAD_SIZE + payload_len))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_record<const H: usize>(header: &[u8; H], data: &[u8]) -> Vec<u8> {
        let payload_len = (H + data.len()) as u32;
        let payload_len_bytes = payload_len.to_le_bytes();
        let crc = calculate_crc32c(&payload_len_bytes, header, data);

        let mut buffer = Vec::new();
        buffer.extend_from_slice(&payload_len_bytes);
        buffer.extend_from_slice(&crc.to_le_bytes());
        buffer.extend_from_slice(header);
        buffer.extend_from_slice(data);
        buffer
    }

    #[test]
    fn test_parse_simple_record() {
        let header = [];
        let data = b"hello world";
        let buffer = create_record(&header, data);

        let (parsed_header, parsed_data, size) = parse_record::<0>(&buffer, 0).unwrap();
        assert_eq!(parsed_header, header);
        assert_eq!(parsed_data, data);
        assert_eq!(size, RECORD_HEAD_SIZE + data.len());
    }

    #[test]
    fn test_parse_empty_record() {
        let header = [];
        let data = b"";
        let buffer = create_record(&header, data);

        let (parsed_header, parsed_data, size) = parse_record::<0>(&buffer, 0).unwrap();
        assert_eq!(parsed_header, header);
        assert_eq!(parsed_data, data);
        assert_eq!(size, RECORD_HEAD_SIZE);
    }

    #[test]
    fn test_parse_record_at_offset() {
        let header = [];
        let data1 = b"first record";
        let data2 = b"second record";

        let mut buffer = Vec::new();
        buffer.extend_from_slice(&create_record(&header, data1));
        let second_offset = buffer.len();
        buffer.extend_from_slice(&create_record(&header, data2));

        // Parse first record
        let (_, parsed_data, size) = parse_record::<0>(&buffer, 0).unwrap();
        assert_eq!(parsed_data, data1);
        assert_eq!(size, RECORD_HEAD_SIZE + data1.len());

        // Parse second record
        let (_, parsed_data, size) = parse_record::<0>(&buffer, second_offset).unwrap();
        assert_eq!(parsed_data, data2);
        assert_eq!(size, RECORD_HEAD_SIZE + data2.len());
    }

    #[test]
    fn test_parse_with_header() {
        let header = [1u8, 2, 3, 4, 5, 6, 7, 8];
        let data = b"hello world";
        let buffer = create_record(&header, data);

        let (parsed_header, parsed_data, size) = parse_record::<8>(&buffer, 0).unwrap();
        assert_eq!(parsed_header, header);
        assert_eq!(parsed_data, data);
        assert_eq!(size, RECORD_HEAD_SIZE + header.len() + data.len());
    }

    #[test]
    fn test_parse_truncation_marker() {
        let buffer = vec![0u8; RECORD_HEAD_SIZE + 10];
        let result = parse_record::<0>(&buffer, 0);
        assert!(matches!(
            result,
            Err(ReadError::TruncationMarker { offset: 0 })
        ));
    }

    #[test]
    fn test_parse_insufficient_bytes_for_header() {
        let buffer = vec![0u8; RECORD_HEAD_SIZE - 1];
        let result = parse_record::<0>(&buffer, 0);
        assert!(matches!(
            result,
            Err(ReadError::OutOfBounds {
                offset: 0,
                length: 8,
                flushed_offset: 7,
            })
        ));
    }

    #[test]
    fn test_parse_insufficient_bytes_for_data() {
        let header = [];
        let data = b"hello world";
        let mut buffer = create_record(&header, data);
        buffer.truncate(buffer.len() - 1); // Remove one byte of data

        let result = parse_record::<0>(&buffer, 0);
        assert!(matches!(
            result,
            Err(ReadError::OutOfBounds {
                offset: 0,
                length: 19,
                flushed_offset: 18,
            })
        ));
    }

    #[test]
    fn test_parse_invalid_crc() {
        let header = [];
        let data = b"hello world";
        let mut buffer = create_record(&header, data);

        // Corrupt the CRC
        buffer[LEN_SIZE] ^= 0xFF;

        let result = parse_record::<0>(&buffer, 0);
        assert!(matches!(
            result,
            Err(ReadError::Crc32cMismatch { offset: 0 })
        ));
    }

    #[test]
    fn test_parse_large_record() {
        let header = [];
        let data = vec![0x42u8; 1024 * 1024]; // 1 MB
        let buffer = create_record(&header, &data);

        let (_, parsed_data, size) = parse_record::<0>(&buffer, 0).unwrap();
        assert_eq!(parsed_data, data);
        assert_eq!(size, RECORD_HEAD_SIZE + data.len());
    }

    #[test]
    fn test_parse_multiple_records() {
        let header = [];
        let records = vec![
            b"first".as_slice(),
            b"second record".as_slice(),
            b"third".as_slice(),
        ];

        let mut buffer = Vec::new();
        let mut offsets = Vec::new();

        for data in &records {
            offsets.push(buffer.len());
            buffer.extend_from_slice(&create_record(&header, data));
        }

        // Parse all records
        for (i, data) in records.iter().enumerate() {
            let (_, parsed_data, size) = parse_record::<0>(&buffer, offsets[i]).unwrap();
            assert_eq!(parsed_data, *data);
            assert_eq!(size, RECORD_HEAD_SIZE + data.len());
        }
    }

    #[test]
    fn parse_record_ref_matches_parse_record() {
        // The borrowing variant returns the same data and size as the copying one, at every
        // offset, for both an empty and an eight-byte header. They share `parse_record_parts`,
        // so this pins that they never drift.
        let header0 = [];
        let header8 = [1u8, 2, 3, 4, 5, 6, 7, 8];
        let datas: [&[u8]; 3] = [b"first", b"", b"a slightly longer third record"];

        let mut buffer0 = Vec::new();
        let mut buffer8 = Vec::new();
        let mut offsets = Vec::new();
        for data in &datas {
            offsets.push(buffer0.len());
            buffer0.extend_from_slice(&create_record(&header0, data));
            buffer8.extend_from_slice(&create_record(&header8, data));
        }
        // The eight-byte-header records are a different length, so track their offsets too.
        let mut offsets8 = Vec::new();
        {
            let mut at = 0usize;
            for data in &datas {
                offsets8.push(at);
                at += RECORD_HEAD_SIZE + header8.len() + data.len();
            }
        }

        for (i, data) in datas.iter().enumerate() {
            let (_, owned, owned_size) = parse_record::<0>(&buffer0, offsets[i]).unwrap();
            let (borrowed, borrowed_size) = parse_record_ref::<0>(&buffer0, offsets[i]).unwrap();
            assert_eq!(borrowed, owned.as_slice());
            assert_eq!(borrowed, *data);
            assert_eq!(borrowed_size, owned_size);

            let (_, owned, owned_size) = parse_record::<8>(&buffer8, offsets8[i]).unwrap();
            let (borrowed, borrowed_size) = parse_record_ref::<8>(&buffer8, offsets8[i]).unwrap();
            assert_eq!(borrowed, owned.as_slice());
            assert_eq!(borrowed, *data);
            assert_eq!(borrowed_size, owned_size);
        }
    }

    #[test]
    fn parse_record_ref_reports_crc_mismatch() {
        let data = b"hello world";
        let mut buffer = create_record(&[], data);
        buffer[LEN_SIZE] ^= 0xFF; // corrupt the CRC
        assert!(matches!(
            parse_record_ref::<0>(&buffer, 0),
            Err(ReadError::Crc32cMismatch { offset: 0 })
        ));
    }

    #[test]
    fn parse_record_ref_reports_truncation_marker() {
        let buffer = vec![0u8; RECORD_HEAD_SIZE + 10];
        assert!(matches!(
            parse_record_ref::<0>(&buffer, 0),
            Err(ReadError::TruncationMarker { offset: 0 })
        ));
    }
}
