//! A simple, high-performance segment log implementation.
//!
//! `seglog` provides low-level read and write operations for fixed-size segment files
//! with built-in CRC-32 validation. It's designed for use in event sourcing systems,
//! write-ahead logs, and other append-only storage use cases.
//!
//! # Architecture
//!
//! ## Single Writer, Multiple Readers
//!
//! The segment log follows a **single writer, multiple concurrent readers** model:
//!
//! - **One [`Writer`]** can append records to a segment at a time
//! - **Multiple [`Reader`]s** can concurrently read from the same segment across threads
//! - The [`FlushedOffset`] provides thread-safe coordination between writers and readers
//!
//! This design ensures data consistency without locks on the read path, making it
//! highly efficient for workloads with many concurrent readers.
//!
//! ## Record Format
//!
//! Each record consists of:
//! ```text
//! ┌─────────────┬─────────────┬────────────────┬─────────────────────┐
//! │ Length (4B) │ CRC-32 (4B) │ Header (H B)   │ Data (N bytes)      │
//! └─────────────┴─────────────┴────────────────┴─────────────────────┘
//! ```
//!
//! - **Length**: 32-bit little-endian total payload length (H + N bytes). Bit 30 flags a
//!   control record; bit 31 is reserved and must be zero.
//! - **CRC-32**: 32-bit checksum over length + header + data
//! - **Header**: Fixed-size metadata (H bytes)
//! - **Data**: Variable-length record payload
//!
//! Total record header size is [`RECORD_HEAD_SIZE`] (8 bytes), not including the user header.
//!
//! ## Concurrent Safety via FlushedOffset
//!
//! The [`FlushedOffset`] is an atomic counter that tracks how much data has been
//! safely written to disk:
//!
//! - Writers update it after calling [`Writer::sync`]
//! - Readers check it to avoid reading uncommitted data
//! - It's shared via `Arc` for efficient cloning across threads
//!
//! This ensures readers never see partial writes or corrupted data.
//!
//! # Performance Optimizations
//!
//! ## Read Hints
//!
//! The [`ReadHint`] enum allows optimizing for different access patterns:
//!
//! - **[`ReadHint::Sequential`]**: Uses a 64KB read-ahead buffer for streaming access
//! - **[`ReadHint::Random`]**: Uses optimistic reads to reduce syscalls for small records
//!
//! ## Optimistic Reads
//!
//! For random access, the reader performs an optimistic read of the header plus 2KB
//! of data in a single syscall. If the record fits (most events in event sourcing do),
//! this eliminates one syscall per read, improving performance by ~40% for small records.
//!
//! # Examples
//!
//! ## Writing Records
//!
//! ```rust
//! use seglog::write::Writer;
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! # let dir = tempfile::TempDir::new()?;
//! # let temp = dir.path().join("segment.log");
//! let segment_size = 1024 * 1024; // 1 MB
//! // Writer with no header (H = 0)
//! let mut writer = Writer::<0>::create(&temp, segment_size, 0)?;
//!
//! // Append records with empty header
//! let (offset, len) = writer.append(&[], b"event data")?;
//! writer.append(&[], b"more events")?;
//!
//! // Sync to make data visible to readers
//! writer.sync()?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Reading Records (Sequential)
//!
//! ```rust
//! use seglog::read::Reader;
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! # let dir = tempfile::TempDir::new()?;
//! # let temp = dir.path().join("segment.log");
//! # let mut writer = seglog::write::Writer::<0>::create(&temp, 1024, 0)?;
//! # writer.append(&[], b"event 1")?;
//! # writer.append(&[], b"event 2")?;
//! # writer.sync()?;
//! # let flushed = writer.flushed_offset();
//! # drop(writer);
//! let mut reader = Reader::<0>::open(&temp, Some(flushed))?;
//!
//! // Iterate over all records
//! let mut iter = reader.iter(0);
//! while let Some(record) = iter.next_record()? {
//!     println!("Record: {record:?}");
//! }
//! # Ok(())
//! # }
//! ```
//!
//! ## Reading Records (Random Access)
//!
//! ```rust
//! use seglog::read::{Reader, ReadHint};
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! # let dir = tempfile::TempDir::new()?;
//! # let temp = dir.path().join("segment.log");
//! # let mut writer = seglog::write::Writer::<0>::create(&temp, 1024, 0)?;
//! # let (offset, _) = writer.append(&[], b"specific event")?;
//! # writer.sync()?;
//! # let flushed = writer.flushed_offset();
//! # drop(writer);
//! let mut reader = Reader::<0>::open(&temp, Some(flushed))?;
//!
//! // Read specific record by offset
//! let record = reader.read_record(offset, ReadHint::Random)?;
//! println!("Record: {record:?}");
//! # Ok(())
//! # }
//! ```
//!
//! ## Concurrent Readers
//!
//! ```rust
//! use seglog::read::Reader;
//! use std::thread;
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! # let dir = tempfile::TempDir::new()?;
//! # let temp = dir.path().join("segment.log");
//! # let mut writer = seglog::write::Writer::<0>::create(&temp, 1024, 0)?;
//! # writer.append(&[], b"shared data")?;
//! # writer.sync()?;
//! # let flushed = writer.flushed_offset();
//! # let path = temp.clone();
//! # drop(writer);
//! let reader = Reader::<0>::open(&path, Some(flushed))?;
//!
//! // Clone reader for use in another thread
//! let reader2 = reader.try_clone()?;
//!
//! let handle = thread::spawn(move || {
//!     // Read from the segment in a different thread
//!     // Both readers share the same FlushedOffset
//! });
//! # handle.join().unwrap();
//! # Ok(())
//! # }
//! ```
//!
//! # Segment Lifecycle
//!
//! 1. **Create**: Use [`Writer::<0>::create`] to initialize a new segment
//! 2. **Write**: Append records with [`Writer::append`]
//! 3. **Sync**: Periodically call [`Writer::sync`] to flush data to disk
//! 4. **Read**: Open readers with [`Reader::<0>::open`] using the shared [`FlushedOffset`]
//! 5. **Truncate** (optional): Use [`Writer::set_len`] to truncate the segment
//! 6. **Close**: Call [`Writer::close`] or [`Reader::close`] to ensure cleanup
//!
//! # Error Handling
//!
//! The crate defines two main error types:
//!
//! - [`ReadError`]: CRC mismatch, out of bounds reads, I/O errors
//! - [`WriteError`]: Segment full, I/O errors, read errors during recovery
//!
//! [`Writer`]: write::Writer
//! [`Reader`]: read::Reader
//! [`ReadHint`]: read::ReadHint
//! [`ReadHint::Sequential`]: read::ReadHint::Sequential
//! [`ReadHint::Random`]: read::ReadHint::Random
//! [`Writer::<0>::create`]: write::Writer::<0>::create
//! [`Writer::append`]: write::Writer::append
//! [`Writer::sync`]: write::Writer::sync
//! [`Writer::set_len`]: write::Writer::set_len
//! [`Writer::close`]: write::Writer::close
//! [`Reader::<0>::open`]: read::Reader::<0>::open
//! [`Reader::close`]: read::Reader::close
//! [`ReadError`]: read::ReadError
//! [`WriteError`]: write::WriteError

use std::{
    mem,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

pub mod parse;
pub mod read;
pub mod write;

const LEN_SIZE: usize = mem::size_of::<u32>();
const CRC32C_SIZE: usize = mem::size_of::<u32>();

/// Size of the record header in bytes, consisting of length and CRC-32 checksum.
pub const RECORD_HEAD_SIZE: usize = LEN_SIZE + CRC32C_SIZE;

/// Control-record flag bit in the length field (bit 30).
/// When set, the record is internal framing, not caller data.
pub const CONTROL_FLAG: u32 = 0x4000_0000;

/// Known flag bits (currently just the control flag). Bit 31 is reserved and must be zero:
/// a record with it set is rejected as corrupt. Any bit outside `FLAG_MASK | LENGTH_MASK`
/// is corruption.
pub const FLAG_MASK: u32 = CONTROL_FLAG;

/// Mask to extract actual length from the length field (bits 0..=29).
pub const LENGTH_MASK: u32 = 0x3FFF_FFFF;

/// Largest encodable record length (1 GiB - 1).
pub const MAX_RECORD_LEN: usize = LENGTH_MASK as usize;

/// Control record kinds (first byte of a control record's payload).
pub mod control {
    /// Trailing commit marker. Payload: kind byte + u64 LE highest position.
    pub const BATCH_COMMIT: u8 = 0x01;
}

/// Payload size of a batch commit record: kind byte + u64.
pub const COMMIT_MARKER_PAYLOAD: usize = 1 + 8;

/// Thread-safe atomic offset tracking for flushed data.
///
/// Represents the byte offset in a segment file up to which all data has been safely
/// flushed to disk and can be read concurrently.
#[derive(Clone, Debug)]
pub struct FlushedOffset(Arc<AtomicU64>);

impl FlushedOffset {
    pub(crate) fn new(offset: u64) -> Self {
        FlushedOffset(Arc::new(AtomicU64::new(offset)))
    }

    pub(crate) fn set(&self, offset: u64) {
        self.0.store(offset, Ordering::Release)
    }

    /// Returns the current flushed offset value.
    pub fn load(&self) -> u64 {
        self.0.load(Ordering::Acquire)
    }
}

/// Returns true if `raw` (a record's length field) has any bit set outside the known flags
/// and length. Bit 31 is reserved (formerly the compression flag) and must be zero, so a
/// record with it set is reported as corrupt here rather than silently accepted.
#[allow(clippy::bad_bit_mask)]
#[inline]
pub(crate) const fn has_unknown_flags(raw: u32) -> bool {
    raw & !(FLAG_MASK | LENGTH_MASK) != 0
}

pub fn calculate_crc32c(len_bytes: &[u8; 4], header: &[u8], data: &[u8]) -> u32 {
    let mut crc_hasher = crc32fast::Hasher::new();
    crc_hasher.update(len_bytes);
    crc_hasher.update(header);
    crc_hasher.update(data);
    crc_hasher.finalize()
}

#[cfg(test)]
mod tests {
    use crate::{read::ReadError, write::WriteError};

    use super::*;
    use read::{ReadHint, Reader};
    use std::io::{Seek, Write as _};
    use write::Writer;

    const SEGMENT_SIZE: usize = 1024 * 1024; // 1 MB

    fn temp_path() -> std::path::PathBuf {
        tempfile::Builder::new()
            .suffix(".seg")
            .tempfile()
            .expect("failed to create temp file")
            .into_temp_path()
            .to_path_buf()
    }

    // Writer Basic Operations Tests

    #[test]
    fn test_writer_create() {
        let temp = temp_path();
        let writer = Writer::<0>::create(&temp, SEGMENT_SIZE, 0).expect("failed to create writer");
        assert_eq!(writer.write_offset(), 0);
        assert_eq!(writer.remaining_bytes(), SEGMENT_SIZE as u64);
    }

    #[test]
    fn test_writer_append_single_record() {
        let temp = temp_path();
        let mut writer =
            Writer::<0>::create(&temp, SEGMENT_SIZE, 0).expect("failed to create writer");

        let data = b"hello world";
        let (offset, len) = writer.append(&[], data).expect("failed to append");

        assert_eq!(offset, 0);
        assert_eq!(len, RECORD_HEAD_SIZE + data.len());
        assert_eq!(writer.write_offset(), len as u64);
    }

    #[test]
    fn test_writer_append_multiple_records() {
        let temp = temp_path();
        let mut writer =
            Writer::<0>::create(&temp, SEGMENT_SIZE, 0).expect("failed to create writer");

        let records = vec![b"first".as_slice(), b"second", b"third"];
        let mut expected_offset = 0u64;

        for data in &records {
            let (offset, len) = writer.append(&[], data).expect("failed to append");
            assert_eq!(offset, expected_offset);
            expected_offset += len as u64;
        }

        assert_eq!(writer.write_offset(), expected_offset);
    }

    #[test]
    fn test_writer_remaining_bytes() {
        let temp = temp_path();
        let mut writer =
            Writer::<0>::create(&temp, SEGMENT_SIZE, 0).expect("failed to create writer");

        let initial_remaining = writer.remaining_bytes();
        assert_eq!(initial_remaining, SEGMENT_SIZE as u64);

        let data = b"test data";
        writer.append(&[], data).expect("failed to append");

        let after_remaining = writer.remaining_bytes();
        assert_eq!(
            after_remaining,
            SEGMENT_SIZE as u64 - (RECORD_HEAD_SIZE + data.len()) as u64
        );
    }

    #[test]
    fn test_writer_write_offset() {
        let temp = temp_path();
        let mut writer =
            Writer::<0>::create(&temp, SEGMENT_SIZE, 0).expect("failed to create writer");

        assert_eq!(writer.write_offset(), 0);

        let data = b"data";
        let (_, len) = writer.append(&[], data).expect("failed to append");
        assert_eq!(writer.write_offset(), len as u64);
    }

    // Writer Error Cases Tests

    #[test]
    fn test_writer_segment_full() {
        let small_size = 100;
        let temp = temp_path();
        let mut writer =
            Writer::<0>::create(&temp, small_size, 0).expect("failed to create writer");

        // A record larger than the per-segment record cap is rejected up front.
        writer.set_max_record(small_size);
        let large_data = vec![0u8; small_size];
        let result = writer.append(&[], &large_data);

        assert!(matches!(result, Err(WriteError::RecordTooLarge { .. })));
    }

    #[test]
    fn test_writer_create_already_exists() {
        let temp = temp_path();
        Writer::<0>::create(&temp, SEGMENT_SIZE, 0).expect("failed to create writer");

        let result = Writer::<0>::create(&temp, SEGMENT_SIZE, 0);
        assert!(result.is_err());
    }

    // Writer Sync & Flush Tests

    #[test]
    fn test_writer_sync() {
        let temp = temp_path();
        let mut writer =
            Writer::<0>::create(&temp, SEGMENT_SIZE, 0).expect("failed to create writer");

        let data = b"sync test";
        writer.append(&[], data).expect("failed to append");

        let flushed_before = writer.flushed_offset().load();
        let synced_offset = writer.sync().expect("failed to sync");

        assert_eq!(synced_offset, writer.write_offset());
        assert_eq!(writer.flushed_offset().load(), synced_offset);
        assert!(writer.flushed_offset().load() > flushed_before);
    }

    #[test]
    fn test_writer_flush_writer() {
        let temp = temp_path();
        let mut writer =
            Writer::<0>::create(&temp, SEGMENT_SIZE, 0).expect("failed to create writer");

        writer.append(&[], b"data").expect("failed to append");
        writer.flush_writer().expect("failed to flush");
    }

    #[test]
    fn test_writer_close() {
        let temp = temp_path();
        let mut writer =
            Writer::<0>::create(&temp, SEGMENT_SIZE, 0).expect("failed to create writer");

        writer.append(&[], b"data").expect("failed to append");
        let write_offset = writer.write_offset();
        let flushed = writer.flushed_offset();

        writer.close().expect("failed to close");

        // Verify data was synced by opening a reader
        let reader = Reader::<0>::open(&temp, Some(flushed)).expect("failed to open reader");
        assert_eq!(reader.flushed_offset().load(), write_offset);
    }

    // Writer Truncation Tests

    #[test]
    fn test_writer_set_len() {
        let temp = temp_path();
        let mut writer =
            Writer::<0>::create(&temp, SEGMENT_SIZE, 0).expect("failed to create writer");

        writer.append(&[], b"first").expect("failed to append");
        let truncate_offset = writer.write_offset();
        writer.append(&[], b"second").expect("failed to append");

        writer.set_len(truncate_offset).expect("failed to set_len");
        assert_eq!(writer.write_offset(), truncate_offset);
        assert_eq!(writer.flushed_offset().load(), truncate_offset);
    }

    #[test]
    fn test_writer_set_len_noop() {
        let temp = temp_path();
        let mut writer =
            Writer::<0>::create(&temp, SEGMENT_SIZE, 0).expect("failed to create writer");

        writer.append(&[], b"data").expect("failed to append");
        let offset = writer.write_offset();

        writer.set_len(offset + 100).expect("failed to set_len");
        assert_eq!(writer.write_offset(), offset);
    }

    // Writer Open & Recovery Tests

    #[test]
    fn test_writer_open_existing() {
        let temp = temp_path();
        let mut writer =
            Writer::<0>::create(&temp, SEGMENT_SIZE, 0).expect("failed to create writer");

        writer.append(&[], b"first").expect("failed to append");
        writer.append(&[], b"second").expect("failed to append");
        writer.commit(0).expect("failed to commit");
        let offset_before = writer.write_offset();
        drop(writer);

        let mut writer = Writer::<0>::open(&temp, SEGMENT_SIZE, 0).expect("failed to open writer");
        assert_eq!(writer.write_offset(), offset_before);

        writer.append(&[], b"third").expect("failed to append");
        assert!(writer.write_offset() > offset_before);
    }

    #[test]
    fn test_writer_open_with_corruption() {
        let temp = temp_path();
        let mut writer =
            Writer::<0>::create(&temp, SEGMENT_SIZE, 0).expect("failed to create writer");

        writer.append(&[], b"good data").expect("failed to append");
        writer.commit(0).expect("failed to commit");
        let good_offset = writer.write_offset();

        // Manually corrupt the file by writing garbage after valid data
        drop(writer);
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .open(&temp)
            .expect("failed to open file");
        file.seek(std::io::SeekFrom::Start(good_offset))
            .expect("failed to seek");
        file.write_all(&[0xFF; 100])
            .expect("failed to write garbage");
        drop(file);

        // Opening should detect corruption and stop at the last valid record
        let writer = Writer::<0>::open(&temp, SEGMENT_SIZE, 0).expect("failed to open writer");
        assert_eq!(writer.write_offset(), good_offset);
    }

    // Reader Basic Operations Tests

    #[test]
    fn test_reader_open() {
        let temp = temp_path();
        let mut writer =
            Writer::<0>::create(&temp, SEGMENT_SIZE, 0).expect("failed to create writer");
        writer.append(&[], b"data").expect("failed to append");
        writer.sync().expect("failed to sync");
        let flushed = writer.flushed_offset();
        drop(writer);

        let reader = Reader::<0>::open(&temp, Some(flushed)).expect("failed to open reader");
        assert!(reader.flushed_offset().load() > 0);
    }

    #[test]
    fn test_reader_read_record_random() {
        let temp = temp_path();
        let mut writer =
            Writer::<0>::create(&temp, SEGMENT_SIZE, 0).expect("failed to create writer");

        let data = b"test data";
        writer.append(&[], data).expect("failed to append");
        writer.sync().expect("failed to sync");
        let flushed = writer.flushed_offset();
        drop(writer);

        let mut reader = Reader::<0>::open(&temp, Some(flushed)).expect("failed to open reader");
        let record = reader
            .read_record(0, ReadHint::Random)
            .expect("failed to read");
        assert_eq!(&*record.data, data);
        assert_eq!(record.offset, 0);
        assert_eq!(record.len, RECORD_HEAD_SIZE + data.len());
        assert_eq!(&*record.header, &[]);
    }

    #[test]
    fn test_reader_read_record_sequential() {
        let temp = temp_path();
        let mut writer =
            Writer::<0>::create(&temp, SEGMENT_SIZE, 0).expect("failed to create writer");

        let data = b"sequential data";
        writer.append(&[], data).expect("failed to append");
        writer.sync().expect("failed to sync");
        let flushed = writer.flushed_offset();
        drop(writer);

        let mut reader = Reader::<0>::open(&temp, Some(flushed)).expect("failed to open reader");
        let record = reader
            .read_record(0, ReadHint::Sequential)
            .expect("failed to read");
        assert_eq!(&*record.data, data);
        assert_eq!(record.offset, 0);
        assert_eq!(record.len, RECORD_HEAD_SIZE + data.len());
        assert_eq!(&*record.header, &[]);
    }

    #[test]
    fn test_reader_read_bytes() {
        let temp = temp_path();
        let mut writer =
            Writer::<0>::create(&temp, SEGMENT_SIZE, 0).expect("failed to create writer");

        let data = b"raw bytes";
        writer.append(&[], data).expect("failed to append");
        writer.sync().expect("failed to sync");
        let flushed = writer.flushed_offset();
        drop(writer);

        let reader = Reader::<0>::open(&temp, Some(flushed)).expect("failed to open reader");
        let mut buf = vec![0; RECORD_HEAD_SIZE + data.len()];
        reader
            .read_bytes(0, &mut buf)
            .expect("failed to read bytes");
        assert_eq!(buf.len(), RECORD_HEAD_SIZE + data.len());
    }

    #[test]
    fn test_reader_try_clone() {
        let temp = temp_path();
        let mut writer =
            Writer::<0>::create(&temp, SEGMENT_SIZE, 0).expect("failed to create writer");
        writer.append(&[], b"data").expect("failed to append");
        writer.sync().expect("failed to sync");
        let flushed = writer.flushed_offset();
        drop(writer);

        let reader = Reader::<0>::open(&temp, Some(flushed)).expect("failed to open reader");
        let cloned = reader.try_clone().expect("failed to clone");

        assert_eq!(
            reader.flushed_offset().load(),
            cloned.flushed_offset().load()
        );
    }

    #[test]
    fn test_reader_close() {
        let temp = temp_path();
        let writer = Writer::<0>::create(&temp, SEGMENT_SIZE, 0).expect("failed to create writer");
        writer.close().expect("failed to close writer");

        let reader = Reader::<0>::open(&temp, None).expect("failed to open reader");
        reader.close();
    }

    // Reader Error Cases Tests

    #[test]
    fn test_reader_crc_mismatch() {
        let temp = temp_path();
        let mut writer =
            Writer::<0>::create(&temp, SEGMENT_SIZE, 0).expect("failed to create writer");

        writer.append(&[], b"data").expect("failed to append");
        let _offset = writer.write_offset();
        writer.sync().expect("failed to sync");
        drop(writer);

        // Corrupt the data by modifying a byte in the data portion
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .open(&temp)
            .expect("failed to open file");
        file.seek(std::io::SeekFrom::Start(RECORD_HEAD_SIZE as u64))
            .expect("failed to seek");
        file.write_all(&[0xFF]).expect("failed to write");
        drop(file);

        let mut reader = Reader::<0>::open(&temp, None).expect("failed to open reader");
        let result = reader.read_record(0, ReadHint::Random);

        assert!(matches!(
            result,
            Err(ReadError::Crc32cMismatch { offset: 0 })
        ));
    }

    #[test]
    fn test_reader_out_of_bounds() {
        let temp = temp_path();
        let writer = Writer::<0>::create(&temp, SEGMENT_SIZE, 0).expect("failed to create writer");
        let flushed = writer.flushed_offset();
        assert_eq!(flushed.load(), 0);
        drop(writer);

        let mut reader = Reader::<0>::open(&temp, Some(flushed)).expect("failed to open reader");
        let result = reader.read_record(1000, ReadHint::Random);

        assert!(matches!(
            result,
            Err(ReadError::OutOfBounds {
                offset: 1000,
                length: 8,
                flushed_offset: 0,
            })
        ));
    }

    #[test]
    fn test_reader_truncation_marker() {
        let temp = temp_path();
        let mut writer =
            Writer::<0>::create(&temp, SEGMENT_SIZE, 0).expect("failed to create writer");

        writer.append(&[], b"first").expect("failed to append");
        let truncate_offset = writer.write_offset();
        assert_eq!(truncate_offset, 13);
        writer.append(&[], b"second").expect("failed to append");
        writer.set_len(truncate_offset).expect("failed to truncate");

        let flushed = writer.flushed_offset();
        assert_eq!(flushed.load(), 13);
        drop(writer);

        let mut reader = Reader::<0>::open(&temp, Some(flushed)).expect("failed to open reader");
        let result = reader.read_record(truncate_offset, ReadHint::Random);

        assert!(matches!(
            result,
            Err(ReadError::OutOfBounds {
                offset: 13,
                length: 8,
                flushed_offset: 13,
            })
        ));
    }

    // Iterator Tests

    #[test]
    fn test_iter_all_records() {
        let temp = temp_path();
        let mut writer =
            Writer::<0>::create(&temp, SEGMENT_SIZE, 0).expect("failed to create writer");

        let records = vec![b"first".as_slice(), b"second", b"third"];
        for data in &records {
            writer.append(&[], data).expect("failed to append");
        }
        writer.sync().expect("failed to sync");
        let flushed = writer.flushed_offset();
        drop(writer);

        let mut reader = Reader::<0>::open(&temp, Some(flushed)).expect("failed to open reader");
        let mut iter = reader.iter(0);

        let mut expected_offset = 0u64;
        for expected in &records {
            let record = iter
                .next_record()
                .expect("failed to read")
                .expect("no record");
            assert_eq!(&*record.data, *expected);
            assert_eq!(record.offset, expected_offset);
            assert_eq!(record.len, RECORD_HEAD_SIZE + expected.len());
            assert_eq!(&*record.header, &[]);
            expected_offset += record.len as u64;
        }

        assert!(iter.next_record().expect("failed to read").is_none());
    }

    #[test]
    fn test_iter_from_offset() {
        let temp = temp_path();
        let mut writer =
            Writer::<0>::create(&temp, SEGMENT_SIZE, 0).expect("failed to create writer");

        writer.append(&[], b"first").expect("failed to append");
        let second_offset = writer.write_offset();
        writer.append(&[], b"second").expect("failed to append");
        writer.append(&[], b"third").expect("failed to append");
        writer.sync().expect("failed to sync");
        let flushed = writer.flushed_offset();
        drop(writer);

        let mut reader = Reader::<0>::open(&temp, Some(flushed)).expect("failed to open reader");
        let mut iter = reader.iter(second_offset);

        let record = iter
            .next_record()
            .expect("failed to read")
            .expect("no record");
        assert_eq!(&*record.data, b"second");
        assert_eq!(record.offset, second_offset);
        assert_eq!(record.len, RECORD_HEAD_SIZE + b"second".len());
        assert_eq!(&*record.header, &[]);

        let third_offset = second_offset + record.len as u64;
        let record = iter
            .next_record()
            .expect("failed to read")
            .expect("no record");
        assert_eq!(&*record.data, b"third");
        assert_eq!(record.offset, third_offset);
        assert_eq!(record.len, RECORD_HEAD_SIZE + b"third".len());
        assert_eq!(&*record.header, &[]);

        assert!(iter.next_record().expect("failed to read").is_none());
    }

    #[test]
    fn test_iter_empty_segment() {
        let temp = temp_path();
        let writer = Writer::<0>::create(&temp, SEGMENT_SIZE, 0).expect("failed to create writer");
        let flushed = writer.flushed_offset();
        drop(writer);

        let mut reader = Reader::<0>::open(&temp, Some(flushed)).expect("failed to open reader");
        let mut iter = reader.iter(0);

        assert!(iter.next_record().expect("failed to read").is_none());
    }

    #[test]
    fn test_iter_single_record() {
        let temp = temp_path();
        let mut writer =
            Writer::<0>::create(&temp, SEGMENT_SIZE, 0).expect("failed to create writer");

        writer.append(&[], b"only one").expect("failed to append");
        writer.sync().expect("failed to sync");
        let flushed = writer.flushed_offset();
        drop(writer);

        let mut reader = Reader::<0>::open(&temp, Some(flushed)).expect("failed to open reader");
        let mut iter = reader.iter(0);

        let record = iter
            .next_record()
            .expect("failed to read")
            .expect("no record");
        assert_eq!(&*record.data, b"only one");
        assert_eq!(record.offset, 0);
        assert_eq!(record.len, RECORD_HEAD_SIZE + b"only one".len());
        assert_eq!(&*record.header, &[]);

        assert!(iter.next_record().expect("failed to read").is_none());
    }

    #[test]
    fn test_iter_skips_commit_markers() {
        // The whole SegmentSet layer above relies on iter yielding only data
        // records and silently skipping the commit markers between batches.
        let temp = temp_path();
        let mut writer =
            Writer::<0>::create(&temp, SEGMENT_SIZE, 0).expect("failed to create writer");

        writer.append(&[], b"a").expect("failed to append");
        writer.append(&[], b"b").expect("failed to append");
        writer.commit(1).expect("failed to commit"); // marker after batch 1
        writer.append(&[], b"c").expect("failed to append");
        writer.commit(2).expect("failed to commit"); // marker after batch 2
        let flushed = writer.flushed_offset();
        drop(writer);

        let mut reader = Reader::<0>::open(&temp, Some(flushed)).expect("failed to open reader");
        let mut iter = reader.iter(0);

        for expected in [b"a".as_slice(), b"b", b"c"] {
            let record = iter
                .next_record()
                .expect("failed to read")
                .expect("expected a data record, not a skipped marker");
            assert_eq!(&*record.data, expected);
        }
        assert!(iter.next_record().expect("failed to read").is_none());
    }

    #[test]
    fn test_rewind_to_discards_uncommitted_records() {
        // Models the dangerous batch-abort path: some records land, the commit
        // never happens (here simulated by skipping it), the batch is rewound, and a
        // later batch must not adopt the orphans as its own.
        let temp = temp_path();
        let mut writer =
            Writer::<0>::create(&temp, SEGMENT_SIZE, 0).expect("failed to create writer");

        writer.append(&[], b"keep").expect("failed to append");
        writer.commit(0).expect("failed to commit");
        let committed = writer.write_offset();

        // Partial batch: records appended, commit never reached.
        writer.append(&[], b"discard-1").expect("failed to append");
        writer.append(&[], b"discard-2").expect("failed to append");
        writer.rewind_to(committed).expect("failed to rewind");

        // The cursor is back exactly at the flushed point, so no sync is now owed:
        // a spurious fsync (or a rollover seal that silently no-ops) is the bug this
        // guards against.
        assert_eq!(writer.write_offset(), committed);
        assert_eq!(writer.flushed_offset().load(), writer.write_offset());

        // A fresh batch after the rewind overwrites the discarded region and lands
        // contiguously right after "keep".
        writer
            .append(&[], b"replacement")
            .expect("failed to append");
        writer.commit(1).expect("failed to commit");
        let flushed = writer.flushed_offset();
        drop(writer);

        let mut reader = Reader::<0>::open(&temp, Some(flushed)).expect("failed to open reader");
        let mut iter = reader.iter(0);
        assert_eq!(&*iter.next_record().unwrap().unwrap().data, b"keep");
        assert_eq!(&*iter.next_record().unwrap().unwrap().data, b"replacement");
        assert!(
            iter.next_record().unwrap().is_none(),
            "orphan records must not survive"
        );

        // And recovery agrees: reopening lands at the committed end.
        let writer = Writer::<0>::open(&temp, SEGMENT_SIZE, 0).expect("failed to reopen");
        assert_eq!(writer.last_committed_position(), Some(1));
    }

    #[test]
    fn test_peek_classifies_records() {
        use read::RecordKind;

        let temp = temp_path();
        let mut writer =
            Writer::<0>::create(&temp, SEGMENT_SIZE, 0).expect("failed to create writer");
        let (data_offset, data_len) = writer.append(&[], b"data-1").expect("failed to append");
        writer.commit(1).expect("failed to commit"); // control marker after the data record
        let flushed = writer.flushed_offset();
        drop(writer);

        let mut reader = Reader::<0>::open(&temp, Some(flushed)).expect("failed to open reader");

        // Data record, then the commit marker, then end.
        assert_eq!(
            reader.peek(data_offset).unwrap(),
            RecordKind::Data {
                total_len: data_len
            }
        );
        let marker_offset = data_offset + data_len as u64;
        let marker_len = match reader.peek(marker_offset).unwrap() {
            RecordKind::Control { total_len } => total_len,
            other => panic!("expected a control marker, got {other:?}"),
        };
        assert_eq!(
            reader.peek(marker_offset + marker_len as u64).unwrap(),
            RecordKind::End
        );
    }

    #[test]
    fn test_sequential_read_borrows_even_large_records() {
        use std::borrow::Cow;

        // The SegmentSet scan relies on this: a sequential read always returns data
        // borrowing the read-ahead buffer, even for payloads larger than the
        // optimistic/fallback buffers, so scans stay zero-copy.
        let temp = temp_path();
        let mut writer =
            Writer::<0>::create(&temp, SEGMENT_SIZE, 0).expect("failed to create writer");
        let big = vec![0x5Au8; 100_000];
        writer.append(&[], &big).expect("failed to append");
        writer.sync().expect("failed to sync");
        let flushed = writer.flushed_offset();
        drop(writer);

        let mut reader = Reader::<0>::open(&temp, Some(flushed)).expect("failed to open reader");
        let record = reader
            .read_record(0, ReadHint::Sequential)
            .expect("failed to read");
        assert!(
            matches!(record.data, Cow::Borrowed(_)),
            "sequential reads must borrow so SegmentSet scans stay zero-copy"
        );
        assert_eq!(&*record.data, &big[..]);
    }

    // Concurrent Read/Write Tests

    #[test]
    fn test_flushed_offset_sync() {
        let temp = temp_path();
        let mut writer =
            Writer::<0>::create(&temp, SEGMENT_SIZE, 0).expect("failed to create writer");

        let flushed = writer.flushed_offset();
        assert_eq!(flushed.load(), 0);

        writer.append(&[], b"data").expect("failed to append");
        assert_eq!(flushed.load(), 0); // Not yet flushed

        writer.sync().expect("failed to sync");
        assert!(flushed.load() > 0); // Now flushed
    }

    #[test]
    fn test_concurrent_read_write() {
        let temp = temp_path();
        let mut writer =
            Writer::<0>::create(&temp, SEGMENT_SIZE, 0).expect("failed to create writer");

        let flushed = writer.flushed_offset();
        let mut reader =
            Reader::<0>::open(&temp, Some(flushed.clone())).expect("failed to open reader");

        // Reader can't read unflushed data
        assert!(reader.read_record(0, ReadHint::Random).is_err());

        writer.append(&[], b"data").expect("failed to append");
        writer.sync().expect("failed to sync");

        // Now reader can read
        let record = reader
            .read_record(0, ReadHint::Random)
            .expect("failed to read");
        assert_eq!(&*record.data, b"data");
        assert_eq!(record.offset, 0);
        assert_eq!(record.len, RECORD_HEAD_SIZE + b"data".len());
        assert_eq!(&*record.header, &[]);
    }

    // Edge Cases Tests

    #[test]
    fn test_empty_segment() {
        let temp = temp_path();
        let writer = Writer::<0>::create(&temp, SEGMENT_SIZE, 0).expect("failed to create writer");
        let flushed = writer.flushed_offset();
        drop(writer);

        let mut reader = Reader::<0>::open(&temp, Some(flushed)).expect("failed to open reader");
        assert_eq!(reader.flushed_offset().load(), 0);

        let result = reader.read_record(0, ReadHint::Random);
        assert!(result.is_err());
    }

    #[test]
    fn test_large_record() {
        let temp = temp_path();
        let mut writer =
            Writer::<0>::create(&temp, SEGMENT_SIZE, 0).expect("failed to create writer");

        // Create a record larger than internal buffers
        let large_data = vec![0x42u8; 100_000];
        writer.append(&[], &large_data).expect("failed to append");
        writer.sync().expect("failed to sync");
        let flushed = writer.flushed_offset();
        drop(writer);

        let mut reader = Reader::<0>::open(&temp, Some(flushed)).expect("failed to open reader");
        let record = reader
            .read_record(0, ReadHint::Random)
            .expect("failed to read");
        assert_eq!(&*record.data, &large_data[..]);
        assert_eq!(record.offset, 0);
        assert_eq!(record.len, RECORD_HEAD_SIZE + large_data.len());
        assert_eq!(&*record.header, &[]);
    }

    #[test]
    fn test_max_segment_size() {
        let small_size = 1000;
        let temp = temp_path();
        let mut writer =
            Writer::<0>::create(&temp, small_size, 0).expect("failed to create writer");

        // Fill segment almost completely
        let data = vec![0u8; 100];
        while writer.remaining_bytes() > (RECORD_HEAD_SIZE + data.len()) as u64 {
            writer.append(&[], &data).expect("failed to append");
        }

        let remaining = writer.remaining_bytes();
        assert!(remaining < (RECORD_HEAD_SIZE + data.len()) as u64);

        // Should not be able to fit another record
        let result = writer.append(&[], &data);
        assert!(matches!(
            result,
            Err(WriteError::SegmentFull {
                attempted: 972,
                available: 28,
            })
        ));
    }

    #[test]
    fn test_record_boundaries() {
        let temp = temp_path();
        let mut writer =
            Writer::<0>::create(&temp, SEGMENT_SIZE, 0).expect("failed to create writer");

        // Test records of varying sizes
        let sizes = vec![0, 1, 7, 8, 15, 16, 255, 256, 4095, 4096];

        for size in sizes {
            let data = vec![0xAAu8; size];
            let (offset, len) = writer.append(&[], &data).expect("failed to append");
            assert_eq!(len, RECORD_HEAD_SIZE + size);

            // Verify we can read it back immediately
            writer.sync().expect("failed to sync");
            let flushed = writer.flushed_offset();

            let mut reader =
                Reader::<0>::open(&temp, Some(flushed.clone())).expect("failed to open reader");
            let record = reader
                .read_record(offset, ReadHint::Random)
                .expect("failed to read");
            assert_eq!(record.data.len(), size);
            assert_eq!(record.offset, offset);
            assert_eq!(record.len, len);
            assert_eq!(&*record.header, &[]);
        }
    }

    // Start Offset Tests

    #[test]
    fn test_writer_create_with_start_offset() {
        let temp = temp_path();
        const HEADER_SIZE: u64 = 64;
        let mut writer =
            Writer::<0>::create(&temp, SEGMENT_SIZE, HEADER_SIZE).expect("failed to create writer");

        assert_eq!(writer.write_offset(), HEADER_SIZE);
        assert_eq!(writer.remaining_bytes(), SEGMENT_SIZE as u64 - HEADER_SIZE);

        // First record should start at HEADER_SIZE
        let (offset, len) = writer.append(&[], b"data").expect("failed to append");
        assert_eq!(offset, HEADER_SIZE);
        assert_eq!(writer.write_offset(), HEADER_SIZE + len as u64);
    }

    #[test]
    fn test_writer_with_header_data() {
        use std::os::unix::fs::FileExt;

        let temp = temp_path();
        const HEADER_SIZE: u64 = 32;
        let mut writer =
            Writer::<0>::create(&temp, SEGMENT_SIZE, HEADER_SIZE).expect("failed to create writer");

        // Write header data before start_offset
        let magic = b"MAGIC";
        let version = 1u16.to_le_bytes();
        writer
            .file()
            .write_all_at(magic, 0)
            .expect("failed to write magic");
        writer
            .file()
            .write_all_at(&version, magic.len() as u64)
            .expect("failed to write version");

        // Append records (should start at HEADER_SIZE)
        let (offset, _) = writer.append(&[], b"record1").expect("failed to append");
        assert_eq!(offset, HEADER_SIZE);
        writer.append(&[], b"record2").expect("failed to append");
        writer.sync().expect("failed to sync");
        drop(writer);

        // Verify header is intact
        let file = std::fs::File::open(&temp).expect("failed to open file");
        let mut magic_read = vec![0u8; magic.len()];
        file.read_exact_at(&mut magic_read, 0)
            .expect("failed to read magic");
        assert_eq!(&magic_read, magic);

        let mut version_read = [0u8; 2];
        file.read_exact_at(&mut version_read, magic.len() as u64)
            .expect("failed to read version");
        assert_eq!(version_read, version);
    }

    #[test]
    fn test_writer_open_with_start_offset() {
        let temp = temp_path();
        const HEADER_SIZE: u64 = 64;
        let mut writer =
            Writer::<0>::create(&temp, SEGMENT_SIZE, HEADER_SIZE).expect("failed to create writer");

        writer.append(&[], b"first").expect("failed to append");
        writer.append(&[], b"second").expect("failed to append");
        writer.commit(0).expect("failed to commit");
        let offset_before = writer.write_offset();
        drop(writer);

        // Open with same start_offset
        let mut writer =
            Writer::<0>::open(&temp, SEGMENT_SIZE, HEADER_SIZE).expect("failed to open writer");
        assert_eq!(writer.write_offset(), offset_before);

        // Should be able to continue appending
        writer.append(&[], b"third").expect("failed to append");
        assert!(writer.write_offset() > offset_before);
    }

    #[test]
    fn test_reader_with_start_offset() {
        let temp = temp_path();
        const HEADER_SIZE: u64 = 64;
        let mut writer =
            Writer::<0>::create(&temp, SEGMENT_SIZE, HEADER_SIZE).expect("failed to create writer");

        let (first_offset, _) = writer.append(&[], b"first").expect("failed to append");
        let (second_offset, _) = writer.append(&[], b"second").expect("failed to append");
        writer.sync().expect("failed to sync");
        let flushed = writer.flushed_offset();
        drop(writer);

        assert_eq!(first_offset, HEADER_SIZE);

        // Read records starting from HEADER_SIZE
        let mut reader = Reader::<0>::open(&temp, Some(flushed)).expect("failed to open reader");
        let record = reader
            .read_record(first_offset, ReadHint::Random)
            .expect("failed to read");
        assert_eq!(&*record.data, b"first");
        assert_eq!(record.offset, first_offset);
        assert_eq!(record.len, RECORD_HEAD_SIZE + b"first".len());
        assert_eq!(&*record.header, &[]);

        let record = reader
            .read_record(second_offset, ReadHint::Random)
            .expect("failed to read");
        assert_eq!(&*record.data, b"second");
        assert_eq!(record.offset, second_offset);
        assert_eq!(record.len, RECORD_HEAD_SIZE + b"second".len());
        assert_eq!(&*record.header, &[]);
    }

    #[test]
    fn test_iter_with_start_offset() {
        let temp = temp_path();
        const HEADER_SIZE: u64 = 64;
        let mut writer =
            Writer::<0>::create(&temp, SEGMENT_SIZE, HEADER_SIZE).expect("failed to create writer");

        let records = vec![b"first".as_slice(), b"second", b"third"];
        for data in &records {
            writer.append(&[], data).expect("failed to append");
        }
        writer.sync().expect("failed to sync");
        let flushed = writer.flushed_offset();
        drop(writer);

        // Iterator should start from HEADER_SIZE
        let mut reader = Reader::<0>::open(&temp, Some(flushed)).expect("failed to open reader");
        let mut iter = reader.iter(HEADER_SIZE);

        let mut expected_offset = HEADER_SIZE;
        for expected in &records {
            let record = iter
                .next_record()
                .expect("failed to read")
                .expect("no record");
            assert_eq!(record.offset, expected_offset);
            assert!(record.offset >= HEADER_SIZE);
            assert_eq!(&*record.data, *expected);
            assert_eq!(record.len, RECORD_HEAD_SIZE + expected.len());
            assert_eq!(&*record.header, &[]);
            expected_offset += record.len as u64;
        }

        assert!(iter.next_record().expect("failed to read").is_none());
    }

    #[test]
    fn test_start_offset_remaining_bytes() {
        let temp = temp_path();
        const HEADER_SIZE: u64 = 100;
        let segment_size = 1000;
        let mut writer =
            Writer::<0>::create(&temp, segment_size, HEADER_SIZE).expect("failed to create writer");

        // Remaining should not include header space
        assert_eq!(writer.remaining_bytes(), segment_size as u64 - HEADER_SIZE);

        let data = vec![0u8; 50];
        writer.append(&[], &data).expect("failed to append");

        assert_eq!(
            writer.remaining_bytes(),
            segment_size as u64 - HEADER_SIZE - (RECORD_HEAD_SIZE + data.len()) as u64
        );
    }

    #[test]
    fn test_open_with_corruption_after_header() {
        use std::io::Seek;
        use std::io::Write as _;

        let temp = temp_path();
        const HEADER_SIZE: u64 = 64;
        let mut writer =
            Writer::<0>::create(&temp, SEGMENT_SIZE, HEADER_SIZE).expect("failed to create writer");

        writer.append(&[], b"good data").expect("failed to append");
        writer.commit(0).expect("failed to commit");
        let good_offset = writer.write_offset();

        // Manually corrupt the file after valid data
        drop(writer);
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .open(&temp)
            .expect("failed to open file");
        file.seek(std::io::SeekFrom::Start(good_offset))
            .expect("failed to seek");
        file.write_all(&[0xFF; 100])
            .expect("failed to write garbage");
        drop(file);

        // Opening should detect corruption and stop at the last valid record
        let writer =
            Writer::<0>::open(&temp, SEGMENT_SIZE, HEADER_SIZE).expect("failed to open writer");
        assert_eq!(writer.write_offset(), good_offset);
    }

    // Header Tests

    #[test]
    fn test_append_with_header_h8() {
        let temp = temp_path();
        let mut writer =
            Writer::<8>::create(&temp, SEGMENT_SIZE, 0).expect("failed to create writer");

        let header = [1u8, 2, 3, 4, 5, 6, 7, 8];
        let data = b"hello world";
        let (offset, len) = writer.append(&header, data).expect("failed to append");
        assert_eq!(offset, 0);
        assert_eq!(len, RECORD_HEAD_SIZE + 8 + data.len());

        writer.sync().expect("failed to sync");
        let flushed = writer.flushed_offset();
        drop(writer);

        let mut reader = Reader::<8>::open(&temp, Some(flushed)).expect("failed to open reader");
        let record = reader
            .read_record(offset, ReadHint::Random)
            .expect("failed to read");

        assert_eq!(&*record.header, &header);
        assert_eq!(&*record.data, data);
        assert_eq!(record.offset, offset);
        assert_eq!(record.len, len);
    }

    #[test]
    fn test_append_with_header_h16() {
        let temp = temp_path();
        let mut writer =
            Writer::<16>::create(&temp, SEGMENT_SIZE, 0).expect("failed to create writer");

        let header = [0xAAu8; 16];
        let data = b"test data";
        let (offset, len) = writer.append(&header, data).expect("failed to append");

        writer.sync().expect("failed to sync");
        let flushed = writer.flushed_offset();
        drop(writer);

        let mut reader = Reader::<16>::open(&temp, Some(flushed)).expect("failed to open reader");
        let record = reader
            .read_record(offset, ReadHint::Random)
            .expect("failed to read");

        assert_eq!(&*record.header, &header);
        assert_eq!(&*record.data, data);
        assert_eq!(record.offset, offset);
        assert_eq!(record.len, len);
    }

    #[test]
    fn test_append_with_header_h32() {
        let temp = temp_path();
        let mut writer =
            Writer::<32>::create(&temp, SEGMENT_SIZE, 0).expect("failed to create writer");

        let header = [0x42u8; 32];
        let data = b"larger header test";
        let (offset, len) = writer.append(&header, data).expect("failed to append");

        writer.sync().expect("failed to sync");
        let flushed = writer.flushed_offset();
        drop(writer);

        let mut reader = Reader::<32>::open(&temp, Some(flushed)).expect("failed to open reader");
        let record = reader
            .read_record(offset, ReadHint::Random)
            .expect("failed to read");

        assert_eq!(&*record.header, &header);
        assert_eq!(&*record.data, data);
        assert_eq!(record.offset, offset);
        assert_eq!(record.len, len);
    }

    #[test]
    fn test_append_multiple_records_with_headers() {
        let temp = temp_path();
        let mut writer =
            Writer::<8>::create(&temp, SEGMENT_SIZE, 0).expect("failed to create writer");

        let records = vec![
            ([1u8, 0, 0, 0, 0, 0, 0, 0], b"first".as_slice()),
            ([2u8, 0, 0, 0, 0, 0, 0, 0], b"second".as_slice()),
            ([3u8, 0, 0, 0, 0, 0, 0, 0], b"third".as_slice()),
        ];

        let mut offsets = Vec::new();
        for (header, data) in &records {
            let (offset, _) = writer.append(header, data).expect("failed to append");
            offsets.push(offset);
        }

        writer.sync().expect("failed to sync");
        let flushed = writer.flushed_offset();
        drop(writer);

        let mut reader = Reader::<8>::open(&temp, Some(flushed)).expect("failed to open reader");

        for (i, (expected_header, expected_data)) in records.iter().enumerate() {
            let record = reader
                .read_record(offsets[i], ReadHint::Random)
                .expect("failed to read");

            assert_eq!(&*record.header, expected_header);
            assert_eq!(&*record.data, *expected_data);
            assert_eq!(record.offset, offsets[i]);
        }
    }

    #[test]
    fn test_header_preserved_in_record() {
        let temp = temp_path();
        let mut writer =
            Writer::<8>::create(&temp, SEGMENT_SIZE, 0).expect("failed to create writer");

        // Use distinct header bytes to ensure they're preserved
        let header = [0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF];
        let data = b"check header preservation";
        writer.append(&header, data).expect("failed to append");

        writer.sync().expect("failed to sync");
        let flushed = writer.flushed_offset();
        drop(writer);

        let mut reader = Reader::<8>::open(&temp, Some(flushed)).expect("failed to open reader");
        let record = reader
            .read_record(0, ReadHint::Random)
            .expect("failed to read");

        // Verify exact header bytes
        assert_eq!(&*record.header, &header);
        assert_eq!(record.header[0], 0x01);
        assert_eq!(record.header[7], 0xEF);
    }

    #[test]
    fn test_iter_with_headers() {
        let temp = temp_path();
        let mut writer =
            Writer::<8>::create(&temp, SEGMENT_SIZE, 0).expect("failed to create writer");

        let records = vec![
            ([10u8, 0, 0, 0, 0, 0, 0, 0], b"first".as_slice()),
            ([20u8, 0, 0, 0, 0, 0, 0, 0], b"second".as_slice()),
            ([30u8, 0, 0, 0, 0, 0, 0, 0], b"third".as_slice()),
        ];

        for (header, data) in &records {
            writer.append(header, data).expect("failed to append");
        }

        writer.sync().expect("failed to sync");
        let flushed = writer.flushed_offset();
        drop(writer);

        let mut reader = Reader::<8>::open(&temp, Some(flushed)).expect("failed to open reader");
        let mut iter = reader.iter(0);

        let mut expected_offset = 0u64;
        for (expected_header, expected_data) in &records {
            let record = iter
                .next_record()
                .expect("failed to read")
                .expect("no record");

            assert_eq!(&*record.header, expected_header);
            assert_eq!(&*record.data, *expected_data);
            assert_eq!(record.offset, expected_offset);
            assert_eq!(record.len, RECORD_HEAD_SIZE + 8 + expected_data.len());
            expected_offset += record.len as u64;
        }

        assert!(iter.next_record().expect("failed to read").is_none());
    }

    #[test]
    fn test_random_and_sequential_read_with_headers() {
        let temp = temp_path();
        let mut writer =
            Writer::<8>::create(&temp, SEGMENT_SIZE, 0).expect("failed to create writer");

        let header = [0xFFu8; 8];
        let data = b"test both read hints";
        writer.append(&header, data).expect("failed to append");

        writer.sync().expect("failed to sync");
        let flushed = writer.flushed_offset();
        drop(writer);

        let mut reader = Reader::<8>::open(&temp, Some(flushed)).expect("failed to open reader");

        // Test Random hint
        let record_random = reader
            .read_record(0, ReadHint::Random)
            .expect("failed to read with random hint");
        assert_eq!(&*record_random.header, &header);
        assert_eq!(&*record_random.data, data);

        // Test Sequential hint
        let record_sequential = reader
            .read_record(0, ReadHint::Sequential)
            .expect("failed to read with sequential hint");
        assert_eq!(&*record_sequential.header, &header);
        assert_eq!(&*record_sequential.data, data);
    }

    #[test]
    fn test_writer_open_with_headers() {
        let temp = temp_path();
        let mut writer =
            Writer::<8>::create(&temp, SEGMENT_SIZE, 0).expect("failed to create writer");

        let header1 = [1u8; 8];
        let header2 = [2u8; 8];
        writer.append(&header1, b"first").expect("failed to append");
        writer
            .append(&header2, b"second")
            .expect("failed to append");
        writer.commit(0).expect("failed to commit");
        let expected_offset = writer.write_offset();
        drop(writer);

        // Reopen and verify recovery
        let mut writer =
            Writer::<8>::open(&temp, SEGMENT_SIZE, 0).expect("failed to reopen writer");
        assert_eq!(writer.write_offset(), expected_offset);

        // Append another record
        let header3 = [3u8; 8];
        writer.append(&header3, b"third").expect("failed to append");
        writer.commit(1).expect("failed to commit");
        let flushed = writer.flushed_offset();
        drop(writer);

        // Verify all three records
        let mut reader = Reader::<8>::open(&temp, Some(flushed)).expect("failed to open reader");
        let mut iter = reader.iter(0);

        let r1 = iter
            .next_record()
            .expect("failed to read")
            .expect("no record");
        assert_eq!(&*r1.header, &header1);

        let r2 = iter
            .next_record()
            .expect("failed to read")
            .expect("no record");
        assert_eq!(&*r2.header, &header2);

        let r3 = iter
            .next_record()
            .expect("failed to read")
            .expect("no record");
        assert_eq!(&*r3.header, &header3);
    }

    // Reserved-flag Tests

    #[test]
    fn reserved_high_bit_in_length_is_rejected() {
        // Bit 31 of the length field is reserved (formerly the compression flag) and must be
        // zero. A record with it set is rejected as corrupt, before the CRC is even checked.
        use std::io::Read as _;

        let temp = temp_path();
        let mut writer =
            Writer::<0>::create(&temp, SEGMENT_SIZE, 0).expect("failed to create writer");
        writer.append(&[], b"hello").expect("failed to append");
        writer.sync().expect("failed to sync");
        let flushed = writer.flushed_offset();
        drop(writer);

        // Flip bit 31 of the length field (the first 4 bytes, little-endian) directly on disk.
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&temp)
            .unwrap();
        let mut len_bytes = [0u8; 4];
        file.read_exact(&mut len_bytes).unwrap();
        let raw = u32::from_le_bytes(len_bytes) | 0x8000_0000;
        file.seek(std::io::SeekFrom::Start(0)).unwrap();
        file.write_all(&raw.to_le_bytes()).unwrap();
        file.sync_all().unwrap();
        drop(file);

        let mut reader = Reader::<0>::open(&temp, Some(flushed)).expect("failed to open reader");
        let err = reader.read_record(0, ReadHint::Random).unwrap_err();
        assert!(matches!(err, ReadError::Corrupt { .. }), "got {err:?}");
    }

    // replace_header Tests

    #[test]
    fn test_replace_header_h0_simple() {
        let temp = temp_path();
        let mut writer =
            Writer::<0>::create(&temp, SEGMENT_SIZE, 0).expect("failed to create writer");

        let data = b"test data for header replacement";
        writer.append(&[], data).expect("failed to append");

        writer.sync().expect("failed to sync");
        let flushed = writer.flushed_offset();
        drop(writer);

        let mut reader = Reader::<0>::open(&temp, Some(flushed)).expect("failed to open reader");

        // Replace empty header with another empty header (should work)
        reader
            .replace_header(0, [])
            .expect("failed to replace header");

        // Verify record is still readable
        let record = reader
            .read_record(0, ReadHint::Random)
            .expect("failed to read");
        assert_eq!(&*record.data, data);
        assert_eq!(&*record.header, &[]);
    }

    #[test]
    fn test_replace_header_h8() {
        let temp = temp_path();
        let mut writer =
            Writer::<8>::create(&temp, SEGMENT_SIZE, 0).expect("failed to create writer");

        let original_header = [1u8, 2, 3, 4, 5, 6, 7, 8];
        let data = b"test data for header replacement";
        writer
            .append(&original_header, data)
            .expect("failed to append");

        writer.sync().expect("failed to sync");
        let flushed = writer.flushed_offset();
        drop(writer);

        let mut reader = Reader::<8>::open(&temp, Some(flushed)).expect("failed to open reader");

        // Replace with new header
        let new_header = [10u8, 20, 30, 40, 50, 60, 70, 80];
        reader
            .replace_header(0, new_header)
            .expect("failed to replace header");

        // Verify new header is in place
        let record = reader
            .read_record(0, ReadHint::Random)
            .expect("failed to read");
        assert_eq!(&*record.header, &new_header);
        assert_eq!(&*record.data, data);
    }

    #[test]
    fn test_replace_header_h16() {
        let temp = temp_path();
        let mut writer =
            Writer::<16>::create(&temp, SEGMENT_SIZE, 0).expect("failed to create writer");

        let original_header = [0xAAu8; 16];
        let data = b"data with 16-byte header";
        writer
            .append(&original_header, data)
            .expect("failed to append");

        writer.sync().expect("failed to sync");
        let flushed = writer.flushed_offset();
        drop(writer);

        let mut reader = Reader::<16>::open(&temp, Some(flushed)).expect("failed to open reader");

        // Replace header
        let new_header = [0xBBu8; 16];
        reader
            .replace_header(0, new_header)
            .expect("failed to replace header");

        // Verify replacement
        let record = reader
            .read_record(0, ReadHint::Random)
            .expect("failed to read");
        assert_eq!(&*record.header, &new_header);
        assert_eq!(&*record.data, data);
    }

    #[test]
    fn test_replace_header_updates_crc() {
        let temp = temp_path();
        let mut writer =
            Writer::<8>::create(&temp, SEGMENT_SIZE, 0).expect("failed to create writer");

        let original_header = [1u8; 8];
        let data = b"test crc update";
        writer
            .append(&original_header, data)
            .expect("failed to append");

        writer.sync().expect("failed to sync");
        let flushed = writer.flushed_offset();
        drop(writer);

        let mut reader = Reader::<8>::open(&temp, Some(flushed)).expect("failed to open reader");

        // Replace header
        let new_header = [2u8; 8];
        reader
            .replace_header(0, new_header)
            .expect("failed to replace header");

        // CRC should be updated - record should be readable without CRC error
        let record = reader
            .read_record(0, ReadHint::Random)
            .expect("failed to read - CRC should be valid");
        assert_eq!(&*record.header, &new_header);
        assert_eq!(&*record.data, data);
    }

    #[test]
    fn test_replace_header_preserves_data() {
        let temp = temp_path();
        let mut writer =
            Writer::<8>::create(&temp, SEGMENT_SIZE, 0).expect("failed to create writer");

        let original_header = [0x11u8; 8];
        let data = b"this data should remain unchanged";
        writer
            .append(&original_header, data)
            .expect("failed to append");

        writer.sync().expect("failed to sync");
        let flushed = writer.flushed_offset();
        drop(writer);

        let mut reader = Reader::<8>::open(&temp, Some(flushed)).expect("failed to open reader");

        // Read original data
        let original_record = reader
            .read_record(0, ReadHint::Random)
            .expect("failed to read");
        let original_data = original_record.data.to_vec();

        // Replace header
        let new_header = [0x22u8; 8];
        reader
            .replace_header(0, new_header)
            .expect("failed to replace header");

        // Verify data is unchanged
        let new_record = reader
            .read_record(0, ReadHint::Random)
            .expect("failed to read");
        assert_eq!(&*new_record.data, &original_data);
        assert_eq!(&*new_record.data, data);
    }

    #[test]
    fn test_replaced_header_readable_random() {
        let temp = temp_path();
        let mut writer =
            Writer::<8>::create(&temp, SEGMENT_SIZE, 0).expect("failed to create writer");

        let original_header = [0xFFu8; 8];
        let data = b"readable after replacement";
        writer
            .append(&original_header, data)
            .expect("failed to append");

        writer.sync().expect("failed to sync");
        let flushed = writer.flushed_offset();
        drop(writer);

        let mut reader = Reader::<8>::open(&temp, Some(flushed)).expect("failed to open reader");

        let new_header = [0x00u8; 8];
        reader
            .replace_header(0, new_header)
            .expect("failed to replace header");

        // Read with Random hint
        let record = reader
            .read_record(0, ReadHint::Random)
            .expect("failed to read with random hint");
        assert_eq!(&*record.header, &new_header);
        assert_eq!(&*record.data, data);
    }

    #[test]
    fn test_replaced_header_readable_sequential() {
        let temp = temp_path();
        let mut writer =
            Writer::<8>::create(&temp, SEGMENT_SIZE, 0).expect("failed to create writer");

        let original_header = [0x11u8; 8];
        let data = b"sequential read after replacement";
        writer
            .append(&original_header, data)
            .expect("failed to append");

        writer.sync().expect("failed to sync");
        let flushed = writer.flushed_offset();
        drop(writer);

        let mut reader = Reader::<8>::open(&temp, Some(flushed)).expect("failed to open reader");

        let new_header = [0x22u8; 8];
        reader
            .replace_header(0, new_header)
            .expect("failed to replace header");

        // Read with Sequential hint
        let record = reader
            .read_record(0, ReadHint::Sequential)
            .expect("failed to read with sequential hint");
        assert_eq!(&*record.header, &new_header);
        assert_eq!(&*record.data, data);
    }

    #[test]
    fn test_replace_header_multiple_times() {
        let temp = temp_path();
        let mut writer =
            Writer::<8>::create(&temp, SEGMENT_SIZE, 0).expect("failed to create writer");

        let original_header = [1u8; 8];
        let data = b"replace multiple times";
        writer
            .append(&original_header, data)
            .expect("failed to append");

        writer.sync().expect("failed to sync");
        let flushed = writer.flushed_offset();
        drop(writer);

        let mut reader = Reader::<8>::open(&temp, Some(flushed)).expect("failed to open reader");

        // Replace multiple times
        for i in 2u8..10u8 {
            let new_header = [i; 8];
            reader
                .replace_header(0, new_header)
                .expect("failed to replace header");

            let record = reader
                .read_record(0, ReadHint::Random)
                .expect("failed to read");
            assert_eq!(&*record.header, &new_header);
            assert_eq!(&*record.data, data);
        }
    }

    #[test]
    fn test_replace_header_survives_reopen() {
        let temp = temp_path();
        let mut writer =
            Writer::<8>::create(&temp, SEGMENT_SIZE, 0).expect("failed to create writer");

        let original_header = [0xAAu8; 8];
        let data = b"persistence test";
        writer
            .append(&original_header, data)
            .expect("failed to append");

        writer.sync().expect("failed to sync");
        let flushed = writer.flushed_offset();
        drop(writer);

        // Open reader and replace header
        let mut reader =
            Reader::<8>::open(&temp, Some(flushed.clone())).expect("failed to open reader");
        let new_header = [0xBBu8; 8];
        reader
            .replace_header(0, new_header)
            .expect("failed to replace header");
        drop(reader);

        // Reopen reader and verify header change persists
        let mut reader2 = Reader::<8>::open(&temp, Some(flushed)).expect("failed to reopen reader");
        let record = reader2
            .read_record(0, ReadHint::Random)
            .expect("failed to read");
        assert_eq!(&*record.header, &new_header);
        assert_eq!(&*record.data, data);
    }

    #[test]
    fn test_replace_header_invalid_offset() {
        let temp = temp_path();
        let mut writer =
            Writer::<8>::create(&temp, SEGMENT_SIZE, 0).expect("failed to create writer");

        let header = [1u8; 8];
        let data = b"test data";
        writer.append(&header, data).expect("failed to append");

        writer.sync().expect("failed to sync");
        let flushed = writer.flushed_offset();
        drop(writer);

        let mut reader = Reader::<8>::open(&temp, Some(flushed)).expect("failed to open reader");

        // Try to replace header at non-existent offset
        let new_header = [2u8; 8];
        let result = reader.replace_header(9999, new_header);
        assert!(result.is_err());
    }

    #[test]
    fn test_replace_header_out_of_bounds() {
        let temp = temp_path();
        let writer = Writer::<8>::create(&temp, SEGMENT_SIZE, 0).expect("failed to create writer");
        let flushed = writer.flushed_offset();
        drop(writer);

        let mut reader = Reader::<8>::open(&temp, Some(flushed)).expect("failed to open reader");

        // Try to replace header beyond flushed offset
        let new_header = [1u8; 8];
        let result = reader.replace_header(0, new_header);
        assert!(result.is_err());
    }

    // Integration Tests

    // Batch Commit & Durability Tests

    #[test]
    fn test_append_batch_recovers_full() {
        let temp = temp_path();
        let mut writer =
            Writer::<0>::create(&temp, SEGMENT_SIZE, 0).expect("failed to create writer");

        writer
            .append_batch(&[(&[], b"a1"), (&[], b"a2")], 1)
            .expect("failed to append batch");
        let committed = writer.write_offset();
        drop(writer);

        let writer = Writer::<0>::open(&temp, SEGMENT_SIZE, 0).expect("failed to open writer");
        assert_eq!(writer.write_offset(), committed);
        assert_eq!(writer.last_committed_position(), Some(1));
    }

    #[test]
    fn test_recovery_rolls_back_torn_batch() {
        // A committed batch, then a second batch truncated at several points. In every case
        // recovery must roll back to the end of the first batch's commit marker.
        let record = RECORD_HEAD_SIZE + 2; // 8-byte head + 2-byte payload, no header
        let cuts = [
            5,                                 // mid record (inside b1)
            record,                            // at a record boundary (end of b1)
            3 * record,                        // at the boundary right before the marker
            3 * record + 4,                    // mid marker (inside its header)
            3 * record + RECORD_HEAD_SIZE + 4, // mid marker (inside its payload)
        ];

        for cut_within in cuts {
            let temp = temp_path();
            let mut writer =
                Writer::<0>::create(&temp, SEGMENT_SIZE, 0).expect("failed to create writer");

            writer
                .append_batch(&[(&[], b"a1"), (&[], b"a2")], 1)
                .expect("failed to append batch 1");
            let safe = writer.write_offset();

            writer
                .append_batch(&[(&[], b"b1"), (&[], b"b2"), (&[], b"b3")], 4)
                .expect("failed to append batch 2");
            drop(writer);

            // Simulate a crash mid-batch by truncating the tail of batch 2.
            let cut = safe + cut_within as u64;
            std::fs::OpenOptions::new()
                .write(true)
                .open(&temp)
                .expect("failed to open file")
                .set_len(cut)
                .expect("failed to truncate");

            let writer = Writer::<0>::open(&temp, SEGMENT_SIZE, 0).expect("failed to open writer");
            assert_eq!(
                writer.write_offset(),
                safe,
                "cut at {cut} should roll back to {safe}"
            );
            assert_eq!(writer.last_committed_position(), Some(1));
        }
    }

    #[test]
    fn test_recovery_rejects_batch_with_corrupt_record() {
        let temp = temp_path();
        let mut writer =
            Writer::<0>::create(&temp, SEGMENT_SIZE, 0).expect("failed to create writer");

        writer
            .append_batch(&[(&[], b"committed")], 1)
            .expect("failed to append batch 1");
        let safe = writer.write_offset();

        // A fully synced 3-record batch with an intact marker.
        writer
            .append_batch(&[(&[], b"r1data"), (&[], b"r2data"), (&[], b"r3data")], 4)
            .expect("failed to append batch 2");
        drop(writer);

        // Corrupt a byte in the data of record 1 of the second batch. Its marker is intact,
        // but validating the whole run must reject the entire batch.
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .open(&temp)
            .expect("failed to open file");
        file.seek(std::io::SeekFrom::Start(safe + RECORD_HEAD_SIZE as u64))
            .expect("failed to seek");
        file.write_all(&[0xFF]).expect("failed to corrupt");
        drop(file);

        let writer = Writer::<0>::open(&temp, SEGMENT_SIZE, 0).expect("failed to open writer");
        assert_eq!(writer.write_offset(), safe);
        assert_eq!(writer.last_committed_position(), Some(1));
    }

    #[test]
    fn test_control_length_not_misread_as_31_bit() {
        // The commit marker's length field has bit 30 (CONTROL_FLAG) set. Under the old
        // 31-bit mask this would decode as ~1 GiB and walk the scan off the segment; under
        // the 30-bit mask it is a 9-byte control record.
        let temp = temp_path();
        let mut writer =
            Writer::<0>::create(&temp, SEGMENT_SIZE, 0).expect("failed to create writer");

        writer
            .append_batch(&[(&[], b"x")], 7)
            .expect("failed to append batch");
        let flushed = writer.flushed_offset();
        drop(writer);

        let marker_offset = (RECORD_HEAD_SIZE + 1) as u64; // after the single 1-byte record

        // Sanity: the raw length field really does have bit 30 set and exceeds the segment
        // when masked with the old 31-bit mask.
        let raw = (COMMIT_MARKER_PAYLOAD as u32) | CONTROL_FLAG;
        assert!((raw & 0x7FFF_FFFF) as usize > SEGMENT_SIZE);

        let mut reader = Reader::<0>::open(&temp, Some(flushed)).expect("failed to open reader");
        let result = reader.read_record(marker_offset, ReadHint::Random);
        assert!(matches!(
            result,
            Err(ReadError::ControlRecord {
                offset,
                len,
            }) if offset == marker_offset && len == RECORD_HEAD_SIZE + COMMIT_MARKER_PAYLOAD
        ));

        // Iteration skips the marker and yields exactly the one data record.
        let mut iter = reader.iter(0);
        let rec = iter.next_record().expect("read").expect("record");
        assert_eq!(&*rec.data, b"x");
        assert!(iter.next_record().expect("read").is_none());
    }

    #[test]
    fn test_create_relative_path_no_parent() {
        let dir = tempfile::TempDir::new().expect("failed to create temp dir");
        let original = std::env::current_dir().expect("failed to read cwd");
        std::env::set_current_dir(dir.path()).expect("failed to set cwd");

        // A bare filename has an empty parent component; create must fall back to ".".
        let result = Writer::<0>::create(std::path::Path::new("segment.log"), SEGMENT_SIZE, 0);

        std::env::set_current_dir(&original).expect("failed to restore cwd");

        let writer = result.expect("create with bare filename should succeed");
        assert_eq!(writer.write_offset(), 0);
    }

    #[test]
    fn test_oversized_record_rejected_at_boundary() {
        let temp = temp_path();
        let mut writer =
            Writer::<0>::create(&temp, SEGMENT_SIZE, 0).expect("failed to create writer");

        // Default cap is a quarter of the segment.
        assert_eq!(writer.max_record(), SEGMENT_SIZE / 4);

        writer.set_max_record(1000);
        assert_eq!(writer.max_record(), 1000);

        // Exactly at the cap (total on-disk length == max_record) is accepted.
        let at_cap = vec![0u8; 1000 - RECORD_HEAD_SIZE];
        writer.append(&[], &at_cap).expect("record at the cap");

        // One byte over is rejected.
        let over_cap = vec![0u8; 1000 - RECORD_HEAD_SIZE + 1];
        let result = writer.append(&[], &over_cap);
        assert!(matches!(
            result,
            Err(WriteError::RecordTooLarge {
                size: 1001,
                max: 1000,
            })
        ));
    }

    #[test]
    fn test_set_max_record_clamped_to_hard_limit() {
        let temp = temp_path();
        let small = 1000;
        let mut writer = Writer::<0>::create(&temp, small, 0).expect("failed to create writer");

        // Requesting more than the segment allows clamps to leave room for a commit marker.
        writer.set_max_record(usize::MAX);
        assert_eq!(
            writer.max_record(),
            small - (RECORD_HEAD_SIZE + COMMIT_MARKER_PAYLOAD)
        );
    }

    #[test]
    fn test_create_start_offset_too_large() {
        let temp = temp_path();
        let result = Writer::<0>::create(&temp, 100, 100);
        assert!(matches!(
            result,
            Err(WriteError::InvalidStartOffset {
                size: 100,
                start_offset: 100,
            })
        ));
    }
}
