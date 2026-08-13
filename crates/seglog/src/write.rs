use std::fs::{File, OpenOptions};
use std::hint;
use std::io::{self, BufWriter, Seek, SeekFrom, Write};
use std::os::unix::fs::FileExt;
use std::path::Path;

use thiserror::Error;

use crate::read::{ReadError, is_truncation_marker};
use crate::{
    COMMIT_MARKER_PAYLOAD, CONTROL_FLAG, FlushedOffset, LEN_SIZE, LENGTH_MASK, MAX_RECORD_LEN,
    RECORD_HEAD_SIZE, calculate_crc32c, control, has_unknown_flags,
};

const WRITE_BUF_SIZE: usize = 16 * 1024; // 16 KB buffer

/// Errors that can occur during segment writing operations.
#[derive(Debug, Error)]
pub enum WriteError {
    #[error("segment full: attempted to write {attempted} bytes, only {available} bytes remaining")]
    SegmentFull { attempted: u64, available: u64 },
    #[error("segment size {size} must be greater than start offset {start_offset}")]
    InvalidStartOffset { size: usize, start_offset: u64 },
    #[error("record of {size} bytes exceeds the maximum record size of {max} bytes")]
    RecordTooLarge { size: usize, max: usize },
    #[error(transparent)]
    Read(#[from] ReadError),
    #[error(transparent)]
    Io(#[from] io::Error),
    #[cfg(target_os = "linux")]
    #[error(transparent)]
    Nix(#[from] nix::errno::Errno),
}

/// Writer for appending records to a segment file.
///
/// Manages buffered writes to a segment file with a fixed maximum size.
/// Tracks both the current write offset and the last flushed offset for concurrent read safety.
///
/// The generic parameter `H` is the size of the fixed per-record header in bytes. It is a
/// const generic (rather than a runtime value) so [`append`](Writer::append) can take a
/// `&[u8; H]` header on the stack.
#[derive(Debug)]
pub struct Writer<const H: usize> {
    writer: BufWriter<File>,
    size: usize,
    start_offset: u64,
    write_offset: u64,
    flushed_offset: FlushedOffset,
    dirty: bool,
    /// Largest total on-disk record length (header + payload) a single append may write.
    max_record: usize,
    /// Highest position from the last recovered commit marker, if any.
    last_position: Option<u64>,
}

impl Writer<0> {
    #[inline]
    pub fn append_data(&mut self, data: &[u8]) -> Result<(u64, usize), WriteError> {
        self.append(&[], data)
    }
}

impl<const H: usize> Writer<H> {
    /// Creates a new segment file at the specified path with the given size.
    ///
    /// # Arguments
    ///
    /// * `path` - The file path where the segment will be created
    /// * `size` - The total size of the segment file in bytes
    /// * `start_offset` - The byte offset where records will begin (reserves space for headers)
    ///
    /// The `start_offset` parameter allows reserving space at the beginning of the file for
    /// application-specific headers (e.g., magic bytes, version, metadata). Records will be
    /// written starting from this offset. Use `file()` to write header data to offsets before
    /// `start_offset`.
    ///
    /// On Linux, pre-allocates disk space using `fallocate` for better performance.
    /// Returns an error if the file already exists.
    ///
    /// # Example
    ///
    /// ```rust
    /// use seglog::write::Writer;
    /// # use std::io::Write;
    /// # use std::os::unix::fs::FileExt;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let dir = tempfile::TempDir::new()?;
    /// # let temp = dir.path().join("segment.log");
    /// const START_OFFSET: u64 = 16;
    /// let mut writer = Writer::<8>::create(&temp, 1024 * 1024, START_OFFSET)?;
    ///
    /// // Write file header before start_offset
    /// let magic_bytes = b"MYSEG";
    /// writer.file().write_all_at(magic_bytes, 0)?;
    ///
    /// // Append records with 8-byte headers (automatically start at START_OFFSET)
    /// let header = [0u8; 8];
    /// writer.append(&header, b"event data")?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn create(
        path: impl AsRef<Path>,
        size: usize,
        start_offset: u64,
    ) -> Result<Self, WriteError> {
        if size as u64 <= start_offset {
            return Err(WriteError::InvalidStartOffset { size, start_offset });
        }

        let path = path.as_ref();
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(path)?;

        // Pre-allocate on Linux
        #[cfg(target_os = "linux")]
        {
            // Crash point: ENOSPC on segment extension (the fallocate that grows a new segment).
            crate::crash_io!("segment_extend");
            nix::fcntl::fallocate(&file, nix::fcntl::FallocateFlags::empty(), 0, size as i64)?;
        }

        // Make the file, its size (a metadata change from fallocate, hence sync_all), and its
        // directory entry durable. The file must be durable before the directory entry that
        // names it, so sync the file first, then the parent directory.
        file.sync_all()?;
        #[cfg(unix)]
        {
            let parent = match path.parent() {
                Some(p) if !p.as_os_str().is_empty() => p,
                _ => Path::new("."),
            };
            File::open(parent)?.sync_all()?;
        }

        let write_offset = start_offset;
        let flushed_offset = FlushedOffset::new(write_offset);

        let mut writer = BufWriter::with_capacity(WRITE_BUF_SIZE, file);
        writer.seek(SeekFrom::Start(write_offset))?;

        Ok(Writer {
            writer,
            size,
            start_offset,
            write_offset,
            flushed_offset,
            dirty: false,
            max_record: default_max_record(size, start_offset),
            last_position: None,
        })
    }

    /// Opens an existing segment file for writing.
    ///
    /// # Arguments
    ///
    /// * `path` - The file path of the existing segment
    /// * `size` - The total size of the segment file in bytes
    /// * `start_offset` - The byte offset where records begin (must match the value used in `create`)
    ///
    /// Scans the file from `start_offset` to find the committed logical end and positions the
    /// write offset there. A batch is committed only if every record from the previous commit
    /// point validates by CRC and the run ends in a valid commit marker; records after the last
    /// valid marker are an incomplete batch and are rolled back.
    ///
    /// The `start_offset` must match the value used when the segment was created with `create()`.
    /// This allows the segment to skip over any header data when scanning for valid records.
    ///
    /// # Example
    ///
    /// ```rust
    /// use seglog::write::Writer;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let dir = tempfile::TempDir::new()?;
    /// # let temp = dir.path().join("segment.log");
    /// # let mut writer = Writer::<0>::create(&temp, 1024, 64)?;
    /// # writer.append(&[], b"data")?;
    /// # writer.commit(0)?;
    /// # drop(writer);
    /// const HEADER_SIZE: u64 = 64;
    ///
    /// // Open existing segment (skips header when scanning)
    /// let mut writer = Writer::<0>::open(&temp, 1024, HEADER_SIZE)?;
    ///
    /// // Continue appending records, committing the batch to make it durable
    /// writer.append(&[], b"more data")?;
    /// writer.commit(1)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn open(
        path: impl AsRef<Path>,
        size: usize,
        start_offset: u64,
    ) -> Result<Self, WriteError> {
        if size as u64 <= start_offset {
            return Err(WriteError::InvalidStartOffset { size, start_offset });
        }

        let file = OpenOptions::new().read(true).write(true).open(&path)?;

        let recovered = recover(&file, size, start_offset)?;
        let write_offset = recovered.committed_offset;
        #[cfg(feature = "tracing")]
        if let Some(position) = recovered.last_position {
            tracing::trace!("recovered committed position {position} at offset {write_offset}");
        }

        let mut writer = BufWriter::with_capacity(WRITE_BUF_SIZE, file);
        writer.seek(SeekFrom::Start(write_offset))?;

        let flushed_offset = FlushedOffset::new(write_offset);

        Ok(Writer {
            writer,
            size,
            start_offset,
            write_offset,
            flushed_offset,
            dirty: false,
            max_record: default_max_record(size, start_offset),
            last_position: recovered.last_position,
        })
    }

    /// Returns a reference to the file handle.
    pub fn file(&self) -> &File {
        self.writer.get_ref()
    }

    /// Returns the last flushed read only atomic offset.
    ///
    /// Any content before this value at any given time is immutable and safe to be read concurrently.
    #[inline]
    pub fn flushed_offset(&self) -> FlushedOffset {
        self.flushed_offset.clone()
    }

    /// Appends a record to the segment.
    ///
    /// # Arguments
    ///
    /// * `header` - Fixed-size header metadata (H bytes)
    /// * `data` - Variable-length data payload
    ///
    /// Returns the offset where the record was written and the total bytes written (including all headers and data).
    /// Returns an error if the segment does not have enough space remaining.
    pub fn append(&mut self, header: &[u8; H], data: &[u8]) -> Result<(u64, usize), WriteError> {
        let offset = self.write_offset;

        let total_record_len = RECORD_HEAD_SIZE + H + data.len();
        if total_record_len > self.max_record {
            hint::cold_path();
            return Err(WriteError::RecordTooLarge {
                size: total_record_len,
                max: self.max_record,
            });
        }
        if offset as usize + total_record_len > self.size {
            hint::cold_path();
            return Err(WriteError::SegmentFull {
                attempted: offset,
                available: self.size as u64 - offset,
            });
        }

        self.dirty = true;

        // Crash point: a short write on the segment file (a data record that makes no
        // progress). Surfaces as an error so the batch rewinds and nothing is acked.
        crate::crash_io!("commit_shortwrite");

        // The length field is the total payload length (header + data); bit 30 (control) and
        // bit 31 (reserved) are always zero for a caller data record.
        let length_bytes = ((H + data.len()) as u32).to_le_bytes();
        let crc = calculate_crc32c(&length_bytes, header, data);

        self.writer.write_all(&length_bytes)?;
        self.writer.write_all(&crc.to_le_bytes())?;
        self.writer.write_all(header)?;
        self.writer.write_all(data)?;

        self.write_offset += total_record_len as u64;

        Ok((offset, total_record_len))
    }

    /// Appends a batch of records followed by a commit marker, then syncs.
    ///
    /// The records and the trailing marker are made durable as a unit by a single sync. On
    /// recovery the batch is kept only if every record validates and the marker is present,
    /// so a crash mid-batch rolls the whole batch back. `highest_position` is the highest
    /// global position in the batch and is stored in the marker.
    ///
    /// The caller must ensure the batch fits within the segment (roll over before writing a
    /// batch rather than splitting one across segments).
    pub fn append_batch(
        &mut self,
        records: &[(&[u8; H], &[u8])],
        highest_position: u64,
    ) -> Result<u64, WriteError> {
        for (header, data) in records {
            self.append(header, data)?;
        }
        self.commit(highest_position)
    }

    /// Writes a commit marker covering all records appended since the last commit, then syncs.
    ///
    /// `highest_position` is the highest global position in the batch, stored in the marker so
    /// recovery can restore the next position.
    pub fn commit(&mut self, highest_position: u64) -> Result<u64, WriteError> {
        self.write_commit_marker(highest_position)?;
        // Crash point: batch assembled and the commit marker written, before the fsync. The
        // marker sits in the buffer or page cache; recovery must discard the whole batch.
        crate::crash_point!("commit_before_fsync");
        self.last_position = Some(highest_position);
        self.sync()
    }

    fn write_commit_marker(&mut self, highest_position: u64) -> Result<(), WriteError> {
        let offset = self.write_offset;
        let total = RECORD_HEAD_SIZE + COMMIT_MARKER_PAYLOAD;
        if offset as usize + total > self.size {
            return Err(WriteError::SegmentFull {
                attempted: offset,
                available: self.size as u64 - offset,
            });
        }

        let length_with_flag = (COMMIT_MARKER_PAYLOAD as u32) | CONTROL_FLAG;
        let length_bytes = length_with_flag.to_le_bytes();

        let mut payload = [0u8; COMMIT_MARKER_PAYLOAD];
        payload[0] = control::BATCH_COMMIT;
        payload[1..].copy_from_slice(&highest_position.to_le_bytes());

        let crc = calculate_crc32c(&length_bytes, &[], &payload);

        self.dirty = true;
        self.writer.write_all(&length_bytes)?;
        self.writer.write_all(&crc.to_le_bytes())?;
        // Crash point: leave a torn trailing marker. The header, CRC, and the kind byte reach the
        // page cache, but the position bytes stay as the fallocated zeros, so the marker's CRC no
        // longer matches. Recovery must reject this run (the marker is invalid); a recovery that
        // skips the trailing-record CRC would wrongly adopt it.
        #[cfg(feature = "crash-points")]
        if crate::crash_points::armed("torn_marker") {
            let _ = self.writer.write_all(&payload[..1]);
            let _ = self.writer.flush();
            std::process::abort();
        }
        self.writer.write_all(&payload)?;
        self.write_offset += total as u64;

        Ok(())
    }

    /// Returns the maximum total record length (header + payload) a single append may write.
    pub fn max_record(&self) -> usize {
        self.max_record
    }

    /// Sets the maximum record size, clamped so a commit marker always still fits after the
    /// largest record.
    pub fn set_max_record(&mut self, max_record: usize) {
        self.max_record = max_record.min(record_hard_limit(self.size, self.start_offset));
    }

    /// Highest position from the last recovered or written commit marker, if any.
    pub fn last_committed_position(&self) -> Option<u64> {
        self.last_position
    }

    /// Returns the current write offset where the next record will be written.
    pub fn write_offset(&self) -> u64 {
        self.write_offset
    }

    /// Returns the number of bytes remaining in the segment.
    pub fn remaining_bytes(&self) -> u64 {
        self.size as u64 - self.write_offset
    }

    /// Truncates the segment to the specified offset.
    ///
    /// Writes a zero-filled header as a truncation marker at the offset and updates
    /// both the write offset and flushed offset. No-op if the offset is >= current write offset.
    pub fn set_len(&mut self, offset: u64) -> Result<(), WriteError> {
        if offset >= self.write_offset {
            hint::cold_path();
            return Ok(());
        }

        self.sync()?;

        self.flushed_offset.set(offset);
        self.write_offset = offset;

        // Write full zero header as clear truncation marker
        let zero_header = [0u8; RECORD_HEAD_SIZE];
        self.writer.get_ref().write_all_at(&zero_header, offset)?;
        self.writer.get_ref().sync_data()?;

        Ok(())
    }

    /// Discards everything appended since `offset`, resetting the write cursor there.
    ///
    /// Used to roll back a partially written batch after an append error. It is only
    /// meaningful to rewind into unsynced territory: `offset` must be at or after the
    /// last flushed offset (nothing durable is being undone) and at or before the
    /// current write offset. No-op if `offset >= write_offset`.
    ///
    /// The bytes on disk beyond `offset` are left in place as garbage. They carry no
    /// commit marker, so `recover` discards them on the next open, and the next
    /// append overwrites them. This relies on segment files never being recycled.
    pub fn rewind_to(&mut self, offset: u64) -> Result<(), WriteError> {
        if offset >= self.write_offset {
            hint::cold_path();
            return Ok(());
        }
        debug_assert!(
            offset >= self.flushed_offset.load(),
            "rewind_to must not discard flushed (durable) data"
        );

        // Reposition the underlying file. `BufWriter::seek` flushes the buffered tail
        // first; those bytes hit the page cache unsynced and will be overwritten by the
        // next append or discarded by recovery.
        self.writer.seek(SeekFrom::Start(offset))?;
        self.write_offset = offset;
        // Nothing new sits between the flushed point and the cursor now, so clear
        // dirty when they coincide; a later `sync` on an untouched writer must be a
        // genuine no-op, not a spurious fsync.
        self.dirty = offset > self.flushed_offset.load();
        Ok(())
    }

    /// Flushes the buffered writer without syncing to disk.
    ///
    /// This ensures data is written to the OS but does not guarantee persistence to disk.
    pub fn flush_writer(&mut self) -> Result<(), WriteError> {
        self.writer.flush()?;
        Ok(())
    }

    /// Flushes the file, ensuring all data is persisted to disk.
    pub fn sync(&mut self) -> Result<u64, WriteError> {
        if self.dirty {
            #[cfg(feature = "tracing")]
            tracing::trace!("flushing writer");
            self.writer.flush()?;
            // Crash point: fsync returns EIO. Tephra must treat this as fatal for the batch
            // and ack nothing from it, never retry over a dropped dirty page.
            crate::crash_io!("commit_fsync");
            self.writer.get_ref().sync_data()?;
            self.flushed_offset.set(self.write_offset);
            self.dirty = false;
        }

        Ok(self.write_offset)
    }

    /// Closes the writer, ensuring all data is synced to disk.
    ///
    /// This is equivalent to calling `sync()` and then dropping the writer.
    pub fn close(mut self) -> Result<(), WriteError> {
        self.sync()?;
        Ok(())
    }
}

/// Largest total record length that still leaves room for a commit marker after `start_offset`.
fn record_hard_limit(size: usize, start_offset: u64) -> usize {
    (size as u64)
        .saturating_sub(start_offset)
        .saturating_sub((RECORD_HEAD_SIZE + COMMIT_MARKER_PAYLOAD) as u64) as usize
}

/// Default max record size. A quarter of the segment avoids one record thrashing rollover,
/// but never exceeds the hard limit or the encodable length.
fn default_max_record(size: usize, start_offset: u64) -> usize {
    (size / 4)
        .min(record_hard_limit(size, start_offset))
        .min(MAX_RECORD_LEN)
}

struct Recovered {
    committed_offset: u64,
    last_position: Option<u64>,
}

/// Scans a segment to find its committed logical end.
///
/// A batch is committed iff every record from the previous commit point onward validates by
/// CRC and the run terminates in a valid commit marker. Marker-present alone is insufficient:
/// fsync gives no ordering guarantee within a flush, so a trailing marker can be durable while
/// an earlier page of the same batch is torn. The entire run is validated, not just the
/// terminator. Records after the last valid marker are an incomplete batch and are rolled back.
///
/// This relies on segment files never being recycled: `fallocate` zeroes a freshly created
/// file, so trailing unwritten space reads back as a truncation marker rather than stale bytes
/// that could pass CRC.
fn recover(file: &File, size: usize, start_offset: u64) -> Result<Recovered, WriteError> {
    let mut committed_offset = start_offset;
    let mut last_position = None;
    let mut cursor = start_offset;

    let mut head = [0u8; RECORD_HEAD_SIZE];
    let mut payload = Vec::new();
    loop {
        let payload_offset = cursor + RECORD_HEAD_SIZE as u64;
        if payload_offset > size as u64 || file.read_exact_at(&mut head, cursor).is_err() {
            break;
        }
        if is_truncation_marker(&head) {
            break;
        }

        let raw = u32::from_le_bytes(head[..LEN_SIZE].try_into().unwrap());
        if has_unknown_flags(raw) {
            break;
        }
        let is_control = raw & CONTROL_FLAG != 0;
        let payload_len = (raw & LENGTH_MASK) as usize;
        let record_end = payload_offset + payload_len as u64;
        if record_end > size as u64 {
            break;
        }

        let crc = u32::from_le_bytes(head[LEN_SIZE..RECORD_HEAD_SIZE].try_into().unwrap());
        payload.resize(payload_len, 0);
        if file.read_exact_at(&mut payload, payload_offset).is_err() {
            break;
        }
        // Header and data are contiguous in `payload`, so the CRC covers the length field plus
        // the whole payload for both data and control records.
        if calculate_crc32c(&raw.to_le_bytes(), &[], &payload) != crc {
            break;
        }

        // Only a valid commit marker advances the committed point; data records and other
        // control kinds are part of the pending, not-yet-committed run.
        if is_control && payload_len == COMMIT_MARKER_PAYLOAD && payload[0] == control::BATCH_COMMIT
        {
            committed_offset = record_end;
            last_position = Some(u64::from_le_bytes(payload[1..].try_into().unwrap()));
            // Crash point: a crash during recovery itself, once a commit marker has been
            // accepted. The next open must reach the same committed point.
            crate::crash_point!("recovery_midway");
        }
        cursor = record_end;
    }

    Ok(Recovered {
        committed_offset,
        last_position,
    })
}
