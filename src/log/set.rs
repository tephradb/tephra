//! SegmentSet: the collection of segment files and the mapping from global
//! positions to bytes.
//!
//! Turns N independent `seglog` files into one logically continuous,
//! position-addressed log. Everything about *what* an event is stays above it;
//! everything about record framing stays below it (in `seglog`).
//!
//! Positions are 1-based: the first event stored is position 1, and `Position::ZERO`
//! is reserved to mean "empty" (no events yet). The first segment's `base_position`
//! is therefore 1.
//!
//! Invariants enforced here:
//!
//! 1. Segments are position-disjoint and contiguous: segment N's `base_position`
//!    equals segment N-1's `base_position + event_count`. The first segment's
//!    `base_position` is 1.
//! 2. Exactly one segment is active (writable) at a time; the rest are sealed
//!    and immutable.
//! 3. A batch never spans segments.
//! 4. Segment files are never recycled or reused.

use std::borrow::Cow;
use std::cmp::Ordering;
use std::fmt;
use std::fs::{self, File};
use std::io;
use std::mem;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};

use thiserror::Error;
use tracing::{trace, warn};

use seglog::read::{ReadError, ReadHint, Reader, RecordKind};
use seglog::write::{WriteError, Writer};
use seglog::{COMMIT_MARKER_PAYLOAD, FlushedOffset, RECORD_HEAD_SIZE};

use crate::Position;
use crate::log::header::{HeaderError, SEGMENT_HEADER_SIZE, SegmentHeader};

/// Number of digits in a segment file name. Twenty digits covers `u64::MAX`, so
/// zero-padded lexicographic order equals numeric order.
const NAME_DIGITS: usize = 20;

/// The first position assigned in a fresh log. `Position::ZERO` is reserved to mean
/// "empty", so events (and the first segment's base) start at 1.
const FIRST_POSITION: u64 = 1;

/// Per-record framing overhead (length + CRC), on top of the record's own bytes.
/// Exposed so the write coordinator can budget batch sizes without importing seglog.
pub const RECORD_OVERHEAD: usize = RECORD_HEAD_SIZE;

/// Fixed overhead a batch pays once for its trailing commit marker (the marker's own
/// record frame plus its payload).
pub const BATCH_OVERHEAD: usize = RECORD_HEAD_SIZE + COMMIT_MARKER_PAYLOAD;

/// Configuration for the segments in a set. All segments in a set share it.
#[derive(Clone, Copy, Debug)]
pub struct SegmentConfig {
    /// Total size of each segment file in bytes (including the header).
    pub segment_size: usize,
    /// Largest total on-disk record length a single record may occupy.
    pub max_record_len: usize,
    /// Bytes reserved at the start of every segment for its [`SegmentHeader`].
    pub header_size: usize,
}

impl SegmentConfig {
    /// Config for the given segment size with the conventional defaults:
    /// `max_record_len = segment_size / 4` and `header_size = SEGMENT_HEADER_SIZE`.
    ///
    /// The result is not guaranteed valid for very small `segment_size`; that is
    /// checked by [`SegmentSet::open`] via [`validate`](Self::validate).
    pub fn new(segment_size: usize) -> Self {
        SegmentConfig {
            segment_size,
            max_record_len: segment_size / 4,
            header_size: SEGMENT_HEADER_SIZE,
        }
    }

    /// Rejects configs that cannot address, or cannot usefully store, records.
    ///
    /// Validated once at open rather than defended against with a panic on every
    /// append (byte offsets are stored as `u32`, so segments must stay under 4 GiB).
    pub fn validate(&self) -> Result<(), LogError> {
        let invalid = |reason: String| Err(LogError::InvalidConfig { reason });

        if self.segment_size <= self.header_size {
            return invalid(format!(
                "segment_size {} must exceed header_size {}",
                self.segment_size, self.header_size
            ));
        }
        if self.segment_size > u32::MAX as usize {
            return invalid(format!(
                "segment_size {} exceeds u32::MAX; byte offsets are stored as u32",
                self.segment_size
            ));
        }
        if self.max_record_len < RECORD_HEAD_SIZE {
            return invalid(format!(
                "max_record_len {} is smaller than a record header ({RECORD_HEAD_SIZE} bytes)",
                self.max_record_len
            ));
        }
        let usable = self.segment_size - self.header_size;
        let need = self.max_record_len + RECORD_HEAD_SIZE + COMMIT_MARKER_PAYLOAD;
        if need > usable {
            return invalid(format!(
                "a max-size record plus its commit marker ({need} bytes) does not fit a \
                 segment's usable space ({usable} bytes)"
            ));
        }
        Ok(())
    }
}

/// A single segment file: one `seglog` file, its base position, and the offset
/// sidecar mapping local position to byte offset.
///
/// Shared behind `Arc` so a reader can hold a segment across a rollover while the
/// set swaps the active segment without invalidating it.
pub struct Segment {
    base_position: Position,
    path: PathBuf,
    /// Durable extent used by readers. `Some` for the active segment (shared with
    /// its writer, so reads follow the flushed point live) and for segments created
    /// this run; `None` for segments sealed at startup, where the reader derives
    /// the extent from the file length.
    ///
    /// The asymmetry is benign: a sealed segment is fully synced, so its committed
    /// end is followed by the zero-filled `fallocate` tail. Reading against the
    /// frozen `Some` extent and reading against the file length (`None`) therefore
    /// yield the same records — both stop at the same truncation marker.
    flushed_offset: Option<FlushedOffset>,
    /// Byte offset of each data record, indexed by `position - base_position`.
    /// Never persisted in v1: derivable by one sequential scan on open.
    offsets: RwLock<Vec<u32>>,
    /// A cached reader for random reads. Segments are immutable, so one open fd is
    /// reusable indefinitely; the `Mutex` serializes the reader's internal buffers.
    reader: Mutex<Option<Reader<0>>>,
}

impl Segment {
    /// The base (first) global position of this segment.
    pub fn base_position(&self) -> Position {
        self.base_position
    }

    /// Number of events (data records) currently in this segment.
    pub fn event_count(&self) -> u64 {
        self.offsets.read().unwrap().len() as u64
    }
}

impl fmt::Debug for Segment {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Segment")
            .field("base_position", &self.base_position)
            .field("path", &self.path)
            .field("event_count", &self.offsets.read().unwrap().len())
            .finish_non_exhaustive()
    }
}

/// A record read back out of the log: its global position and payload.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Record {
    pub position: Position,
    pub data: Vec<u8>,
}

/// A borrowed view of a record yielded by [`Scan`], pointing directly into the
/// reader's read-ahead buffer for zero-copy sequential scans. It is valid only
/// until the next [`Scan::next`] call; use [`to_owned`](RecordRef::to_owned) to
/// keep it beyond that.
#[derive(Clone, Copy, Debug)]
pub struct RecordRef<'a> {
    pub position: Position,
    pub data: &'a [u8],
}

impl RecordRef<'_> {
    /// Copies the view into an owned [`Record`].
    pub fn to_owned(&self) -> Record {
        Record {
            position: self.position,
            data: self.data.to_vec(),
        }
    }
}

/// The inclusive range of positions assigned to an appended batch.
///
/// A batch always contains at least one record, so a range always covers at least
/// one position; there is no empty range.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PositionRange {
    pub first: Position,
    pub last: Position,
}

impl PositionRange {
    /// Number of positions in the range (always at least 1).
    pub fn count(&self) -> u64 {
        (self.last - self.first) + 1
    }
}

/// Owns the collection of segment files and the global-position addressing over them.
#[derive(Debug)]
pub struct SegmentSet {
    dir: PathBuf,
    config: SegmentConfig,
    /// Sealed, immutable segments ordered by `base_position`.
    sealed: Vec<Arc<Segment>>,
    /// The single active (writable) segment.
    active: Arc<Segment>,
    /// The writer for the active segment. Kept out of `Segment` because `Segment`
    /// is shared read-only via `Arc`; only the set writes.
    active_writer: Writer<0>,
    /// The next global position to assign. A fresh log starts at [`FIRST_POSITION`].
    next_position: Position,
}

impl SegmentSet {
    /// Opens (or creates) the segment set rooted at `dir`.
    ///
    /// On an empty directory this creates the first segment (base position 1).
    /// Otherwise it reads every segment header, verifies the base-position chain is
    /// contiguous, and runs crash recovery on the last (active) segment, rolling
    /// back any incomplete trailing batch. Any gap, overlap, or corruption is a hard
    /// error: the set refuses to open rather than guess.
    pub fn open(dir: impl AsRef<Path>, config: SegmentConfig) -> Result<Self, LogError> {
        config.validate()?;
        let dir = dir.as_ref().to_path_buf();

        // 1. Create dir if absent, and make its directory entry durable.
        if !dir.exists() {
            fs::create_dir_all(&dir).map_err(|source| LogError::io(&dir, source))?;
            if let Some(parent) = dir.parent()
                && !parent.as_os_str().is_empty()
            {
                sync_dir(parent).map_err(|source| LogError::io(parent, source))?;
            }
        }

        // 2. Read the directory, keep only well-named segment files, sort numerically.
        let mut entries: Vec<(Position, PathBuf)> = Vec::new();
        for entry in fs::read_dir(&dir).map_err(|source| LogError::io(&dir, source))? {
            let entry = entry.map_err(|source| LogError::io(&dir, source))?;
            if let Some(base) = parse_base_position(&entry.file_name().to_string_lossy()) {
                entries.push((base, entry.path()));
            }
        }
        entries.sort_by_key(|(base, _)| *base);

        // 3. Read each header. An unwritten (all-zero) header is a crash between
        //    create and header write; it is only legal on the last file.
        let n = entries.len();
        let mut valid: Vec<(Position, PathBuf)> = Vec::new();
        for (i, (name_base, path)) in entries.iter().enumerate() {
            let buf = read_header(path)?;
            match SegmentHeader::from_bytes(&buf) {
                Ok(header) => {
                    if header.base_position != *name_base {
                        return Err(LogError::BasePositionMismatch {
                            path: path.clone(),
                            header: header.base_position,
                            name: *name_base,
                        });
                    }
                    valid.push((*name_base, path.clone()));
                }
                Err(HeaderError::Unwritten) if i == n - 1 => {
                    // A segment created but never header-written. Legal only as the
                    // trailing file; drop it and continue.
                    warn!(
                        "deleting unwritten trailing segment {path:?} (crash between create and header write)"
                    );
                    fs::remove_file(path).map_err(|source| LogError::io(path, source))?;
                    sync_dir(&dir).map_err(|source| LogError::io(&dir, source))?;
                }
                Err(HeaderError::Unwritten) => {
                    return Err(LogError::UnwrittenNonLast { path: path.clone() });
                }
                Err(source) => {
                    return Err(LogError::Header {
                        path: path.clone(),
                        source,
                    });
                }
            }
        }

        // 7. Empty directory (or only an unwritten file we just deleted): fresh log.
        if valid.is_empty() {
            let (writer, active) =
                Self::create_segment(&dir, &config, Position::new(FIRST_POSITION))?;
            trace!("initialized empty segment set at {dir:?}");
            return Ok(SegmentSet {
                dir,
                config,
                sealed: Vec::new(),
                active,
                active_writer: writer,
                next_position: Position::new(FIRST_POSITION),
            });
        }

        // 4 & 5. Build sealed segments (scan to count events + rebuild sidecar),
        //         verifying contiguity, then recover the active segment.
        let (active_entry, sealed_entries) = valid.split_last().unwrap();
        let mut sealed = Vec::with_capacity(sealed_entries.len());
        let mut expected_base = Position::new(FIRST_POSITION);
        for (base, path) in sealed_entries {
            if *base != expected_base {
                return Err(LogError::NonContiguous {
                    path: path.clone(),
                    found: *base,
                    expected: expected_base,
                });
            }
            let offsets = scan_offsets(path, None, config.header_size as u64)?;
            expected_base = Position::new(*base + offsets.len() as u64);
            sealed.push(Arc::new(Segment {
                base_position: *base,
                path: path.clone(),
                flushed_offset: None,
                offsets: RwLock::new(offsets),
                reader: Mutex::new(None),
            }));
        }

        let (active_base, active_path) = active_entry;
        if *active_base != expected_base {
            return Err(LogError::NonContiguous {
                path: active_path.clone(),
                found: *active_base,
                expected: expected_base,
            });
        }

        // 6. Recovery: reopen the active segment for writing, rolling back any
        //    incomplete trailing batch to the last valid commit point.
        let mut writer =
            Writer::<0>::open(active_path, config.segment_size, config.header_size as u64)
                .map_err(|source| LogError::write(active_path, source))?;
        writer.set_max_record(config.max_record_len);

        let committed = writer.write_offset();
        if trailing_bytes_present(&writer, committed, config.segment_size) {
            warn!(
                "segment {active_path:?} recovered with rollback, discarding bytes from offset {committed}"
            );
        } else {
            trace!("segment {active_path:?} opened cleanly at offset {committed}");
        }

        let flushed = writer.flushed_offset();
        let offsets = scan_offsets(
            active_path,
            Some(flushed.clone()),
            config.header_size as u64,
        )?;
        let count = offsets.len() as u64;

        // Cross-check the recovered commit marker against the event count. Position
        // assignment is contiguous from the base, so the last marker's highest
        // position must be base + count - 1.
        if let Some(highest) = writer.last_committed_position()
            && highest + 1 != *active_base + count
        {
            return Err(LogError::PositionMismatch {
                path: active_path.clone(),
                found: Position::new(highest),
                expected: Position::new(*active_base + count - 1),
            });
        }

        let next_position = Position::new(*active_base + count);
        let active = Arc::new(Segment {
            base_position: *active_base,
            path: active_path.clone(),
            flushed_offset: Some(flushed),
            offsets: RwLock::new(offsets),
            reader: Mutex::new(None),
        });

        Ok(SegmentSet {
            dir,
            config,
            sealed,
            active,
            active_writer: writer,
            next_position,
        })
    }

    /// Appends a batch of records as a single durable unit and returns the range
    /// of positions assigned. Called only by the write coordinator, single-threaded.
    ///
    /// A batch never spans segments: if it does not fit in the active segment's
    /// remaining space, the set rolls over first. A batch that cannot fit in an
    /// empty segment is rejected rather than looping.
    ///
    /// The append is all-or-nothing: if any record or the commit fails midway, the
    /// writer is rewound so no orphan records are left for the next batch to adopt.
    pub fn append_batch(&mut self, records: &[&[u8]]) -> Result<PositionRange, LogError> {
        if records.is_empty() {
            return Err(LogError::EmptyBatch);
        }

        // 1. Reject empty records (their zero-length frame collides with the
        //    zero-filled segment tail) and records over the configured maximum.
        for record in records {
            if record.is_empty() {
                return Err(LogError::EmptyRecord);
            }
            let record_len = RECORD_HEAD_SIZE + record.len();
            if record_len > self.config.max_record_len {
                return Err(LogError::RecordTooLarge {
                    size: record_len,
                    max: self.config.max_record_len,
                });
            }
        }

        // 2. Total encoded size, including the trailing commit marker.
        let records_len: usize = records.iter().map(|r| RECORD_HEAD_SIZE + r.len()).sum();
        let total_size = records_len + RECORD_HEAD_SIZE + COMMIT_MARKER_PAYLOAD;

        // A batch that can never fit even a fresh segment is a hard error.
        let capacity = self.config.segment_size - self.config.header_size;
        if total_size > capacity {
            return Err(LogError::BatchTooLarge {
                size: total_size,
                capacity,
            });
        }

        // 3. Roll over first if it does not fit in the active segment.
        if total_size as u64 > self.active_writer.remaining_bytes() {
            self.rollover()?;
        }

        // 4. Append records, then a commit marker carrying the highest position,
        //    made durable together by the single sync inside `commit`. On any
        //    failure, rewind so the file matches our in-memory view.
        let first = self.next_position;
        let last = Position::new(first + records.len() as u64 - 1);
        let rewind_to = self.active_writer.write_offset();
        let path = &self.active.path;

        let mut new_offsets = Vec::with_capacity(records.len());
        let outcome = (|| {
            for record in records {
                let (offset, _len) = self
                    .active_writer
                    .append_data(record)
                    .map_err(|source| LogError::write(path, source))?;
                new_offsets.push(
                    u32::try_from(offset)
                        .expect("segment_size <= u32::MAX enforced by SegmentConfig::validate"),
                );
            }
            self.active_writer
                .commit(last.get())
                .map_err(|source| LogError::write(path, source))?;
            Ok(())
        })();

        if let Err(err) = outcome {
            // Discard the partial batch. If even the rewind fails the writer is
            // wedged, so surface that; otherwise surface the original error.
            self.active_writer
                .rewind_to(rewind_to)
                .map_err(|source| LogError::write(path, source))?;
            return Err(err);
        }

        // 5. Extend the active segment's in-memory sidecar.
        self.active
            .offsets
            .write()
            .unwrap()
            .extend_from_slice(&new_offsets);

        // 6. Advance and return.
        self.next_position = last.next();
        Ok(PositionRange { first, last })
    }

    /// Seals the active segment and installs a fresh one at `next_position`.
    fn rollover(&mut self) -> Result<(), LogError> {
        // Seal: everything is already synced (the previous batch ended in a commit),
        // but sync defensively before dropping the writer.
        self.active_writer
            .sync()
            .map_err(|source| LogError::write(&self.active.path, source))?;

        let (writer, new_active) =
            Self::create_segment(&self.dir, &self.config, self.next_position)?;

        let old_active = mem::replace(&mut self.active, new_active);
        self.sealed.push(old_active);
        self.active_writer = writer;

        trace!(
            "rolled over to segment with base_position {}",
            self.next_position
        );
        Ok(())
    }

    /// Creates a new segment file: fallocate + write header + sync.
    fn create_segment(
        dir: &Path,
        config: &SegmentConfig,
        base: Position,
    ) -> Result<(Writer<0>, Arc<Segment>), LogError> {
        let path = dir.join(segment_file_name(base));

        // `create` fallocates (zero-filling) then makes the file and its directory
        // entry durable, so the file reads back as an unwritten segment until we
        // write the header. Writing the header only changes file content, not the
        // directory entry, so `sync_all` on the file is enough — no second fsync
        // of the directory is needed.
        let mut writer = Writer::<0>::create(&path, config.segment_size, config.header_size as u64)
            .map_err(|source| LogError::write(&path, source))?;
        writer.set_max_record(config.max_record_len);

        let header = SegmentHeader::new(base);
        writer
            .file()
            .write_all_at(&header.to_bytes(), 0)
            .map_err(|source| LogError::io(&path, source))?;
        writer
            .file()
            .sync_all()
            .map_err(|source| LogError::io(&path, source))?;

        let segment = Arc::new(Segment {
            base_position: base,
            path,
            flushed_offset: Some(writer.flushed_offset()),
            offsets: RwLock::new(Vec::new()),
            reader: Mutex::new(None),
        });
        Ok((writer, segment))
    }

    /// Reads a single record at `pos`. Optimized for a random access pattern.
    pub fn read_at(&self, pos: Position) -> Result<Record, LogError> {
        let segment = self
            .segment_for(pos)
            .ok_or(LogError::NotFound { position: pos })?;

        let local = pos.offset_from(segment.base_position) as usize;
        let offset = {
            let offsets = segment.offsets.read().unwrap();
            match offsets.get(local) {
                Some(offset) => *offset as u64,
                None => return Err(LogError::NotFound { position: pos }),
            }
        };

        // Reuse the segment's cached reader (one open fd per segment).
        let mut guard = segment.reader.lock().unwrap();
        if guard.is_none() {
            *guard = Some(self.open_reader(segment)?);
        }
        let record = guard
            .as_mut()
            .unwrap()
            .read_record(offset, ReadHint::Random)
            .map_err(|source| LogError::read(&segment.path, source))?;
        Ok(Record {
            position: pos,
            data: record.data.into_owned(),
        })
    }

    /// Returns a sequential scan of every record at or after `pos` (inclusive),
    /// rolling across segment boundaries and skipping control records silently.
    ///
    /// `pos` is clamped up to the first position, so `scan_from(Position::ZERO)` (the
    /// "before everything" empty sentinel) scans the whole log rather than nothing.
    /// It is a thin inclusive wrapper over [`scan_after`](Self::scan_after).
    pub fn scan_from(&self, pos: Position) -> Scan<'_> {
        self.scan_at(pos.max(Position::new(FIRST_POSITION)))
    }

    /// Returns a sequential scan of every record strictly after `pos` (exclusive).
    ///
    /// This is the natural primitive for subscriptions, which hold "the last
    /// position I processed": `scan_after(Position::ZERO)` scans the whole log with no
    /// sentinel special case, and `scan_after(last)` resumes right after `last`.
    pub fn scan_after(&self, pos: Position) -> Scan<'_> {
        self.scan_at(Position::new(pos.get().saturating_add(1)))
    }

    /// Core scan constructor: emits records beginning at `first` (inclusive).
    fn scan_at(&self, first: Position) -> Scan<'_> {
        // Caught up (or the empty sentinel): a subscription sitting at the end is a
        // normal, non-error state, so yield an empty stream rather than failing.
        if first == Position::ZERO || first >= self.next_position {
            return Scan::empty(self);
        }

        let (seg_idx, segment) = if first >= self.active.base_position {
            (self.sealed.len(), &self.active)
        } else {
            let i = self.sealed.partition_point(|s| s.base_position <= first);
            if i == 0 {
                return Scan::failed(self, LogError::NotFound { position: first });
            }
            (i - 1, &self.sealed[i - 1])
        };

        let local = first.offset_from(segment.base_position) as usize;
        let offset = match segment.offsets.read().unwrap().get(local) {
            Some(offset) => *offset as u64,
            None => return Scan::failed(self, LogError::NotFound { position: first }),
        };

        match self.open_reader(segment) {
            Ok(reader) => Scan {
                set: self,
                seg_idx,
                offset,
                position: first,
                reader: Some(reader),
                pending_err: None,
                done: false,
            },
            Err(err) => Scan::failed(self, err),
        }
    }

    /// The highest assigned position, or `Position::ZERO` if the log is empty.
    pub fn last_position(&self) -> Position {
        // Positions are 1-based and `next_position >= FIRST_POSITION`, so this never
        // underflows; an empty log yields `Position::ZERO`, the empty sentinel.
        Position::new(self.next_position - 1)
    }

    /// The next position that will be assigned.
    pub fn next_position(&self) -> Position {
        self.next_position
    }

    /// Largest batch (records plus commit marker) that can fit an empty segment. The
    /// write coordinator budgets against this so a multi-request batch always fits.
    pub fn segment_capacity(&self) -> usize {
        self.config.segment_size - self.config.header_size
    }

    /// Largest a single record may be. A batch containing a larger record is rejected.
    pub fn max_record_len(&self) -> usize {
        self.config.max_record_len
    }

    /// Number of sealed (immutable) segments.
    pub fn sealed_len(&self) -> usize {
        self.sealed.len()
    }

    /// Resolves the segment owning `pos`, or `None` if out of range. Binary search
    /// over the sealed segments, then the active one.
    pub fn segment_for(&self, pos: Position) -> Option<&Arc<Segment>> {
        // Position 0 is the empty sentinel; anything at or past the next position
        // has not been assigned.
        if pos == Position::ZERO || pos >= self.next_position {
            return None;
        }
        if pos >= self.active.base_position {
            return Some(&self.active);
        }
        let i = self.sealed.partition_point(|s| s.base_position <= pos);
        if i == 0 {
            None
        } else {
            Some(&self.sealed[i - 1])
        }
    }

    /// The segment at logical index `idx`: sealed segments first, then the active
    /// one at `sealed.len()`.
    fn segment_at(&self, idx: usize) -> Option<&Arc<Segment>> {
        match idx.cmp(&self.sealed.len()) {
            Ordering::Less => Some(&self.sealed[idx]),
            Ordering::Equal => Some(&self.active),
            Ordering::Greater => None,
        }
    }

    fn open_reader(&self, segment: &Segment) -> Result<Reader<0>, LogError> {
        Reader::<0>::open(&segment.path, segment.flushed_offset.clone())
            .map_err(|source| LogError::read(&segment.path, source))
    }
}

/// Sequential scan over the log, starting from a position and rolling across
/// segment boundaries. Yields records in position order; control records are
/// skipped silently, and it never reads past the active segment's flushed point.
///
/// A failure to open a segment or read a record is surfaced as an `Err` item and
/// terminates the scan — it never looks like a clean end-of-stream.
pub struct Scan<'a> {
    set: &'a SegmentSet,
    /// Logical index of the segment currently being read (see [`SegmentSet::segment_at`]).
    seg_idx: usize,
    /// Byte offset within the current segment of the next record to read.
    offset: u64,
    /// Global position of the next record to emit.
    position: Position,
    /// The reader for the current segment. `Reader` owns its 64 KB read-ahead
    /// buffer, so keeping it here (rather than reopening per record) is what makes
    /// the scan do roughly one syscall per read-ahead window, not one per record.
    reader: Option<Reader<0>>,
    /// A setup error to surface as the first (and only) item.
    pending_err: Option<LogError>,
    done: bool,
}

impl<'a> Scan<'a> {
    fn empty(set: &'a SegmentSet) -> Self {
        Scan {
            set,
            seg_idx: 0,
            offset: 0,
            position: Position::ZERO,
            reader: None,
            pending_err: None,
            done: true,
        }
    }

    fn failed(set: &'a SegmentSet, err: LogError) -> Self {
        Scan {
            pending_err: Some(err),
            done: false,
            ..Scan::empty(set)
        }
    }

    /// Moves to the next segment, opening its reader and pointing at its first
    /// record. Returns `false` when there are no more segments.
    fn advance_segment(&mut self) -> Result<bool, LogError> {
        let next_idx = self.seg_idx + 1;
        let Some(segment) = self.set.segment_at(next_idx) else {
            return Ok(false);
        };
        self.reader = Some(self.set.open_reader(segment)?);
        self.offset = self.set.config.header_size as u64;
        self.seg_idx = next_idx;
        Ok(true)
    }
}

impl Scan<'_> {
    /// Advances to the next record and returns a view borrowing the reader's
    /// read-ahead buffer.
    ///
    /// This is a *lending* iterator, so it is not `std::iter::Iterator` (which can't
    /// yield a borrow of itself). Consume it with
    /// `while let Some(item) = scan.next() { … }`; the returned [`RecordRef`] is
    /// valid only until the following `next` call.
    ///
    /// Returns `None` at the end of the log; a read failure is surfaced once as an
    /// `Err` item and then terminates the scan.
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Option<Result<RecordRef<'_>, LogError>> {
        if let Some(err) = self.pending_err.take() {
            self.done = true;
            return Some(Err(err));
        }
        if self.done {
            return None;
        }

        // Phase 1: position the cursor on the next data record, skipping control
        // records and rolling across segments. This is header-only (`peek`), so it
        // holds no payload borrow while it swaps readers — which is what lets the
        // borrowing read in phase 2 return a view without fighting the borrow checker
        // at a segment boundary.
        let total_len = match self.position_at_data() {
            Ok(Some(total_len)) => total_len,
            Ok(None) => {
                self.done = true;
                return None;
            }
            Err(err) => {
                self.done = true;
                return Some(Err(err));
            }
        };

        // Advance the cursor *before* the borrowing read, so no `self` field is
        // written while the returned view borrows the reader. `total_len` from the
        // header matches the record's framed length, so this is exact.
        let position = self.position;
        let offset = self.offset;
        self.offset = offset + total_len as u64;
        self.position = position.next();

        // Phase 2: one borrowing read of the data record. Sequential reads always
        // borrow the read-ahead buffer (locked by seglog's
        // `test_sequential_read_borrows_even_large_records`), so `data` is a slice
        // into it, zero copy.
        let set = self.set;
        let seg_idx = self.seg_idx;
        let reader = self.reader.as_mut().unwrap();
        match reader.read_record(offset, ReadHint::Sequential) {
            Ok(record) => {
                let data = match record.data {
                    Cow::Borrowed(bytes) => bytes,
                    Cow::Owned(_) => {
                        // Pinned by seglog's `test_sequential_read_borrows_even_large_records`:
                        // a `ReadHint::Sequential` read always returns `Cow::Borrowed`,
                        // even for payloads larger than the optimistic/fallback buffers.
                        unreachable!(
                            "sequential reads borrow the read-ahead buffer \
                             (seglog::test_sequential_read_borrows_even_large_records)"
                        )
                    }
                };
                Some(Ok(RecordRef { position, data }))
            }
            Err(err) => {
                self.done = true;
                Some(Err(LogError::read(scan_segment_path(set, seg_idx), err)))
            }
        }
    }

    /// Positions the cursor on the next data record, skipping control records and
    /// rolling across segment boundaries. Returns the record's framed length, or
    /// `Ok(None)` when the log is exhausted. Header-only, so it holds no payload
    /// borrow while it swaps readers.
    fn position_at_data(&mut self) -> Result<Option<usize>, LogError> {
        let set = self.set;
        loop {
            if self.reader.is_none() && !self.advance_segment()? {
                return Ok(None);
            }
            let seg_idx = self.seg_idx;
            let reader = self.reader.as_mut().unwrap();
            let kind = reader
                .peek(self.offset)
                .map_err(|err| LogError::read(scan_segment_path(set, seg_idx), err))?;
            match kind {
                RecordKind::Data { total_len } => return Ok(Some(total_len)),
                RecordKind::Control { total_len } => self.offset += total_len as u64,
                RecordKind::End => self.reader = None, // advance on the next iteration
            }
        }
    }
}

/// The path of the segment at logical index `idx`, for error reporting.
fn scan_segment_path(set: &SegmentSet, idx: usize) -> PathBuf {
    set.segment_at(idx)
        .map(|segment| segment.path.clone())
        .unwrap_or_default()
}

/// Errors from segment-set operations.
#[derive(Debug, Error)]
pub enum LogError {
    #[error("invalid segment config: {reason}")]
    InvalidConfig { reason: String },
    #[error("i/o error at {path:?}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("segment header error in {path:?}: {source}")]
    Header {
        path: PathBuf,
        #[source]
        source: HeaderError,
    },
    #[error(
        "segment {path:?}: header base_position {header} disagrees with filename position {name}"
    )]
    BasePositionMismatch {
        path: PathBuf,
        header: Position,
        name: Position,
    },
    #[error("unwritten segment {path:?} is not the last segment; refusing to open")]
    UnwrittenNonLast { path: PathBuf },
    #[error("non-contiguous segments: {path:?} has base_position {found}, expected {expected}")]
    NonContiguous {
        path: PathBuf,
        found: Position,
        expected: Position,
    },
    #[error(
        "recovered commit position {found} disagrees with event count (expected highest {expected}) in {path:?}"
    )]
    PositionMismatch {
        path: PathBuf,
        found: Position,
        expected: Position,
    },
    #[error("record of {size} bytes exceeds the maximum record length of {max} bytes")]
    RecordTooLarge { size: usize, max: usize },
    #[error("batch of {size} bytes cannot fit in a segment (capacity {capacity} bytes)")]
    BatchTooLarge { size: usize, capacity: usize },
    #[error("empty batch")]
    EmptyBatch,
    #[error("empty record")]
    EmptyRecord,
    #[error("position {position} not found")]
    NotFound { position: Position },
    #[error("write error at {path:?}: {source}")]
    Write {
        path: PathBuf,
        #[source]
        source: WriteError,
    },
    #[error("read error at {path:?}: {source}")]
    Read {
        path: PathBuf,
        #[source]
        source: ReadError,
    },
}

impl LogError {
    fn io(path: impl AsRef<Path>, source: io::Error) -> Self {
        LogError::Io {
            path: path.as_ref().to_path_buf(),
            source,
        }
    }

    fn write(path: impl AsRef<Path>, source: WriteError) -> Self {
        LogError::Write {
            path: path.as_ref().to_path_buf(),
            source,
        }
    }

    fn read(path: impl AsRef<Path>, source: ReadError) -> Self {
        LogError::Read {
            path: path.as_ref().to_path_buf(),
            source,
        }
    }
}

/// `{base_position:020}.log`.
fn segment_file_name(base: Position) -> String {
    format!("{:0width$}.log", base.get(), width = NAME_DIGITS)
}

/// Parses `base_position` from a segment file name, or `None` if it does not match
/// the `{20 digits}.log` pattern.
fn parse_base_position(name: &str) -> Option<Position> {
    let stem = name.strip_suffix(".log")?;
    if stem.len() != NAME_DIGITS || !stem.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    stem.parse::<u64>().ok().map(Position::new)
}

/// Reads the first [`SEGMENT_HEADER_SIZE`] bytes of a segment file.
fn read_header(path: &Path) -> Result<[u8; SEGMENT_HEADER_SIZE], LogError> {
    let file = File::open(path).map_err(|source| LogError::io(path, source))?;
    let mut buf = [0u8; SEGMENT_HEADER_SIZE];
    file.read_exact_at(&mut buf, 0)
        .map_err(|source| LogError::io(path, source))?;
    Ok(buf)
}

/// Scans a segment for its data-record byte offsets, indexed by local position.
/// Skips control records; stops at the flushed point (or, with `flushed == None`,
/// at the zero-filled tail of a sealed segment).
fn scan_offsets(
    path: &Path,
    flushed: Option<FlushedOffset>,
    header_size: u64,
) -> Result<Vec<u32>, LogError> {
    let mut reader =
        Reader::<0>::open(path, flushed).map_err(|source| LogError::read(path, source))?;
    let mut offsets = Vec::new();
    let mut iter = reader.iter(header_size);
    while let Some(record) = iter
        .next_record()
        .map_err(|source| LogError::read(path, source))?
    {
        offsets.push(
            u32::try_from(record.offset)
                .expect("segment_size <= u32::MAX enforced by SegmentConfig::validate"),
        );
    }
    Ok(offsets)
}

/// Whether a non-zero record header sits at `offset`, i.e. recovery discarded a
/// torn trailing batch (as opposed to a clean end at the zero-filled tail).
fn trailing_bytes_present(writer: &Writer<0>, offset: u64, segment_size: usize) -> bool {
    if offset + RECORD_HEAD_SIZE as u64 > segment_size as u64 {
        return false;
    }
    let mut head = [0u8; RECORD_HEAD_SIZE];
    writer.file().read_exact_at(&mut head, offset).is_ok() && head.iter().any(|&b| b != 0)
}

fn sync_dir(dir: &Path) -> io::Result<()> {
    File::open(dir)?.sync_all()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    const HEADER: usize = SEGMENT_HEADER_SIZE;
    /// Framing overhead of a single record (its length + CRC head).
    const REC_OVERHEAD: usize = RECORD_HEAD_SIZE;
    /// Framing of a batch's trailing commit marker.
    const MARKER: usize = RECORD_HEAD_SIZE + COMMIT_MARKER_PAYLOAD;

    fn open(dir: &Path, segment_size: usize) -> SegmentSet {
        SegmentSet::open(dir, SegmentConfig::new(segment_size)).unwrap()
    }

    /// Appends one single-record batch and returns its position.
    fn append_one(set: &mut SegmentSet, data: &[u8]) -> Position {
        let range = set.append_batch(&[data]).unwrap();
        assert_eq!(range.first, range.last);
        range.first
    }

    /// Drains a lending [`Scan`] into owned records.
    fn drain(mut scan: Scan) -> Vec<Record> {
        let mut out = Vec::new();
        while let Some(item) = scan.next() {
            out.push(item.unwrap().to_owned());
        }
        out
    }

    #[test]
    fn open_empty_creates_first_segment() {
        let dir = TempDir::new().unwrap();
        let set = open(dir.path(), 4096);

        // Fresh log: next position is 1, and last_position is the empty sentinel 0.
        assert_eq!(set.next_position(), Position(1));
        assert_eq!(set.last_position(), Position(0));
        assert_eq!(set.sealed_len(), 0);
        assert!(dir.path().join("00000000000000000001.log").exists());
    }

    #[test]
    fn tiny_config_rejected() {
        let dir = TempDir::new().unwrap();
        // Default max_record_len = 16, header 64, so nothing usable fits.
        let err = SegmentSet::open(dir.path(), SegmentConfig::new(64)).unwrap_err();
        assert!(matches!(err, LogError::InvalidConfig { .. }), "got {err:?}");
    }

    #[test]
    fn reopen_after_clean_shutdown_preserves_state() {
        let dir = TempDir::new().unwrap();
        {
            let mut set = open(dir.path(), 4096);
            for i in 1..=5u64 {
                append_one(&mut set, format!("event-{i}").as_bytes());
            }
            assert_eq!(set.next_position(), Position(6));
        }

        let set = open(dir.path(), 4096);
        assert_eq!(set.next_position(), Position(6));
        assert_eq!(set.last_position(), Position(5));
        for i in 1..=5u64 {
            let record = set.read_at(Position(i)).unwrap();
            assert_eq!(record.position, Position(i));
            assert_eq!(record.data, format!("event-{i}").into_bytes());
        }
    }

    #[test]
    fn rollover_keeps_positions_contiguous() {
        let dir = TempDir::new().unwrap();
        // Small segment so a handful of batches force rollovers.
        let mut set = open(dir.path(), 256);

        let n = 20u64;
        for i in 1..=n {
            let pos = append_one(&mut set, format!("evt{i:03}").as_bytes());
            assert_eq!(pos, Position(i));
        }

        assert_eq!(set.next_position(), Position(n + 1));
        assert!(set.sealed_len() >= 1, "expected at least one rollover");

        for i in 1..=n {
            let record = set.read_at(Position(i)).unwrap();
            assert_eq!(record.data, format!("evt{i:03}").into_bytes());
        }
    }

    #[test]
    fn read_at_across_boundary_for_every_position() {
        let dir = TempDir::new().unwrap();
        let mut set = open(dir.path(), 200);
        let n = 30u64;
        for i in 1..=n {
            append_one(&mut set, format!("r{i:04}").as_bytes());
        }
        assert!(set.sealed_len() >= 2);
        for i in 1..=n {
            assert_eq!(
                set.read_at(Position(i)).unwrap().data,
                format!("r{i:04}").into_bytes()
            );
        }
        // The empty sentinel and a position past the end are both absent.
        assert!(matches!(
            set.read_at(Position(0)),
            Err(LogError::NotFound { .. })
        ));
        assert!(matches!(
            set.read_at(Position(n + 1)),
            Err(LogError::NotFound { .. })
        ));
    }

    #[test]
    fn scan_from_mid_segment_yields_expected_order() {
        let dir = TempDir::new().unwrap();
        let mut set = open(dir.path(), 4096);
        let n = 12u64;
        for i in 1..=n {
            append_one(&mut set, format!("s{i}").as_bytes());
        }

        let start = 5u64;
        let got = drain(set.scan_from(Position(start)));
        assert_eq!(got.len() as u64, n - start + 1);
        for (idx, record) in got.iter().enumerate() {
            let pos = start + idx as u64;
            assert_eq!(record.position, Position(pos));
            assert_eq!(record.data, format!("s{pos}").into_bytes());
        }
    }

    #[test]
    fn scan_across_segments_is_contiguous_and_ordered() {
        let dir = TempDir::new().unwrap();
        let mut set = open(dir.path(), 200);
        let n = 25u64;
        for i in 1..=n {
            append_one(&mut set, format!("x{i:04}").as_bytes());
        }
        assert!(set.sealed_len() >= 2);

        let got = drain(set.scan_from(Position(1)));
        assert_eq!(got.len() as u64, n);
        for (idx, record) in got.iter().enumerate() {
            let pos = idx as u64 + 1;
            assert_eq!(record.position, Position(pos));
            assert_eq!(record.data, format!("x{pos:04}").into_bytes());
        }
    }

    #[test]
    fn scan_from_zero_clamps_to_whole_log() {
        let dir = TempDir::new().unwrap();
        let mut set = open(dir.path(), 4096);
        for i in 1..=3u64 {
            append_one(&mut set, format!("e{i}").as_bytes());
        }

        // scan_from(0) means "from before everything" — the whole log, not nothing.
        let positions: Vec<Position> = drain(set.scan_from(Position(0)))
            .iter()
            .map(|r| r.position)
            .collect();
        assert_eq!(positions, vec![Position(1), Position(2), Position(3)]);

        // Past the end is a normal caught-up state: empty, not an error.
        assert!(set.scan_from(Position(5)).next().is_none());
    }

    #[test]
    fn scan_after_is_exclusive() {
        let dir = TempDir::new().unwrap();
        let mut set = open(dir.path(), 4096);
        for i in 1..=3u64 {
            append_one(&mut set, format!("e{i}").as_bytes());
        }

        // scan_after(0) scans the whole log with no sentinel special case.
        let all: Vec<Position> = drain(set.scan_after(Position(0)))
            .iter()
            .map(|r| r.position)
            .collect();
        assert_eq!(all, vec![Position(1), Position(2), Position(3)]);

        // scan_after(pos) is exclusive: it resumes strictly after `pos`.
        let resumed: Vec<Position> = drain(set.scan_after(Position(1)))
            .iter()
            .map(|r| r.position)
            .collect();
        assert_eq!(resumed, vec![Position(2), Position(3)]);

        // scan_after(last) is the caught-up state: empty, not an error.
        assert!(set.scan_after(set.last_position()).next().is_none());

        // Inclusive/exclusive agree: scan_from(n) == scan_after(n - 1).
        let from2: Vec<Position> = drain(set.scan_from(Position(2)))
            .iter()
            .map(|r| r.position)
            .collect();
        assert_eq!(from2, resumed);
    }

    #[test]
    fn oversized_record_rejected() {
        let dir = TempDir::new().unwrap();
        let mut config = SegmentConfig::new(4096);
        config.max_record_len = 100;
        let mut set = SegmentSet::open(dir.path(), config).unwrap();

        let big = vec![0u8; 200];
        let err = set.append_batch(&[&big]).unwrap_err();
        assert!(
            matches!(err, LogError::RecordTooLarge { .. }),
            "got {err:?}"
        );
    }

    #[test]
    fn batch_larger_than_segment_rejected() {
        let dir = TempDir::new().unwrap();
        let mut set = open(dir.path(), 200);

        // Five 30-byte records plus overhead exceed capacity (segment_size - header),
        // yet each record is under max_record_len.
        let records: Vec<Vec<u8>> = (0..5).map(|_| vec![0u8; 30]).collect();
        let refs: Vec<&[u8]> = records.iter().map(|r| r.as_slice()).collect();
        let err = set.append_batch(&refs).unwrap_err();
        assert!(matches!(err, LogError::BatchTooLarge { .. }), "got {err:?}");
    }

    #[test]
    fn empty_batch_and_empty_record_rejected() {
        let dir = TempDir::new().unwrap();
        let mut set = open(dir.path(), 4096);
        assert!(matches!(set.append_batch(&[]), Err(LogError::EmptyBatch)));
        assert!(matches!(
            set.append_batch(&[b""]),
            Err(LogError::EmptyRecord)
        ));
        // A rejected empty record must not have advanced anything.
        assert_eq!(set.next_position(), Position(1));
    }

    #[test]
    fn crash_after_create_before_header_write_is_deleted() {
        let dir = TempDir::new().unwrap();
        let segment_size = 4096;
        {
            let mut set = open(dir.path(), segment_size);
            for i in 1..=3u64 {
                append_one(&mut set, format!("e{i}").as_bytes());
            }
            assert_eq!(set.next_position(), Position(4));
        }

        // Simulate a crash between create and header write for the next segment: a
        // zero-filled trailing file at the next base position.
        let ghost = dir.path().join(segment_file_name(Position(4)));
        fs::write(&ghost, vec![0u8; segment_size]).unwrap();

        let set = open(dir.path(), segment_size);
        assert!(
            !ghost.exists(),
            "zero-filled trailing segment should be deleted"
        );
        assert_eq!(set.next_position(), Position(4));
        for i in 1..=3u64 {
            assert_eq!(
                set.read_at(Position(i)).unwrap().data,
                format!("e{i}").into_bytes()
            );
        }
    }

    #[test]
    fn missing_middle_segment_fails_open() {
        let dir = TempDir::new().unwrap();
        let segment_size = 200;
        {
            let mut set = open(dir.path(), segment_size);
            for i in 1..=15u64 {
                append_one(&mut set, format!("m{i:03}").as_bytes());
            }
            assert!(set.sealed_len() >= 3, "need several sealed segments");
        }

        let mut files: Vec<PathBuf> = fs::read_dir(dir.path())
            .unwrap()
            .map(|e| e.unwrap().path())
            .filter(|p| p.extension().is_some_and(|e| e == "log"))
            .collect();
        files.sort();
        assert!(files.len() >= 3);
        fs::remove_file(&files[1]).unwrap();

        let err = SegmentSet::open(dir.path(), SegmentConfig::new(segment_size)).unwrap_err();
        assert!(matches!(err, LogError::NonContiguous { .. }), "got {err:?}");
    }

    #[test]
    fn header_base_position_disagreeing_with_filename_fails() {
        let dir = TempDir::new().unwrap();
        let segment_size = 4096;
        {
            let mut set = open(dir.path(), segment_size);
            append_one(&mut set, b"only");
        }

        // The first segment's file is named for base position 1.
        let path = dir.path().join(segment_file_name(Position(1)));
        let bogus = SegmentHeader::new(Position(7));
        let file = File::options().write(true).open(&path).unwrap();
        file.write_all_at(&bogus.to_bytes(), 0).unwrap();
        file.sync_all().unwrap();
        drop(file);

        let err = SegmentSet::open(dir.path(), SegmentConfig::new(segment_size)).unwrap_err();
        assert!(
            matches!(err, LogError::BasePositionMismatch { .. }),
            "got {err:?}"
        );
    }

    #[test]
    fn multi_record_batch_shares_positions() {
        let dir = TempDir::new().unwrap();
        let mut set = open(dir.path(), 4096);
        let range = set.append_batch(&[b"a", b"bb", b"ccc"]).unwrap();
        assert_eq!(range.first, Position(1));
        assert_eq!(range.last, Position(3));
        assert_eq!(range.count(), 3);
        assert_eq!(set.next_position(), Position(4));
        assert_eq!(set.read_at(Position(1)).unwrap().data, b"a");
        assert_eq!(set.read_at(Position(2)).unwrap().data, b"bb");
        assert_eq!(set.read_at(Position(3)).unwrap().data, b"ccc");
    }

    #[test]
    fn append_continues_after_recovery() {
        let dir = TempDir::new().unwrap();
        let segment_size = 4096;
        {
            let mut set = open(dir.path(), segment_size);
            append_one(&mut set, b"before"); // position 1
        }
        let mut set = open(dir.path(), segment_size);
        let pos = append_one(&mut set, b"after"); // position 2
        assert_eq!(pos, Position(2));
        assert_eq!(set.read_at(Position(1)).unwrap().data, b"before");
        assert_eq!(set.read_at(Position(2)).unwrap().data, b"after");
    }

    /// Data payload for the 1-based `position` in the single-record-per-batch logs
    /// built by [`build_single_segment`] and the truncation tests.
    fn payload_for(position: u64, record_len: usize) -> Vec<u8> {
        // Batch index is `position - 1`; the builder tags each with index+1.
        vec![((position - 1) as u8).wrapping_add(1); record_len]
    }

    /// Builds a single-segment log with `batches` single-record batches (positions
    /// 1..=batches), then returns the raw file bytes and the byte offset just past
    /// each batch's commit marker.
    fn build_single_segment(
        dir: &Path,
        segment_size: usize,
        batches: usize,
        record_len: usize,
    ) -> (Vec<u8>, Vec<usize>) {
        let mut set = open(dir, segment_size);
        for p in 1..=batches as u64 {
            append_one(&mut set, &payload_for(p, record_len));
        }
        assert_eq!(set.sealed_len(), 0, "test assumes a single segment");
        drop(set);

        let path = dir.join(segment_file_name(Position(FIRST_POSITION)));
        let bytes = fs::read(&path).unwrap();

        let batch_size = REC_OVERHEAD + record_len + MARKER;
        let commit_ends: Vec<usize> = (0..batches)
            .map(|i| HEADER + (i + 1) * batch_size)
            .collect();
        (bytes, commit_ends)
    }

    #[test]
    fn truncation_mid_batch_rolls_back_to_previous_commit() {
        let segment_size = 4096;
        let batches = 8;
        let record_len = 10;

        let source = TempDir::new().unwrap();
        let (good_bytes, commit_ends) =
            build_single_segment(source.path(), segment_size, batches, record_len);
        let total_end = *commit_ends.last().unwrap();

        // Table-driven over a dense range of truncation offsets: for each cutoff,
        // corrupt the tail and assert recovery rolls back to the last commit marker
        // whose batch lies entirely before the cutoff.
        for cutoff in HEADER..=total_end {
            let dir = TempDir::new().unwrap();
            let mut corrupt = good_bytes.clone();
            for byte in corrupt.iter_mut().skip(cutoff) {
                *byte = 0xFF;
            }
            let path = dir.path().join(segment_file_name(Position(FIRST_POSITION)));
            fs::write(&path, &corrupt).unwrap();

            let set = open(dir.path(), segment_size);

            // `survived` batches map to positions 1..=survived, so next is survived + 1.
            let survived = commit_ends.iter().filter(|&&end| end <= cutoff).count() as u64;
            assert_eq!(
                set.next_position(),
                Position(survived + 1),
                "cutoff {cutoff}: expected {survived} surviving events"
            );

            for p in 1..=survived {
                let record = set.read_at(Position(p)).unwrap();
                assert_eq!(
                    record.data,
                    payload_for(p, record_len),
                    "cutoff {cutoff}, position {p}"
                );
            }
        }
    }

    #[test]
    fn corrupt_record_with_intact_marker_rejects_whole_batch() {
        // The rule that matters most: a batch is committed only if *every* record
        // in it validates, not merely if its trailing marker is present.
        let dir = TempDir::new().unwrap();
        let segment_size = 4096;
        let rec_len = 6;
        {
            let mut set = open(dir.path(), segment_size);
            append_one(&mut set, b"aaaa"); // batch A: position 1, survives
            let recs: Vec<Vec<u8>> = (0..5).map(|i| vec![b'B' + i as u8; rec_len]).collect();
            let refs: Vec<&[u8]> = recs.iter().map(|r| r.as_slice()).collect();
            set.append_batch(&refs).unwrap(); // batch B: positions 2..=6
            assert_eq!(set.next_position(), Position(7));
        }

        // Flip one byte inside record 2 of batch B, leaving batch B's commit marker
        // completely intact.
        let batch_a = REC_OVERHEAD + 4 + MARKER;
        let rec_stride = REC_OVERHEAD + rec_len;
        let rec2_data = HEADER + batch_a + rec_stride + REC_OVERHEAD;
        let path = dir.path().join(segment_file_name(Position(FIRST_POSITION)));
        let file = File::options().read(true).write(true).open(&path).unwrap();
        let mut byte = [0u8; 1];
        file.read_exact_at(&mut byte, rec2_data as u64).unwrap();
        byte[0] ^= 0xFF;
        file.write_all_at(&byte, rec2_data as u64).unwrap();
        file.sync_all().unwrap();
        drop(file);

        let set = open(dir.path(), segment_size);
        assert_eq!(
            set.next_position(),
            Position(2),
            "whole batch B must roll back"
        );
        assert_eq!(set.read_at(Position(1)).unwrap().data, b"aaaa");
        assert!(matches!(
            set.read_at(Position(2)),
            Err(LogError::NotFound { .. })
        ));
    }

    #[test]
    fn physical_truncation_mid_batch_rolls_back() {
        // A short file (real truncation) exercises different recovery paths than
        // garbage-overwrite: reads run off the physical end.
        let dir = TempDir::new().unwrap();
        let segment_size = 4096;
        let record_len = 10;
        let batches = 6;
        {
            let mut set = open(dir.path(), segment_size);
            for p in 1..=batches as u64 {
                append_one(&mut set, &payload_for(p, record_len));
            }
        }

        let batch_size = REC_OVERHEAD + record_len + MARKER;
        let survive = 3usize;
        let cut = HEADER + survive * batch_size + 5; // partway into batch index 3

        let path = dir.path().join(segment_file_name(Position(FIRST_POSITION)));
        let file = File::options().write(true).open(&path).unwrap();
        file.set_len(cut as u64).unwrap();
        file.sync_all().unwrap();
        drop(file);

        let set = open(dir.path(), segment_size);
        assert_eq!(set.next_position(), Position(survive as u64 + 1));
        for p in 1..=survive as u64 {
            assert_eq!(
                set.read_at(Position(p)).unwrap().data,
                payload_for(p, record_len)
            );
        }
    }
}
