//! The on-disk index segment header: a 64-byte, CRC-protected preamble describing one
//! index segment's position range and the byte layout of its regions.
//!
//! Modelled byte-for-byte on [`log::header`](crate::log::header): width-derived offset
//! constants, `const _: () = assert!(...)` layout locks, and the same load-bearing
//! validation order (all-zero, then checksum, then fields). Two things differ, both
//! deliberate:
//!
//! - **Two CRCs.** The header CRC over `[0 .. OFF_HEADER_CRC]` protects the fields the
//!   query path trusts blindly: a wrong `base_position` or `event_count` would poison
//!   the whole position space derived from this segment, the same argument the log
//!   header makes for its own CRC. The separate `body_crc` field covers the regions
//!   after the header (`[SIZE .. EOF]`) and is verified by `super::segment` at open,
//!   not here, since this type sees only the 64-byte header.
//! - **Corruption is recoverable, not fatal.** An index segment is *derived* from the
//!   log (the log is the source of truth). A corrupt index header or body
//!   is rebuilt by replaying the log segment, never a refuse-to-open. The log header,
//!   by contrast, is authoritative and its corruption is fatal. The caller maps every
//!   error here to "rebuild this segment".
//!
//! Section offsets are stored (not just lengths) so a reader locates each region in
//! O(1), and are cross-checked against `event_count` and each other on decode: a
//! header claiming the type column at offset 12 is [`IndexHeaderError::BadSectionLayout`],
//! never a wild read. The one whole-file check (`fst_off + fst_len == file_len`) needs
//! the file length and so lives in `super::segment` alongside the body CRC.

use std::ops::Range;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use thiserror::Error;

use crate::Position;

pub const INDEX_HEADER_SIZE: usize = 64;

/// The type column always begins immediately after the header. Stored in the header as
/// a section offset for forward-compatibility, and validated to equal this on decode.
pub const TYPE_COLUMN_OFFSET: u32 = INDEX_HEADER_SIZE as u32;

const SZ_MAGIC: usize = 4;
const SZ_VERSION: usize = 2;
const SZ_CREATED_AT: usize = 8;
const SZ_BASE_POSITION: usize = 8;
const SZ_EVENT_COUNT: usize = 8;
const SZ_SECTION: usize = 4;
const SZ_CRC: usize = 4;

const OFF_MAGIC: usize = 0;
const OFF_VERSION: usize = OFF_MAGIC + SZ_MAGIC;
const OFF_CREATED_AT: usize = OFF_VERSION + SZ_VERSION;
const OFF_BASE_POSITION: usize = OFF_CREATED_AT + SZ_CREATED_AT;
const OFF_EVENT_COUNT: usize = OFF_BASE_POSITION + SZ_BASE_POSITION;
const OFF_TYPECOL_OFF: usize = OFF_EVENT_COUNT + SZ_EVENT_COUNT;
const OFF_TYPEDICT_OFF: usize = OFF_TYPECOL_OFF + SZ_SECTION;
const OFF_POSTINGS_OFF: usize = OFF_TYPEDICT_OFF + SZ_SECTION;
const OFF_FST_OFF: usize = OFF_POSTINGS_OFF + SZ_SECTION;
const OFF_FST_LEN: usize = OFF_FST_OFF + SZ_SECTION;
const OFF_BODY_CRC: usize = OFF_FST_LEN + SZ_SECTION;
const OFF_PADDING: usize = OFF_BODY_CRC + SZ_SECTION;
const OFF_HEADER_CRC: usize = INDEX_HEADER_SIZE - SZ_CRC;

const _: () = assert!(OFF_PADDING <= OFF_HEADER_CRC);
const _: () = assert!(OFF_HEADER_CRC + SZ_CRC == INDEX_HEADER_SIZE);
const _: () = assert!(TYPE_COLUMN_OFFSET as usize == INDEX_HEADER_SIZE);

/// The parsed index segment header. Field offsets on disk are the constants above; the
/// section offsets partition the body as
/// `[type column | type dictionary | postings | fst]`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct IndexSegmentHeader {
    pub version: u16,
    pub created_at_nanos: u64,
    /// First (minimum) global position this segment indexes. 1-based.
    pub base_position: Position,
    /// Number of events indexed; the type column is exactly this many `u16`s.
    pub event_count: u64,
    /// Byte offset where the type dictionary begins (`= 64 + event_count * 2`).
    pub typedict_off: u32,
    /// Byte offset where the postings region begins.
    pub postings_off: u32,
    /// Byte offset where the FST term dictionary begins.
    pub fst_off: u32,
    /// Length of the FST term dictionary in bytes.
    pub fst_len: u32,
    /// CRC32 of the body `[64 .. EOF]`, verified by the segment reader (not here).
    pub body_crc: u32,
}

impl IndexSegmentHeader {
    pub const MAGIC_BYTES: u32 = u32::from_le_bytes(*b"EVIX");
    pub const VERSION: u16 = 0;

    pub fn created_at(&self) -> SystemTime {
        UNIX_EPOCH + Duration::from_nanos(self.created_at_nanos)
    }

    /// The last (maximum) global position this segment indexes, or `None` if empty.
    /// Used by the query planner to prune a segment whose whole range is at or before
    /// an `after` bound.
    pub fn max_position(&self) -> Option<Position> {
        (self.event_count > 0)
            .then(|| Position::new(self.base_position.get() + self.event_count - 1))
    }

    /// Byte range of the dense type column: `[64 .. typedict_off]`.
    pub fn type_column_range(&self) -> Range<usize> {
        INDEX_HEADER_SIZE..self.typedict_off as usize
    }

    /// Byte range of the type dictionary: `[typedict_off .. postings_off]`.
    pub fn type_dict_range(&self) -> Range<usize> {
        self.typedict_off as usize..self.postings_off as usize
    }

    /// Byte range of the postings region: `[postings_off .. fst_off]`.
    pub fn postings_range(&self) -> Range<usize> {
        self.postings_off as usize..self.fst_off as usize
    }

    /// Byte range of the FST term dictionary: `[fst_off .. fst_off + fst_len]`.
    pub fn fst_range(&self) -> Range<usize> {
        self.fst_off as usize..self.fst_off as usize + self.fst_len as usize
    }

    /// Total on-disk size of the segment: header plus body.
    pub fn segment_len(&self) -> usize {
        self.fst_off as usize + self.fst_len as usize
    }

    pub fn to_bytes(&self) -> [u8; INDEX_HEADER_SIZE] {
        let mut buf = [0u8; INDEX_HEADER_SIZE];
        buf[OFF_MAGIC..OFF_MAGIC + SZ_MAGIC].copy_from_slice(&Self::MAGIC_BYTES.to_le_bytes());
        buf[OFF_VERSION..OFF_VERSION + SZ_VERSION].copy_from_slice(&self.version.to_le_bytes());
        buf[OFF_CREATED_AT..OFF_CREATED_AT + SZ_CREATED_AT]
            .copy_from_slice(&self.created_at_nanos.to_le_bytes());
        buf[OFF_BASE_POSITION..OFF_BASE_POSITION + SZ_BASE_POSITION]
            .copy_from_slice(&self.base_position.get().to_le_bytes());
        buf[OFF_EVENT_COUNT..OFF_EVENT_COUNT + SZ_EVENT_COUNT]
            .copy_from_slice(&self.event_count.to_le_bytes());
        buf[OFF_TYPECOL_OFF..OFF_TYPECOL_OFF + SZ_SECTION]
            .copy_from_slice(&TYPE_COLUMN_OFFSET.to_le_bytes());
        buf[OFF_TYPEDICT_OFF..OFF_TYPEDICT_OFF + SZ_SECTION]
            .copy_from_slice(&self.typedict_off.to_le_bytes());
        buf[OFF_POSTINGS_OFF..OFF_POSTINGS_OFF + SZ_SECTION]
            .copy_from_slice(&self.postings_off.to_le_bytes());
        buf[OFF_FST_OFF..OFF_FST_OFF + SZ_SECTION].copy_from_slice(&self.fst_off.to_le_bytes());
        buf[OFF_FST_LEN..OFF_FST_LEN + SZ_SECTION].copy_from_slice(&self.fst_len.to_le_bytes());
        buf[OFF_BODY_CRC..OFF_BODY_CRC + SZ_SECTION].copy_from_slice(&self.body_crc.to_le_bytes());
        // OFF_PADDING..OFF_HEADER_CRC stays zero.
        let crc = crc32fast::hash(&buf[..OFF_HEADER_CRC]);
        buf[OFF_HEADER_CRC..].copy_from_slice(&crc.to_le_bytes());
        buf
    }

    /// Decodes and validates the 64-byte header. Checks internal consistency only; the
    /// body CRC and the `fst_off + fst_len == file_len` check need the whole file and
    /// are performed by `super::segment`.
    pub fn from_bytes(buf: &[u8; INDEX_HEADER_SIZE]) -> Result<Self, IndexHeaderError> {
        // An unwritten (all-zero) header is not corruption: a segment file created but
        // not yet written. Distinguish it before anything else, exactly like the log.
        if buf.iter().all(|&b| b == 0) {
            return Err(IndexHeaderError::Unwritten);
        }

        // Checksum before any field: a torn write that leaves a plausible magic or a
        // plausible base_position must report corruption, not a format complaint.
        let expected = u32::from_le_bytes(buf[OFF_HEADER_CRC..].try_into().unwrap());
        let computed = crc32fast::hash(&buf[..OFF_HEADER_CRC]);
        if expected != computed {
            return Err(IndexHeaderError::ChecksumMismatch { expected, computed });
        }

        let magic = u32::from_le_bytes(buf[OFF_MAGIC..OFF_MAGIC + SZ_MAGIC].try_into().unwrap());
        if magic != Self::MAGIC_BYTES {
            return Err(IndexHeaderError::BadMagic {
                expected: Self::MAGIC_BYTES,
                found: magic,
            });
        }

        let version = u16::from_le_bytes(
            buf[OFF_VERSION..OFF_VERSION + SZ_VERSION]
                .try_into()
                .unwrap(),
        );
        if version > Self::VERSION {
            return Err(IndexHeaderError::UnsupportedVersion {
                found: version,
                supported: Self::VERSION,
            });
        }

        if buf[OFF_PADDING..OFF_HEADER_CRC].iter().any(|&b| b != 0) {
            return Err(IndexHeaderError::DirtyPadding);
        }

        let created_at_nanos = read_u64(buf, OFF_CREATED_AT);
        let base_position = Position::new(read_u64(buf, OFF_BASE_POSITION));
        let event_count = read_u64(buf, OFF_EVENT_COUNT);
        let typecol_off = read_u32(buf, OFF_TYPECOL_OFF);
        let typedict_off = read_u32(buf, OFF_TYPEDICT_OFF);
        let postings_off = read_u32(buf, OFF_POSTINGS_OFF);
        let fst_off = read_u32(buf, OFF_FST_OFF);
        let fst_len = read_u32(buf, OFF_FST_LEN);
        let body_crc = read_u32(buf, OFF_BODY_CRC);

        // Section layout consistency. The type column is exactly `event_count` u16s
        // starting right after the header, and the regions are laid out in order with
        // no gaps, so every offset is pinned. A lying offset is corruption, not a read.
        if typecol_off != TYPE_COLUMN_OFFSET {
            return Err(IndexHeaderError::BadSectionLayout {
                detail: "type column must begin at offset 64",
            });
        }
        let expected_typedict = event_count
            .checked_mul(2)
            .and_then(|cols| cols.checked_add(INDEX_HEADER_SIZE as u64));
        if expected_typedict != Some(u64::from(typedict_off)) {
            return Err(IndexHeaderError::BadSectionLayout {
                detail: "type dictionary offset does not match event_count",
            });
        }
        if !(typedict_off <= postings_off && postings_off <= fst_off) {
            return Err(IndexHeaderError::BadSectionLayout {
                detail: "section offsets are not monotonically non-decreasing",
            });
        }
        if fst_off.checked_add(fst_len).is_none() {
            return Err(IndexHeaderError::BadSectionLayout {
                detail: "fst_off + fst_len overflows",
            });
        }

        Ok(IndexSegmentHeader {
            version,
            created_at_nanos,
            base_position,
            event_count,
            typedict_off,
            postings_off,
            fst_off,
            fst_len,
            body_crc,
        })
    }
}

fn read_u64(buf: &[u8; INDEX_HEADER_SIZE], off: usize) -> u64 {
    u64::from_le_bytes(buf[off..off + 8].try_into().unwrap())
}

fn read_u32(buf: &[u8; INDEX_HEADER_SIZE], off: usize) -> u32 {
    u32::from_le_bytes(buf[off..off + 4].try_into().unwrap())
}

/// A rejected index segment header. Every variant means "rebuild this segment from the
/// log", never "refuse to open the store": the index is derived and disposable.
#[derive(Debug, Error)]
pub enum IndexHeaderError {
    #[error("index header is unwritten (all zero)")]
    Unwritten,
    #[error("bad magic bytes: expected {expected:#010x}, found {found:#010x}")]
    BadMagic { expected: u32, found: u32 },
    #[error("unsupported index version {found}, this build supports up to {supported}")]
    UnsupportedVersion { found: u16, supported: u16 },
    #[error("index header checksum mismatch: expected {expected:#010x}, computed {computed:#010x}")]
    ChecksumMismatch { expected: u32, computed: u32 },
    #[error("non-zero bytes in index header padding")]
    DirtyPadding,
    #[error("index header section layout is invalid: {detail}")]
    BadSectionLayout { detail: &'static str },
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Layout lock. If this array needs changing, the on-disk format changed: bump
    /// VERSION, do not regenerate the expected bytes. The body CRC field is 0 here so
    /// the flip suite need not model body contents; the body CRC itself is exercised by
    /// the segment round-trip tests.
    const GOLDEN: [u8; INDEX_HEADER_SIZE] = [
        0x45, 0x56, 0x49, 0x58, // EVIX magic bytes
        0x00, 0x00, // version 0
        0x00, 0x00, 0x61, 0xAA, 0x78, 0xA6, 0xD1, 0x12, // 21 December 2012 created at
        0x15, 0xCD, 0x5B, 0x07, 0x00, 0x00, 0x00, 0x00, // 123456789 base position
        0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // event_count 3
        0x40, 0x00, 0x00, 0x00, // typecol_off 64
        0x46, 0x00, 0x00, 0x00, // typedict_off 70 (= 64 + 3*2)
        0x5A, 0x00, 0x00, 0x00, // postings_off 90
        0x64, 0x00, 0x00, 0x00, // fst_off 100
        0x32, 0x00, 0x00, 0x00, // fst_len 50
        0x00, 0x00, 0x00, 0x00, // body_crc 0
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // padding
        0xAF, 0xC8, 0xE3, 0x9F, // header crc32
    ];

    fn golden_header() -> IndexSegmentHeader {
        IndexSegmentHeader {
            version: 0,
            created_at_nanos: 60 * 60 * 24 * 15695 * 1_000_000_000,
            base_position: Position::new(123456789),
            event_count: 3,
            typedict_off: 70,
            postings_off: 90,
            fst_off: 100,
            fst_len: 50,
            body_crc: 0,
        }
    }

    /// Mutate a copy of GOLDEN, then fix up the header checksum so the test exercises
    /// field validation rather than tripping ChecksumMismatch first.
    fn mutated(f: impl FnOnce(&mut [u8; INDEX_HEADER_SIZE])) -> [u8; INDEX_HEADER_SIZE] {
        let mut buf = GOLDEN;
        f(&mut buf);
        let crc = crc32fast::hash(&buf[..OFF_HEADER_CRC]);
        buf[OFF_HEADER_CRC..].copy_from_slice(&crc.to_le_bytes());
        buf
    }

    #[test]
    fn encode() {
        assert_eq!(golden_header().to_bytes(), GOLDEN);
    }

    #[test]
    fn decode() {
        assert_eq!(
            IndexSegmentHeader::from_bytes(&GOLDEN).unwrap(),
            golden_header()
        );
    }

    #[test]
    fn round_trip() {
        for h in [
            golden_header(),
            IndexSegmentHeader {
                version: 0,
                created_at_nanos: 0,
                base_position: Position::new(1),
                event_count: 0,
                typedict_off: 64,
                postings_off: 64,
                fst_off: 64,
                fst_len: 0,
                body_crc: 0,
            },
            IndexSegmentHeader {
                version: 0,
                created_at_nanos: u64::MAX,
                base_position: Position::new(1),
                event_count: 10,
                typedict_off: 84,
                postings_off: 84,
                fst_off: 200,
                fst_len: 1000,
                body_crc: 0xDEAD_BEEF,
            },
        ] {
            assert_eq!(IndexSegmentHeader::from_bytes(&h.to_bytes()).unwrap(), h);
        }
    }

    #[test]
    fn magic_reads_as_evix_on_disk() {
        assert_eq!(&GOLDEN[OFF_MAGIC..OFF_MAGIC + SZ_MAGIC], b"EVIX");
    }

    #[test]
    fn derived_positions_and_ranges() {
        let h = golden_header();
        assert_eq!(h.base_position, Position::new(123456789));
        assert_eq!(h.max_position(), Some(Position::new(123456789 + 3 - 1)));
        assert_eq!(h.type_column_range(), 64..70);
        assert_eq!(h.type_dict_range(), 70..90);
        assert_eq!(h.postings_range(), 90..100);
        assert_eq!(h.fst_range(), 100..150);
        assert_eq!(h.segment_len(), 150);
    }

    #[test]
    fn empty_segment_has_no_max_position() {
        let h = IndexSegmentHeader {
            version: 0,
            created_at_nanos: 0,
            base_position: Position::new(1),
            event_count: 0,
            typedict_off: 64,
            postings_off: 64,
            fst_off: 64,
            fst_len: 0,
            body_crc: 0,
        };
        assert_eq!(h.max_position(), None);
        assert_eq!(h.type_column_range(), 64..64);
    }

    #[test]
    fn unwritten_header_is_not_corruption() {
        let buf = [0u8; INDEX_HEADER_SIZE];
        assert!(matches!(
            IndexSegmentHeader::from_bytes(&buf),
            Err(IndexHeaderError::Unwritten)
        ));
    }

    #[test]
    fn all_ones_is_checksum_mismatch() {
        let buf = [0xFFu8; INDEX_HEADER_SIZE];
        assert!(matches!(
            IndexSegmentHeader::from_bytes(&buf),
            Err(IndexHeaderError::ChecksumMismatch { .. })
        ));
    }

    #[test]
    fn bad_magic_rejected() {
        let buf = mutated(|b| b[OFF_MAGIC] = b'X');
        assert!(matches!(
            IndexSegmentHeader::from_bytes(&buf),
            Err(IndexHeaderError::BadMagic { .. })
        ));
    }

    #[test]
    fn future_version_rejected() {
        let buf = mutated(|b| {
            b[OFF_VERSION..OFF_VERSION + SZ_VERSION]
                .copy_from_slice(&(IndexSegmentHeader::VERSION + 1).to_le_bytes())
        });
        assert!(matches!(
            IndexSegmentHeader::from_bytes(&buf),
            Err(IndexHeaderError::UnsupportedVersion { found, supported })
                if found == IndexSegmentHeader::VERSION + 1
                    && supported == IndexSegmentHeader::VERSION
        ));
    }

    #[test]
    fn dirty_padding_rejected() {
        let buf = mutated(|b| b[OFF_PADDING] = 0x01);
        assert!(matches!(
            IndexSegmentHeader::from_bytes(&buf),
            Err(IndexHeaderError::DirtyPadding)
        ));
    }

    #[test]
    fn type_column_offset_must_be_64() {
        let buf = mutated(|b| {
            b[OFF_TYPECOL_OFF..OFF_TYPECOL_OFF + SZ_SECTION].copy_from_slice(&12u32.to_le_bytes())
        });
        assert!(matches!(
            IndexSegmentHeader::from_bytes(&buf),
            Err(IndexHeaderError::BadSectionLayout { .. })
        ));
    }

    #[test]
    fn typedict_offset_must_match_event_count() {
        // event_count 3 requires typedict_off 70; claim 68 instead.
        let buf = mutated(|b| {
            b[OFF_TYPEDICT_OFF..OFF_TYPEDICT_OFF + SZ_SECTION].copy_from_slice(&68u32.to_le_bytes())
        });
        assert!(matches!(
            IndexSegmentHeader::from_bytes(&buf),
            Err(IndexHeaderError::BadSectionLayout { .. })
        ));
    }

    #[test]
    fn non_monotonic_offsets_rejected() {
        // postings_off before typedict_off.
        let buf = mutated(|b| {
            b[OFF_POSTINGS_OFF..OFF_POSTINGS_OFF + SZ_SECTION].copy_from_slice(&65u32.to_le_bytes())
        });
        assert!(matches!(
            IndexSegmentHeader::from_bytes(&buf),
            Err(IndexHeaderError::BadSectionLayout { .. })
        ));
    }

    #[test]
    fn checksum_is_checked_before_fields() {
        // A bad magic with no checksum fixup must report ChecksumMismatch, not BadMagic.
        let mut buf = GOLDEN;
        buf[OFF_MAGIC] = b'X';
        assert!(matches!(
            IndexSegmentHeader::from_bytes(&buf),
            Err(IndexHeaderError::ChecksumMismatch { .. })
        ));
    }

    #[test]
    fn checksum_covers_all_bytes_before_it() {
        let mut buf = GOLDEN;
        buf[OFF_HEADER_CRC - 1] ^= 0xFF; // last padding byte
        assert!(matches!(
            IndexSegmentHeader::from_bytes(&buf),
            Err(IndexHeaderError::ChecksumMismatch { .. })
        ));
    }

    #[test]
    fn every_single_byte_flip_is_detected() {
        // A torn or bit-rotted header must never decode as a plausible one: a wrong
        // base_position or event_count silently corrupts the segment's position space.
        for i in 0..INDEX_HEADER_SIZE {
            for bit in 0..8 {
                let mut buf = GOLDEN;
                buf[i] ^= 1 << bit;
                assert!(
                    IndexSegmentHeader::from_bytes(&buf).is_err(),
                    "flip at byte {i} bit {bit} was accepted"
                );
            }
        }
    }

    #[test]
    fn layout_offsets_are_sane() {
        const {
            assert!(OFF_MAGIC == 0);
            assert!(OFF_EVENT_COUNT == 22);
            assert!(OFF_BODY_CRC == 50);
            assert!(OFF_PADDING <= OFF_HEADER_CRC);
            assert!(OFF_HEADER_CRC + SZ_CRC == INDEX_HEADER_SIZE);
        }
    }
}
