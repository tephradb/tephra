use std::time::{Duration, SystemTime, UNIX_EPOCH};

use thiserror::Error;

use crate::Position;

pub const SEGMENT_HEADER_SIZE: usize = 64;

const SZ_MAGIC: usize = 4;
const SZ_VERSION: usize = 2;
const SZ_CREATED_AT: usize = 8;
const SZ_BASE_POSITION: usize = 8;
const SZ_CRC: usize = 4;

const OFF_MAGIC: usize = 0;
const OFF_VERSION: usize = OFF_MAGIC + SZ_MAGIC;
const OFF_CREATED_AT: usize = OFF_VERSION + SZ_VERSION;
const OFF_BASE_POSITION: usize = OFF_CREATED_AT + SZ_CREATED_AT;
const OFF_PADDING: usize = OFF_BASE_POSITION + SZ_BASE_POSITION;
const OFF_CRC: usize = SEGMENT_HEADER_SIZE - SZ_CRC;

const _: () = assert!(OFF_PADDING <= OFF_CRC);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SegmentHeader {
    pub version: u16,
    pub created_at_nanos: u64,
    pub base_position: Position,
}

impl SegmentHeader {
    pub const MAGIC_BYTES: u32 = u32::from_le_bytes(*b"EVTS");
    pub const VERSION: u16 = 0;

    pub fn new(base_position: Position) -> Self {
        SegmentHeader {
            version: Self::VERSION,
            created_at_nanos: u64::try_from(
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .expect("time went backwards")
                    .as_nanos(),
            )
            .expect("is it really the year 2554 already?"),
            base_position,
        }
    }

    pub fn created_at(&self) -> SystemTime {
        UNIX_EPOCH + Duration::from_nanos(self.created_at_nanos)
    }

    pub fn to_bytes(&self) -> [u8; SEGMENT_HEADER_SIZE] {
        let mut buf = [0u8; SEGMENT_HEADER_SIZE];
        buf[OFF_MAGIC..OFF_MAGIC + SZ_MAGIC].copy_from_slice(&Self::MAGIC_BYTES.to_le_bytes());
        buf[OFF_VERSION..OFF_VERSION + SZ_VERSION].copy_from_slice(&self.version.to_le_bytes());
        buf[OFF_CREATED_AT..OFF_CREATED_AT + SZ_CREATED_AT]
            .copy_from_slice(&self.created_at_nanos.to_le_bytes());
        buf[OFF_BASE_POSITION..OFF_BASE_POSITION + SZ_BASE_POSITION]
            .copy_from_slice(&self.base_position.get().to_le_bytes());
        // OFF_PADDING..OFF_CRC stays zero
        let crc = crc32fast::hash(&buf[..OFF_CRC]);
        buf[OFF_CRC..].copy_from_slice(&crc.to_le_bytes());
        buf
    }

    pub fn from_bytes(buf: &[u8; SEGMENT_HEADER_SIZE]) -> Result<Self, HeaderError> {
        // An unwritten segment is a normal, expected state: fallocate zeroes,
        // and a crash between create and first header write leaves this.
        // Distinguish it from corruption before anything else.
        if buf.iter().all(|&b| b == 0) {
            return Err(HeaderError::Unwritten);
        }

        // Checksum first. Every field below is only meaningful if the header
        // is intact, and a torn first-64-bytes write would otherwise yield a
        // plausible but wrong base_position.
        let expected = u32::from_le_bytes(buf[OFF_CRC..].try_into().unwrap());
        let computed = crc32fast::hash(&buf[..OFF_CRC]);
        if expected != computed {
            return Err(HeaderError::ChecksumMismatch { expected, computed });
        }

        let magic = u32::from_le_bytes(buf[OFF_MAGIC..OFF_MAGIC + SZ_MAGIC].try_into().unwrap());
        if magic != Self::MAGIC_BYTES {
            return Err(HeaderError::BadMagic {
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
            return Err(HeaderError::UnsupportedVersion {
                found: version,
                supported: Self::VERSION,
            });
        }

        if buf[OFF_PADDING..OFF_CRC].iter().any(|&b| b != 0) {
            return Err(HeaderError::DirtyPadding);
        }

        let created_at_nanos = u64::from_le_bytes(
            buf[OFF_CREATED_AT..OFF_CREATED_AT + SZ_CREATED_AT]
                .try_into()
                .unwrap(),
        );
        let base_position = u64::from_le_bytes(
            buf[OFF_BASE_POSITION..OFF_BASE_POSITION + SZ_BASE_POSITION]
                .try_into()
                .unwrap(),
        );

        Ok(SegmentHeader {
            version,
            created_at_nanos,
            base_position: Position::new(base_position),
        })
    }
}

#[derive(Debug, Error)]
pub enum HeaderError {
    #[error("segment header is unwritten (all zero)")]
    Unwritten,
    #[error("bad magic bytes: expected {expected:#010x}, found {found:#010x}")]
    BadMagic { expected: u32, found: u32 },
    #[error("unsupported segment version {found}, this build supports up to {supported}")]
    UnsupportedVersion { found: u16, supported: u16 },
    #[error(
        "segment header checksum mismatch: expected {expected:#010x}, computed {computed:#010x}"
    )]
    ChecksumMismatch { expected: u32, computed: u32 },
    #[error("non-zero bytes in segment header padding")]
    DirtyPadding,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Layout lock. If this array needs changing, the on-disk format changed:
    /// bump VERSION, do not regenerate the expected bytes.
    const GOLDEN: [u8; SEGMENT_HEADER_SIZE] = [
        0x45, 0x56, 0x54, 0x53, // EVTS magic bytes
        0x00, 0x00, // version 0
        0x00, 0x00, 0x61, 0xAA, 0x78, 0xA6, 0xD1, 0x12, // 21 December 2012 created at
        0x15, 0xCD, 0x5B, 0x07, 0x00, 0x00, 0x00, 0x00, // 123456789 base position
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // padding
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // padding
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // padding
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // padding
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // padding
        0xA5, 0xD5, 0x41, 0x79, // crc32 checksum
    ];

    fn golden_header() -> SegmentHeader {
        SegmentHeader {
            version: 0,
            created_at_nanos: 60 * 60 * 24 * 15695 * 1_000_000_000,
            base_position: Position(123456789),
        }
    }

    /// Mutate a copy of GOLDEN, then fix up the checksum so the test exercises
    /// the field validation rather than tripping ChecksumMismatch first.
    fn mutated(f: impl FnOnce(&mut [u8; SEGMENT_HEADER_SIZE])) -> [u8; SEGMENT_HEADER_SIZE] {
        let mut buf = GOLDEN;
        f(&mut buf);
        let crc = crc32fast::hash(&buf[..OFF_CRC]);
        buf[OFF_CRC..].copy_from_slice(&crc.to_le_bytes());
        buf
    }

    #[test]
    fn encode() {
        assert_eq!(golden_header().to_bytes(), GOLDEN);
    }

    #[test]
    fn decode() {
        assert_eq!(SegmentHeader::from_bytes(&GOLDEN).unwrap(), golden_header());
    }

    #[test]
    fn round_trip() {
        for h in [
            golden_header(),
            SegmentHeader {
                version: 0,
                created_at_nanos: 0,
                base_position: Position(0),
            },
            SegmentHeader {
                version: 0,
                created_at_nanos: u64::MAX,
                base_position: Position(u64::MAX),
            },
        ] {
            assert_eq!(SegmentHeader::from_bytes(&h.to_bytes()).unwrap(), h);
        }
    }

    #[test]
    fn new_round_trips() {
        let h = SegmentHeader::new(Position(42));
        let decoded = SegmentHeader::from_bytes(&h.to_bytes()).unwrap();
        assert_eq!(decoded, h);
        assert_eq!(decoded.version, SegmentHeader::VERSION);
        assert_eq!(decoded.base_position, Position(42));
    }

    #[test]
    fn unwritten_segment_is_not_corruption() {
        // fallocate zeroes, so this is the expected state of a segment created
        // but not yet header-written. Must be distinguishable from damage.
        let buf = [0u8; SEGMENT_HEADER_SIZE];
        assert!(matches!(
            SegmentHeader::from_bytes(&buf),
            Err(HeaderError::Unwritten)
        ));
    }

    #[test]
    fn all_ones_is_checksum_mismatch() {
        // The other degenerate buffer: erased flash, or a wild write.
        let buf = [0xFFu8; SEGMENT_HEADER_SIZE];
        assert!(matches!(
            SegmentHeader::from_bytes(&buf),
            Err(HeaderError::ChecksumMismatch { .. })
        ));
    }

    #[test]
    fn bad_magic_rejected() {
        let buf = mutated(|b| b[OFF_MAGIC] = b'X');
        assert!(matches!(
            SegmentHeader::from_bytes(&buf),
            Err(HeaderError::BadMagic { .. })
        ));
    }

    #[test]
    fn future_version_rejected() {
        let buf = mutated(|b| {
            b[OFF_VERSION..OFF_VERSION + SZ_VERSION]
                .copy_from_slice(&(SegmentHeader::VERSION + 1).to_le_bytes())
        });
        assert!(matches!(
            SegmentHeader::from_bytes(&buf),
            Err(HeaderError::UnsupportedVersion { found, supported })
                if found == SegmentHeader::VERSION + 1 && supported == SegmentHeader::VERSION
        ));
    }

    #[test]
    fn dirty_padding_rejected() {
        let buf = mutated(|b| b[OFF_PADDING] = 0x01);
        assert!(matches!(
            SegmentHeader::from_bytes(&buf),
            Err(HeaderError::DirtyPadding)
        ));
    }

    #[test]
    fn every_single_byte_flip_is_detected() {
        // A torn or bit-rotted header must never decode as a plausible one:
        // a wrong base_position silently corrupts the entire position space.
        for i in 0..SEGMENT_HEADER_SIZE {
            for bit in 0..8 {
                let mut buf = GOLDEN;
                buf[i] ^= 1 << bit;
                assert!(
                    SegmentHeader::from_bytes(&buf).is_err(),
                    "flip at byte {i} bit {bit} was accepted"
                );
            }
        }
    }

    #[test]
    fn checksum_is_checked_before_fields() {
        // Ordering is load-bearing: a torn write that happens to leave a bad
        // magic or version should report corruption, not a format complaint,
        // or debugging leads in the wrong direction.
        let mut buf = GOLDEN;
        buf[OFF_MAGIC] = b'X'; // no checksum fixup
        assert!(matches!(
            SegmentHeader::from_bytes(&buf),
            Err(HeaderError::ChecksumMismatch { .. })
        ));

        let mut buf = GOLDEN;
        buf[OFF_PADDING] = 0x01;
        assert!(matches!(
            SegmentHeader::from_bytes(&buf),
            Err(HeaderError::ChecksumMismatch { .. })
        ));
    }

    #[test]
    fn checksum_covers_all_bytes_before_it() {
        let mut buf = GOLDEN;
        buf[OFF_CRC - 1] ^= 0xFF; // last padding byte
        assert!(matches!(
            SegmentHeader::from_bytes(&buf),
            Err(HeaderError::ChecksumMismatch { .. })
        ));
    }

    #[test]
    fn magic_reads_as_evts_on_disk() {
        assert_eq!(&GOLDEN[OFF_MAGIC..OFF_MAGIC + SZ_MAGIC], b"EVTS");
    }

    #[test]
    fn layout_offsets_are_sane() {
        const {
            assert!(OFF_MAGIC == 0);
            assert!(OFF_PADDING <= OFF_CRC);
            assert!(OFF_CRC + SZ_CRC == SEGMENT_HEADER_SIZE);
        }
    }
}
