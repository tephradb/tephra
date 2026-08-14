//! The on-disk index segment: one immutable file per sealed log segment, holding the
//! same index an [`ActiveTail`] holds in memory, in a compact form a reader can query
//! without decoding the whole thing.
//!
//! Layout (all little-endian), described by [`IndexSegmentHeader`]:
//!
//! ```text
//! [ 64-byte header | type column | type dictionary | postings region | FST ]
//! ```
//!
//! - **type column**: `event_count` `u16`s, `column[local]` is the event's type id.
//! - **type dictionary**: per id in order a `u16` length and the UTF-8 type name, giving
//!   the name-to-id and id-to-name maps the type filter needs. No count is stored: the id
//!   is the entry's position and the region length (from the header) implies the count,
//!   which also lets the dictionary hold the full `u16::MAX + 1` types the column allows.
//! - **postings region**: the varint delta blocks for multi-event tags (see
//!   [`postings`](super::postings)).
//! - **FST**: an `fst::Map` from tag string to the `u64` value that locates its postings
//!   (an inlined singleton, or an offset into the postings region).
//!
//! Reading loads the whole file into one `Arc<[u8]>` and verifies both CRCs once (mmap
//! is deliberately not used). The FST and every region are then read
//! straight out of that shared buffer, so a segment is cheap to clone and share across
//! readers. A corrupt segment is never fatal: the caller rebuilds it from the log, which
//! is the source of truth.

use std::borrow::Cow;
use std::fmt;
use std::fs::File;
use std::io::{self, Write};
use std::ops::Range;
use std::path::Path;
use std::str;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use thiserror::Error;

use crate::Position;

use super::SegmentIndex;
use super::header::{INDEX_HEADER_SIZE, IndexHeaderError, IndexSegmentHeader};
use super::postings::{decode_postings, encode_postings, posting_len};
use super::tail::ActiveTail;

/// A shared subslice of an `Arc<[u8]>`: shares ownership of the whole segment buffer but
/// exposes only one region. Lets `fst::Map` own its bytes (it needs `AsRef<[u8]>` over
/// exactly the FST region) without a second allocation or a self-referential borrow.
#[derive(Clone)]
struct SharedSlice {
    data: Arc<[u8]>,
    range: Range<usize>,
}

impl AsRef<[u8]> for SharedSlice {
    fn as_ref(&self) -> &[u8] {
        &self.data[self.range.clone()]
    }
}

/// An immutable, memory-resident index over one sealed log segment.
pub struct IndexSegment {
    header: IndexSegmentHeader,
    /// The whole segment file. Shared, so cloning an `IndexSegment`'s regions is a
    /// refcount bump, never a copy.
    data: Arc<[u8]>,
    /// Tag string to its posting-locating value.
    map: fst::Map<SharedSlice>,
    /// Type name indexed by id; the dense type column stores these ids.
    type_names: Vec<Box<str>>,
}

impl IndexSegment {
    /// Encodes `index` into the on-disk byte layout. Pure: no I/O, so it is testable and
    /// reused by both the file writer and [`from_bytes`](Self::from_bytes).
    pub fn encode(index: &ActiveTail) -> Vec<u8> {
        let created_at_nanos = u64::try_from(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("time went backwards")
                .as_nanos(),
        )
        .expect("is it really the year 2554 already?");
        Self::encode_at(index, created_at_nanos)
    }

    /// [`encode`](Self::encode) with an explicit timestamp, for deterministic tests.
    pub fn encode_at(index: &ActiveTail, created_at_nanos: u64) -> Vec<u8> {
        // Type column: one u16 per event, in local-position order.
        let mut type_column = Vec::with_capacity(index.len() as usize * 2);
        for type_id in index.type_column() {
            type_column.extend_from_slice(&type_id.to_le_bytes());
        }

        // Type dictionary: `(len, utf8)` per id in order. No count is stored: the id is
        // the entry's position and the number of entries is implied by the region length
        // (the header delimits it). This also sidesteps a u16 count overflowing at the
        // `u16::MAX + 1` distinct types the type column can address.
        let mut type_dict = Vec::new();
        for name in index.type_names() {
            type_dict.extend_from_slice(&fit_u16(name.len(), "type name length").to_le_bytes());
            type_dict.extend_from_slice(name.as_bytes());
        }

        // Postings region + FST. Each term's value is assigned as its block is laid down,
        // then the FST is built from the sorted (tag, value) pairs.
        let mut postings_region = Vec::new();
        let mut builder = fst::MapBuilder::memory();
        for (tag, postings) in index.terms_sorted_with_postings() {
            let value = encode_postings(&postings, &mut postings_region);
            builder
                .insert(tag.as_bytes(), value)
                .expect("terms are fed to the FST in sorted key order");
        }
        let fst_bytes = builder
            .into_inner()
            .expect("in-memory FST build cannot fail on I/O");

        let typedict_off = fit_u32(INDEX_HEADER_SIZE + type_column.len(), "typedict offset");
        let postings_off = fit_u32(typedict_off as usize + type_dict.len(), "postings offset");
        let fst_off = fit_u32(postings_off as usize + postings_region.len(), "fst offset");
        let fst_len = fit_u32(fst_bytes.len(), "fst length");

        // Assemble body first so the body CRC can be computed, then prepend the header.
        let mut out = Vec::with_capacity(fst_off as usize + fst_len as usize);
        out.extend_from_slice(&[0u8; INDEX_HEADER_SIZE]);
        out.extend_from_slice(&type_column);
        out.extend_from_slice(&type_dict);
        out.extend_from_slice(&postings_region);
        out.extend_from_slice(&fst_bytes);

        let body_crc = crc32fast::hash(&out[INDEX_HEADER_SIZE..]);
        let header = IndexSegmentHeader {
            version: IndexSegmentHeader::VERSION,
            created_at_nanos,
            base_position: index.base(),
            event_count: index.len() as u64,
            typedict_off,
            postings_off,
            fst_off,
            fst_len,
            body_crc,
        };
        out[..INDEX_HEADER_SIZE].copy_from_slice(&header.to_bytes());
        out
    }

    /// Opens a segment from its full file bytes, validating the header, the whole-file
    /// length, and the body CRC. Any failure is recoverable: the caller rebuilds from the
    /// log rather than refusing to open.
    pub fn from_bytes(data: Arc<[u8]>) -> Result<Self, IndexSegmentError> {
        if data.len() < INDEX_HEADER_SIZE {
            return Err(IndexSegmentError::TooShort { len: data.len() });
        }
        let header_bytes: &[u8; INDEX_HEADER_SIZE] = data[..INDEX_HEADER_SIZE].try_into().unwrap();
        let header = IndexSegmentHeader::from_bytes(header_bytes)?;

        if header.segment_len() != data.len() {
            return Err(IndexSegmentError::LengthMismatch {
                header: header.segment_len(),
                actual: data.len(),
            });
        }

        let computed = crc32fast::hash(&data[INDEX_HEADER_SIZE..]);
        if computed != header.body_crc {
            return Err(IndexSegmentError::BodyChecksumMismatch {
                expected: header.body_crc,
                computed,
            });
        }

        let type_names = parse_type_dict(&data[header.type_dict_range()])?;

        let fst_region = SharedSlice {
            data: Arc::clone(&data),
            range: header.fst_range(),
        };
        let map = fst::Map::new(fst_region).map_err(|source| IndexSegmentError::Fst {
            detail: source.to_string(),
        })?;

        Ok(IndexSegment {
            header,
            data,
            map,
            type_names,
        })
    }

    /// The parsed header, used by [`IndexSet`](super::IndexSet) for pruning.
    pub fn header(&self) -> &IndexSegmentHeader {
        &self.header
    }

    /// The postings region, addressed by the FST values.
    fn postings_region(&self) -> &[u8] {
        &self.data[self.header.postings_range()]
    }
}

/// Writes `bytes` to `path` durably: the file is synced, then the parent directory is
/// synced so the new name is durable too.
pub fn write_segment_file(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let mut file = File::create(path)?;
    // Crash point: ENOSPC on index flush. The index is disposable, so this must degrade to a
    // rebuild from the log, never corrupt the store or block the (already durable) write.
    seglog::crash_io!("index_flush");
    file.write_all(bytes)?;
    // Crash point: the .idx content is written but not yet fsynced. A crash here must leave a
    // store that rebuilds the missing or partial index from the log on open.
    seglog::crash_point!("index_after_write");
    file.sync_all()?;
    // Crash point: the .idx file is fsynced but its directory entry is not yet durable.
    seglog::crash_point!("index_after_sync");
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        File::open(parent)?.sync_all()?;
    }
    Ok(())
}

/// The on-disk index segment reader arm of [`SegmentIndex`]: postings are decoded from
/// the varint region into owned vecs ([`Cow::Owned`]), the counterpart to the tail
/// index's borrowed slices.
impl SegmentIndex for IndexSegment {
    fn base(&self) -> Position {
        self.header.base_position
    }

    fn len(&self) -> u32 {
        // Bounded by the source log segment (segment_size <= u32::MAX), so this fits.
        self.header.event_count as u32
    }

    fn term_postings(&self, tag: &str) -> Option<Cow<'_, [u32]>> {
        let value = self.map.get(tag.as_bytes())?;
        // The body CRC validated at open, and we never emit the reserved dense tier, so a
        // decode failure here is an integrity bug in a segment we already trusted.
        let postings = decode_postings(value, self.postings_region())
            .expect("postings decode on a CRC-validated index segment");
        Some(postings)
    }

    fn term_len(&self, tag: &str) -> Option<u32> {
        let value = self.map.get(tag.as_bytes())?;
        // Exact and cheap: the tier gives a singleton's 1 directly and reads only a small
        // term's leading count varint. Like `term_postings`, a decode failure on an
        // already-CRC-validated segment is an integrity bug, not a normal outcome.
        let len = posting_len(value, self.postings_region())
            .expect("posting length on a CRC-validated index segment");
        Some(len)
    }

    fn type_id(&self, name: &str) -> Option<u16> {
        // Type cardinality is low (10s to 100s), so a linear scan beats hashing.
        self.type_names
            .iter()
            .position(|n| n.as_ref() == name)
            .map(|i| i as u16)
    }

    fn type_at(&self, local: u32) -> u16 {
        let start = self.header.type_column_range().start + local as usize * 2;
        u16::from_le_bytes(self.data[start..start + 2].try_into().unwrap())
    }
}

impl fmt::Debug for IndexSegment {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IndexSegment")
            .field("base_position", &self.header.base_position)
            .field("event_count", &self.header.event_count)
            .field("types", &self.type_names.len())
            .finish_non_exhaustive()
    }
}

/// Reads the type dictionary: `(u16 length, UTF-8 name)` per id in order, until the
/// region is exhausted. The header delimits the region, so no count is stored; ids are
/// assigned by position.
fn parse_type_dict(mut buf: &[u8]) -> Result<Vec<Box<str>>, IndexSegmentError> {
    let mut names = Vec::new();
    while !buf.is_empty() {
        let len = take_u16(&mut buf)? as usize;
        if buf.len() < len {
            return Err(IndexSegmentError::MalformedTypeDict {
                detail: "type name runs past the dictionary",
            });
        }
        let (name, rest) = buf.split_at(len);
        let name = str::from_utf8(name).map_err(|_| IndexSegmentError::MalformedTypeDict {
            detail: "type name is not valid UTF-8",
        })?;
        names.push(Box::from(name));
        buf = rest;
    }
    Ok(names)
}

fn take_u16(buf: &mut &[u8]) -> Result<u16, IndexSegmentError> {
    if buf.len() < 2 {
        return Err(IndexSegmentError::MalformedTypeDict {
            detail: "truncated length field",
        });
    }
    let (head, rest) = buf.split_at(2);
    *buf = rest;
    Ok(u16::from_le_bytes(head.try_into().unwrap()))
}

fn fit_u16(value: usize, what: &str) -> u16 {
    u16::try_from(value).unwrap_or_else(|_| panic!("{what} exceeds u16 in one index segment"))
}

fn fit_u32(value: usize, what: &str) -> u32 {
    // Every offset is bounded by the source log segment, and SegmentConfig::validate
    // pins segment_size <= u32::MAX, so this conversion is infallible in practice.
    u32::try_from(value).unwrap_or_else(|_| {
        panic!("{what} exceeds u32; segment_size <= u32::MAX guarantees it does not")
    })
}

/// A rejected on-disk index segment. Like [`IndexHeaderError`], every variant means
/// "rebuild this segment from the log", never "refuse to open the store".
#[derive(Debug, Error)]
pub enum IndexSegmentError {
    #[error("index segment is shorter ({len} bytes) than the header")]
    TooShort { len: usize },
    #[error(transparent)]
    Header(#[from] IndexHeaderError),
    #[error("index segment length mismatch: header says {header} bytes, file is {actual}")]
    LengthMismatch { header: usize, actual: usize },
    #[error(
        "index segment body checksum mismatch: expected {expected:#010x}, computed {computed:#010x}"
    )]
    BodyChecksumMismatch { expected: u32, computed: u32 },
    #[error("malformed type dictionary: {detail}")]
    MalformedTypeDict { detail: &'static str },
    #[error("fst term dictionary is invalid: {detail}")]
    Fst { detail: String },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::{Event, EventType, Tag, Tags};
    use crate::index::search;
    use crate::query::{Query, QueryItem};
    use smallvec::SmallVec;

    fn tags(items: &[&str]) -> Tags {
        Tags::new(
            items
                .iter()
                .map(|s| Tag::new(*s).unwrap())
                .collect::<SmallVec<[Tag; 4]>>(),
        )
        .unwrap()
    }

    fn event(ty: &str, tag_strs: &[&str]) -> Event {
        Event::new(&EventType::new(ty).unwrap(), &tags(tag_strs), b"payload").unwrap()
    }

    /// Builds a tail index over the five-event fixture (base 1), matching search.rs.
    fn fixture() -> ActiveTail {
        let events = [
            event("Registered", &[]),
            event("Enrolled", &["course:c1"]),
            event("Enrolled", &["course:c1", "student:s1"]),
            event("Renamed", &["student:s1"]),
            event("Registered", &["course:c1"]),
        ];
        let index = ActiveTail::new(Position::new(1));
        for (i, ev) in events.iter().enumerate() {
            index
                .push(Position::new(1 + i as u64), ev.as_ref())
                .unwrap();
        }
        index
    }

    fn sealed(index: &ActiveTail) -> IndexSegment {
        let bytes = IndexSegment::encode(index);
        IndexSegment::from_bytes(Arc::from(bytes)).unwrap()
    }

    #[test]
    fn round_trips_through_bytes() {
        let seg = sealed(&fixture());
        assert_eq!(seg.base(), Position::new(1));
        assert_eq!(seg.len(), 5);
        assert_eq!(seg.header().max_position(), Some(Position::new(5)));
    }

    #[test]
    fn segment_answers_queries_identically_to_the_tail_index() {
        // The on-disk segment must give the same positions as the in-memory tail for
        // every query shape: this is the on-disk half of the one-evaluator contract.
        let tail = fixture();
        let seg = sealed(&tail);
        let queries = [
            Query::all(),
            Query::items(Vec::new()),
            Query::item(QueryItem::with_tags(tags(&["course:c1"]))),
            Query::item(QueryItem::with_tags(tags(&["course:c1", "student:s1"]))),
            Query::item(QueryItem::of_types(vec![
                EventType::new("Registered").unwrap(),
            ])),
            Query::item(QueryItem::new(
                vec![EventType::new("Enrolled").unwrap()],
                tags(&["course:c1"]),
            )),
            Query::items(vec![
                QueryItem::of_types(vec![EventType::new("Renamed").unwrap()]),
                QueryItem::with_tags(tags(&["course:c1"])),
            ]),
            Query::item(QueryItem::with_tags(tags(&["ghost:x"]))),
        ];
        for query in &queries {
            for after in 0..=5 {
                let from_tail: Vec<Position> =
                    search(&tail.view_full(), query, Position::new(after)).collect();
                let from_seg: Vec<Position> = search(&seg, query, Position::new(after)).collect();
                assert_eq!(from_tail, from_seg, "query {query:?} after {after}");
            }
        }
    }

    #[test]
    fn singleton_and_multi_postings_both_survive() {
        // student:s1 appears twice (tier1 deltas); a lone tag appears once (tier0 inline).
        let index = ActiveTail::new(Position::new(1));
        index
            .push(Position::new(1), event("E", &["only:once"]).as_ref())
            .unwrap();
        index
            .push(Position::new(2), event("E", &["twice:here"]).as_ref())
            .unwrap();
        index
            .push(Position::new(3), event("E", &["twice:here"]).as_ref())
            .unwrap();
        let seg = sealed(&index);
        assert_eq!(
            seg.term_postings("only:once").unwrap().into_owned(),
            vec![0]
        );
        assert_eq!(
            seg.term_postings("twice:here").unwrap().into_owned(),
            vec![1, 2]
        );
        assert!(seg.term_postings("absent").is_none());
    }

    #[test]
    fn term_len_is_exact_and_matches_the_postings() {
        // only:once is a tier0 singleton (length 1 from the FST value); twice:here is a
        // tier1 small term whose length is its leading count varint. Both must equal the
        // decoded posting-list length, without materializing it, and an absent tag is None.
        let index = ActiveTail::new(Position::new(1));
        index
            .push(Position::new(1), event("E", &["only:once"]).as_ref())
            .unwrap();
        index
            .push(Position::new(2), event("E", &["twice:here"]).as_ref())
            .unwrap();
        index
            .push(Position::new(3), event("E", &["twice:here"]).as_ref())
            .unwrap();
        let seg = sealed(&index);
        assert_eq!(seg.term_len("only:once"), Some(1));
        assert_eq!(seg.term_len("twice:here"), Some(2));
        assert_eq!(seg.term_len("absent"), None);
        assert_eq!(
            seg.term_len("twice:here").unwrap() as usize,
            seg.term_postings("twice:here").unwrap().len()
        );
    }

    #[test]
    fn body_checksum_mismatch_is_detected() {
        let mut bytes = IndexSegment::encode(&fixture());
        // Flip a byte in the body (past the header) without fixing the CRC.
        let last = bytes.len() - 1;
        bytes[last] ^= 0xFF;
        assert!(matches!(
            IndexSegment::from_bytes(Arc::from(bytes)),
            Err(IndexSegmentError::BodyChecksumMismatch { .. })
        ));
    }

    #[test]
    fn truncated_segment_is_a_length_mismatch() {
        let mut bytes = IndexSegment::encode(&fixture());
        bytes.truncate(bytes.len() - 1);
        assert!(matches!(
            IndexSegment::from_bytes(Arc::from(bytes)),
            Err(IndexSegmentError::LengthMismatch { .. })
        ));
    }

    #[test]
    fn empty_tail_index_round_trips() {
        let index = ActiveTail::new(Position::new(1));
        let seg = sealed(&index);
        assert_eq!(seg.len(), 0);
        assert_eq!(seg.header().max_position(), None);
        assert!(seg.term_postings("anything").is_none());
    }

    #[test]
    fn max_distinct_types_round_trips_without_panic() {
        // The dense type column addresses u16::MAX + 1 = 65536 distinct types, and the
        // interner permits exactly that many, so a segment can legitimately hold all of
        // them. The type dictionary must encode and reload every one: a u16 count field
        // would have overflowed at precisely this boundary and panicked the writer thread.
        let n = u16::MAX as u64 + 1; // 65536
        let index = ActiveTail::new(Position::new(1));
        for i in 0..n {
            let ev = event(&format!("T{i}"), &[]);
            index.push(Position::new(1 + i), ev.as_ref()).unwrap();
        }

        let seg = sealed(&index);
        assert_eq!(seg.len(), n as u32);

        // The last-assigned type id is u16::MAX, and both the dictionary and the column
        // round-trip it.
        let last_ty = format!("T{}", n - 1);
        let id = seg.type_id(&last_ty).unwrap();
        assert_eq!(id, u16::MAX);
        assert_eq!(seg.type_at((n - 1) as u32), id);

        // A type-only query returns exactly the one event carrying that type.
        let q = Query::item(QueryItem::of_types(vec![EventType::new(last_ty).unwrap()]));
        let got: Vec<Position> = search(&seg, &q, Position::ZERO).collect();
        assert_eq!(got, vec![Position::new(n)]);
    }
}
