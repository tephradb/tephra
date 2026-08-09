//! Event codec.
//!
//! An event carries a **type**, a set of **tags**, and an opaque **payload**. The
//! type and tags are the addressable surface (they drive queries and the append
//! condition); the payload is bytes the store never interprets. The name and tag types
//! themselves ([`EventType`], [`Tag`], [`Tags`]) live in `dcbdb-core` and are re-exported
//! here; this module owns the on-disk/in-memory encoding.
//!
//! The in-memory layout is the on-disk layout. An [`Event`] owns one contiguous
//! buffer holding a small length header followed by the type, the tags in sorted
//! order, and the payload. That same buffer is what the log stores verbatim as a
//! record, so decoding is parsing a few integers rather than copying: [`EventRef`]
//! borrows the record buffer directly and materialises nothing on the read path.
//!
//! Encoded layout (little-endian), where `n` is the tag count:
//!
//! ```text
//! +-----------+-----------+-------------+-----+---------------+
//! | type_len  | tag_count | tag_len[0]  | ... | tag_len[n-1]  |  header
//! |  u16      |  u16      |  u16        |     |  u16          |
//! +-----------+-----------+-------------+-----+---------------+
//! | type bytes | tag[0] bytes | ... | tag[n-1] bytes | payload |  data
//! +------------+--------------+-----+----------------+---------+
//! ```
//!
//! The payload has no length field: it is whatever remains after the tags, so an
//! event's encoded form is exactly `header + type + tags + payload` with no slack.
//! Because tags are stored sorted and deduplicated, identical tag sets always encode
//! to identical bytes: the encoding is canonical.

use std::fmt;
use std::str;

use thiserror::Error;

pub use dcbdb_core::{EventType, MAX_NAME_LEN, NameError, Tag, Tags, TagsError};

/// Fixed part of the encoded header: `type_len` (`u16`) plus `tag_count` (`u16`).
const FIXED_HEADER: usize = 4;

/// Byte offset where the type begins, given the tag count.
fn header_len(tag_count: usize) -> usize {
    FIXED_HEADER + 2 * tag_count
}

// ---------------------------------------------------------------------------
// Codec errors
// ---------------------------------------------------------------------------

/// Error encoding an [`Event`].
#[derive(Debug, Error, PartialEq, Eq)]
pub enum EncodeError {
    #[error("event has {count} tags, exceeding the maximum of {max}")]
    TooManyTags { count: usize, max: usize },
    #[error("encoded event data region of {size} bytes exceeds the {max}-byte maximum")]
    TooLarge { size: u64, max: u64 },
}

/// Error decoding an [`EventRef`] from bytes.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum DecodeError {
    #[error("buffer is shorter than the encoded event claims")]
    Truncated,
    #[error("encoded event data region exceeds the addressable maximum")]
    TooLarge,
    #[error("event type is empty")]
    EmptyType,
    #[error("a tag is empty")]
    EmptyTag,
    #[error("type or tag bytes are not valid UTF-8")]
    InvalidUtf8,
    #[error("tags are not in strictly ascending order")]
    TagsNotSorted,
}

// ---------------------------------------------------------------------------
// Event and EventRef
// ---------------------------------------------------------------------------

/// An owned event: one contiguous buffer plus the cached offsets needed to slice it.
///
/// The owned counterpart of [`EventRef`]. The borrowed form is the primitive used on
/// the read path; this is the convenience obtained via [`EventRef::to_owned`] or built
/// directly with [`Event::new`].
#[derive(Clone, PartialEq, Eq)]
pub struct Event {
    buf: Box<[u8]>,
    /// Byte offset where the payload begins: `header_len + type_len + sum(tag_lens)`.
    /// A cached prefix sum; a decode invariant, never recomputed per access. The type
    /// length and per-tag lengths are not cached: they are single `u16` reads out of
    /// `buf`'s header, so caching them would only duplicate bytes already present.
    data_offset: u32,
}

impl Event {
    /// Encodes an event into its canonical contiguous form.
    pub fn new(event_type: &EventType, tags: &Tags, payload: &[u8]) -> Result<Self, EncodeError> {
        let ty = event_type.as_str();
        let tag_slice = tags.as_slice();
        let tag_count = tag_slice.len();
        if tag_count > u16::MAX as usize {
            return Err(EncodeError::TooManyTags {
                count: tag_count,
                max: u16::MAX as usize,
            });
        }

        // Lengths are bounded by `MAX_NAME_LEN == u16::MAX` (enforced when the
        // `EventType`/`Tag` were constructed), so every `as u16` below is lossless.
        // The size is computed in `u64` so the prefix sum cannot overflow `usize` on a
        // 32-bit target before the `u32::MAX` check rejects it; this mirrors the guard
        // in `EventRef::from_bytes`.
        let type_len = ty.len();
        let tags_total: u64 = tag_slice.iter().map(|t| t.as_str().len() as u64).sum();
        let data_start = header_len(tag_count) as u64 + type_len as u64 + tags_total;
        if data_start > u32::MAX as u64 {
            return Err(EncodeError::TooLarge {
                size: data_start,
                max: u32::MAX as u64,
            });
        }
        let data_offset = data_start as u32;

        let mut buf = Vec::with_capacity(data_start as usize + payload.len());
        buf.extend_from_slice(&(type_len as u16).to_le_bytes());
        buf.extend_from_slice(&(tag_count as u16).to_le_bytes());
        for t in tag_slice {
            buf.extend_from_slice(&(t.as_str().len() as u16).to_le_bytes());
        }
        buf.extend_from_slice(ty.as_bytes());
        for t in tag_slice {
            buf.extend_from_slice(t.as_str().as_bytes());
        }
        buf.extend_from_slice(payload);

        Ok(Event {
            buf: buf.into_boxed_slice(),
            data_offset,
        })
    }

    /// Borrows this event as an [`EventRef`] over the same buffer.
    pub fn as_ref(&self) -> EventRef<'_> {
        EventRef {
            buf: &self.buf,
            data_offset: self.data_offset,
        }
    }

    pub fn event_type(&self) -> &str {
        decode_type(&self.buf)
    }

    pub fn tags(&self) -> TagsRef<'_> {
        decode_tags(&self.buf)
    }

    /// The opaque payload bytes.
    pub fn data(&self) -> &[u8] {
        decode_data(&self.buf, self.data_offset)
    }

    /// The whole encoded event, ready to store as a log record verbatim.
    pub fn as_bytes(&self) -> &[u8] {
        &self.buf
    }
}

impl fmt::Debug for Event {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.as_ref().fmt(f)
    }
}

/// A borrowed event: a view over an encoded buffer, allocating nothing.
///
/// This is the primitive on the high-volume read path. A scan decodes each record
/// into an `EventRef` borrowing the reader's buffer, filters on type and tags, and
/// touches the payload only for events that match.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct EventRef<'a> {
    buf: &'a [u8],
    data_offset: u32,
}

impl<'a> EventRef<'a> {
    /// Decodes an event from its encoded bytes, validating structure and content.
    ///
    /// Checks bounds, that the type and tags are valid UTF-8, non-empty, and (for
    /// tags) strictly ascending. Everything after the tags is the payload. The
    /// accessors rely on these checks, so they do no validation of their own.
    pub fn from_bytes(buf: &'a [u8]) -> Result<Self, DecodeError> {
        if buf.len() < FIXED_HEADER {
            return Err(DecodeError::Truncated);
        }
        let type_len = read_u16(buf, 0) as usize;
        let tag_count = read_u16(buf, 2) as usize;

        let hlen = header_len(tag_count);
        if buf.len() < hlen {
            return Err(DecodeError::Truncated);
        }
        if type_len == 0 {
            return Err(DecodeError::EmptyType);
        }

        // Sum the tag lengths from the header, rejecting empty tags. Guard each
        // addition so a lying header cannot wrap `usize`.
        let mut tags_total: usize = 0;
        for i in 0..tag_count {
            let len = read_u16(buf, FIXED_HEADER + 2 * i) as usize;
            if len == 0 {
                return Err(DecodeError::EmptyTag);
            }
            tags_total = tags_total.checked_add(len).ok_or(DecodeError::TooLarge)?;
        }

        // Prefix sum locating the payload; require the buffer to actually hold it.
        let data_start = hlen
            .checked_add(type_len)
            .and_then(|x| x.checked_add(tags_total))
            .ok_or(DecodeError::TooLarge)?;
        if buf.len() < data_start {
            return Err(DecodeError::Truncated);
        }
        let data_offset = u32::try_from(data_start).map_err(|_| DecodeError::TooLarge)?;

        // Validate the type, then each tag: UTF-8 and strictly ascending order. The
        // accessors decode these ranges unchecked, so this is where the UTF-8 and
        // ordering invariants are established. Every slice below stays within
        // `data_start`, already checked to be in bounds.
        let type_end = hlen + type_len;
        str::from_utf8(&buf[hlen..type_end]).map_err(|_| DecodeError::InvalidUtf8)?;

        let mut pos = type_end;
        let mut prev: Option<&str> = None;
        for i in 0..tag_count {
            let len = read_u16(buf, FIXED_HEADER + 2 * i) as usize;
            let end = pos + len;
            let tag = str::from_utf8(&buf[pos..end]).map_err(|_| DecodeError::InvalidUtf8)?;
            if let Some(p) = prev
                && tag <= p
            {
                return Err(DecodeError::TagsNotSorted);
            }
            prev = Some(tag);
            pos = end;
        }

        Ok(EventRef { buf, data_offset })
    }

    pub fn event_type(&self) -> &'a str {
        decode_type(self.buf)
    }

    pub fn tags(&self) -> TagsRef<'a> {
        decode_tags(self.buf)
    }

    /// The opaque payload bytes.
    pub fn data(&self) -> &'a [u8] {
        decode_data(self.buf, self.data_offset)
    }

    /// The whole encoded event this view borrows.
    pub fn as_bytes(&self) -> &'a [u8] {
        self.buf
    }

    /// Copies this view into an owned [`Event`].
    pub fn to_owned(&self) -> Event {
        Event {
            buf: Box::from(self.buf),
            data_offset: self.data_offset,
        }
    }
}

impl fmt::Debug for EventRef<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EventRef")
            .field("event_type", &self.event_type())
            .field("tags", &self.tags().collect::<Vec<_>>())
            .field("data_len", &self.data().len())
            .finish()
    }
}

// ---------------------------------------------------------------------------
// Shared decode helpers and the borrowed tag iterator
// ---------------------------------------------------------------------------

/// Reads a little-endian `u16` at byte offset `at`. Callers pass a validated event
/// buffer, so `at + 2 <= buf.len()`.
fn read_u16(buf: &[u8], at: usize) -> u16 {
    u16::from_le_bytes([buf[at], buf[at + 1]])
}

fn decode_type(buf: &[u8]) -> &str {
    let type_len = read_u16(buf, 0) as usize;
    let tag_count = read_u16(buf, 2) as usize;
    let start = header_len(tag_count);
    // SAFETY: `EventRef::from_bytes` validated this range as UTF-8, and `Event::new`
    // copied it from a `&str`. Both construction paths guarantee valid UTF-8.
    unsafe { str::from_utf8_unchecked(&buf[start..start + type_len]) }
}

fn decode_tags(buf: &[u8]) -> TagsRef<'_> {
    let type_len = read_u16(buf, 0) as usize;
    let tag_count = read_u16(buf, 2) as usize;
    let lens = &buf[FIXED_HEADER..FIXED_HEADER + 2 * tag_count];
    let start = header_len(tag_count) + type_len;
    TagsRef {
        data: &buf[start..],
        lens,
        idx: 0,
    }
}

fn decode_data(buf: &[u8], data_offset: u32) -> &[u8] {
    &buf[data_offset as usize..]
}

/// Borrowing iterator over an event's tags, yielding each as a `&str`.
///
/// The tags accessor cannot return `&[Tag]`: a slice needs fixed-stride elements,
/// but tags are variable-length strings packed contiguously, so a `&[Tag]` would
/// require a separate fat-pointer array or an allocation, defeating the zero-copy
/// read path. Yielding borrowed `&str`s is the zero-copy shape. Tags arrive in
/// sorted order, so iteration is sorted too.
///
/// `lens` is the raw little-endian `u16` length header (2 bytes per tag), read
/// straight out of the event buffer rather than a decoded side array, so the view
/// stays allocation-free.
///
/// `Copy` because it is three trivially-copyable fields (two shared slices and a
/// cursor): copying yields an independent cursor over the same tags, which is what
/// the match predicate needs to re-walk the tags without re-decoding the event.
#[derive(Clone, Copy)]
pub struct TagsRef<'a> {
    data: &'a [u8],
    lens: &'a [u8],
    idx: usize,
}

impl<'a> Iterator for TagsRef<'a> {
    type Item = &'a str;

    fn next(&mut self) -> Option<&'a str> {
        let off = self.idx * 2;
        if off + 2 > self.lens.len() {
            return None;
        }
        let len = u16::from_le_bytes([self.lens[off], self.lens[off + 1]]) as usize;
        self.idx += 1;
        let (head, tail) = self.data.split_at(len);
        self.data = tail;
        // SAFETY: validated UTF-8 at construction (see `decode_type`).
        Some(unsafe { str::from_utf8_unchecked(head) })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let rem = self.lens.len() / 2 - self.idx;
        (rem, Some(rem))
    }
}

impl ExactSizeIterator for TagsRef<'_> {}

#[cfg(test)]
mod tests {
    use super::*;

    use smallvec::SmallVec;

    fn ty(s: &str) -> EventType {
        EventType::new(s).unwrap()
    }

    fn tag(s: &str) -> Tag {
        Tag::new(s).unwrap()
    }

    fn tags(items: &[&str]) -> Tags {
        Tags::new(items.iter().map(|s| tag(s)).collect::<SmallVec<[Tag; 4]>>()).unwrap()
    }

    // --- Event round-trip ---

    #[test]
    fn round_trip_full_event() {
        let event = Event::new(
            &ty("Registered"),
            &tags(&["course:c1", "student:s1"]),
            b"payload",
        )
        .unwrap();

        let decoded = EventRef::from_bytes(event.as_bytes()).unwrap();
        assert_eq!(decoded.event_type(), "Registered");
        assert_eq!(
            decoded.tags().collect::<Vec<_>>(),
            vec!["course:c1", "student:s1"]
        );
        assert_eq!(decoded.data(), b"payload");
        assert_eq!(decoded.tags().len(), 2);
    }

    #[test]
    fn round_trip_no_tags_empty_payload() {
        let event = Event::new(&ty("Ping"), &Tags::empty(), b"").unwrap();
        let decoded = EventRef::from_bytes(event.as_bytes()).unwrap();
        assert_eq!(decoded.event_type(), "Ping");
        assert_eq!(decoded.tags().count(), 0);
        assert_eq!(decoded.data(), b"");
    }

    #[test]
    fn owned_and_borrowed_agree() {
        let event = Event::new(&ty("T"), &tags(&["a", "bb", "ccc"]), b"data").unwrap();
        let borrowed = EventRef::from_bytes(event.as_bytes()).unwrap();
        let owned = borrowed.to_owned();

        assert_eq!(owned.event_type(), borrowed.event_type());
        assert_eq!(
            owned.tags().collect::<Vec<_>>(),
            borrowed.tags().collect::<Vec<_>>()
        );
        assert_eq!(owned.data(), borrowed.data());
        assert_eq!(owned.as_bytes(), borrowed.as_bytes());
        // The owned copy encodes identically to the source event.
        assert_eq!(owned, event);
    }

    #[test]
    fn encoding_is_canonical() {
        // Same tag set, different construction order, must encode identically.
        let a = Event::new(&ty("T"), &tags(&["z", "a", "m"]), b"p").unwrap();
        let b = Event::new(&ty("T"), &tags(&["a", "m", "z"]), b"p").unwrap();
        assert_eq!(a.as_bytes(), b.as_bytes());
    }

    #[test]
    fn tags_never_touch_payload() {
        // A payload whose bytes look like more tag data must not be yielded as a tag.
        let event = Event::new(&ty("T"), &tags(&["aa"]), b"\xff\xff\xff\xff").unwrap();
        let decoded = EventRef::from_bytes(event.as_bytes()).unwrap();
        assert_eq!(decoded.tags().collect::<Vec<_>>(), vec!["aa"]);
        assert_eq!(decoded.data(), b"\xff\xff\xff\xff");
    }

    // --- Decode rejection ---

    #[test]
    fn decode_rejects_short_header() {
        assert_eq!(EventRef::from_bytes(&[]), Err(DecodeError::Truncated));
        assert_eq!(
            EventRef::from_bytes(&[1, 0, 0]),
            Err(DecodeError::Truncated)
        );
    }

    #[test]
    fn decode_rejects_truncated_tag_lens() {
        // type_len = 1, tag_count = 2, but only one tag_len field present.
        let buf = [1u8, 0, 2, 0, 5, 0];
        assert_eq!(EventRef::from_bytes(&buf), Err(DecodeError::Truncated));
    }

    #[test]
    fn decode_rejects_lying_type_len() {
        // type_len = 10, no tags, but no type bytes follow the header.
        let buf = [10u8, 0, 0, 0];
        assert_eq!(EventRef::from_bytes(&buf), Err(DecodeError::Truncated));
    }

    #[test]
    fn decode_rejects_lying_tag_len() {
        // type_len = 1, tag_count = 1, tag_len = 9. Data holds the type byte but not
        // the promised 9 tag bytes.
        let mut buf = vec![1u8, 0, 1, 0, 9, 0];
        buf.push(b'T');
        buf.extend_from_slice(b"short");
        assert_eq!(EventRef::from_bytes(&buf), Err(DecodeError::Truncated));
    }

    #[test]
    fn decode_rejects_empty_type() {
        let buf = [0u8, 0, 0, 0];
        assert_eq!(EventRef::from_bytes(&buf), Err(DecodeError::EmptyType));
    }

    #[test]
    fn decode_rejects_empty_tag() {
        // type_len = 1, tag_count = 1, tag_len = 0.
        let buf = [1u8, 0, 1, 0, 0, 0, b'T'];
        assert_eq!(EventRef::from_bytes(&buf), Err(DecodeError::EmptyTag));
    }

    #[test]
    fn decode_rejects_invalid_utf8() {
        // type_len = 1, no tags, type byte is an invalid UTF-8 lead byte.
        let buf = [1u8, 0, 0, 0, 0xFF];
        assert_eq!(EventRef::from_bytes(&buf), Err(DecodeError::InvalidUtf8));
    }

    #[test]
    fn decode_rejects_unsorted_tags() {
        // type_len = 1, tag_count = 2, tags "b" then "a": descending.
        let buf = [1u8, 0, 2, 0, 1, 0, 1, 0, b'T', b'b', b'a'];
        assert_eq!(EventRef::from_bytes(&buf), Err(DecodeError::TagsNotSorted));
    }

    #[test]
    fn decode_rejects_duplicate_tags() {
        // Equal adjacent tags are not strictly ascending.
        let buf = [1u8, 0, 2, 0, 1, 0, 1, 0, b'T', b'a', b'a'];
        assert_eq!(EventRef::from_bytes(&buf), Err(DecodeError::TagsNotSorted));
    }

    #[test]
    fn decode_accepts_manually_built_bytes() {
        // The mirror of the rejection tests: a hand-built valid buffer decodes.
        let buf = [1u8, 0, 2, 0, 1, 0, 1, 0, b'T', b'a', b'b', b'x', b'y'];
        let decoded = EventRef::from_bytes(&buf).unwrap();
        assert_eq!(decoded.event_type(), "T");
        assert_eq!(decoded.tags().collect::<Vec<_>>(), vec!["a", "b"]);
        assert_eq!(decoded.data(), b"xy");
    }
}
