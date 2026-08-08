//! Tiered posting-list encoding for the on-disk index segment, and the unsigned
//! LEB128 varint it is built on.
//!
//! A term's postings are stored in one of a few representations, chosen by how many
//! events carry the tag (CLAUDE.md 7, "tiered postings by term frequency"). The tier
//! and its payload are packed into the `u64` value the FST term dictionary maps each
//! tag to, so a lookup that hits the FST already knows how to find the list without a
//! second indirection:
//!
//! - **tier0, singleton.** Exactly one event carries the tag. The single local
//!   position is inlined directly in the FST value; there is no entry in the postings
//!   region at all.
//! - **tier1, small.** Two or more events. The FST value is a byte offset into the
//!   postings region, where the list is a `varint(count)` followed by `count` varint
//!   deltas (`first` absolute, then `pos[i] - pos[i-1]`), reconstructing the ascending
//!   local positions.
//! - **tier2, dense (Roaring).** Reserved but never emitted in phase 5b: a Roaring
//!   bitmap for very frequent tags. Decoding one is a hard, named error rather than a
//!   silent skip, so the day it is added it cannot be mistaken for a corrupt list.
//!
//! Everything here is pure over `&[u8]` / `&[u32]`, with no I/O, so it is testable in
//! isolation and reused unchanged by the sealer and the reader.

use std::borrow::Cow;

use thiserror::Error;

/// The two high bits of an FST value select the posting tier; the low 62 bits are the
/// tier's payload (an inlined position for tier0, a region byte offset for tier1). A
/// segment is bounded well under 4 GiB, so neither a local position nor a region
/// offset comes close to needing more than 32 bits, let alone 62.
const TIER_SHIFT: u32 = 62;
const PAYLOAD_MASK: u64 = (1u64 << TIER_SHIFT) - 1;

const TIER_SINGLETON: u64 = 0;
const TIER_SMALL: u64 = 1;
const TIER_DENSE: u64 = 2;

/// A malformed or unsupported posting encoding. On a segment whose body CRC validates
/// these cannot arise; they exist so decoding a corrupt or future-format region fails
/// loudly instead of returning a wrong posting list.
#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub enum PostingsError {
    #[error("varint overflows u64 (more than 10 bytes, or a 10th byte with high bits set)")]
    VarintOverflow,
    #[error("varint runs past the end of the buffer")]
    VarintTruncated,
    #[error("posting block offset {offset} is out of bounds for a {len}-byte region")]
    OffsetOutOfBounds { offset: usize, len: usize },
    #[error("local position overflows u32 while decoding delta-encoded postings")]
    PositionOverflow,
    #[error("dense (Roaring) postings are reserved but not implemented until after phase 5b")]
    ReservedDenseTier,
}

/// Appends `value` to `out` as unsigned LEB128: seven bits per byte, low group first,
/// high bit set on every byte but the last.
pub fn encode_varint(mut value: u64, out: &mut Vec<u8>) {
    loop {
        let byte = (value & 0x7f) as u8;
        value >>= 7;
        if value == 0 {
            out.push(byte);
            return;
        }
        out.push(byte | 0x80);
    }
}

/// Decodes one unsigned LEB128 value from the front of `buf`, returning it and the
/// number of bytes consumed.
///
/// A `u64` needs at most ten LEB128 bytes; the tenth may only carry a single payload
/// bit, so more than ten bytes, or a tenth byte with any other high bit set, is a hard
/// [`PostingsError::VarintOverflow`] rather than a silent wrap.
pub fn decode_varint(buf: &[u8]) -> Result<(u64, usize), PostingsError> {
    let mut value: u64 = 0;
    let mut shift: u32 = 0;
    for (i, &byte) in buf.iter().enumerate() {
        if i == 9 && byte > 0x01 {
            return Err(PostingsError::VarintOverflow);
        }
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Ok((value, i + 1));
        }
        shift += 7;
        if i == 9 {
            return Err(PostingsError::VarintOverflow);
        }
    }
    Err(PostingsError::VarintTruncated)
}

/// Encodes a term's ascending local `postings` and returns the `u64` FST value.
///
/// A singleton is inlined in the value (tier0) and touches `region` not at all;
/// anything longer is appended to `region` as a delta block and the value is the tier1
/// tag over the block's byte offset. `postings` must be non-empty and strictly
/// ascending (both hold by construction: every interned term has at least one event,
/// and the tail index feeds positions in order).
pub fn encode_postings(postings: &[u32], region: &mut Vec<u8>) -> u64 {
    debug_assert!(
        !postings.is_empty(),
        "a term always has at least one posting"
    );
    debug_assert!(
        postings.windows(2).all(|w| w[0] < w[1]),
        "postings must be strictly ascending"
    );

    if postings.len() == 1 {
        return (TIER_SINGLETON << TIER_SHIFT) | u64::from(postings[0]);
    }

    let offset = region.len() as u64;
    encode_varint(postings.len() as u64, region);
    let mut prev = 0u32;
    for (i, &pos) in postings.iter().enumerate() {
        let delta = if i == 0 { pos } else { pos - prev };
        encode_varint(u64::from(delta), region);
        prev = pos;
    }
    (TIER_SMALL << TIER_SHIFT) | offset
}

/// Decodes the postings a `value` from the FST term dictionary points at.
///
/// A singleton is materialized from the value alone (`Cow::Owned` of one element); a
/// small term is decoded from `region` at the value's offset. The result is ascending
/// local positions. Returns [`Cow`] so callers share one shape with the in-memory tail
/// index, whose postings are a borrowed slice.
pub fn decode_postings(value: u64, region: &[u8]) -> Result<Cow<'static, [u32]>, PostingsError> {
    let tier = value >> TIER_SHIFT;
    let payload = value & PAYLOAD_MASK;
    match tier {
        TIER_SINGLETON => Ok(Cow::Owned(vec![payload as u32])),
        TIER_SMALL => decode_delta_block(payload as usize, region).map(Cow::Owned),
        TIER_DENSE => Err(PostingsError::ReservedDenseTier),
        _ => unreachable!("tier is two bits, values 0..=3, and 3 is unused"),
    }
}

/// The number of postings a `value` from the FST term dictionary points at, without
/// materializing the list.
///
/// A singleton is length `1` from the value alone; a small term reads only the leading
/// `varint(count)` at its offset (not the deltas), which is what makes exact posting
/// lengths "free from the term dictionary" (CLAUDE.md 8) for the query planner. The
/// reserved dense tier is a named error, exactly as [`decode_postings`].
pub fn posting_len(value: u64, region: &[u8]) -> Result<u32, PostingsError> {
    let tier = value >> TIER_SHIFT;
    let payload = value & PAYLOAD_MASK;
    match tier {
        TIER_SINGLETON => Ok(1),
        TIER_SMALL => {
            let offset = payload as usize;
            let cursor = region
                .get(offset..)
                .ok_or(PostingsError::OffsetOutOfBounds {
                    offset,
                    len: region.len(),
                })?;
            let (count, _) = decode_varint(cursor)?;
            u32::try_from(count).map_err(|_| PostingsError::PositionOverflow)
        }
        TIER_DENSE => Err(PostingsError::ReservedDenseTier),
        _ => unreachable!("tier is two bits, values 0..=3, and 3 is unused"),
    }
}

/// Reads a `varint(count)` + `count` delta block at `offset` into ascending positions.
fn decode_delta_block(offset: usize, region: &[u8]) -> Result<Vec<u32>, PostingsError> {
    let mut cursor = region
        .get(offset..)
        .ok_or(PostingsError::OffsetOutOfBounds {
            offset,
            len: region.len(),
        })?;

    let (count, consumed) = decode_varint(cursor)?;
    cursor = &cursor[consumed..];

    let mut out = Vec::with_capacity(count as usize);
    let mut pos: u32 = 0;
    for i in 0..count {
        let (delta, consumed) = decode_varint(cursor)?;
        cursor = &cursor[consumed..];
        let delta = u32::try_from(delta).map_err(|_| PostingsError::PositionOverflow)?;
        pos = if i == 0 {
            delta
        } else {
            pos.checked_add(delta)
                .ok_or(PostingsError::PositionOverflow)?
        };
        out.push(pos);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn varint_round_trips_boundary_values() {
        for value in [
            0u64,
            1,
            127,
            128,
            255,
            300,
            16_383,
            16_384,
            u32::MAX as u64,
            u64::MAX / 2,
            u64::MAX - 1,
            u64::MAX,
        ] {
            let mut buf = Vec::new();
            encode_varint(value, &mut buf);
            let (decoded, consumed) = decode_varint(&buf).unwrap();
            assert_eq!(decoded, value, "round trip for {value}");
            assert_eq!(consumed, buf.len(), "consumed all bytes for {value}");
        }
    }

    #[test]
    fn varint_consumes_only_its_own_bytes() {
        let mut buf = Vec::new();
        encode_varint(300, &mut buf);
        buf.extend_from_slice(&[0xAA, 0xBB]); // trailing, unrelated
        let (decoded, consumed) = decode_varint(&buf).unwrap();
        assert_eq!(decoded, 300);
        assert_eq!(consumed, 2); // 300 fits in two LEB128 bytes
    }

    #[test]
    fn varint_truncated_is_an_error() {
        // A continuation bit set on the final available byte: the value is unfinished.
        assert_eq!(decode_varint(&[0x80]), Err(PostingsError::VarintTruncated));
        assert_eq!(decode_varint(&[]), Err(PostingsError::VarintTruncated));
    }

    #[test]
    fn varint_overflow_is_an_error() {
        // Eleven continuation bytes: more than a u64 can hold.
        let overflow = [0x80u8; 11];
        assert_eq!(decode_varint(&overflow), Err(PostingsError::VarintOverflow));
        // Ten bytes where the tenth carries more than the one legal payload bit.
        let mut ten = vec![0x80u8; 9];
        ten.push(0x02);
        assert_eq!(decode_varint(&ten), Err(PostingsError::VarintOverflow));
        // u64::MAX encodes as ten bytes with a 0x01 tenth byte: legal, not overflow.
        let mut max = Vec::new();
        encode_varint(u64::MAX, &mut max);
        assert_eq!(max.len(), 10);
        assert_eq!(decode_varint(&max).unwrap().0, u64::MAX);
    }

    #[test]
    fn singleton_inlines_and_touches_no_region() {
        let mut region = Vec::new();
        let value = encode_postings(&[42], &mut region);
        assert!(region.is_empty(), "singleton must not write to the region");
        assert_eq!(value >> TIER_SHIFT, TIER_SINGLETON);
        assert_eq!(
            decode_postings(value, &region).unwrap().into_owned(),
            vec![42]
        );
    }

    #[test]
    fn small_term_round_trips_as_deltas() {
        let mut region = Vec::new();
        let postings = [0u32, 1, 5, 6, 1000, 1_000_000];
        let value = encode_postings(&postings, &mut region);
        assert_eq!(value >> TIER_SHIFT, TIER_SMALL);
        assert!(!region.is_empty());
        assert_eq!(
            decode_postings(value, &region).unwrap().into_owned(),
            postings.to_vec()
        );
    }

    #[test]
    fn multiple_terms_share_one_region() {
        let mut region = Vec::new();
        let a = encode_postings(&[2, 4, 6], &mut region);
        let b = encode_postings(&[1, 3], &mut region);
        let c = encode_postings(&[9], &mut region); // singleton, no region write
        assert_eq!(
            decode_postings(a, &region).unwrap().into_owned(),
            vec![2, 4, 6]
        );
        assert_eq!(
            decode_postings(b, &region).unwrap().into_owned(),
            vec![1, 3]
        );
        assert_eq!(decode_postings(c, &region).unwrap().into_owned(), vec![9]);
    }

    #[test]
    fn posting_len_matches_the_decoded_length_without_walking_it() {
        let mut region = Vec::new();
        // Singleton: length 1 from the value alone, region untouched.
        let one = encode_postings(&[42], &mut region);
        assert_eq!(posting_len(one, &region).unwrap(), 1);
        // Small term: length is the leading count varint, equal to the decoded list length.
        let postings = [0u32, 1, 5, 6, 1000, 1_000_000];
        let small = encode_postings(&postings, &mut region);
        assert_eq!(posting_len(small, &region).unwrap(), postings.len() as u32);
        assert_eq!(
            posting_len(small, &region).unwrap() as usize,
            decode_postings(small, &region).unwrap().len()
        );
        // Reserved dense tier is a named error, like decode.
        assert_eq!(
            posting_len(TIER_DENSE << TIER_SHIFT, &region),
            Err(PostingsError::ReservedDenseTier)
        );
    }

    #[test]
    fn dense_tier_is_a_named_error() {
        let value = TIER_DENSE << TIER_SHIFT;
        assert_eq!(
            decode_postings(value, &[]),
            Err(PostingsError::ReservedDenseTier)
        );
    }

    #[test]
    fn out_of_bounds_offset_is_an_error() {
        let value = (TIER_SMALL << TIER_SHIFT) | 100;
        assert_eq!(
            decode_postings(value, &[0x01, 0x00]),
            Err(PostingsError::OffsetOutOfBounds {
                offset: 100,
                len: 2
            })
        );
    }
}
