//! Seeded, regenerable event content.
//!
//! Every event is a pure function of `(seed, seq)`, so any acked write can be regenerated and
//! compared byte for byte after recovery, and any recovered event can be traced back to the
//! `seq` that produced it (the `seq` is embedded in the first eight payload bytes). Nothing
//! about an event needs to be remembered: the seed plus the witness log is enough to
//! reconstruct the whole expected store.

use tephra_client::Event;

/// Number of distinct `entity:` tag values. Small enough that many events share a tag, so DCB
/// conditions on overlapping tags actually collide.
pub const ENTITY_MOD: u64 = 64;
/// Number of distinct `shard:` tag values.
pub const SHARD_MOD: u64 = 8;

const EVENT_TYPES: [&str; 5] = ["Enrolled", "Dropped", "Graded", "Paid", "Refunded"];

/// The event types the workload produces, exposed so the invariant checker can build type
/// queries over exactly this set.
pub const EVENT_TYPES_PUBLIC: [&str; 5] = EVENT_TYPES;

/// A deterministic scalar hash / PRNG step (SplitMix64).
fn splitmix64(x: u64) -> u64 {
    let mut z = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// The regenerated content of the event at `seq` under `seed`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GenEvent {
    pub event_type: String,
    /// Sorted to match the server's canonical tag order.
    pub tags: Vec<String>,
    pub payload: Vec<u8>,
}

/// Regenerates the exact event content for `(seed, seq)`.
pub fn gen_event(seed: u64, seq: u64) -> GenEvent {
    let h = splitmix64(seed ^ seq.wrapping_mul(0x1000_0000_0000_0001));

    let event_type = EVENT_TYPES[(h % EVENT_TYPES.len() as u64) as usize].to_string();

    let entity = seq % ENTITY_MOD;
    let shard = (h >> 8) % SHARD_MOD;
    let mut tags = vec![format!("entity:{entity}"), format!("shard:{shard}")];
    tags.sort();
    tags.dedup();

    // Payload: seq in the first eight bytes so a recovered event self-identifies, then a
    // deterministic byte stream whose length varies with the event.
    let body_len = 16 + (h % 48) as usize;
    let mut payload = Vec::with_capacity(8 + body_len);
    payload.extend_from_slice(&seq.to_le_bytes());
    let mut state = splitmix64(h);
    while payload.len() < 8 + body_len {
        state = splitmix64(state);
        payload.extend_from_slice(&state.to_le_bytes());
    }
    payload.truncate(8 + body_len);

    GenEvent {
        event_type,
        tags,
        payload,
    }
}

/// Builds the client [`Event`] for `(seed, seq)`.
pub fn client_event(seed: u64, seq: u64) -> Event {
    let g = gen_event(seed, seq);
    let tag_refs: Vec<&str> = g.tags.iter().map(String::as_str).collect();
    Event::new(g.event_type.as_str(), tag_refs, g.payload).expect("generated event is always valid")
}

/// Extracts the embedded `seq` from a recovered payload, or `None` if it is too short to carry
/// one (which would itself be a corruption: every generated payload is at least 8 bytes).
pub fn seq_of_payload(payload: &[u8]) -> Option<u64> {
    payload
        .get(..8)
        .map(|bytes| u64::from_le_bytes(bytes.try_into().unwrap()))
}
