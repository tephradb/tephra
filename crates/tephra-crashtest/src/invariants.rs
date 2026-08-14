//! The invariant checks run after every crash and restart.
//!
//! Each check returns human-readable violation strings rather than panicking, so one cycle can
//! surface several independent problems and the driver can copy artifacts before moving on. The
//! witness log is the authority for what was acked; the recovered log (read back through the
//! client as `Query::all`) is the authority for what the store now holds.

use std::collections::{BTreeMap, BTreeSet};
use std::io;
use std::net::SocketAddr;
use std::thread;
use std::time::Duration;

use tephra_client::{
    AppendCondition, Client, ClientError, ErrorCode, EventType, Position, Query, QueryItem,
    SequencedEvent, Tag, Tags,
};

use crate::server::ServerProcess;
use crate::witness::Ground;
use crate::workload::{self, EVENT_TYPES_PUBLIC, gen_event, seq_of_payload};

/// The event type used for the harness's own monotonic/DCB probe appends. The seeded workload
/// never produces this type, so recovered events carrying it are excluded from the durability,
/// phantom, and torn-record checks.
const PROBE_TYPE: &str = "Probe";

/// The outcome of one cycle's invariant run.
pub struct Outcome {
    pub violations: Vec<String>,
    pub sent: usize,
    pub acked: usize,
    pub recovered: usize,
}

/// Runs every invariant against `server` after recovery. Consumes the server (it restarts it
/// once for the index-rebuild check) and returns the possibly-restarted server plus the outcome
/// so the caller can tear it down or copy artifacts.
pub fn check(server: ServerProcess, ground: &Ground) -> io::Result<(ServerProcess, Outcome)> {
    let mut v = Vec::new();

    let Some(recovered) = read_only_checks(server.addr, ground, &mut v) else {
        let outcome = Outcome {
            violations: v,
            sent: ground.sent.len(),
            acked: ground.acked.len(),
            recovered: 0,
        };
        return Ok((server, outcome));
    };

    let mut by_pos: BTreeMap<u64, &SequencedEvent> = BTreeMap::new();
    for ev in &recovered {
        by_pos.insert(ev.position().get(), ev);
    }
    let recovered_max = by_pos.keys().next_back().copied().unwrap_or(0);

    // Position monotonicity across restarts: the next append must land at recovered_max + 1.
    check_monotonic_append(server.addr, recovered_max, &mut v);

    // DCB condition integrity: a condition that conflicts with a recovered event must still be
    // rejected; a non-conflicting one must succeed. The conflict tag is taken from an event
    // actually present in the recovered log (not from the witness), so a durability failure is not
    // misreported here as a DCB failure.
    let present_tag = present_entity_tag(&recovered);
    check_dcb_integrity(server.addr, present_tag.as_deref(), &mut v);

    // Index and log agree (rebuild half): the query answers must be identical after the index is
    // deleted and rebuilt from the log.
    let server = check_index_rebuild(server, &mut v)?;

    let outcome = Outcome {
        violations: v,
        sent: ground.sent.len(),
        acked: ground.acked.len(),
        recovered: recovered.len(),
    };
    Ok((server, outcome))
}

/// The read-only invariants (1 durability, 2 prefix, 3 no phantom, 4 in-flight, 5 no torn, 7
/// index-vs-log read half). Returns the recovered log on success, or `None` if it could not be
/// read (the reason is pushed to `v`). Shared by the full `check` and the read-only `verify_dir`.
pub fn read_only_checks(
    addr: SocketAddr,
    ground: &Ground,
    v: &mut Vec<String>,
) -> Option<Vec<SequencedEvent>> {
    // Read the recovered log as a streamed prefix: a record the client cannot decode ends the
    // stream, but everything before it is still the intact prefix we must check. A decode error is
    // itself a finding (a torn/corrupt record surfaced by recovery, invariant 5), not a reason to
    // skip durability, prefix, and phantom checks against the good prefix.
    let (recovered, read_err) = match read_prefix(addr) {
        Ok(pair) => pair,
        Err(err) => {
            v.push(format!(
                "could not connect to read the recovered log: {err}"
            ));
            return None;
        }
    };
    if let Some(err) = read_err {
        v.push(format!(
            "torn/undecodable record surfaced during the recovery read (invariant 5): {err}"
        ));
    }
    let mut by_pos: BTreeMap<u64, &SequencedEvent> = BTreeMap::new();
    for ev in &recovered {
        by_pos.insert(ev.position().get(), ev);
    }
    check_torn_and_prefix(&recovered, &by_pos, v);
    check_durability(ground, &by_pos, v);
    check_no_phantom(ground, &recovered, v);
    check_in_flight(ground, &by_pos, v);
    check_index_agrees_with_log(addr, &recovered, v);
    drop(by_pos);
    Some(recovered)
}

/// Runs only the read-only invariants against an already-running server (used by the power-loss
/// replay, where the store is opened but not mutated). Returns the violations and the recovered
/// event count so a caller can show the test did real work.
pub fn verify_readonly(addr: SocketAddr, ground: &Ground) -> (Vec<String>, usize) {
    let mut v = Vec::new();
    let recovered = read_only_checks(addr, ground, &mut v)
        .map(|events| events.len())
        .unwrap_or(0);
    (v, recovered)
}

/// Invariant 5 (no torn records) and 2 (prefix property).
fn check_torn_and_prefix(
    recovered: &[SequencedEvent],
    by_pos: &BTreeMap<u64, &SequencedEvent>,
    v: &mut Vec<String>,
) {
    // A torn trailing record can never be returned: the client read already failed to decode it
    // (handled by the caller), and every payload must carry a seq. A payload shorter than 8 bytes
    // is a corrupt record that slipped through. `Probe`-typed events are the harness's own
    // monotonic/DCB probes (never a workload type), so they are excluded.
    for ev in recovered {
        if ev.event().event_type() == PROBE_TYPE {
            continue;
        }
        if seq_of_payload(ev.event().payload()).is_none() {
            v.push(format!(
                "torn/short record returned at position {}: payload {} bytes",
                ev.position().get(),
                ev.event().payload().len()
            ));
        }
    }

    // Prefix: positions must be exactly 1..=max with no holes.
    if let Some(&max) = by_pos.keys().next_back() {
        for p in 1..=max {
            if !by_pos.contains_key(&p) {
                v.push(format!(
                    "prefix property broken: position {p} missing but {max} present (hole)"
                ));
            }
        }
    }
}

/// Invariant 1 (durability): every acked write is present, at its position, byte-identical.
fn check_durability(ground: &Ground, by_pos: &BTreeMap<u64, &SequencedEvent>, v: &mut Vec<String>) {
    for (&seq, &(first, last)) in &ground.acked {
        debug_assert_eq!(first, last, "harness sends one event per append");
        let pos = last;
        let Some(ev) = by_pos.get(&pos) else {
            v.push(format!(
                "durability: acked seq {seq} (position {pos}) is absent after recovery"
            ));
            continue;
        };
        let expected = gen_event(ground.seed, seq);
        if let Some(diff) = event_diff(ev, &expected) {
            v.push(format!(
                "durability: acked seq {seq} at position {pos} does not match what was sent: {diff}"
            ));
        }
    }
}

/// Invariant 3 (no phantom acks): every recovered event was sent, matches its regenerated
/// content, and if it was acked it sits at the acked position.
fn check_no_phantom(ground: &Ground, recovered: &[SequencedEvent], v: &mut Vec<String>) {
    for ev in recovered {
        if ev.event().event_type() == PROBE_TYPE {
            continue; // harness probe, not part of the seeded workload
        }
        let pos = ev.position().get();
        let Some(seq) = seq_of_payload(ev.event().payload()) else {
            continue; // already reported as a torn record
        };
        if !ground.sent.contains(&seq) {
            v.push(format!(
                "phantom ack: position {pos} holds seq {seq}, which was never sent"
            ));
            continue;
        }
        let expected = gen_event(ground.seed, seq);
        if let Some(diff) = event_diff(ev, &expected) {
            v.push(format!(
                "phantom/corrupt: position {pos} seq {seq} content does not match regenerated: {diff}"
            ));
        }
        if let Some(&(_, acked_pos)) = ground.acked.get(&seq)
            && acked_pos != pos
        {
            v.push(format!(
                "position drift: seq {seq} acked at {acked_pos} but recovered at {pos}"
            ));
        }
    }
}

/// Invariant 4 (in-flight are either/or, never partial). Present in-flight writes are validated
/// by the phantom check; here we assert the prefix through the highest acked position is intact,
/// so an absent in-flight write can never leave an acked write stranded past a hole.
fn check_in_flight(ground: &Ground, by_pos: &BTreeMap<u64, &SequencedEvent>, v: &mut Vec<String>) {
    let max_acked = ground.max_acked_position();
    if max_acked == 0 {
        return;
    }
    for p in 1..=max_acked {
        if !by_pos.contains_key(&p) {
            v.push(format!(
                "in-flight/durability: position {p} missing at or below the highest acked position {max_acked}"
            ));
        }
    }
}

/// Invariant 7 (read-only half): index/planner answers equal the log-filtered answers.
fn check_index_agrees_with_log(
    addr: SocketAddr,
    recovered: &[SequencedEvent],
    v: &mut Vec<String>,
) {
    let mut client = match connect(addr) {
        Ok(client) => client,
        Err(err) => {
            v.push(format!("index check: could not connect: {err}"));
            return;
        }
    };
    for query in query_battery() {
        let expected = filter_positions(recovered, &query);
        let actual = match read_positions(&mut client, query.clone()) {
            Ok(positions) => positions,
            Err(err) => {
                v.push(format!("index check: query {query:?} failed: {err}"));
                continue;
            }
        };
        if expected != actual {
            v.push(format!(
                "index/log disagree for {query:?}: index returned {} positions, log filter {} (index-only={:?}, log-only={:?})",
                actual.len(),
                expected.len(),
                actual.difference(&expected).take(8).collect::<Vec<_>>(),
                expected.difference(&actual).take(8).collect::<Vec<_>>(),
            ));
        }
    }
}

/// Invariant 6: the next append lands exactly at recovered_max + 1.
fn check_monotonic_append(addr: SocketAddr, recovered_max: u64, v: &mut Vec<String>) {
    let mut client = match connect(addr) {
        Ok(client) => client,
        Err(err) => {
            v.push(format!("monotonic check: could not connect: {err}"));
            return;
        }
    };
    let probe = tephra_client::Event::new(PROBE_TYPE, ["probe:monotonic"], b"probe".to_vec())
        .expect("probe event valid");
    match client.append([probe], None) {
        Ok(result) => {
            if result.first.get() != recovered_max + 1 {
                v.push(format!(
                    "position monotonicity: after recovering head {recovered_max}, append landed at {} (expected {})",
                    result.first.get(),
                    recovered_max + 1
                ));
            }
        }
        Err(err) => v.push(format!("monotonic check: probe append failed: {err}")),
    }
}

/// The `entity:` tag of some non-probe event in the recovered log, so a uniqueness guard on it is
/// guaranteed to conflict. Taken from what is actually present, not from the witness.
fn present_entity_tag(recovered: &[SequencedEvent]) -> Option<String> {
    recovered
        .iter()
        .filter(|ev| ev.event().event_type() != PROBE_TYPE)
        .flat_map(|ev| ev.event().tags())
        .find(|tag| tag.starts_with("entity:"))
        .map(str::to_string)
}

/// Invariant 8: DCB conflict state survives recovery.
fn check_dcb_integrity(
    addr: SocketAddr,
    existing_tag: Option<&str>,
    v: &mut Vec<String>,
) {
    // Guard on a tag known to be present in the recovered log, so the uniqueness guard must fire.
    let Some(existing_tag) = existing_tag else {
        return; // nothing present to conflict with
    };

    let mut client = match connect(addr) {
        Ok(client) => client,
        Err(err) => {
            v.push(format!("dcb check: could not connect: {err}"));
            return;
        }
    };

    // Conflicting: an event with this tag already exists, so the guard must reject.
    let conflict_cond = AppendCondition::new(tag_query(&[existing_tag]));
    let ev = tephra_client::Event::new(PROBE_TYPE, ["probe:dcb-conflict"], b"x".to_vec()).unwrap();
    match client.append([ev], Some(conflict_cond)) {
        Err(ClientError::Server {
            code: ErrorCode::Conflict,
            ..
        }) => {}
        Ok(_) => v.push(format!(
            "dcb integrity: append guarded on existing tag {existing_tag} was NOT rejected after recovery"
        )),
        Err(err) => v.push(format!(
            "dcb integrity: guarded append failed with an unexpected error: {err}"
        )),
    }

    // Non-conflicting: guard on a tag that can never have been generated (entity ids are < mod).
    let absent_tag = "entity:99999999";
    let ok_cond = AppendCondition::new(tag_query(&[absent_tag]));
    let ev = tephra_client::Event::new(PROBE_TYPE, ["probe:dcb-ok"], b"y".to_vec()).unwrap();
    if let Err(err) = client.append([ev], Some(ok_cond)) {
        v.push(format!(
            "dcb integrity: append guarded on absent tag {absent_tag} was wrongly rejected: {err}"
        ));
    }
}

/// Invariant 7 (rebuild half): query answers are identical after deleting and rebuilding the
/// index from the log.
fn check_index_rebuild(server: ServerProcess, v: &mut Vec<String>) -> io::Result<ServerProcess> {
    let queries = query_battery();
    let before: Vec<BTreeSet<u64>> = {
        let mut client = match connect(server.addr) {
            Ok(client) => client,
            Err(err) => {
                v.push(format!(
                    "index rebuild: could not connect before rebuild: {err}"
                ));
                return Ok(server);
            }
        };
        queries
            .iter()
            .map(|q| read_positions(&mut client, q.clone()).unwrap_or_default())
            .collect()
    };

    let server = server.restart_rebuilding_index()?;

    let mut client = match connect(server.addr) {
        Ok(client) => client,
        Err(err) => {
            v.push(format!(
                "index rebuild: could not connect after rebuild: {err}"
            ));
            return Ok(server);
        }
    };
    for (query, before) in queries.iter().zip(before.iter()) {
        match read_positions(&mut client, query.clone()) {
            Ok(after) if &after == before => {}
            Ok(after) => v.push(format!(
                "index rebuild changed the answer for {query:?}: {} positions before, {} after",
                before.len(),
                after.len()
            )),
            Err(err) => v.push(format!(
                "index rebuild: query {query:?} failed after rebuild: {err}"
            )),
        }
    }
    Ok(server)
}

// --- helpers ---

fn connect(addr: SocketAddr) -> io::Result<Client> {
    // The listening line is already seen, but retry briefly to smooth over the accept race.
    let mut last = None;
    for _ in 0..100 {
        match Client::connect(addr) {
            Ok(client) => return Ok(client),
            Err(err) => {
                last = Some(err);
                thread::sleep(Duration::from_millis(10));
            }
        }
    }
    Err(last.unwrap_or_else(|| io::Error::other("connect failed")))
}

/// Streams the whole log, collecting events up to the first the client cannot decode. Returns the
/// intact prefix and, if the stream ended on a decode error, a description of it (a torn/corrupt
/// record surfacing). Only a connection failure is an `Err`; a mid-stream decode error is reported
/// so the caller can still check the prefix.
fn read_prefix(addr: SocketAddr) -> io::Result<(Vec<SequencedEvent>, Option<String>)> {
    let mut client = connect(addr)?;
    let mut stream = client
        .read(Query::all(), Position::ZERO, None)
        .map_err(io::Error::other)?;
    let mut events = Vec::new();
    let mut decode_err = None;
    for item in stream.by_ref() {
        match item {
            Ok(ev) => events.push(ev),
            Err(err) => {
                let after = events.last().map(|e| e.position().get()).unwrap_or(0);
                decode_err = Some(format!("read failed after position {after}: {err}"));
                break;
            }
        }
    }
    events.sort_by_key(|e| e.position().get());
    Ok((events, decode_err))
}

fn read_positions(client: &mut Client, query: Query) -> Result<BTreeSet<u64>, ClientError> {
    let (events, _) = client.read_all(query, Position::ZERO, None)?;
    Ok(events.iter().map(|e| e.position().get()).collect())
}

/// The queries checked against the log: every event type, and a spread of entity/shard tags.
fn query_battery() -> Vec<Query> {
    let mut queries = Vec::new();
    for ty in EVENT_TYPES_PUBLIC {
        queries.push(Query::item(QueryItem::of_types(vec![
            EventType::new(ty).unwrap(),
        ])));
    }
    for entity in [0u64, 1, 7, 13, 63] {
        queries.push(tag_query(&[&format!("entity:{entity}")]));
    }
    for shard in [0u64, 3, 7] {
        queries.push(tag_query(&[&format!("shard:{shard}")]));
    }
    // A two-tag AND item (entity + shard), the DCB-style overlapping query.
    queries.push(tag_query(&["entity:1", "shard:3"]));
    queries
}

fn tag_query(tags: &[&str]) -> Query {
    let tags = Tags::new(tags.iter().map(|t| Tag::new(*t).unwrap())).unwrap();
    Query::item(QueryItem::with_tags(tags))
}

/// Filters the recovered log by the same predicate the query expresses, returning positions.
fn filter_positions(recovered: &[SequencedEvent], query: &Query) -> BTreeSet<u64> {
    recovered
        .iter()
        .filter(|ev| query_matches(query, ev))
        .map(|ev| ev.position().get())
        .collect()
}

/// The match predicate: OR across items, within an item type is one-of and tags are all-of.
fn query_matches(query: &Query, ev: &SequencedEvent) -> bool {
    match query {
        Query::All => true,
        Query::Items(items) => items.iter().any(|item| item_matches(item, ev)),
    }
}

fn item_matches(item: &QueryItem, ev: &SequencedEvent) -> bool {
    let type_ok = item.types.is_empty()
        || item
            .types
            .iter()
            .any(|t| t.as_str() == ev.event().event_type());
    if !type_ok {
        return false;
    }
    let event_tags: BTreeSet<&str> = ev.event().tags().collect();
    item.tags.iter().all(|t| event_tags.contains(t.as_str()))
}

/// Returns a human description of the first field that differs, or `None` if identical.
fn event_diff(ev: &SequencedEvent, expected: &workload::GenEvent) -> Option<String> {
    let event = ev.event();
    if event.event_type() != expected.event_type {
        return Some(format!(
            "type {:?} != {:?}",
            event.event_type(),
            expected.event_type
        ));
    }
    let tags: Vec<String> = event.tags().map(str::to_string).collect();
    if tags != expected.tags {
        return Some(format!("tags {tags:?} != {:?}", expected.tags));
    }
    if event.payload() != expected.payload.as_slice() {
        return Some(format!(
            "payload {} bytes != {} bytes",
            event.payload().len(),
            expected.payload.len()
        ));
    }
    None
}
