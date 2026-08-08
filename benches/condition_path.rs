//! Condition-path benchmark: the append-condition uniqueness guard, index vs scan (phase 6d).
//!
//! The uniqueness guard (`after: 0`, "fail if **any** event matches", the `after`-omitted
//! shape in CLAUDE.md 1) is the most common DCB condition and the one phase 6d most improves.
//! Its durable arm always falls through the tips (the floor starts above `0`, so it can never
//! rule `after: 0` out), and 6d resolves that fallthrough with an early-terminating index
//! existence check instead of a linear log decode. This bench measures the two, so the
//! crossover (O(surviving segments) FST probes vs O(total events) decode) is visible as
//! history grows.
//!
//! ## Isolating the check cost
//!
//! We want the cost of the *condition evaluation*, not of an append. A **conflicting** guard
//! is rejected before anything is staged, so it does no `append_batch` and no fsync, and it
//! never mutates the log: a perfectly repeatable measurement of pure check cost. To make that
//! conflict cost equal to the common no-match case (which must consult all history but would
//! mutate on success), the conflicting tag sits on the **last** event: the index still probes
//! every segment (all miss but the last), and the scan still decodes the whole log, exactly as
//! a genuine no-match would. So a rejected last-position guard is a clean proxy for the
//! all-history uniqueness check.
//!
//! Each size runs two stores built from the identical workload: one on the index arm
//! (`condition_force_scan = false`), one forced onto the scan oracle
//! (`condition_force_scan = true`).
//!
//! ## Running
//!
//! ```text
//! cargo bench --bench condition_path
//! DCBDB_BENCH_DIR=/mnt/nvme cargo bench --bench condition_path   # on real storage
//! ```
//!
//! ## The cache caveat (read this before trusting a number)
//!
//! The scan arm's cost is dominated by decoding the whole log; the index arm's by FST probes
//! over resident segments. On a `tmpfs` `TempDir` (the default under `/tmp`) with a warm cache
//! both are CPU-bound, so these numbers measure decode/probe cost, useful for spotting the
//! shape and regressions, not the durable-storage latency (the scan would pay real sequential
//! I/O on a cold cache). Point `DCBDB_BENCH_DIR` at the device under test. This mirrors the
//! fsync/tmpfs caveat in `write_path.rs` and `read_path.rs`, and relates to phase 8's deferred
//! "condition-check latency with and without tips".

use std::hint::black_box;
use std::mem;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use tempfile::TempDir;

use dcbdb::event::{Event, EventType, Tag, Tags};
use dcbdb::log::set::{SegmentConfig, SegmentSet};
use dcbdb::query::{AppendCondition, Query, QueryItem};
use dcbdb::writer::{AppendError, WriteCoordinator, WriteHandle, WriterConfig};

/// Small segments so the workload seals many on-disk segments: the index arm then makes real
/// per-segment FST probes, which is what the crossover is about.
const SEGMENT_SIZE: usize = 1 << 16; // 64 KiB
const MAX_BATCH_BYTES: usize = 1 << 14; // 16 KiB, under the segment capacity
const PAYLOAD: usize = 32;

/// History sizes: total events, hence roughly the scan's per-check decode work and the number
/// of segments the index arm probes.
const SIZES: [u64; 3] = [2_000, 8_000, 32_000];

/// The tag carried only by the last event, so a guard on it conflicts against a match at the
/// very end: the index probes every segment and the scan decodes the whole log, the
/// all-history cost the common no-match case also pays.
const SENTINEL: &str = "sentinel:hit";

/// A live store built from a fixed workload, pinned to one condition arm. Field order is drop
/// order: `handle`, then `coord` (joins the writer thread on drop), then `_dir`.
struct Store {
    handle: WriteHandle,
    coord: Option<WriteCoordinator>,
    _dir: TempDir,
}

impl Store {
    /// Builds a store of `n` events, the last carrying [`SENTINEL`], pinned to `force_scan`.
    fn new(force_scan: bool, n: u64) -> Store {
        let dir = match std::env::var_os("DCBDB_BENCH_DIR") {
            Some(base) => TempDir::new_in(base).expect("create scratch dir under DCBDB_BENCH_DIR"),
            None => TempDir::new().expect("create scratch dir"),
        };
        let set = SegmentSet::open(dir.path(), SegmentConfig::new(SEGMENT_SIZE))
            .expect("open segment set");
        let cfg = WriterConfig {
            max_batch_bytes: MAX_BATCH_BYTES,
            verify_tips: false,
            condition_force_scan: force_scan,
            ..WriterConfig::default()
        };
        let (coord, handle) = WriteCoordinator::start(set, cfg).expect("start coordinator");

        let payload = vec![0u8; PAYLOAD];
        let per_batch = (MAX_BATCH_BYTES / (PAYLOAD + 128)).clamp(1, 200);
        let mut batch: Vec<Event> = Vec::new();
        for seq in 0..n {
            batch.push(make_event(seq, seq == n - 1, &payload));
            if batch.len() == per_batch {
                handle
                    .append(mem::take(&mut batch), None)
                    .expect("append batch");
            }
        }
        if !batch.is_empty() {
            handle.append(batch, None).expect("append tail batch");
        }

        Store {
            handle,
            coord: Some(coord),
            _dir: dir,
        }
    }
}

impl Drop for Store {
    fn drop(&mut self) {
        if let Some(coord) = self.coord.take() {
            coord.shutdown();
        }
    }
}

fn event_type() -> EventType {
    EventType::new("Appended").expect("valid type")
}

/// An event with one filler tag, plus [`SENTINEL`] when it is the last one.
fn make_event(seq: u64, is_last: bool, payload: &[u8]) -> Event {
    let mut picked = vec![Tag::new(format!("f:{}", seq % 64)).expect("valid tag")];
    if is_last {
        picked.push(Tag::new(SENTINEL).expect("valid tag"));
    }
    let tags = Tags::new(picked).expect("valid tag set");
    Event::new(&event_type(), &tags, payload).expect("encode event")
}

/// The `after: 0` uniqueness guard on [`SENTINEL`]: it conflicts against the last event, so
/// the append is rejected without staging or fsync.
fn sentinel_guard() -> AppendCondition {
    let tag = Tag::new(SENTINEL).expect("valid tag");
    AppendCondition::new(Query::item(QueryItem::with_tags(
        Tags::new(vec![tag]).expect("tag set"),
    )))
}

/// Runs one guarded append that must conflict, measuring only the condition check (rejected
/// before any append, so no mutation and no fsync).
fn run_check(handle: &WriteHandle) {
    let throwaway = Event::new(&event_type(), &Tags::empty(), b"x").expect("encode");
    match handle.append(vec![throwaway], Some(sentinel_guard())) {
        Err(AppendError::Conflict { .. }) => {}
        other => panic!("expected a conflict, got {other:?}"),
    }
}

fn bench_condition_check(c: &mut Criterion) {
    let mut group = c.benchmark_group("condition_uniqueness_guard");
    group.sample_size(20);

    for &n in &SIZES {
        // Per event of history: the scan's per-check decode cost shows up directly, and the
        // index arm's near-flatness against it is the crossover.
        group.throughput(Throughput::Elements(n));

        let index_store = Store::new(false, n);
        let scan_store = Store::new(true, n);

        group.bench_with_input(BenchmarkId::new("index", n), &n, |b, _| {
            b.iter(|| run_check(black_box(&index_store.handle)));
        });
        group.bench_with_input(BenchmarkId::new("scan", n), &n, |b, _| {
            b.iter(|| run_check(black_box(&scan_store.handle)));
        });
    }
    group.finish();
}

criterion_group!(benches, bench_condition_check);
criterion_main!(benches);
