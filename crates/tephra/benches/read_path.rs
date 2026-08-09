//! Read-path benchmark: the index-vs-scan crossover.
//!
//! This is not a tuned `K`, but the measurement
//! that reveals *where* the index stops paying for itself. A selective query answered
//! through the index touches few events (one random fetch each); a broad query answered by a
//! sequential scan reads the whole range once at disk bandwidth. Somewhere between those the
//! two cross, and its position depends on selectivity, range width, and payload size. The
//! bench measures both arms over the same store so the crossover is visible as the point
//! where the curves swap.
//!
//! Each group runs two stores built from the identical workload: one pinned to force the
//! index arm (`scan_bias = 1`), one to force the scan arm (`scan_bias = u32::MAX`). The
//! planner's job in production is to pick the lower of the two per read; here we measure both
//! so we can see what it is choosing between.
//!
//! Groups:
//!
//! - `read_selectivity`: whole-log reads of a query matching a controlled fraction of events
//!   (`1/1000` .. `1/2`). The headline crossover: the index wins while the fraction is
//!   small, the scan wins once it is large.
//! - `read_range_width`: a fixed selectivity read with `after` sweeping the log (whole,
//!   half, recent tail). Pruning shrinks the scan's work but not the index's per-match fetch.
//! - `read_payload_size`: a fixed selectivity read over a range of event payload sizes.
//!   Larger payloads make each random fetch dearer, pushing the crossover toward the scan.
//!
//! ## Running
//!
//! ```text
//! cargo bench --bench read_path
//! cargo bench --bench read_path -- read_selectivity        # one group
//! TEPHRA_BENCH_DIR=/mnt/nvme cargo bench --bench read_path   # on real storage
//! ```
//!
//! ## The cache caveat (read this before trusting a number)
//!
//! The index arm's cost is dominated by **random** fetches (`read_at_local`) and the scan
//! arm's by **sequential** reads. That asymmetry is the whole crossover, and it only shows
//! its true shape against real storage with a cold page cache. A `TempDir` under `/tmp` is
//! usually a `tmpfs` (RAM) mount where a "random" read is as cheap as a sequential one, so
//! the crossover looks far flatter than on disk, and these runs are all effectively *hot*
//! (the data stays resident across iterations). Numbers from a tmpfs measure CPU and
//! decode/intersect cost, useful for spotting regressions, but not the durable-storage
//! crossover. Point `TEPHRA_BENCH_DIR` at the device under test, and treat the first read
//! after a fresh process as the only cold sample. This mirrors the fsync caveat in
//! `write_path.rs`.

use std::hint::black_box;
use std::mem;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use smallvec::SmallVec;
use tempfile::TempDir;

use tephra::Position;
use tephra::event::{Event, EventType, Tag, Tags};
use tephra::log::set::{SegmentConfig, SegmentSet};
use tephra::query::{Query, QueryItem};
use tephra::read::ReadConfig;
use tephra::writer::{WriteCoordinator, WriteHandle, WriterConfig};

/// Events in each store. Enough to span several segments (so the index arm does real random
/// log fetches over sealed on-disk segments), small enough to keep bench setup quick.
const N: u64 = 20_000;

/// Small segments so the workload seals many on-disk segments rather than living wholly in
/// the in-memory active tail: the index arm then pays real per-fetch I/O, which is what the
/// crossover is about.
const SEGMENT_SIZE: usize = 1 << 18; // 256 KiB
const MAX_BATCH_BYTES: usize = 1 << 16; // 64 KiB, under the segment capacity

/// Default payload for the selectivity and range groups: small events, the case where the
/// per-fetch fixed cost matters most.
const DEFAULT_PAYLOAD: usize = 64;

/// Selectivity denominators: a query for `s{d}:0` matches every `d`-th event, i.e. a `1/d`
/// fraction of the log.
const DENOMS: [u64; 4] = [1000, 100, 10, 2];

/// Force the index arm (never scans while the estimate does not exceed the range).
const FORCE_INDEX: u32 = 1;
/// Force the scan arm (scans for any non-empty estimate).
const FORCE_SCAN: u32 = u32::MAX;

/// A live store built from a fixed workload, pinned to one planner arm. Field order is drop
/// order: `handle`, then `coord` (which joins the writer thread on drop), then `_dir`.
struct Store {
    handle: WriteHandle,
    coord: Option<WriteCoordinator>,
    _dir: TempDir,
}

impl Store {
    /// Builds a store of [`N`] events, each carrying one tag per selectivity level, pinned to
    /// `scan_bias` and `payload`-byte payloads.
    fn new(scan_bias: u32, payload: usize) -> Store {
        let dir = match std::env::var_os("TEPHRA_BENCH_DIR") {
            Some(base) => TempDir::new_in(base).expect("create scratch dir under TEPHRA_BENCH_DIR"),
            None => TempDir::new().expect("create scratch dir"),
        };
        let set = SegmentSet::open(dir.path(), SegmentConfig::new(SEGMENT_SIZE))
            .expect("open segment set");
        let cfg = WriterConfig {
            max_batch_bytes: MAX_BATCH_BYTES,
            verify_tips: false,
            read: ReadConfig { scan_bias },
            ..WriterConfig::default()
        };
        let (coord, handle) = WriteCoordinator::start(set, cfg).expect("start coordinator");

        // Populate in batches so setup is not one fsync per event. Size the batch by count so
        // its bytes stay under `MAX_BATCH_BYTES` (and thus the segment capacity) even at the
        // largest payload: a batch that overflowed a segment would be rejected `TooLarge`.
        let payload = vec![0u8; payload];
        let per_batch = (MAX_BATCH_BYTES / (payload.len() + 128)).clamp(1, 200);
        let mut batch: Vec<Event> = Vec::new();
        for seq in 0..N {
            batch.push(make_event(seq, &payload));
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

/// An event tagged at every selectivity level: `s{d}:{seq % d}` for each denominator, so one
/// dataset serves every selectivity (a query for `s{d}:0` matches the `1/d` fraction).
fn make_event(seq: u64, payload: &[u8]) -> Event {
    let picked: SmallVec<[Tag; 4]> = DENOMS
        .iter()
        .map(|d| Tag::new(format!("s{d}:{}", seq % d)).expect("valid tag"))
        .collect();
    let tags = Tags::new(picked).expect("valid tag set");
    Event::new(&event_type(), &tags, payload).expect("encode event")
}

/// The query selecting the `1/denom` fraction: events whose `s{denom}` bucket is 0.
fn query_for(denom: u64) -> Query {
    let tag = Tag::new(format!("s{denom}:0")).expect("valid tag");
    Query::item(QueryItem::with_tags(Tags::new(vec![tag]).expect("tag set")))
}

/// Runs one read to exhaustion and returns how many events it yielded.
fn run_read(handle: &WriteHandle, query: Query, after: Position) -> u64 {
    let mut reads = handle.read(query, after);
    let mut count = 0u64;
    while let Some(item) = reads.next() {
        item.expect("read");
        count += 1;
    }
    count
}

fn bench_selectivity(c: &mut Criterion) {
    let mut group = c.benchmark_group("read_selectivity");
    group.sample_size(20);

    let index_store = Store::new(FORCE_INDEX, DEFAULT_PAYLOAD);
    let scan_store = Store::new(FORCE_SCAN, DEFAULT_PAYLOAD);

    for &denom in &DENOMS {
        // Throughput is per event *returned*, so the two arms are comparable at each
        // selectivity: the scan's near-constant work per read shows up as its per-returned-
        // event cost worsening as the fraction shrinks, which is the crossover.
        let matches = N / denom;
        group.throughput(Throughput::Elements(matches));
        let label = format!("1_over_{denom}");

        group.bench_with_input(BenchmarkId::new("index", &label), &denom, |b, &denom| {
            b.iter(|| {
                black_box(run_read(
                    &index_store.handle,
                    query_for(denom),
                    Position::ZERO,
                ))
            });
        });
        group.bench_with_input(BenchmarkId::new("scan", &label), &denom, |b, &denom| {
            b.iter(|| {
                black_box(run_read(
                    &scan_store.handle,
                    query_for(denom),
                    Position::ZERO,
                ))
            });
        });
    }
    group.finish();
}

fn bench_range_width(c: &mut Criterion) {
    let mut group = c.benchmark_group("read_range_width");
    group.sample_size(20);

    // A middling selectivity, so pruning (which shrinks the scan's range but not the index's
    // per-match fetch count) is what moves the two arms apart.
    const DENOM: u64 = 100;
    let index_store = Store::new(FORCE_INDEX, DEFAULT_PAYLOAD);
    let scan_store = Store::new(FORCE_SCAN, DEFAULT_PAYLOAD);

    for &(label, after) in &[
        ("whole", 0u64),
        ("half", N / 2),
        ("recent_tenth", N * 9 / 10),
    ] {
        // Events returned = matches strictly after `after`, roughly the surviving fraction.
        let matches = (N - after) / DENOM;
        group.throughput(Throughput::Elements(matches.max(1)));
        let after = Position::new(after);

        group.bench_with_input(BenchmarkId::new("index", label), &after, |b, &after| {
            b.iter(|| black_box(run_read(&index_store.handle, query_for(DENOM), after)));
        });
        group.bench_with_input(BenchmarkId::new("scan", label), &after, |b, &after| {
            b.iter(|| black_box(run_read(&scan_store.handle, query_for(DENOM), after)));
        });
    }
    group.finish();
}

fn bench_payload_size(c: &mut Criterion) {
    let mut group = c.benchmark_group("read_payload_size");
    group.sample_size(20);

    // A selectivity where index and scan are close, so payload size (which dearens each
    // random fetch on the index arm but only the sequential read on the scan arm) is what
    // tips the balance.
    const DENOM: u64 = 20;
    let matches = N / DENOM;

    for &payload in &[64usize, 1024, 4096] {
        group.throughput(Throughput::Elements(matches));
        let index_store = Store::new(FORCE_INDEX, payload);
        let scan_store = Store::new(FORCE_SCAN, payload);

        group.bench_with_input(BenchmarkId::new("index", payload), &payload, |b, _| {
            b.iter(|| {
                black_box(run_read(
                    &index_store.handle,
                    query_for(DENOM),
                    Position::ZERO,
                ))
            });
        });
        group.bench_with_input(BenchmarkId::new("scan", payload), &payload, |b, _| {
            b.iter(|| {
                black_box(run_read(
                    &scan_store.handle,
                    query_for(DENOM),
                    Position::ZERO,
                ))
            });
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_selectivity,
    bench_range_width,
    bench_payload_size
);
criterion_main!(benches);
