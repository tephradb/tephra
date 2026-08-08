//! Write-path benchmarks for the event store.
//!
//! These measure layer 2 (the write coordinator) end to end through its public API:
//! encode an [`Event`] on the caller thread, hand it to the writer via
//! [`WriteHandle::append`], and block until the batch is durable. That is the same path a
//! real client takes, so the numbers here are the store's append latency and throughput,
//! not a microbenchmark of an internal function.
//!
//! Groups:
//!
//! - `append_latency`: one event per `append`, synchronous. The fsync-bound latency floor
//!   (one durable append per call), reported as appends/sec.
//! - `batch_size`: N events in a single `append` call. Shows fsync amortization *within*
//!   one atomic request.
//! - `payload_size`: single-event appends over a range of payload sizes, reported as
//!   bytes/sec, to separate the fixed per-append cost from payload cost.
//! - `group_commit`: T concurrent writer threads hammering `append`. The headline number:
//!   the coordinator coalesces many independent appends into one fsync, so aggregate
//!   throughput should climb with thread count until the fsync ceiling is reached.
//! - `conditional_append`: single-event appends each guarded by a uniqueness condition,
//!   isolating the append-condition overhead (the durable `TagTips` fast-reject) against
//!   the unconditional baseline.
//!
//! ## Running
//!
//! ```text
//! cargo bench --bench write_path
//! cargo bench --bench write_path -- group_commit        # one group
//! DCBDB_BENCH_DIR=/mnt/nvme cargo bench --bench write_path   # on real storage
//! ```
//!
//! ## The fsync caveat (read this before trusting a number)
//!
//! The whole point of this store is that "the ceiling is fsync, not ordering" (CLAUDE.md
//! 10). A `TempDir` under `/tmp` is very often a `tmpfs` (RAM) mount, where `fsync` is
//! effectively a no-op, so latencies look 10x to 100x better than any real disk. Numbers
//! from a tmpfs measure the coordinator's CPU and coalescing behaviour, which is useful
//! for spotting regressions, but they are **not** a fair comparison against another
//! durable store. To measure the real durability ceiling, point `DCBDB_BENCH_DIR` at a
//! directory on the actual storage device under test.

use std::hint::black_box;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use criterion::{
    BenchmarkId, Criterion, SamplingMode, Throughput, criterion_group, criterion_main,
};
use tempfile::TempDir;

use dcbdb::event::{Event, EventType, Tag, Tags};
use dcbdb::log::set::{SegmentConfig, SegmentSet};
use dcbdb::query::{AppendCondition, Query, QueryItem};
use dcbdb::writer::{WriteCoordinator, WriteHandle, WriterConfig};

/// 1 GiB segments, matching the `seglog` bench. Large enough that these runs never roll
/// over, so periodic index-seal cost is kept out of the steady-state append measurement
/// (rollover behaviour is a separate concern, exercised by the integration tests).
const SEGMENT_SIZE: usize = 1024 * 1024 * 1024;

/// Default payload size for the latency, batch, and concurrency groups: small events are
/// the dominant real workload and the case where the fixed per-append cost matters most.
const DEFAULT_PAYLOAD: usize = 128;

/// A live coordinator over a scratch directory, torn down on drop.
///
/// Field order is drop order: `handle` then `coord` (whose drop signals shutdown and
/// joins the writer thread) then `_dir` (which removes the files). The directory must
/// outlive the writer, so it is declared last.
struct Harness {
    handle: WriteHandle,
    coord: Option<WriteCoordinator>,
    _dir: TempDir,
}

impl Harness {
    fn new() -> Harness {
        let dir = match std::env::var_os("DCBDB_BENCH_DIR") {
            Some(base) => TempDir::new_in(base).expect("create scratch dir under DCBDB_BENCH_DIR"),
            None => TempDir::new().expect("create scratch dir"),
        };
        let set = SegmentSet::open(dir.path(), SegmentConfig::new(SEGMENT_SIZE))
            .expect("open segment set");
        // Default config, except verify_tips stays off: the paranoid cross-check doubles
        // condition work and would distort the conditional-append numbers.
        let cfg = WriterConfig {
            verify_tips: false,
            ..WriterConfig::default()
        };
        let (coord, handle) = WriteCoordinator::start(set, cfg).expect("start coordinator");
        Harness {
            handle,
            coord: Some(coord),
            _dir: dir,
        }
    }
}

impl Drop for Harness {
    fn drop(&mut self) {
        // Explicit shutdown before the TempDir is removed, so the writer thread is joined
        // while its files still exist.
        if let Some(coord) = self.coord.take() {
            coord.shutdown();
        }
    }
}

/// A process-wide counter so successive events carry distinct sequence numbers (and, in
/// the conditional group, distinct guard tags). Keeps tag cardinality realistic without
/// coordinating across threads.
static SEQ: AtomicU64 = AtomicU64::new(0);

fn next_seq() -> u64 {
    SEQ.fetch_add(1, Ordering::Relaxed)
}

fn event_type() -> EventType {
    EventType::new("Appended").expect("valid type")
}

/// An event tagged by a bounded-cardinality entity id, so posting lists grow the way a
/// real workload's would rather than becoming one giant singleton term.
fn make_event(seq: u64, payload: &[u8]) -> Event {
    let tags = Tags::new(vec![
        Tag::new(format!("entity:{}", seq & 0xFFF)).expect("valid tag"),
    ])
    .expect("valid tag set");
    Event::new(&event_type(), &tags, payload).expect("encode event")
}

fn bench_append_latency(c: &mut Criterion) {
    let mut group = c.benchmark_group("append_latency");
    group.throughput(Throughput::Elements(1));
    // fsync-bound: fewer, longer samples keep the wall-clock reasonable on real disks.
    group.sample_size(50);
    group.measurement_time(Duration::from_secs(10));

    let harness = Harness::new();
    let payload = vec![0u8; DEFAULT_PAYLOAD];
    group.bench_function("single_event", |b| {
        b.iter(|| {
            let ev = make_event(next_seq(), &payload);
            let range = harness.handle.append(vec![ev], None).expect("append");
            black_box(range);
        });
    });
    group.finish();
}

fn bench_batch_size(c: &mut Criterion) {
    let mut group = c.benchmark_group("batch_size");
    group.sample_size(50);
    group.measurement_time(Duration::from_secs(10));

    let harness = Harness::new();
    let payload = vec![0u8; DEFAULT_PAYLOAD];
    for &batch in &[1usize, 8, 64, 512] {
        // Throughput is per event, so larger batches should show a lower per-event cost:
        // the single fsync is shared across the whole atomic request.
        group.throughput(Throughput::Elements(batch as u64));
        // A template cloned per event: encoding is caller work but trivial against fsync,
        // and cloning keeps the loop focused on the write path rather than on formatting.
        let template = make_event(next_seq(), &payload);
        group.bench_with_input(BenchmarkId::from_parameter(batch), &batch, |b, &batch| {
            b.iter(|| {
                let events = vec![template.clone(); batch];
                let range = harness.handle.append(events, None).expect("append");
                black_box(range);
            });
        });
    }
    group.finish();
}

fn bench_payload_size(c: &mut Criterion) {
    let mut group = c.benchmark_group("payload_size");
    group.sample_size(50);
    group.measurement_time(Duration::from_secs(10));

    for &size in &[64usize, 256, 1024, 4096, 16384] {
        group.throughput(Throughput::Bytes(size as u64));
        let harness = Harness::new();
        let payload = vec![0u8; size];
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, _| {
            b.iter(|| {
                let ev = make_event(next_seq(), &payload);
                let range = harness.handle.append(vec![ev], None).expect("append");
                black_box(range);
            });
        });
    }
    group.finish();
}

fn bench_group_commit(c: &mut Criterion) {
    let mut group = c.benchmark_group("group_commit");
    // Flat sampling: each iteration runs a whole concurrent burst, so it is expensive and
    // its cost does not scale linearly with iteration count. Flat mode times each sample
    // as one unit rather than assuming the linear model, which is what criterion's
    // "unable to complete N samples" warning is asking for here.
    group.sampling_mode(SamplingMode::Flat);
    group.sample_size(20);
    group.measurement_time(Duration::from_secs(15));

    // Each thread appends this many events per timed iteration; large enough that
    // thread-spawn cost is amortized against the appends (and their fsyncs).
    const CHUNK: u64 = 256;
    let payload = Arc::new(vec![0u8; DEFAULT_PAYLOAD]);

    for &threads in &[1usize, 2, 4, 8, 16] {
        // Aggregate events retired per iteration, so criterion reports total appends/sec
        // across all writers: the coalescing win shows up as this rising with thread count.
        group.throughput(Throughput::Elements(threads as u64 * CHUNK));
        let harness = Harness::new();
        group.bench_with_input(
            BenchmarkId::from_parameter(threads),
            &threads,
            |b, &threads| {
                b.iter_custom(|iters| {
                    let mut elapsed = Duration::ZERO;
                    for _ in 0..iters {
                        let barrier = Arc::new(Barrier::new(threads));
                        let mut workers = Vec::with_capacity(threads);
                        for _ in 0..threads {
                            let handle = harness.handle.clone();
                            let barrier = barrier.clone();
                            let payload = payload.clone();
                            workers.push(thread::spawn(move || {
                                // Release all writers at once so they contend for the same
                                // drain window, which is what makes group commit coalesce.
                                barrier.wait();
                                let start = Instant::now();
                                for _ in 0..CHUNK {
                                    let ev = make_event(next_seq(), &payload);
                                    handle.append(vec![ev], None).expect("append");
                                }
                                start.elapsed()
                            }));
                        }
                        // Wall-clock span of the run is the slowest writer, so the reported
                        // throughput reflects real end-to-end concurrency, not a sum.
                        let mut slowest = Duration::ZERO;
                        for w in workers {
                            slowest = slowest.max(w.join().expect("writer thread"));
                        }
                        elapsed += slowest;
                    }
                    elapsed
                });
            },
        );
    }
    group.finish();
}

fn bench_conditional_append(c: &mut Criterion) {
    let mut group = c.benchmark_group("conditional_append");
    group.throughput(Throughput::Elements(1));
    group.sample_size(50);
    group.measurement_time(Duration::from_secs(10));

    let payload = vec![0u8; DEFAULT_PAYLOAD];

    // Baseline: the same single-event append with no condition, so the conditional cost is
    // read directly as the delta against this bar.
    let harness = Harness::new();
    group.bench_function("unconditional", |b| {
        b.iter(|| {
            let ev = make_event(next_seq(), &payload);
            let range = harness.handle.append(vec![ev], None).expect("append");
            black_box(range);
        });
    });
    drop(harness);

    // The uniqueness-guard pattern: every append reserves a fresh unique tag, so the
    // condition never conflicts and every write succeeds. Absent-and-warm tags resolve to
    // DefinitelyNoMatch in TagTips, so this measures the fast-reject path (no log scan),
    // which is the common DCB case.
    let harness = Harness::new();
    group.bench_function("unique_guard", |b| {
        b.iter(|| {
            let n = next_seq();
            let tag = Tag::new(format!("unique:{n}")).expect("valid tag");
            let tags = Tags::new(vec![tag]).expect("valid tag set");
            let ev = Event::new(&event_type(), &tags, &payload).expect("encode event");
            let condition = AppendCondition::new(Query::item(QueryItem::with_tags(tags.clone())));
            let range = harness
                .handle
                .append(vec![ev], Some(condition))
                .expect("append");
            black_box(range);
        });
    });
    group.finish();
}

criterion_group!(
    benches,
    bench_append_latency,
    bench_batch_size,
    bench_payload_size,
    bench_group_commit,
    bench_conditional_append,
);
criterion_main!(benches);
