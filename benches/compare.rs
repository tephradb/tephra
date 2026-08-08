//! Write-path comparison: dcbdb vs umadb, embedded (no gRPC).
//!
//! Gated behind the `umadb-compare` feature, since it pulls in umadb:
//!
//! ```text
//! cargo bench --features umadb-compare --bench compare
//! cargo bench --features umadb-compare --bench compare -- append_latency
//! DCBDB_BENCH_DIR=/mnt/nvme cargo bench --features umadb-compare --bench compare
//! ```
//!
//! Without the feature the bench is a no-op stub, so the default build never needs umadb.
//!
//! Both engines are driven through their embedded, synchronous append API, one durable
//! commit per call, under an identical workload (same event type, tag shape, payload and
//! batch sizes as the `write_path` bench). Each criterion group pairs the two engines
//! (`dcbdb` vs `umadb`) so the bars sit side by side.
//!
//! ## What each engine is doing
//!
//! - **dcbdb**: `WriteHandle::append` hands the batch to the single writer thread, which
//!   group-commits under one fsync and feeds the index inline. A single-threaded caller
//!   therefore pays one cross-thread handoff per append (inherent to the coordinator
//!   design); the coalescing win only appears under concurrency, measured in the
//!   `write_path` bench, not here.
//! - **umadb**: `UmaDb::append` builds a writer, mutates its copy-on-write B+trees (events
//!   plus tags), and commits inline on the calling thread with two fsyncs (dirty pages,
//!   then the header). No background thread, no coalescing.
//!
//! Both are fully durable per call, so this is an apples-to-apples "cost of one durable
//! append" comparison of the two storage designs (append-only log plus sparse index vs COW
//! B+tree). The same fsync caveat as `write_path` applies: run on real storage via
//! `DCBDB_BENCH_DIR`, not a tmpfs, or the fsync ceiling is hidden.
//!
//! ## Concurrency is intentionally omitted
//!
//! The embedded `UmaDb::append` is single-writer by convention (its `Mvcc` takes no writer
//! lock; serialization is the job of umadb's server writer thread, out of scope for an
//! embedded write-path comparison). Driving it from several threads would race, so there
//! is no `group_commit` group here; dcbdb's concurrency story lives in `write_path`.

#[cfg(not(feature = "umadb-compare"))]
fn main() {
    eprintln!(
        "the `compare` bench requires umadb: run with `--features umadb-compare` \
         (see the module docs)"
    );
}

#[cfg(feature = "umadb-compare")]
fn main() {
    imp::run();
}

#[cfg(feature = "umadb-compare")]
mod imp {
    use std::hint::black_box;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Duration;

    use criterion::{BenchmarkId, Criterion, Throughput};
    use tempfile::TempDir;

    use dcbdb::event::{Event, EventType, Tag, Tags};
    use dcbdb::log::set::{SegmentConfig, SegmentSet};
    use dcbdb::query::{AppendCondition, Query, QueryItem};
    use dcbdb::writer::{WriteCoordinator, WriteHandle, WriterConfig};

    use umadb_core::db::UmaDb;
    use umadb_core::mvcc::{Mvcc, StorageOptions};
    use umadb_dcb::{DcbAppendCondition, DcbEvent, DcbEventStoreSync, DcbQuery, DcbQueryItem};

    /// 1 GiB segments for dcbdb, matching its own bench so neither run rolls over mid-sample.
    const SEGMENT_SIZE: usize = 1024 * 1024 * 1024;

    /// Default payload for the latency, batch, and conditional groups.
    const DEFAULT_PAYLOAD: usize = 128;

    /// Shared sequence so events (and, in the conditional group, guard tags) stay distinct
    /// across both engines and every benchmark.
    static SEQ: AtomicU64 = AtomicU64::new(0);

    fn next_seq() -> u64 {
        SEQ.fetch_add(1, Ordering::Relaxed)
    }

    /// A scratch directory, overridable via `DCBDB_BENCH_DIR` so both engines land on the
    /// same physical device under test.
    fn scratch_dir() -> TempDir {
        match std::env::var_os("DCBDB_BENCH_DIR") {
            Some(base) => TempDir::new_in(base).expect("create scratch dir under DCBDB_BENCH_DIR"),
            None => TempDir::new().expect("create scratch dir"),
        }
    }

    // --------------------------- dcbdb harness ---------------------------

    /// A live dcbdb coordinator over a scratch directory, torn down on drop (writer thread
    /// joined before the directory is removed: `handle`, then `coord`, then `_dir`).
    struct Dcbdb {
        handle: WriteHandle,
        coord: Option<WriteCoordinator>,
        _dir: TempDir,
    }

    impl Dcbdb {
        fn new() -> Dcbdb {
            let dir = scratch_dir();
            let set = SegmentSet::open(dir.path(), SegmentConfig::new(SEGMENT_SIZE))
                .expect("open segment set");
            let cfg = WriterConfig {
                verify_tips: false,
                ..WriterConfig::default()
            };
            let (coord, handle) = WriteCoordinator::start(set, cfg).expect("start coordinator");
            Dcbdb {
                handle,
                coord: Some(coord),
                _dir: dir,
            }
        }
    }

    impl Drop for Dcbdb {
        fn drop(&mut self) {
            if let Some(coord) = self.coord.take() {
                coord.shutdown();
            }
        }
    }

    fn dcbdb_event(seq: u64, payload: &[u8]) -> Event {
        let tags = Tags::new(vec![
            Tag::new(format!("entity:{}", seq & 0xFFF)).expect("valid tag"),
        ])
        .expect("valid tag set");
        Event::new(
            &EventType::new("Appended").expect("valid type"),
            &tags,
            payload,
        )
        .expect("encode")
    }

    // --------------------------- umadb harness ---------------------------

    /// A live umadb store over a scratch directory. `UmaDb` holds an `Arc<Mvcc>`; dropping
    /// it releases the file, and the `TempDir` (declared last) then removes it.
    struct Uma {
        db: UmaDb,
        _dir: TempDir,
    }

    impl Uma {
        fn new() -> Uma {
            let dir = scratch_dir();
            let opts = StorageOptions::default().db_path(dir.path());
            let mvcc = Mvcc::new(false, opts).expect("open mvcc");
            Uma {
                db: UmaDb::from_arc(Arc::new(mvcc)),
                _dir: dir,
            }
        }
    }

    fn uma_event(seq: u64, payload: &[u8]) -> DcbEvent {
        DcbEvent::new()
            .event_type("Appended")
            .tags([format!("entity:{}", seq & 0xFFF)])
            .data(payload.to_vec())
    }

    // ------------------------------- groups -------------------------------

    fn append_latency(c: &mut Criterion) {
        let mut group = c.benchmark_group("append_latency");
        group.throughput(Throughput::Elements(1));
        group.sample_size(50);
        group.measurement_time(Duration::from_secs(10));
        let payload = vec![0u8; DEFAULT_PAYLOAD];

        {
            let engine = Dcbdb::new();
            group.bench_function("dcbdb", |b| {
                b.iter(|| {
                    let ev = dcbdb_event(next_seq(), &payload);
                    black_box(engine.handle.append(vec![ev], None).expect("append"));
                });
            });
        }
        {
            let engine = Uma::new();
            group.bench_function("umadb", |b| {
                b.iter(|| {
                    let ev = uma_event(next_seq(), &payload);
                    black_box(engine.db.append(vec![ev], None, None).expect("append"));
                });
            });
        }
        group.finish();
    }

    fn batch_size(c: &mut Criterion) {
        let mut group = c.benchmark_group("batch_size");
        group.sample_size(50);
        group.measurement_time(Duration::from_secs(10));
        let payload = vec![0u8; DEFAULT_PAYLOAD];

        for &batch in &[1usize, 8, 64, 512] {
            group.throughput(Throughput::Elements(batch as u64));

            let engine = Dcbdb::new();
            let template = dcbdb_event(next_seq(), &payload);
            group.bench_with_input(BenchmarkId::new("dcbdb", batch), &batch, |b, &batch| {
                b.iter(|| {
                    let events = vec![template.clone(); batch];
                    black_box(engine.handle.append(events, None).expect("append"));
                });
            });
            drop(engine);

            let engine = Uma::new();
            let template = uma_event(next_seq(), &payload);
            group.bench_with_input(BenchmarkId::new("umadb", batch), &batch, |b, &batch| {
                b.iter(|| {
                    let events = vec![template.clone(); batch];
                    black_box(engine.db.append(events, None, None).expect("append"));
                });
            });
        }
        group.finish();
    }

    fn payload_size(c: &mut Criterion) {
        let mut group = c.benchmark_group("payload_size");
        group.sample_size(50);
        group.measurement_time(Duration::from_secs(10));

        for &size in &[64usize, 256, 1024, 4096, 16384] {
            group.throughput(Throughput::Bytes(size as u64));
            let payload = vec![0u8; size];

            let engine = Dcbdb::new();
            group.bench_with_input(BenchmarkId::new("dcbdb", size), &size, |b, _| {
                b.iter(|| {
                    let ev = dcbdb_event(next_seq(), &payload);
                    black_box(engine.handle.append(vec![ev], None).expect("append"));
                });
            });
            drop(engine);

            let engine = Uma::new();
            group.bench_with_input(BenchmarkId::new("umadb", size), &size, |b, _| {
                b.iter(|| {
                    let ev = uma_event(next_seq(), &payload);
                    black_box(engine.db.append(vec![ev], None, None).expect("append"));
                });
            });
        }
        group.finish();
    }

    fn conditional_append(c: &mut Criterion) {
        let mut group = c.benchmark_group("conditional_append");
        group.throughput(Throughput::Elements(1));
        group.sample_size(50);
        group.measurement_time(Duration::from_secs(10));
        let payload = vec![0u8; DEFAULT_PAYLOAD];

        // Baselines: the same single append with no condition.
        {
            let engine = Dcbdb::new();
            group.bench_function("dcbdb_unconditional", |b| {
                b.iter(|| {
                    let ev = dcbdb_event(next_seq(), &payload);
                    black_box(engine.handle.append(vec![ev], None).expect("append"));
                });
            });
        }
        {
            let engine = Uma::new();
            group.bench_function("umadb_unconditional", |b| {
                b.iter(|| {
                    let ev = uma_event(next_seq(), &payload);
                    black_box(engine.db.append(vec![ev], None, None).expect("append"));
                });
            });
        }

        // Uniqueness guard: every append reserves a fresh unique tag, so the condition
        // never conflicts and the write always succeeds. `after` omitted means "fail if any
        // event matches", identical semantics in both engines.
        {
            let engine = Dcbdb::new();
            group.bench_function("dcbdb_unique_guard", |b| {
                b.iter(|| {
                    let n = next_seq();
                    let tags = Tags::new(vec![Tag::new(format!("unique:{n}")).expect("tag")])
                        .expect("tag set");
                    let ev =
                        Event::new(&EventType::new("Appended").expect("type"), &tags, &payload)
                            .expect("encode");
                    let condition = AppendCondition::new(Query::item(QueryItem::with_tags(tags)));
                    black_box(
                        engine
                            .handle
                            .append(vec![ev], Some(condition))
                            .expect("append"),
                    );
                });
            });
        }
        {
            let engine = Uma::new();
            group.bench_function("umadb_unique_guard", |b| {
                b.iter(|| {
                    let n = next_seq();
                    let tag = format!("unique:{n}");
                    let ev = DcbEvent::new()
                        .event_type("Appended")
                        .tags([tag.clone()])
                        .data(payload.clone());
                    let condition = DcbAppendCondition::new(
                        DcbQuery::new().item(DcbQueryItem::new().tags([tag])),
                    );
                    black_box(
                        engine
                            .db
                            .append(vec![ev], Some(condition), None)
                            .expect("append"),
                    );
                });
            });
        }
        group.finish();
    }

    pub fn run() {
        let mut criterion = Criterion::default().configure_from_args();
        append_latency(&mut criterion);
        batch_size(&mut criterion);
        payload_size(&mut criterion);
        conditional_append(&mut criterion);
        criterion.final_summary();
    }
}
