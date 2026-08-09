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
//! ## Configuring umadb fairly
//!
//! umadb's in-process **page cache** (a cache of *deserialized* pages) is off in its default
//! `StorageOptions`. Left off, umadb re-parses every B+tree page from raw bytes on every read
//! traversal, even when those bytes are warm in the OS cache, while dcbdb keeps its index
//! fully resident (an `Arc<[u8]>` per `.idx`) and its log warm. That is not a like-for-like
//! comparison: one engine is measured warm, the other effectively cold-parses each read. So
//! umadb runs here with its page cache enabled (`uma_options`), sized to hold the working set,
//! so both engines are measured with their hot data in memory. `pipelined_writer` stays off,
//! so commits remain durable and the write comparison is unchanged.
//!
//! ## Concurrency is intentionally omitted
//!
//! The embedded `UmaDb::append` is single-writer by convention (its `Mvcc` takes no writer
//! lock; serialization is the job of umadb's server writer thread, out of scope for an
//! embedded write-path comparison). Driving it from several threads would race, so there
//! is no `group_commit` group here; dcbdb's concurrency story lives in `write_path`.
//!
//! ## The read comparison
//!
//! Reads are the larger half of an event store's work (projection catch-up, decision-model
//! reads, subscription resume), so four read groups sit alongside the write groups, over an
//! identical corpus appended to both engines:
//!
//! - `read_full_scan`: read the whole log (`Query::all` / a `None` query). dcbdb's zero-copy
//!   sequential log scan vs umadb's in-order B+tree traversal, the highest-volume DCB read.
//! - `read_selectivity`: a tag query at graded selectivity (`1/1000` .. `1/10`). dcbdb's
//!   index plus per-match fetch vs umadb's tags-tree lookups, the decision-model read.
//! - `read_range`: a fixed selectivity with `after` sweeping the log (whole, half, recent
//!   tail), the subscription-resume shape. Exercises dcbdb's whole-segment header pruning.
//! - `read_single_entity_and_payload`: one unique-tag read (load one aggregate), plus a
//!   payload-size sweep on a fixed query.
//!
//! Unlike the write-compare's 1 GiB segments, the read corpus uses small dcbdb segments so
//! it seals many on-disk segments: a store *with history*, where reads hit sealed segments
//! and the on-disk index rather than answering wholly from the resident active tail (which
//! would only flatter dcbdb). dcbdb runs with its default planner (`scan_bias`), so it is
//! measured as it actually behaves; the forced index-vs-scan crossover is `read_path`'s job.
//!
//! ### Eager vs lazy: making the two do equal work
//!
//! umadb's embedded `read` is **eager**: it materializes the entire result into a
//! `Vec<DcbSequencedEvent>` (owned, decoded events with payloads) before returning. dcbdb's
//! `read` is a **lazy** lending iterator that decodes one event at a time. To compare them
//! fairly, every read is drained to exhaustion and each event's payload is touched
//! (`black_box`), so neither engine is credited for laziness a consumer never receives. The
//! asymmetry (umadb allocates the whole result up front; dcbdb lends per event) is a real
//! design difference, surfaced here rather than hidden. Before timing, `assert_same_result`
//! runs both engines once and checks they return the identical ascending positions, so every
//! measured comparison is provably over the same logical work.

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

    use dcbdb::Position;
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

    /// umadb's in-process page cache (a cache of *deserialized* pages) is disabled by
    /// default, which would make it re-parse every B+tree page from raw bytes on every read
    /// even when those bytes are warm in the OS cache. dcbdb keeps its index fully resident
    /// (an `Arc<[u8]>` per `.idx`) and its log warm in the OS cache, so to compare like for
    /// like (both engines measured with their hot data in memory) umadb runs with a page
    /// cache sized to hold the whole working set. 1 GiB covers the largest corpus here (50k
    /// events at a 4 KiB payload is roughly 200 MB) with room to spare. `pipelined_writer`
    /// stays off so every commit is durable, preserving the "cost of one durable append"
    /// write comparison.
    const UMA_PAGE_CACHE_MB: usize = 1024;

    /// umadb storage options under `dir`, with the page cache enabled (see
    /// [`UMA_PAGE_CACHE_MB`]).
    fn uma_options(dir: &std::path::Path) -> StorageOptions {
        StorageOptions::default()
            .db_path(dir)
            .page_cache_max_mb(UMA_PAGE_CACHE_MB)
    }

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
            let opts = uma_options(dir.path());
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

    // ------------------------------- reads --------------------------------

    /// Corpus size: enough events to span many sealed segments (so reads hit the on-disk
    /// index and log, not just the resident active tail), quick enough to build per group.
    const READ_N: u64 = 50_000;
    /// Small dcbdb segments so the corpus seals many on-disk segments: a store with history,
    /// mirroring `read_path.rs` rather than the write-compare's single 1 GiB segment.
    const READ_SEGMENT_SIZE: usize = 1 << 18; // 256 KiB
    const READ_MAX_BATCH_BYTES: usize = 1 << 16; // 64 KiB, under the segment capacity
    /// Default payload for every read group except the payload sweep.
    const READ_PAYLOAD: usize = 128;
    /// Selectivity denominators: a query for `s{d}:0` matches the `1/d` fraction of the log.
    const SELECTIVITY_DENOMS: [u64; 3] = [1000, 100, 10];

    /// The tag strings carried by event `seq`: three graded selectivity buckets (a query for
    /// `s{d}:0` matches every `d`-th event) plus a unique per-entity tag (for the
    /// single-entity read). One shape, encoded identically into both engines.
    fn read_tags(seq: u64) -> [String; 4] {
        [
            format!("s1000:{}", seq % 1000),
            format!("s100:{}", seq % 100),
            format!("s10:{}", seq % 10),
            format!("entity:{seq}"),
        ]
    }

    fn dcbdb_read_event(seq: u64, payload: &[u8]) -> Event {
        let picked: Vec<Tag> = read_tags(seq)
            .iter()
            .map(|s| Tag::new(s).expect("valid tag"))
            .collect();
        let tags = Tags::new(picked).expect("valid tag set");
        Event::new(
            &EventType::new("Appended").expect("valid type"),
            &tags,
            payload,
        )
        .expect("encode")
    }

    fn uma_read_event(seq: u64, payload: &[u8]) -> DcbEvent {
        DcbEvent::new()
            .event_type("Appended")
            .tags(read_tags(seq))
            .data(payload.to_vec())
    }

    impl Dcbdb {
        /// Builds a dcbdb store of [`READ_N`] events with `payload`-byte payloads, over small
        /// segments so the corpus seals many on-disk segments.
        fn corpus(payload_size: usize) -> Dcbdb {
            let dir = scratch_dir();
            let set = SegmentSet::open(dir.path(), SegmentConfig::new(READ_SEGMENT_SIZE))
                .expect("open segment set");
            let cfg = WriterConfig {
                max_batch_bytes: READ_MAX_BATCH_BYTES,
                verify_tips: false,
                ..WriterConfig::default()
            };
            let (coord, handle) = WriteCoordinator::start(set, cfg).expect("start coordinator");

            let payload = vec![0u8; payload_size];
            let per_batch = (READ_MAX_BATCH_BYTES / (payload_size + 128)).clamp(1, 200);
            let mut batch: Vec<Event> = Vec::new();
            for seq in 0..READ_N {
                batch.push(dcbdb_read_event(seq, &payload));
                if batch.len() == per_batch {
                    handle
                        .append(std::mem::take(&mut batch), None)
                        .expect("append batch");
                }
            }
            if !batch.is_empty() {
                handle.append(batch, None).expect("append tail batch");
            }
            Dcbdb {
                handle,
                coord: Some(coord),
                _dir: dir,
            }
        }
    }

    impl Uma {
        /// Builds a umadb store of [`READ_N`] events with `payload`-byte payloads, the same
        /// workload dcbdb's [`Dcbdb::corpus`] builds.
        fn corpus(payload_size: usize) -> Uma {
            let dir = scratch_dir();
            let opts = uma_options(dir.path());
            let mvcc = Mvcc::new(false, opts).expect("open mvcc");
            let db = UmaDb::from_arc(Arc::new(mvcc));

            let payload = vec![0u8; payload_size];
            let per_batch = (READ_MAX_BATCH_BYTES / (payload_size + 128)).clamp(1, 200);
            let mut batch: Vec<DcbEvent> = Vec::new();
            for seq in 0..READ_N {
                batch.push(uma_read_event(seq, &payload));
                if batch.len() == per_batch {
                    db.append(std::mem::take(&mut batch), None, None)
                        .expect("append batch");
                }
            }
            if !batch.is_empty() {
                db.append(batch, None, None).expect("append tail batch");
            }
            Uma { db, _dir: dir }
        }
    }

    /// A dcbdb single-tag query.
    fn dcb_tag_query(tag: &str) -> Query {
        Query::item(QueryItem::with_tags(
            Tags::new(vec![Tag::new(tag).expect("valid tag")]).expect("tag set"),
        ))
    }

    /// The umadb single-tag query, matching [`dcb_tag_query`]'s semantics.
    fn uma_tag_query(tag: &str) -> Option<DcbQuery> {
        Some(DcbQuery::new().item(DcbQueryItem::new().tags([tag.to_string()])))
    }

    /// The umadb start bound for a dcbdb `after`: position 0 (the "before everything"
    /// sentinel) maps to `None` (from the beginning), any other to `Some(after)` (exclusive).
    fn uma_start(after: u64) -> Option<u64> {
        if after == 0 { None } else { Some(after) }
    }

    /// Drains a dcbdb read to exhaustion, touching each event's payload, and returns the
    /// count. Equalizes work against umadb's eager materialization: the lazy iterator is
    /// consumed fully so no laziness is credited that a consumer never receives.
    fn drain_dcbdb(handle: &WriteHandle, query: Query, after: Position) -> u64 {
        let mut reads = handle.read(query, after);
        let mut count = 0u64;
        while let Some(item) = reads.next() {
            let seq = item.expect("read");
            black_box(seq.event.data());
            count += 1;
        }
        count
    }

    /// Drains a umadb read to exhaustion, touching each event's payload, and returns the
    /// count.
    fn drain_uma(db: &UmaDb, query: Option<DcbQuery>, start: Option<u64>) -> u64 {
        let response = db.read(query, start, false, None).expect("read");
        let mut count = 0u64;
        for item in response {
            let event = item.expect("read");
            black_box(&event.event.data);
            count += 1;
        }
        count
    }

    /// The ascending positions a dcbdb read yields.
    fn positions_dcbdb(handle: &WriteHandle, query: Query, after: Position) -> Vec<u64> {
        let mut reads = handle.read(query, after);
        let mut out = Vec::new();
        while let Some(item) = reads.next() {
            out.push(item.expect("read").position.get());
        }
        out
    }

    /// The ascending positions a umadb read yields.
    fn positions_uma(db: &UmaDb, query: Option<DcbQuery>, start: Option<u64>) -> Vec<u64> {
        let response = db.read(query, start, false, None).expect("read");
        response.map(|item| item.expect("read").position).collect()
    }

    /// Asserts the two engines return the identical ascending position vector for a read, so
    /// every timed comparison is provably over the same logical work (the crux of comparing
    /// correctly). A divergence panics bench setup rather than silently timing different work.
    fn assert_same_result(
        handle: &WriteHandle,
        db: &UmaDb,
        dcb_q: Query,
        uma_q: Option<DcbQuery>,
        after: u64,
    ) {
        let d = positions_dcbdb(handle, dcb_q, Position::new(after));
        let u = positions_uma(db, uma_q, uma_start(after));
        assert_eq!(
            d.len(),
            u.len(),
            "dcbdb ({}) and umadb ({}) disagreed on result count (after {after})",
            d.len(),
            u.len(),
        );
        assert_eq!(
            d, u,
            "dcbdb and umadb disagreed on the result positions (after {after})"
        );
    }

    /// Read everything: dcbdb's zero-copy sequential log scan vs umadb's in-order B+tree
    /// traversal. The highest-volume DCB read (projection catch-up, subscription resume).
    fn read_full_scan(c: &mut Criterion) {
        let mut group = c.benchmark_group("read_full_scan");
        group.throughput(Throughput::Elements(READ_N));
        group.sample_size(20);

        let dcbdb = Dcbdb::corpus(READ_PAYLOAD);
        let uma = Uma::corpus(READ_PAYLOAD);
        assert_same_result(&dcbdb.handle, &uma.db, Query::all(), None, 0);

        group.bench_function("dcbdb", |b| {
            b.iter(|| black_box(drain_dcbdb(&dcbdb.handle, Query::all(), Position::ZERO)));
        });
        group.bench_function("umadb", |b| {
            b.iter(|| black_box(drain_uma(&uma.db, None, None)));
        });
        group.finish();
    }

    /// A tag query at graded selectivity: dcbdb's index plus per-match fetch vs umadb's
    /// tags-tree lookups. The decision-model read.
    fn read_selectivity(c: &mut Criterion) {
        let mut group = c.benchmark_group("read_selectivity");
        group.sample_size(20);

        let dcbdb = Dcbdb::corpus(READ_PAYLOAD);
        let uma = Uma::corpus(READ_PAYLOAD);

        for &d in &SELECTIVITY_DENOMS {
            group.throughput(Throughput::Elements(READ_N / d));
            let tag = format!("s{d}:0");
            let dq = dcb_tag_query(&tag);
            let uq = uma_tag_query(&tag);
            assert_same_result(&dcbdb.handle, &uma.db, dq.clone(), uq.clone(), 0);

            let label = format!("1_over_{d}");
            group.bench_with_input(BenchmarkId::new("dcbdb", &label), &dq, |b, dq| {
                b.iter(|| black_box(drain_dcbdb(&dcbdb.handle, dq.clone(), Position::ZERO)));
            });
            group.bench_with_input(BenchmarkId::new("umadb", &label), &uq, |b, uq| {
                b.iter(|| black_box(drain_uma(&uma.db, uq.clone(), None)));
            });
        }
        group.finish();
    }

    /// A fixed selectivity with `after` sweeping the log: the subscription-resume shape.
    /// Exercises dcbdb's whole-segment header pruning vs umadb's tree seek.
    fn read_range(c: &mut Criterion) {
        let mut group = c.benchmark_group("read_range");
        group.sample_size(20);

        const DENOM: u64 = 100;
        let dcbdb = Dcbdb::corpus(READ_PAYLOAD);
        let uma = Uma::corpus(READ_PAYLOAD);
        let tag = format!("s{DENOM}:0");
        let dq = dcb_tag_query(&tag);
        let uq = uma_tag_query(&tag);

        for &(label, after) in &[
            ("whole", 0u64),
            ("half", READ_N / 2),
            ("recent_tenth", READ_N * 9 / 10),
        ] {
            // Surviving matches strictly after `after`.
            group.throughput(Throughput::Elements(((READ_N - after) / DENOM).max(1)));
            assert_same_result(&dcbdb.handle, &uma.db, dq.clone(), uq.clone(), after);

            group.bench_with_input(BenchmarkId::new("dcbdb", label), &after, |b, &after| {
                b.iter(|| black_box(drain_dcbdb(&dcbdb.handle, dq.clone(), Position::new(after))));
            });
            group.bench_with_input(BenchmarkId::new("umadb", label), &after, |b, &after| {
                b.iter(|| black_box(drain_uma(&uma.db, uq.clone(), uma_start(after))));
            });
        }
        group.finish();
    }

    /// Load one aggregate (a unique-tag read matching a single event), then a payload-size
    /// sweep on a fixed selective query: larger events dearen each fetch and materialization.
    fn read_single_entity_and_payload(c: &mut Criterion) {
        {
            let mut group = c.benchmark_group("read_single_entity");
            group.throughput(Throughput::Elements(1));
            group.sample_size(50);

            let dcbdb = Dcbdb::corpus(READ_PAYLOAD);
            let uma = Uma::corpus(READ_PAYLOAD);
            let tag = format!("entity:{}", READ_N / 2);
            let dq = dcb_tag_query(&tag);
            let uq = uma_tag_query(&tag);
            assert_same_result(&dcbdb.handle, &uma.db, dq.clone(), uq.clone(), 0);

            group.bench_function("dcbdb", |b| {
                b.iter(|| black_box(drain_dcbdb(&dcbdb.handle, dq.clone(), Position::ZERO)));
            });
            group.bench_function("umadb", |b| {
                b.iter(|| black_box(drain_uma(&uma.db, uq.clone(), None)));
            });
            group.finish();
        }

        {
            let mut group = c.benchmark_group("read_payload_size");
            group.sample_size(20);

            const DENOM: u64 = 100;
            let tag = format!("s{DENOM}:0");
            for &payload in &[64usize, 1024, 4096] {
                group.throughput(Throughput::Elements(READ_N / DENOM));

                let dcbdb = Dcbdb::corpus(payload);
                let uma = Uma::corpus(payload);
                let dq = dcb_tag_query(&tag);
                let uq = uma_tag_query(&tag);
                assert_same_result(&dcbdb.handle, &uma.db, dq.clone(), uq.clone(), 0);

                group.bench_with_input(BenchmarkId::new("dcbdb", payload), &dq, |b, dq| {
                    b.iter(|| black_box(drain_dcbdb(&dcbdb.handle, dq.clone(), Position::ZERO)));
                });
                group.bench_with_input(BenchmarkId::new("umadb", payload), &uq, |b, uq| {
                    b.iter(|| black_box(drain_uma(&uma.db, uq.clone(), None)));
                });
            }
            group.finish();
        }
    }

    pub fn run() {
        let mut criterion = Criterion::default().configure_from_args();
        append_latency(&mut criterion);
        batch_size(&mut criterion);
        payload_size(&mut criterion);
        conditional_append(&mut criterion);
        read_full_scan(&mut criterion);
        read_selectivity(&mut criterion);
        read_range(&mut criterion);
        read_single_entity_and_payload(&mut criterion);
        criterion.final_summary();
    }
}
