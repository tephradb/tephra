//! Append-only, concurrently-readable columnar storage for the active tail.
//!
//! The active segment's index is fed by the single writer thread and read by any number of
//! caller threads, bounded on read by the published watermark (CLAUDE.md 9). These two
//! structures are the storage that makes that sound without `unsafe`:
//!
//! - [`ChunkedVec<T>`]: a chunked vector whose chunks never move once allocated, so a
//!   reference to element `i` stays valid for the life of the structure. Growth appends a
//!   whole chunk and republishes the backbone behind a `RwLock<Arc<..>>`; the chunk count
//!   grows by **doubling**, so total growth work is `O(n)` (not `O(n^2)` from cloning the
//!   backbone on every chunk). The slots carry their own interior mutability (`T` is an
//!   atomic, or has atomic fields), so this type never hands out `&mut T`: the writer's
//!   stores and readers' loads never alias.
//! - [`PostingSlot`]: one tag's posting list, inline for the common rare-tag case (heap-free)
//!   and spilling to a [`ChunkedVec`] only when hot (UmaDB-style tiering, CLAUDE.md 15).
//!
//! ## Two orderings, kept separate
//!
//! - **Slot contents** are stored/loaded `Relaxed`. Their visibility is provided by a
//!   *higher* release/acquire edge: for the type column the writer's watermark store, for a
//!   posting list the slot's own [`PostingSlot::len`] (Release on write, Acquire on read).
//!   The reader only ever reads slots below that bound, and reads/writes never touch the
//!   same slot, so `Relaxed` on the value itself is sound.
//! - **Backbone structure** is obtained through the `RwLock`, *not* the watermark. A reader
//!   clones the backbone after acquiring its bound; [`Snap::get`] is bounds-checked and
//!   returns `None` past the clone rather than indexing out of range, so a backbone that has
//!   not yet grown can never panic a reader.

use std::sync::Arc;
use std::sync::RwLock;
use std::sync::atomic::{AtomicU32, Ordering};

/// Elements per chunk. Growth adds a whole chunk, so existing element addresses never move.
/// One chunk of `AtomicU32` is 4 KiB; of `AtomicU16`, 2 KiB.
const CHUNK: usize = 1024;

/// Postings kept inline in a [`PostingSlot`] before spilling to a [`ChunkedVec`]. Four
/// matches the dominant 1-to-4-tag event shape (`Tags` is `SmallVec<[Tag; 4]>`), so a rare
/// tag's whole posting list stays heap-free.
const INLINE: usize = 4;

/// A fresh chunk of `CHUNK` default-constructed slots. `Arc<[T]>` so cloning the backbone is
/// a shallow copy of chunk handles, never of slot contents.
fn new_chunk<T: Default>() -> Arc<[T]> {
    (0..CHUNK).map(|_| T::default()).collect()
}

/// A chunked, append-only vector of `T`, single-producer (the writer thread) /
/// multi-consumer (reader threads).
pub struct ChunkedVec<T> {
    /// The published backbone: chunk handles in order. Grown by doubling and swapped whole
    /// (never mutated in place), so a reader holding a clone is unaffected.
    backbone: RwLock<Arc<Vec<Arc<[T]>>>>,
    /// The writer's append cursor (the element count). Not the reader's bound: readers clamp
    /// to a watermark (type column) or a per-slot length (postings), so this stays `Relaxed`.
    len: AtomicU32,
}

impl<T: Default> Default for ChunkedVec<T> {
    fn default() -> Self {
        ChunkedVec {
            backbone: RwLock::new(Arc::new(Vec::new())),
            len: AtomicU32::new(0),
        }
    }
}

impl<T: Default> ChunkedVec<T> {
    pub fn new() -> Self {
        Self::default()
    }

    /// The number of elements appended so far (the writer cursor).
    pub fn len(&self) -> u32 {
        self.len.load(Ordering::Relaxed)
    }

    /// Ensures the backbone has a chunk for element index `i`. Writer-only; single producer,
    /// so no other writer races. Growth doubles the chunk count, making total growth work
    /// across a segment `O(chunks)` rather than `O(chunks^2)`.
    fn ensure(&self, i: usize) {
        let need = i / CHUNK + 1;
        if self.backbone.read().unwrap().len() >= need {
            return;
        }
        let mut guard = self.backbone.write().unwrap();
        if guard.len() >= need {
            return;
        }
        let target = need.max(guard.len().saturating_mul(2));
        let mut next: Vec<Arc<[T]>> = (**guard).clone();
        while next.len() < target {
            next.push(new_chunk());
        }
        *guard = Arc::new(next);
    }

    /// Appends one element, initializing its slot via `f`, and returns its index. The count
    /// is bumped last (`Relaxed`): readers do not bound on this count, so no release is
    /// needed here (the type column's visibility comes from the watermark, a posting list's
    /// from [`PostingSlot::len`]).
    pub fn push_with(&self, f: impl FnOnce(&T)) -> u32 {
        let i = self.len.load(Ordering::Relaxed) as usize;
        self.ensure(i);
        {
            let bb = self.backbone.read().unwrap();
            f(&bb[i / CHUNK][i % CHUNK]);
        }
        self.len.store((i + 1) as u32, Ordering::Relaxed);
        i as u32
    }

    /// Runs `f` against the existing slot at `i`. The caller guarantees `i` was previously
    /// appended (so its chunk exists).
    pub fn with(&self, i: u32, f: impl FnOnce(&T)) {
        let i = i as usize;
        let bb = self.backbone.read().unwrap();
        f(&bb[i / CHUNK][i % CHUNK]);
    }

    /// Clones the current backbone for lock-free reading. Take this **after** acquiring the
    /// bound (watermark or slot length) so the clone covers every element below that bound.
    pub fn snapshot(&self) -> Snap<T> {
        Snap {
            backbone: Arc::clone(&self.backbone.read().unwrap()),
        }
    }
}

/// A reader's cloned view of a [`ChunkedVec`] backbone. Reads are lock-free and
/// bounds-checked; the chunks it holds are immutable and stay valid for its lifetime.
pub struct Snap<T> {
    backbone: Arc<Vec<Arc<[T]>>>,
}

impl<T> Snap<T> {
    /// The slot at element index `i`, or `None` if `i` is beyond the chunks this snapshot
    /// captured (a slot appended after the snapshot was taken). Never panics.
    pub fn get(&self, i: u32) -> Option<&T> {
        let i = i as usize;
        self.backbone.get(i / CHUNK).map(|chunk| &chunk[i % CHUNK])
    }

    /// The number of element slots this snapshot can address (`chunks * CHUNK`).
    pub fn covered(&self) -> u32 {
        (self.backbone.len() * CHUNK) as u32
    }
}

/// One tag's posting list: the ascending local positions of the events carrying it.
///
/// Rare tags (the common case) keep all their postings inline, heap-free; a hot tag spills
/// the overflow to a [`ChunkedVec`]. The reader path is driven by [`len`](Self::len): it is
/// stored **last, with Release**, so a reader that observes a count also observes every value
/// (and, past `INLINE`, the initialized spill) written before it.
#[derive(Default)]
pub struct PostingSlot {
    /// The posting count. Release on write / Acquire on read is the ordering edge for this
    /// slot's contents (the type column uses the watermark instead).
    len: AtomicU32,
    inline: [AtomicU32; INLINE],
    /// Allocated only once a tag exceeds `INLINE` postings. `OnceLock::get` supplies the
    /// acquire for the spill's own storage once `len > INLINE` is observed.
    spill: std::sync::OnceLock<ChunkedVec<AtomicU32>>,
}

impl PostingSlot {
    /// Appends one local position. Writer-only, and single-producer per slot. Postings arrive
    /// in ascending order (feed order), so the list stays sorted by construction.
    pub fn push_local(&self, local: u32) {
        let n = self.len.load(Ordering::Relaxed) as usize;
        if n < INLINE {
            self.inline[n].store(local, Ordering::Relaxed);
        } else {
            let spill = self.spill.get_or_init(ChunkedVec::new);
            spill.push_with(|slot| slot.store(local, Ordering::Relaxed));
        }
        // Publish the new count last: a reader keys off this (Acquire), so every value store
        // above (and the spill init on the boundary push) happens-before the reader sees it.
        self.len.store((n + 1) as u32, Ordering::Release);
    }

    /// Appends this slot's ascending locals with `local < upto` to `out`. Reader-side.
    ///
    /// Reads `len` first (Acquire), then chooses inline-vs-spill from it: never the reverse,
    /// because a `len > INLINE` is what guarantees the spill `OnceLock` is initialized.
    /// Postings are ascending, so it stops at the first `local >= upto`.
    pub fn collect_below(&self, upto: u32, out: &mut Vec<u32>) {
        let n = self.len.load(Ordering::Acquire) as usize;
        let inline_n = n.min(INLINE);
        for slot in &self.inline[..inline_n] {
            let local = slot.load(Ordering::Relaxed);
            if local >= upto {
                return;
            }
            out.push(local);
        }
        if n > INLINE
            && let Some(spill) = self.spill.get()
        {
            let snap = spill.snapshot();
            for j in 0..(n - INLINE) as u32 {
                let Some(slot) = snap.get(j) else { break };
                let local = slot.load(Ordering::Relaxed);
                if local >= upto {
                    return;
                }
                out.push(local);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU16, AtomicU64};
    use std::thread;

    #[test]
    fn grows_across_chunk_boundaries_and_reads_back() {
        let col: ChunkedVec<AtomicU32> = ChunkedVec::new();
        let n = CHUNK as u32 * 3 + 7; // several chunks plus a partial one
        for v in 0..n {
            col.push_with(|slot| slot.store(v * 2, Ordering::Relaxed));
        }
        assert_eq!(col.len(), n);
        let snap = col.snapshot();
        assert!(snap.covered() >= n);
        for v in 0..n {
            assert_eq!(snap.get(v).unwrap().load(Ordering::Relaxed), v * 2);
        }
        // Past the appended range: no panic, just `None` beyond the covered chunks.
        assert!(snap.get(snap.covered()).is_none());
    }

    #[test]
    fn reader_sees_a_consistent_prefix_under_a_concurrent_writer() {
        // The active-tail pattern in miniature: a writer appends values and publishes a
        // watermark (Release); a reader loads the watermark (Acquire), snapshots, and reads
        // `0..watermark`. Every value it reads must be the one the writer stored, never a
        // torn or default slot, however the two threads interleave.
        let col: Arc<ChunkedVec<AtomicU32>> = Arc::new(ChunkedVec::new());
        let watermark = Arc::new(AtomicU64::new(0));
        let total = 50_000u32;

        let writer = {
            let col = Arc::clone(&col);
            let watermark = Arc::clone(&watermark);
            thread::spawn(move || {
                for v in 0..total {
                    col.push_with(|slot| slot.store(v + 1, Ordering::Relaxed));
                    watermark.store(v as u64 + 1, Ordering::Release);
                }
            })
        };

        let reader = {
            let col = Arc::clone(&col);
            let watermark = Arc::clone(&watermark);
            thread::spawn(move || {
                loop {
                    let wm = watermark.load(Ordering::Acquire);
                    let snap = col.snapshot();
                    for i in 0..wm as u32 {
                        // value at local i is i+1, set before the watermark passed i.
                        assert_eq!(snap.get(i).unwrap().load(Ordering::Relaxed), i + 1);
                    }
                    if wm >= total as u64 {
                        return wm;
                    }
                }
            })
        };

        writer.join().unwrap();
        assert_eq!(reader.join().unwrap(), total as u64);
    }

    #[test]
    fn posting_slot_stays_inline_then_spills() {
        let slot = PostingSlot::default();
        // Two postings: fully inline, no spill allocated.
        slot.push_local(0);
        slot.push_local(3);
        assert!(slot.spill.get().is_none());
        let mut out = Vec::new();
        slot.collect_below(u32::MAX, &mut out);
        assert_eq!(out, vec![0, 3]);

        // Grow past INLINE: the tail spills, and the full list is still ascending.
        for local in 4..20 {
            slot.push_local(local);
        }
        assert!(slot.spill.get().is_some());
        out.clear();
        slot.collect_below(u32::MAX, &mut out);
        let mut expected = vec![0, 3];
        expected.extend(4..20);
        assert_eq!(out, expected);
    }

    #[test]
    fn posting_slot_truncates_by_upto_across_the_spill_boundary() {
        let slot = PostingSlot::default();
        for local in 0..20 {
            slot.push_local(local);
        }
        // `upto` inside the inline range.
        let mut out = Vec::new();
        slot.collect_below(3, &mut out);
        assert_eq!(out, vec![0, 1, 2]);
        // `upto` inside the spill range.
        out.clear();
        slot.collect_below(10, &mut out);
        assert_eq!(out, (0..10).collect::<Vec<_>>());
    }

    #[test]
    fn type_column_slots_are_u16() {
        // The type column uses AtomicU16 slots; exercise the same storage at that width.
        let col: ChunkedVec<AtomicU16> = ChunkedVec::new();
        for v in 0..2000u16 {
            col.push_with(|slot| slot.store(v, Ordering::Relaxed));
        }
        let snap = col.snapshot();
        assert_eq!(snap.get(0).unwrap().load(Ordering::Relaxed), 0);
        assert_eq!(snap.get(1999).unwrap().load(Ordering::Relaxed), 1999);
    }
}
