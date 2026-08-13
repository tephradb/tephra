//! The witness log: the durable ground truth for what was sent and what was acked.
//!
//! It lives on a directory separate from the Tephra data dir and is fsynced per record, so it
//! survives a crash of the harness itself, not just the server. Every line is one fact:
//!
//! ```text
//! SEED <u64>
//! SENT <seq>
//! ACKED <seq> <first> <last>
//! ```
//!
//! Reading it back yields the set of sent seqs and the map of acked seq to position, which the
//! invariant checker treats as authoritative. Nothing about what was acked is ever kept only in
//! memory.

use std::collections::{HashMap, HashSet};
use std::fs::{File, OpenOptions};
use std::io::{self, BufRead, BufReader, Write};
use std::path::Path;
use std::sync::Mutex;

/// A per-record-fsynced append-only witness file.
pub struct Witness {
    inner: Mutex<File>,
}

impl Witness {
    /// Creates a fresh witness log at `path` and records the seed as its first line.
    pub fn create(path: &Path, seed: u64) -> io::Result<Witness> {
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;
        let witness = Witness {
            inner: Mutex::new(file),
        };
        witness.write_line(format!("SEED {seed}"))?;
        Ok(witness)
    }

    fn write_line(&self, line: String) -> io::Result<()> {
        let mut file = self.inner.lock().expect("witness mutex poisoned");
        file.write_all(line.as_bytes())?;
        file.write_all(b"\n")?;
        // Per-record durability: the witness must never lose a fact to a harness crash.
        file.sync_data()?;
        Ok(())
    }

    /// Records that `seq` is about to be sent. Written and fsynced before the append leaves the
    /// harness, so a seq that reached the server always has a preceding SENT on disk.
    pub fn sent(&self, seq: u64) -> io::Result<()> {
        self.write_line(format!("SENT {seq}"))
    }

    /// Records that `seq` was acked at the given position range.
    pub fn acked(&self, seq: u64, first: u64, last: u64) -> io::Result<()> {
        self.write_line(format!("ACKED {seq} {first} {last}"))
    }
}

/// The ground truth parsed back from a witness file.
#[derive(Debug, Default)]
pub struct Ground {
    pub seed: u64,
    pub sent: HashSet<u64>,
    /// seq -> (first, last). Each append here carries exactly one event, so first == last.
    pub acked: HashMap<u64, (u64, u64)>,
}

impl Ground {
    /// Parses a witness file back into the sent set and the acked map.
    pub fn read(path: &Path) -> io::Result<Ground> {
        let file = File::open(path)?;
        let mut ground = Ground::default();
        for line in BufReader::new(file).lines() {
            let line = line?;
            let mut parts = line.split_whitespace();
            // A torn trailing line is tolerated, not a panic: the harness may die between a
            // record's payload write and its newline (or mid-payload), leaving a partial final
            // line. An incomplete SENT was never actually sent (SENT is fsynced before the append),
            // and an incomplete ACKED merely drops the "acked" fact for a write that is present
            // anyway, which only weakens a check, never falsifies one. Skip any line missing a
            // field.
            match parts.next() {
                Some("SEED") => {
                    if let Some(seed) = next_u64(&mut parts) {
                        ground.seed = seed;
                    }
                }
                Some("SENT") => {
                    if let Some(seq) = next_u64(&mut parts) {
                        ground.sent.insert(seq);
                    }
                }
                Some("ACKED") => {
                    if let (Some(seq), Some(first), Some(last)) = (
                        next_u64(&mut parts),
                        next_u64(&mut parts),
                        next_u64(&mut parts),
                    ) {
                        ground.acked.insert(seq, (first, last));
                    }
                }
                _ => {}
            }
        }
        Ok(ground)
    }

    /// The highest position among all acked writes, or 0 if none were acked.
    pub fn max_acked_position(&self) -> u64 {
        self.acked
            .values()
            .map(|&(_, last)| last)
            .max()
            .unwrap_or(0)
    }
}

/// The next whitespace token parsed as a `u64`, or `None` if absent or malformed (a torn line).
fn next_u64<'a>(parts: &mut impl Iterator<Item = &'a str>) -> Option<u64> {
    parts.next().and_then(|s| s.parse().ok())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    /// A witness with a torn final line (the harness died mid-write) must parse the complete facts
    /// and silently drop the partial one, not panic.
    #[test]
    fn read_tolerates_a_torn_trailing_line() {
        for torn in ["ACKED 5 10", "SENT ", "ACK", ""] {
            let dir = tempfile::TempDir::new().unwrap();
            let path = dir.path().join("witness.log");
            {
                let mut f = File::create(&path).unwrap();
                // Complete facts, then a torn final line with no trailing newline.
                write!(f, "SEED 7\nSENT 1\nACKED 1 1 1\nSENT 2\n{torn}").unwrap();
            }
            let ground = Ground::read(&path).expect("read must not fail on a torn trailing line");
            assert_eq!(ground.seed, 7);
            assert!(ground.sent.contains(&1));
            assert_eq!(ground.acked.get(&1), Some(&(1, 1)));
            // The torn ACKED 5 line is dropped; seq 5 is never recorded as acked.
            assert!(
                !ground.acked.contains_key(&5),
                "torn ACKED must be dropped ({torn:?})"
            );
        }
    }
}
