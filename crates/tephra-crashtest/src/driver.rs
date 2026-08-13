//! The concurrent write workload and an optional order-checking subscriber.
//!
//! Writers each own a client and hammer the server with seeded single-event appends, recording
//! `SENT` before and `ACKED` after through the witness. One event per append keeps the seq to
//! position mapping exact. Conditions never change event content (the guard is a separate query),
//! so a conditional append that is rejected simply never lands and the witness never records an
//! ack for it, which is a valid in-flight-absent outcome.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use tephra_client::{
    AppendCondition, Client, ClientError, ErrorCode, Position, Query, QueryItem, SubEvent, Tag,
    Tags,
};

use crate::witness::Witness;
use crate::workload::{self, client_event};

/// Which write pattern the driver runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Workload {
    /// Unconditional appends only.
    PureAppend,
    /// Every append carries a DCB condition (a mix of always-pass and likely-conflict guards).
    Conditional,
    /// Unconditional appends with a concurrent subscriber verifying live ordering.
    Subscription,
    /// A rotation of the above per seq.
    Mixed,
}

impl std::str::FromStr for Workload {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, String> {
        match s {
            "pure" | "append" => Ok(Workload::PureAppend),
            "conditional" | "dcb" => Ok(Workload::Conditional),
            "subscription" | "subscribe" => Ok(Workload::Subscription),
            "mixed" => Ok(Workload::Mixed),
            other => Err(format!("unknown workload {other:?}")),
        }
    }
}

/// Shared handles for a running workload.
pub struct Running {
    pub stop: Arc<AtomicBool>,
    pub sent: Arc<AtomicU64>,
    writers: Vec<JoinHandle<()>>,
    subscriber: Option<JoinHandle<()>>,
    /// Ordering problems seen by the subscriber, if any.
    pub sub_violations: Arc<Mutex<Vec<String>>>,
}

impl Running {
    /// Signals stop and joins every workload thread.
    pub fn stop_and_join(self) -> Vec<String> {
        self.stop.store(true, Ordering::SeqCst);
        for w in self.writers {
            let _ = w.join();
        }
        if let Some(sub) = self.subscriber {
            let _ = sub.join();
        }
        Arc::try_unwrap(self.sub_violations)
            .map(|m| m.into_inner().unwrap_or_default())
            .unwrap_or_default()
    }
}

/// Starts `writers` writer threads (and a subscriber for the subscription workload) against
/// `addr`, all writing to `witness`. Content is seeded by `seed`.
pub fn start(
    addr: SocketAddr,
    witness: Arc<Witness>,
    seed: u64,
    writers: usize,
    workload: Workload,
) -> Running {
    let stop = Arc::new(AtomicBool::new(false));
    let sent = Arc::new(AtomicU64::new(0));
    let seq = Arc::new(AtomicU64::new(1));
    let sub_violations = Arc::new(Mutex::new(Vec::new()));

    let mut handles = Vec::with_capacity(writers);
    for _ in 0..writers {
        let stop = Arc::clone(&stop);
        let sent = Arc::clone(&sent);
        let seq = Arc::clone(&seq);
        let witness = Arc::clone(&witness);
        handles.push(thread::spawn(move || {
            writer_loop(addr, &stop, &sent, &seq, &witness, seed, workload);
        }));
    }

    let subscriber = if workload == Workload::Subscription {
        let stop = Arc::clone(&stop);
        let violations = Arc::clone(&sub_violations);
        Some(thread::spawn(move || {
            subscriber_loop(addr, &stop, &violations);
        }))
    } else {
        None
    };

    Running {
        stop,
        sent,
        writers: handles,
        subscriber,
        sub_violations,
    }
}

fn writer_loop(
    addr: SocketAddr,
    stop: &AtomicBool,
    sent: &AtomicU64,
    seq: &AtomicU64,
    witness: &Witness,
    seed: u64,
    workload: Workload,
) {
    let Some(mut client) = connect_retry(addr, stop) else {
        return;
    };
    while !stop.load(Ordering::Relaxed) {
        let s = seq.fetch_add(1, Ordering::SeqCst);
        // SENT is durable before the event can reach the server.
        if witness.sent(s).is_err() {
            return;
        }
        sent.fetch_add(1, Ordering::Relaxed);

        let event = client_event(seed, s);
        let condition = condition_for(workload, s);
        match client.append([event], condition) {
            Ok(result) => {
                if witness
                    .acked(s, result.first.get(), result.last.get())
                    .is_err()
                {
                    return;
                }
            }
            Err(ClientError::Server {
                code: ErrorCode::Conflict,
                ..
            }) => {
                // A rejected conditional append never lands. Correctly left unacked.
            }
            Err(_) => {
                // Transport error: the server almost certainly died. Stop cleanly; this seq is
                // an in-flight write (sent, not acked), which the invariants allow either way.
                return;
            }
        }
    }
}

/// The guard for an append, chosen so content is never altered and acked semantics stay clean.
fn condition_for(workload: Workload, seq: u64) -> Option<AppendCondition> {
    let guard = |tag: String| Some(AppendCondition::new(single_tag_query(&tag)));
    match workload {
        Workload::PureAppend | Workload::Subscription => None,
        // Odd seqs guard on a tag that can never exist (always succeed, exercising the durable
        // condition lookup); even seqs guard on their own entity tag (usually a real conflict,
        // exercising rejection under group commit).
        Workload::Conditional => {
            if seq % 2 == 1 {
                guard(format!("nonexist:{seq}"))
            } else {
                guard(format!("entity:{}", seq % workload::ENTITY_MOD))
            }
        }
        Workload::Mixed => match seq % 3 {
            0 => None,
            1 => guard(format!("nonexist:{seq}")),
            _ => guard(format!("entity:{}", seq % workload::ENTITY_MOD)),
        },
    }
}

/// A subscriber that verifies live delivery is strictly ascending with no gap up to each
/// caught-up marker. Records any violation; exits when the server dies or stop is set.
fn subscriber_loop(addr: SocketAddr, stop: &AtomicBool, violations: &Mutex<Vec<String>>) {
    let Some(mut client) = connect_retry(addr, stop) else {
        return;
    };
    let (stream, _cancel) = match client.subscribe(Query::all(), Position::ZERO) {
        Ok(pair) => pair,
        Err(_) => return,
    };
    let mut last = 0u64;
    for item in stream {
        if stop.load(Ordering::Relaxed) {
            return;
        }
        match item {
            Ok(SubEvent::Event(ev)) => {
                let pos = ev.position().get();
                if pos <= last {
                    violations.lock().unwrap().push(format!(
                        "subscription delivered {pos} after {last} (not ascending)"
                    ));
                } else if pos != last + 1 {
                    violations.lock().unwrap().push(format!(
                        "subscription gap: delivered {pos} after {last} (missing {})",
                        last + 1
                    ));
                }
                last = pos;
            }
            Ok(SubEvent::CaughtUp(_)) => {}
            Err(_) => return,
        }
    }
}

fn single_tag_query(tag: &str) -> Query {
    let tags = Tags::new([Tag::new(tag).unwrap()]).unwrap();
    Query::item(QueryItem::with_tags(tags))
}

fn connect_retry(addr: SocketAddr, stop: &AtomicBool) -> Option<Client> {
    for _ in 0..200 {
        if stop.load(Ordering::Relaxed) {
            return None;
        }
        match Client::connect(addr) {
            Ok(client) => return Some(client),
            Err(_) => thread::sleep(Duration::from_millis(5)),
        }
    }
    None
}
