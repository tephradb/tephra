//! Crash consistency and fault injection test harness for Tephra.
//!
//! The unit of work is a *cycle*: open a fresh store on real disk, drive concurrent seeded
//! writers while recording a durable witness log, crash the server (a `SIGKILL` at a seeded time,
//! or a self-`abort` at an armed crash point), restart, and check every invariant against the
//! witness. Cycles are independent and seeded, so a failure reproduces from its seed and cycle
//! index alone, and the whole data dir plus witness is copied to an artifact directory.

use std::env;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::process;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

pub mod driver;
pub mod invariants;
pub mod server;
pub mod witness;
pub mod workload;

pub use driver::Workload;

use server::{ExitKind, ServerProcess};
use witness::{Ground, Witness};

/// How the server is crashed in a cycle.
#[derive(Debug, Clone)]
pub enum Crash {
    /// SIGKILL the child at a seeded delay after writers start.
    SigkillTimed,
    /// Arm a crash point (`TEPHRA_CRASH_POINT` value) and let the server abort itself during
    /// normal operation.
    Point(String),
    /// SIGKILL the server first to leave committed data, then restart it with the crash point
    /// armed so it aborts *during recovery*, then recover cleanly and check. Covers a crash while
    /// recovery is in progress.
    PointDuringRecovery(String),
    /// Arm an I/O fault point (`eio`, `enospc`, `shortwrite`) that returns an error rather than
    /// aborting, run the workload while it fires, then SIGKILL at a seeded time and recover. Tests
    /// that a batch whose fsync or write failed is never acked and the store stays consistent.
    IoFault(String),
}

/// Static configuration for a run of many cycles.
pub struct HarnessConfig {
    pub server_bin: PathBuf,
    pub data_root: PathBuf,
    pub artifact_dir: PathBuf,
    pub seed: u64,
    pub writers: usize,
    pub segment_size: usize,
    pub workload: Workload,
    pub crash: Crash,
    /// Keep the per-cycle data dir even when the cycle passes (for debugging).
    pub keep_dirs: bool,
    /// If set, witness logs go here (one per cycle) instead of inside the per-cycle data dir. Used
    /// when the data dir is on a device under test (dm-log-writes) so the ground truth survives.
    pub witness_root: Option<PathBuf>,
}

/// A recovered server plus any ordering violations the subscriber saw before the crash.
struct RestartOutcome {
    server: ServerProcess,
    sub_violations: Vec<String>,
}

impl RestartOutcome {
    fn new(server: ServerProcess, sub_violations: Vec<String>) -> Self {
        RestartOutcome {
            server,
            sub_violations,
        }
    }
}

/// The result of one cycle.
pub struct CycleResult {
    pub cycle: u64,
    pub cycle_seed: u64,
    /// Whether the crash actually happened (a SIGKILL always does; an armed point may not be hit).
    pub crash_hit: bool,
    pub violations: Vec<String>,
    pub sent: usize,
    pub acked: usize,
    pub recovered: usize,
    pub artifact: Option<PathBuf>,
}

impl CycleResult {
    pub fn passed(&self) -> bool {
        self.violations.is_empty()
    }
}

/// One targeted crash-point scenario for Phase 2.
pub struct Phase2Site {
    pub name: &'static str,
    pub crash: Crash,
    pub segment_size: usize,
    pub writers: usize,
    pub workload: Workload,
}

/// The Phase 2 battery: one scenario per instrumented abort site, with a segment size and skip
/// count chosen so the site is actually reached (rollover/index sites use small segments; the
/// skip lets several batches commit first so recovery has real prior state to preserve).
pub fn phase2_battery() -> Vec<Phase2Site> {
    let site = |name, point: &str, segment_size, workload| Phase2Site {
        name,
        crash: Crash::Point(point.to_string()),
        segment_size,
        writers: 12,
        workload,
    };
    vec![
        site(
            "commit_before_fsync",
            "commit_before_fsync:abort:5",
            65536,
            Workload::PureAppend,
        ),
        site(
            "after_fsync_before_ack",
            "after_fsync_before_ack:abort:5",
            65536,
            Workload::PureAppend,
        ),
        site(
            "partial_ack",
            "partial_ack:abort:8",
            65536,
            Workload::PureAppend,
        ),
        site(
            "segment_created_before_commit",
            "segment_created_before_commit:abort:2",
            16384,
            Workload::PureAppend,
        ),
        site(
            "index_after_write",
            "index_after_write:abort:1",
            16384,
            Workload::PureAppend,
        ),
        site(
            "index_after_sync",
            "index_after_sync:abort:1",
            16384,
            Workload::PureAppend,
        ),
        Phase2Site {
            name: "recovery_midway",
            crash: Crash::PointDuringRecovery("recovery_midway:abort:3".to_string()),
            segment_size: 16384,
            writers: 12,
            workload: Workload::PureAppend,
        },
    ]
}

/// What a Phase 3 I/O-fault scenario expects of the acked count, on top of all invariants.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase3Expect {
    /// Every fsync fails, so nothing may be acked at all (acks are gated on fsync).
    AckedZero,
    /// The fault engages after some writes, so acks are allowed; only consistency is checked.
    AckedAny,
}

/// One Phase 3 I/O-fault scenario.
pub struct Phase3Site {
    pub name: &'static str,
    pub crash: Crash,
    pub segment_size: usize,
    pub writers: usize,
    pub workload: Workload,
    pub expect: Phase3Expect,
}

/// The Phase 3 battery: fsync `EIO`, `ENOSPC` on segment extension and index flush, and a short
/// write. Skip counts let the store initialise (segment extension and index flush use small
/// segments so rollover, and thus a later extension / an index seal, actually happen).
pub fn phase3_battery() -> Vec<Phase3Site> {
    let io = |name, point: &str, segment_size, expect| Phase3Site {
        name,
        crash: Crash::IoFault(point.to_string()),
        segment_size,
        writers: 12,
        workload: Workload::PureAppend,
        expect,
    };
    vec![
        // Every commit fsync returns EIO: no batch can become durable, so nothing may be acked.
        io(
            "fsync_eio_all",
            "commit_fsync:eio:0",
            65536,
            Phase3Expect::AckedZero,
        ),
        // fsync starts failing after a few batches: earlier acks are durable, later ones fail.
        io(
            "fsync_eio_after",
            "commit_fsync:eio:6",
            65536,
            Phase3Expect::AckedAny,
        ),
        // ENOSPC when a rollover tries to fallocate the next segment (initial create is skipped).
        io(
            "enospc_segment_extend",
            "segment_extend:enospc:1",
            16384,
            Phase3Expect::AckedAny,
        ),
        // ENOSPC writing the derived index: disposable, logged and ignored, writes still ack.
        io(
            "enospc_index_flush",
            "index_flush:enospc:0",
            16384,
            Phase3Expect::AckedAny,
        ),
        // A short write on a data record: the batch must rewind and never be acked.
        io(
            "short_write",
            "commit_shortwrite:shortwrite:6",
            65536,
            Phase3Expect::AckedAny,
        ),
    ]
}

/// Derives a cycle's seed from the run seed and cycle index (well spread, fully reproducible).
pub fn cycle_seed(seed: u64, cycle: u64) -> u64 {
    seed.wrapping_add(cycle.wrapping_mul(0x9E37_79B9_7F4A_7C15))
}

/// Locates the server binary: `TEPHRA_SERVER_BIN` if set, else `target/debug/tephra-server`
/// relative to the workspace root.
pub fn default_server_bin() -> PathBuf {
    if let Ok(path) = env::var("TEPHRA_SERVER_BIN") {
        return PathBuf::from(path);
    }
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
    let workspace = manifest.ancestors().nth(2).unwrap_or(manifest);
    workspace.join("target/debug/tephra-server")
}

/// Runs one crash cycle end to end.
pub fn run_cycle(cfg: &HarnessConfig, cycle: u64) -> io::Result<CycleResult> {
    let seed = cycle_seed(cfg.seed, cycle);

    let cycle_dir = cfg.data_root.join(format!("cycle-{cycle:06}"));
    let data_dir = cycle_dir.join("data");
    let config_path = cycle_dir.join("config.toml");
    fs::create_dir_all(&data_dir)?;
    // Keep the witness off the data device when a witness root is given, so it is not itself lost
    // to a simulated power cut on the device under test.
    let witness_path = match &cfg.witness_root {
        Some(root) => {
            fs::create_dir_all(root)?;
            root.join(format!("cycle-{cycle:06}.log"))
        }
        None => cycle_dir.join("witness.log"),
    };
    write_config(&config_path, cfg.segment_size)?;

    // Arm the point on the initial boot for an operational crash or an I/O fault; a recovery crash
    // boots clean first and arms the point on a later restart.
    let initial_point = match &cfg.crash {
        Crash::Point(point) | Crash::IoFault(point) => Some(point.as_str()),
        Crash::SigkillTimed | Crash::PointDuringRecovery(_) => None,
    };

    // Spawn, drive, crash.
    let mut server = ServerProcess::spawn(&cfg.server_bin, &data_dir, &config_path, initial_point)?;
    let witness = Arc::new(Witness::create(&witness_path, seed)?);
    let running = driver::start(
        server.addr,
        Arc::clone(&witness),
        seed,
        cfg.writers,
        cfg.workload,
    );

    let (crash_hit, server) = match &cfg.crash {
        Crash::SigkillTimed | Crash::IoFault(_) => {
            let delay = 10 + (seed % 391);
            thread::sleep(Duration::from_millis(delay));
            server.kill()?;
            let sub = running.stop_and_join();
            (true, RestartOutcome::new(server.restart_clean()?, sub))
        }
        Crash::Point(_) => {
            let hit = wait_for_point(&mut server);
            let sub = running.stop_and_join();
            (hit, RestartOutcome::new(server.restart_clean()?, sub))
        }
        Crash::PointDuringRecovery(point) => {
            // First leave committed data on disk with a timed SIGKILL.
            let delay = 10 + (seed % 391);
            thread::sleep(Duration::from_millis(delay));
            server.kill()?;
            let sub = running.stop_and_join();
            drop(server);
            // Restart with the point armed: it aborts inside recovery (before it can listen).
            let aborted = ServerProcess::run_until_abort(
                &cfg.server_bin,
                &data_dir,
                &config_path,
                point,
                Duration::from_secs(20),
            )?;
            // Now recover cleanly.
            let clean = ServerProcess::spawn(&cfg.server_bin, &data_dir, &config_path, None)?;
            (aborted, RestartOutcome::new(clean, sub))
        }
    };

    let RestartOutcome {
        server,
        mut sub_violations,
    } = server;
    let ground = Ground::read(&witness_path)?;
    let (server, mut outcome) = invariants::check(server, &ground)?;
    outcome.violations.append(&mut sub_violations);

    // Artifacts on failure; otherwise clean up unless asked to keep.
    let artifact = if outcome.violations.is_empty() {
        drop(server);
        if !cfg.keep_dirs {
            let _ = fs::remove_dir_all(&cycle_dir);
        }
        None
    } else {
        let dest = cfg
            .artifact_dir
            .join(format!("seed-{}-cycle-{cycle}", cfg.seed));
        fs::create_dir_all(&dest)?;
        copy_dir_all(&cycle_dir, &dest.join("store"))?;
        fs::write(dest.join("stderr.txt"), server.stderr_tail().join("\n"))?;
        fs::write(
            dest.join("violations.txt"),
            format!(
                "run_seed={} cycle={} cycle_seed={} crash={:?}\ncrash_hit={}\n\n{}",
                cfg.seed,
                cycle,
                seed,
                cfg.crash,
                crash_hit,
                outcome.violations.join("\n"),
            ),
        )?;
        drop(server);
        Some(dest)
    };

    Ok(CycleResult {
        cycle,
        cycle_seed: seed,
        crash_hit,
        violations: outcome.violations,
        sent: outcome.sent,
        acked: outcome.acked,
        recovered: outcome.recovered,
        artifact,
    })
}

/// Runs a single cycle's workload and SIGKILLs the server, then stops, leaving the store on disk
/// and the witness written. It does not restart or check, so an external driver (the dm-flakey
/// power-loss script) can simulate a power cut on the device and then verify with `spawn_and_verify`.
/// Returns the acked count.
pub fn run_workload_only(
    cfg: &HarnessConfig,
    cycle: u64,
    workload_ms: Option<u64>,
) -> io::Result<usize> {
    let seed = cycle_seed(cfg.seed, cycle);
    let cycle_dir = cfg.data_root.join(format!("cycle-{cycle:06}"));
    let data_dir = cycle_dir.join("data");
    let config_path = cycle_dir.join("config.toml");
    fs::create_dir_all(&data_dir)?;
    let witness_path = match &cfg.witness_root {
        Some(root) => {
            fs::create_dir_all(root)?;
            root.join(format!("cycle-{cycle:06}.log"))
        }
        None => cycle_dir.join("witness.log"),
    };
    write_config(&config_path, cfg.segment_size)?;

    let mut server = ServerProcess::spawn(&cfg.server_bin, &data_dir, &config_path, None)?;
    let witness = Arc::new(Witness::create(&witness_path, seed)?);
    let running = driver::start(
        server.addr,
        Arc::clone(&witness),
        seed,
        cfg.writers,
        cfg.workload,
    );
    // A fixed duration when given (so a power-loss run can guarantee enough events to roll over),
    // otherwise the seeded delay.
    let delay = workload_ms.unwrap_or(10 + (seed % 391));
    thread::sleep(Duration::from_millis(delay));
    server.kill()?;
    running.stop_and_join();
    drop(server);
    Ok(Ground::read(&witness_path)?.acked.len())
}

/// Opens an existing store read-only-style (a normal boot, then only reads) and runs the
/// read-only invariants against `witness_path`. Used by the dm-log-writes replay to check a
/// recovered store at a flush boundary. The data dir must be writable (the server opens it
/// read-write to recover), so the caller mounts the replayed device read-write.
pub fn spawn_and_verify(
    server_bin: &Path,
    data_dir: &Path,
    segment_size: usize,
    witness_path: &Path,
) -> io::Result<Vec<String>> {
    let config_path = env::temp_dir().join(format!("tephra-verify-{}.toml", process::id()));
    write_config(&config_path, segment_size)?;
    let server = ServerProcess::spawn(server_bin, data_dir, &config_path, None)?;
    let ground = Ground::read(witness_path)?;
    let (violations, recovered) = invariants::verify_readonly(server.addr, &ground);
    println!(
        "verify: acked={} present, recovered={} total events, violations={}",
        ground.acked.len(),
        recovered,
        violations.len()
    );
    drop(server);
    let _ = fs::remove_file(&config_path);
    Ok(violations)
}

/// Waits for an armed crash point to abort the server. Returns whether it did within the budget;
/// if it never fires, the server is killed so the cycle can still restart and check.
fn wait_for_point(server: &mut ServerProcess) -> bool {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        match server.wait_for_exit(Duration::from_millis(20)) {
            Ok(ExitKind::Abnormal) => return true,
            Ok(ExitKind::Clean) => return false,
            Ok(ExitKind::StillRunning) if Instant::now() >= deadline => {
                let _ = server.kill();
                return false;
            }
            Ok(ExitKind::StillRunning) => {}
            Err(_) => return false,
        }
    }
}

fn write_config(path: &Path, segment_size: usize) -> io::Result<()> {
    // Only the segment size is set here; bind and data_dir come from the command line, everything
    // else stays at the server's defaults.
    fs::write(path, format!("[segment]\nsize = {segment_size}\n"))
}

fn copy_dir_all(src: &Path, dst: &Path) -> io::Result<()> {
    fs::create_dir_all(dst)?;
    for entry in fs::read_dir(src)? {
        let entry = entry?;
        let ty = entry.file_type()?;
        let to = dst.join(entry.file_name());
        if ty.is_dir() {
            copy_dir_all(&entry.path(), &to)?;
        } else {
            fs::copy(entry.path(), to)?;
        }
    }
    Ok(())
}
