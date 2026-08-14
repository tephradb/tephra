//! Driver binary for the Tephra crash-consistency suite.
//!
//! Runs many crash cycles, printing a per-cycle line and a final summary. Any invariant
//! violation prints the seed and cycle to reproduce it and copies the store to the artifact dir.

use std::collections::BTreeMap;
use std::env;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::Instant;

use argh::FromArgs;
use tephra_crashtest::{Crash, HarnessConfig, Workload, default_server_bin, run_cycle};

/// Crash-consistency and fault-injection harness for Tephra.
#[derive(FromArgs)]
struct Args {
    /// run seed (cycle seeds derive from this); default 1
    #[argh(option, default = "1")]
    seed: u64,

    /// number of crash cycles to run; default 100
    #[argh(option, default = "100")]
    cycles: u64,

    /// concurrent writer threads; default 8
    #[argh(option, default = "8")]
    writers: usize,

    /// segment size in bytes (small values force frequent rollover); default 65536
    #[argh(option, default = "65536")]
    segment_size: usize,

    /// workload: pure | conditional | subscription | mixed; default pure
    #[argh(option, default = "String::from(\"pure\")")]
    workload: String,

    /// arm a crash point (TEPHRA_CRASH_POINT value, e.g. "after_fsync_before_ack:abort") instead
    /// of a timed SIGKILL
    #[argh(option)]
    crash_point: Option<String>,

    /// root directory for per-cycle data dirs; default $TMPDIR/tephra-crashtest
    #[argh(option)]
    data_root: Option<String>,

    /// directory for failure artifacts; default ./crashtest-artifacts
    #[argh(option, default = "String::from(\"crashtest-artifacts\")")]
    artifact_dir: String,

    /// path to the tephra-server binary; default $TEPHRA_SERVER_BIN or target/debug/tephra-server
    #[argh(option)]
    server_bin: Option<String>,

    /// keep per-cycle data dirs even when the cycle passes
    #[argh(switch)]
    keep_dirs: bool,

    /// stop at the first failing cycle
    #[argh(switch)]
    stop_on_fail: bool,

    /// run the Phase 2 battery: one targeted abort per instrumented crash site
    #[argh(switch)]
    phase2: bool,

    /// cycles per site in the Phase 2 battery; default 6
    #[argh(option, default = "6")]
    phase2_cycles: u64,

    /// run the Phase 3 battery: fsync EIO, ENOSPC, and short-write injection
    #[argh(switch)]
    phase3: bool,

    /// cycles per site in the Phase 3 battery; default 6
    #[argh(option, default = "6")]
    phase3_cycles: u64,

    /// place witness logs here instead of inside the data dir (for the dm-log-writes power-loss
    /// test, where the data dir is on the device under test)
    #[argh(option)]
    witness_root: Option<String>,

    /// verify an existing store: open the data dir at this path and run the read-only invariants
    /// against --witness, then exit (used by the dm-log-writes replay)
    #[argh(option)]
    verify_dir: Option<String>,

    /// the witness log to verify against in --verify-dir mode
    #[argh(option)]
    witness: Option<String>,

    /// run one cycle's workload then SIGKILL and stop (no restart or check); leaves the store on
    /// disk for an external power-loss driver
    #[argh(switch)]
    workload_only: bool,

    /// workload duration in ms for --workload-only (overrides the seeded delay); use a value large
    /// enough to force a segment rollover
    #[argh(option)]
    workload_ms: Option<u64>,
}

fn main() -> ExitCode {
    let args: Args = argh::from_env();

    let workload: Workload = match args.workload.parse() {
        Ok(w) => w,
        Err(err) => {
            eprintln!("error: {err}");
            return ExitCode::FAILURE;
        }
    };

    let data_root = args
        .data_root
        .map(PathBuf::from)
        .unwrap_or_else(|| env::temp_dir().join("tephra-crashtest"));

    let crash = match &args.crash_point {
        Some(point) => Crash::Point(point.clone()),
        None => Crash::SigkillTimed,
    };

    let cfg = HarnessConfig {
        server_bin: args
            .server_bin
            .map(PathBuf::from)
            .unwrap_or_else(default_server_bin),
        data_root,
        artifact_dir: PathBuf::from(&args.artifact_dir),
        seed: args.seed,
        writers: args.writers,
        segment_size: args.segment_size,
        workload,
        crash,
        keep_dirs: args.keep_dirs,
        witness_root: args.witness_root.map(PathBuf::from),
    };

    if !cfg.server_bin.exists() {
        eprintln!(
            "error: server binary not found at {}\nbuild it first (cargo build -p tephra-server [--features crash-points]) or pass --server-bin",
            cfg.server_bin.display()
        );
        return ExitCode::FAILURE;
    }

    if let Some(dir) = &args.verify_dir {
        let Some(witness) = &args.witness else {
            eprintln!("error: --verify-dir requires --witness");
            return ExitCode::FAILURE;
        };
        return match tephra_crashtest::spawn_and_verify(
            &cfg.server_bin,
            Path::new(dir),
            cfg.segment_size,
            Path::new(witness),
        ) {
            Ok(v) if v.is_empty() => {
                println!("verify_dir {dir}: ok");
                ExitCode::SUCCESS
            }
            Ok(v) => {
                println!("verify_dir {dir}: {} violations", v.len());
                for line in v.iter().take(20) {
                    println!("  - {line}");
                }
                ExitCode::FAILURE
            }
            Err(err) => {
                eprintln!("verify_dir {dir}: error: {err}");
                ExitCode::FAILURE
            }
        };
    }

    if args.workload_only {
        return match tephra_crashtest::run_workload_only(&cfg, 0, args.workload_ms) {
            Ok(acked) => {
                println!("workload_only: acked={acked}");
                ExitCode::SUCCESS
            }
            Err(err) => {
                eprintln!("workload_only: error: {err}");
                ExitCode::FAILURE
            }
        };
    }

    if args.phase2 {
        return run_phase2(&cfg, args.phase2_cycles);
    }
    if args.phase3 {
        return run_phase3(&cfg, args.phase3_cycles);
    }

    println!(
        "crashtest: seed={} cycles={} writers={} workload={:?} crash={:?} segment_size={}",
        cfg.seed, args.cycles, cfg.writers, cfg.workload, cfg.crash, cfg.segment_size
    );

    let start = Instant::now();
    let mut failures = 0u64;
    let mut hits = 0u64;
    let mut total_acked = 0usize;
    let mut violation_counts: BTreeMap<String, u64> = BTreeMap::new();

    for cycle in 0..args.cycles {
        match run_cycle(&cfg, cycle) {
            Ok(result) => {
                total_acked += result.acked;
                if result.crash_hit {
                    hits += 1;
                }
                if result.passed() {
                    if cycle % 25 == 0 || args.cycles < 20 {
                        println!(
                            "cycle {cycle}: ok (acked={}, recovered={}, crash_hit={})",
                            result.acked, result.recovered, result.crash_hit
                        );
                    }
                } else {
                    failures += 1;
                    for line in &result.violations {
                        let key = line.split(':').next().unwrap_or(line).to_string();
                        *violation_counts.entry(key).or_default() += 1;
                    }
                    println!(
                        "cycle {cycle}: FAIL seed={} cycle_seed={} ({} violations) artifact={}",
                        cfg.seed,
                        result.cycle_seed,
                        result.violations.len(),
                        result
                            .artifact
                            .as_ref()
                            .map(|p| p.display().to_string())
                            .unwrap_or_default()
                    );
                    for line in result.violations.iter().take(10) {
                        println!("    - {line}");
                    }
                    if args.stop_on_fail {
                        break;
                    }
                }
            }
            Err(err) => {
                eprintln!("cycle {cycle}: harness error: {err}");
                failures += 1;
                if args.stop_on_fail {
                    break;
                }
            }
        }
    }

    let elapsed = start.elapsed();
    let rate = if elapsed.as_secs_f64() > 0.0 {
        args.cycles as f64 / elapsed.as_secs_f64() * 3600.0
    } else {
        0.0
    };
    println!(
        "\nsummary: {} cycles in {:.1}s ({:.0}/hour), crash_hit={}, acked_total={}, failures={}",
        args.cycles,
        elapsed.as_secs_f64(),
        rate,
        hits,
        total_acked,
        failures
    );
    if !violation_counts.is_empty() {
        println!("violation breakdown:");
        for (kind, count) in &violation_counts {
            println!("  {count:>5}  {kind}");
        }
    }

    if failures == 0 {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}

/// Runs the Phase 2 battery: each instrumented abort site gets its own scenario, driven until the
/// site fires, restarted, and checked against all invariants.
fn run_phase2(base: &tephra_crashtest::HarnessConfig, cycles_per_site: u64) -> ExitCode {
    use tephra_crashtest::{HarnessConfig, phase2_battery, run_cycle};

    println!(
        "phase 2: targeted crash points, seed={} cycles_per_site={}\n",
        base.seed, cycles_per_site
    );
    println!(
        "{:<32} {:>7} {:>5} {:>10} {:>11}",
        "site", "cycles", "hits", "violations", "first_hit"
    );

    let start = Instant::now();
    let mut total_failures = 0u64;
    let mut total_cycles = 0u64;

    for site in phase2_battery() {
        let cfg = HarnessConfig {
            server_bin: base.server_bin.clone(),
            data_root: base.data_root.join(site.name),
            artifact_dir: base.artifact_dir.clone(),
            seed: base.seed,
            writers: site.writers,
            segment_size: site.segment_size,
            workload: site.workload,
            crash: site.crash.clone(),
            keep_dirs: base.keep_dirs,
            witness_root: base.witness_root.clone(),
        };

        let mut hits = 0u64;
        let mut violations = 0u64;
        let mut first_hit: Option<u64> = None;
        let mut messages: Vec<(u64, Vec<String>, Option<PathBuf>)> = Vec::new();

        for cycle in 0..cycles_per_site {
            total_cycles += 1;
            match run_cycle(&cfg, cycle) {
                Ok(result) => {
                    if result.crash_hit {
                        hits += 1;
                        first_hit.get_or_insert(cycle);
                    }
                    if !result.passed() {
                        violations += 1;
                        messages.push((cycle, result.violations, result.artifact));
                    }
                }
                Err(err) => {
                    violations += 1;
                    messages.push((cycle, vec![format!("harness error: {err}")], None));
                }
            }
        }

        total_failures += violations;
        println!(
            "{:<32} {:>7} {:>5} {:>10} {:>11}",
            site.name,
            cycles_per_site,
            hits,
            violations,
            first_hit
                .map(|c| c.to_string())
                .unwrap_or_else(|| "never".to_string()),
        );
        for (cycle, lines, artifact) in messages {
            println!(
                "    cycle {cycle} FAIL (seed {}) artifact={}",
                base.seed,
                artifact
                    .map(|p| p.display().to_string())
                    .unwrap_or_default()
            );
            for line in lines.iter().take(8) {
                println!("      - {line}");
            }
        }
    }

    println!(
        "\nphase 2 summary: {} scenarios, {} cycles in {:.1}s, failures={}",
        phase2_battery().len(),
        total_cycles,
        start.elapsed().as_secs_f64(),
        total_failures
    );
    if total_failures == 0 {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}

/// Runs the Phase 3 battery: fsync EIO, ENOSPC on segment extension and index flush, and a short
/// write. Each scenario runs the fault while writing, then SIGKILLs and recovers, checking all
/// invariants plus the fault-specific expectation on the acked count.
fn run_phase3(base: &tephra_crashtest::HarnessConfig, cycles_per_site: u64) -> ExitCode {
    use tephra_crashtest::{HarnessConfig, Phase3Expect, phase3_battery, run_cycle};

    println!(
        "phase 3: lying storage, seed={} cycles_per_site={}\n",
        base.seed, cycles_per_site
    );
    println!(
        "{:<24} {:>7} {:>11} {:>13} {:>11}",
        "fault", "cycles", "acked_sum", "expectation", "violations"
    );

    let start = Instant::now();
    let mut total_failures = 0u64;
    let mut total_cycles = 0u64;

    for site in phase3_battery() {
        let cfg = HarnessConfig {
            server_bin: base.server_bin.clone(),
            data_root: base.data_root.join(site.name),
            artifact_dir: base.artifact_dir.clone(),
            seed: base.seed,
            writers: site.writers,
            segment_size: site.segment_size,
            workload: site.workload,
            crash: site.crash.clone(),
            keep_dirs: base.keep_dirs,
            witness_root: base.witness_root.clone(),
        };

        let mut violations = 0u64;
        let mut acked_sum = 0usize;
        let mut messages: Vec<(u64, Vec<String>)> = Vec::new();

        for cycle in 0..cycles_per_site {
            total_cycles += 1;
            match run_cycle(&cfg, cycle) {
                Ok(result) => {
                    acked_sum += result.acked;
                    let mut lines = result.violations;
                    // The fault-specific expectation on top of the invariants. AckedZero is only
                    // meaningful if writes were actually attempted: sent == 0 means the fault never
                    // engaged (for example a kill before any fsync), so acked == 0 proves nothing.
                    if site.expect == Phase3Expect::AckedZero {
                        if result.sent == 0 {
                            lines.push(
                                "inconclusive: no writes were attempted, so a total fsync failure was not exercised".to_string(),
                            );
                        } else if result.acked != 0 {
                            lines.push(format!(
                                "expected zero acks under a total fsync failure, but {} of {} sent were acked",
                                result.acked, result.sent
                            ));
                        }
                    }
                    if !lines.is_empty() {
                        violations += 1;
                        messages.push((cycle, lines));
                    }
                }
                Err(err) => {
                    violations += 1;
                    messages.push((cycle, vec![format!("harness error: {err}")]));
                }
            }
        }

        total_failures += violations;
        let expectation = match site.expect {
            Phase3Expect::AckedZero => "acked==0",
            Phase3Expect::AckedAny => "consistent",
        };
        println!(
            "{:<24} {:>7} {:>11} {:>13} {:>11}",
            site.name, cycles_per_site, acked_sum, expectation, violations
        );
        for (cycle, lines) in messages {
            println!("    cycle {cycle} FAIL (seed {})", base.seed);
            for line in lines.iter().take(8) {
                println!("      - {line}");
            }
        }
    }

    println!(
        "\nphase 3 summary: {} scenarios, {} cycles in {:.1}s, failures={}",
        phase3_battery().len(),
        total_cycles,
        start.elapsed().as_secs_f64(),
        total_failures
    );
    if total_failures == 0 {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}
