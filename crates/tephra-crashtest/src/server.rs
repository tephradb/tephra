//! Spawns and controls the Tephra server as a real child process.
//!
//! A child process (not an in-process thread) is required so that `SIGKILL` and a
//! self-`abort` from a crash point are genuine process deaths with no chance to flush or run
//! destructors. Readiness is taken from the server's own `tephra server listening` log line on
//! stderr, so there is no fixed sleep: the harness proceeds the instant the socket is up, which
//! is what keeps the crash-cycle rate high.

use std::io::{self, BufRead, BufReader};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, RecvTimeoutError};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

/// How long to wait for the server to log that it is listening before giving up.
const READY_TIMEOUT: Duration = Duration::from_secs(30);
/// How many recent stderr lines to keep for a failure artifact.
const STDERR_TAIL: usize = 200;

/// A running server child, its bound address, and a background stderr drain.
pub struct ServerProcess {
    child: Child,
    pub addr: SocketAddr,
    stderr: Arc<Mutex<Vec<String>>>,
    bin: PathBuf,
    data_dir: PathBuf,
    config: PathBuf,
}

impl ServerProcess {
    /// Spawns the server on `data_dir` with an ephemeral port, optionally arming a crash point
    /// (`TEPHRA_CRASH_POINT` value). Blocks until the listening line is seen.
    pub fn spawn(
        bin: &Path,
        data_dir: &Path,
        config: &Path,
        crash_point: Option<&str>,
    ) -> io::Result<ServerProcess> {
        let mut cmd = Command::new(bin);
        cmd.arg("--data-dir")
            .arg(data_dir)
            .arg("--bind")
            .arg("127.0.0.1:0")
            .arg("--config")
            .arg(config)
            .arg("--log")
            .arg("info")
            // Keep logs free of ANSI colour so artifacts are readable (the parser also strips it).
            .env("NO_COLOR", "1")
            // Pipe both streams and scan both for the listening line: the tracing subscriber
            // writes to stdout by default, but a panic goes to stderr, and we want both in the
            // failure tail.
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        match crash_point {
            Some(point) => {
                cmd.env("TEPHRA_CRASH_POINT", point);
            }
            None => {
                cmd.env_remove("TEPHRA_CRASH_POINT");
            }
        }

        let mut child = cmd.spawn()?;
        let stdout = child.stdout.take().expect("stdout piped");
        let stderr = child.stderr.take().expect("stderr piped");
        let tail = Arc::new(Mutex::new(Vec::new()));
        let (ready_tx, ready_rx) = mpsc::channel::<SocketAddr>();
        let ready_tx = Arc::new(Mutex::new(Some(ready_tx)));

        // A reader thread per stream: each reports the bound address if it sees the listening
        // line, keeps a shared rolling tail for artifacts, and drains the rest so a chatty server
        // never blocks on a full pipe.
        for stream in [
            Box::new(stdout) as Box<dyn std::io::Read + Send>,
            Box::new(stderr),
        ] {
            let tail = Arc::clone(&tail);
            let ready_tx = Arc::clone(&ready_tx);
            thread::spawn(move || {
                for line in BufReader::new(stream).lines() {
                    let Ok(line) = line else { break };
                    if let Some(addr) = parse_listening(&line)
                        && let Some(tx) = ready_tx.lock().expect("ready tx").take()
                    {
                        let _ = tx.send(addr);
                    }
                    let mut tail = tail.lock().expect("stderr tail mutex");
                    if tail.len() == STDERR_TAIL {
                        tail.remove(0);
                    }
                    tail.push(line);
                }
            });
        }

        // Wait for the listening line, but fail fast if the child exits first (for example a
        // store that refuses to open after a crash), rather than blocking for the full timeout.
        let deadline = Instant::now() + READY_TIMEOUT;
        let addr = loop {
            match ready_rx.recv_timeout(Duration::from_millis(100)) {
                Ok(addr) => break addr,
                Err(RecvTimeoutError::Disconnected) => {
                    return Err(exited_before_listening(&mut child, &tail));
                }
                Err(RecvTimeoutError::Timeout) => {
                    if child.try_wait()?.is_some() {
                        return Err(exited_before_listening(&mut child, &tail));
                    }
                    if Instant::now() >= deadline {
                        let _ = child.kill();
                        let _ = child.wait();
                        return Err(io::Error::new(
                            io::ErrorKind::TimedOut,
                            format!(
                                "server did not report listening within {READY_TIMEOUT:?}; output tail:\n{}",
                                tail.lock().expect("stderr tail mutex").join("\n")
                            ),
                        ));
                    }
                }
            }
        };

        Ok(ServerProcess {
            child,
            addr,
            stderr: tail,
            bin: bin.to_path_buf(),
            data_dir: data_dir.to_path_buf(),
            config: config.to_path_buf(),
        })
    }

    /// Launches the server with a crash point armed and waits for it to abort during recovery,
    /// which happens inside `SegmentSet::open`, before it ever binds. Returns whether it exited
    /// abnormally (the point fired). If the point is not reached, recovery finishes and the server
    /// starts listening; that is detected from the log and reported as "did not fire" promptly,
    /// rather than blocking out the whole timeout.
    pub fn run_until_abort(
        bin: &Path,
        data_dir: &Path,
        config: &Path,
        point: &str,
        timeout: Duration,
    ) -> io::Result<bool> {
        let mut child = Command::new(bin)
            .arg("--data-dir")
            .arg(data_dir)
            .arg("--bind")
            .arg("127.0.0.1:0")
            .arg("--config")
            .arg(config)
            .arg("--log")
            .arg("info")
            .env("NO_COLOR", "1")
            .env("TEPHRA_CRASH_POINT", point)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;

        // Watch both streams for the listening line: seeing it means recovery finished without the
        // point firing, so this cycle exercised no crash during recovery.
        let listening = Arc::new(AtomicBool::new(false));
        for stream in [
            child
                .stdout
                .take()
                .map(|s| Box::new(s) as Box<dyn std::io::Read + Send>),
            child
                .stderr
                .take()
                .map(|s| Box::new(s) as Box<dyn std::io::Read + Send>),
        ]
        .into_iter()
        .flatten()
        {
            let listening = Arc::clone(&listening);
            thread::spawn(move || {
                for line in BufReader::new(stream).lines() {
                    let Ok(line) = line else { break };
                    if line.contains("listening") {
                        listening.store(true, Ordering::SeqCst);
                    }
                }
            });
        }

        let deadline = std::time::Instant::now() + timeout;
        loop {
            if let Some(status) = child.try_wait()? {
                return Ok(!status.success());
            }
            if listening.load(Ordering::SeqCst) || std::time::Instant::now() >= deadline {
                let _ = child.kill();
                let _ = child.wait();
                return Ok(false);
            }
            thread::sleep(Duration::from_millis(2));
        }
    }

    /// Sends `SIGKILL` (via `Child::kill` on Unix) and reaps the child. Nothing graceful.
    pub fn kill(&mut self) -> io::Result<()> {
        self.child.kill()?;
        self.child.wait()?;
        Ok(())
    }

    /// Waits for the child to exit on its own (used when a crash point aborts it) and returns
    /// whether it exited abnormally (killed or aborted by signal).
    pub fn wait_for_exit(&mut self, timeout: Duration) -> io::Result<ExitKind> {
        let deadline = std::time::Instant::now() + timeout;
        loop {
            match self.child.try_wait()? {
                Some(status) => {
                    return Ok(if status.success() {
                        ExitKind::Clean
                    } else {
                        ExitKind::Abnormal
                    });
                }
                None if std::time::Instant::now() >= deadline => return Ok(ExitKind::StillRunning),
                None => thread::sleep(Duration::from_millis(2)),
            }
        }
    }

    /// Restarts the server on the same data dir with no crash point (a clean recovery boot).
    pub fn restart_clean(self) -> io::Result<ServerProcess> {
        let (bin, data_dir, config) =
            (self.bin.clone(), self.data_dir.clone(), self.config.clone());
        // Dropping the old handle kills and reaps its child, so the data dir is free to reopen.
        drop(self);
        ServerProcess::spawn(&bin, &data_dir, &config, None)
    }

    /// The recent stderr lines, for a failure artifact.
    pub fn stderr_tail(&self) -> Vec<String> {
        self.stderr.lock().expect("stderr tail mutex").clone()
    }

    /// The data directory this server was opened on.
    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    /// Kills the child, deletes the derived index directory, and restarts on the same log so the
    /// index is rebuilt from the log. Used by the index-vs-log invariant.
    pub fn restart_rebuilding_index(self) -> io::Result<ServerProcess> {
        let (bin, data_dir, config) =
            (self.bin.clone(), self.data_dir.clone(), self.config.clone());
        // Dropping the old handle kills and reaps its child before we touch the index dir.
        drop(self);
        let index_dir = data_dir.join("index");
        if index_dir.exists() {
            std::fs::remove_dir_all(&index_dir)?;
        }
        ServerProcess::spawn(&bin, &data_dir, &config, None)
    }
}

impl Drop for ServerProcess {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// How a child process ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExitKind {
    Clean,
    Abnormal,
    StillRunning,
}

/// Extracts the bound address from the listening line. ANSI colour codes can split the
/// `addr=127.0.0.1:PORT` field, so strip them first, then take the first whitespace token that
/// parses as a socket address.
fn parse_listening(line: &str) -> Option<SocketAddr> {
    let clean = strip_ansi(line);
    if !clean.contains("listening") {
        return None;
    }
    clean
        .split(|c: char| c.is_whitespace() || c == '=')
        .find_map(|token| token.parse::<SocketAddr>().ok())
}

/// Builds an error for a child that exited before it ever logged the listening line, reaping it
/// and attaching the captured output tail. This is how a store that refuses to open after a crash
/// surfaces, quickly and with the reason, instead of a slow timeout.
fn exited_before_listening(child: &mut Child, tail: &Arc<Mutex<Vec<String>>>) -> io::Error {
    let status = child.wait();
    io::Error::other(format!(
        "server exited before listening ({status:?}); output tail:\n{}",
        tail.lock().expect("stderr tail mutex").join("\n")
    ))
}

/// Removes `ESC [ ... m` colour sequences from a line.
fn strip_ansi(line: &str) -> String {
    let mut out = String::with_capacity(line.len());
    let mut chars = line.chars();
    while let Some(c) = chars.next() {
        if c == '\u{1b}' {
            for skip in chars.by_ref() {
                if skip == 'm' {
                    break;
                }
            }
        } else {
            out.push(c);
        }
    }
    out
}
