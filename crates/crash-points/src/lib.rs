//! Gated crash and fault injection points.
//!
//! A named site in production code becomes a crash or a fault when this crate's `enabled`
//! feature is on and the `TEPHRA_CRASH_POINT` environment variable selects it. With the
//! feature off, every macro below expands to nothing: no branch, no symbol, no cost. That is
//! what lets the same instrumented binary ship in release and drive the crash suite in test.
//!
//! ## Environment format
//!
//! ```text
//! TEPHRA_CRASH_POINT=<site>:<action>[:<skip>]
//! ```
//!
//! - `site` is the string passed to [`crash_point!`](crate::crash_point) or [`crash_io!`](crate::crash_io).
//! - `action` is one of `abort`, `eio`, `enospc`, `shortwrite`.
//! - `skip` (default 0) is how many hits of that site to let pass before firing, so a narrow
//!   window can be targeted deterministically (the writer thread hits these sites in a fixed
//!   order, so the count is stable given a seed apart from thread scheduling above it).
//!
//! `abort` fires through [`crash_point!`](crate::crash_point) (a hard [`std::process::abort`], no unwinding, no
//! flush, the closest in-process analogue to a power cut at that line). `eio`, `enospc`, and
//! `shortwrite` fire through [`crash_io!`](crate::crash_io), which returns the corresponding [`std::io::Error`]
//! from the enclosing function so the real error path is exercised.

#[cfg(feature = "enabled")]
mod imp {
    use std::io;
    use std::sync::OnceLock;
    use std::sync::atomic::{AtomicU64, Ordering};

    /// What a configured site does when it fires.
    #[derive(Clone, Copy, PartialEq, Eq)]
    pub(crate) enum Action {
        Abort,
        Eio,
        Enospc,
        ShortWrite,
    }

    struct Plan {
        site: &'static str,
        action: Action,
        skip: u64,
    }

    // Leak the site string so it can be `&'static` and compared cheaply. Parsed once.
    fn plan() -> Option<&'static Plan> {
        static PLAN: OnceLock<Option<Plan>> = OnceLock::new();
        PLAN.get_or_init(|| {
            let raw = std::env::var("TEPHRA_CRASH_POINT").ok()?;
            let mut parts = raw.splitn(3, ':');
            let site = parts.next()?.trim();
            if site.is_empty() {
                return None;
            }
            let action = match parts.next()?.trim() {
                "abort" => Action::Abort,
                "eio" => Action::Eio,
                "enospc" => Action::Enospc,
                "shortwrite" => Action::ShortWrite,
                other => panic!("unknown crash-point action {other:?}"),
            };
            let skip = match parts.next() {
                Some(count) => count
                    .trim()
                    .parse()
                    .unwrap_or_else(|_| panic!("crash-point skip count must be a u64")),
                None => 0,
            };
            Some(Plan {
                site: Box::leak(site.to_owned().into_boxed_str()),
                action,
                skip,
            })
        })
        .as_ref()
    }

    static HITS: AtomicU64 = AtomicU64::new(0);

    /// Returns true when this call is the one that should fire (the `skip`-th hit onwards).
    fn should_fire(plan: &Plan, site: &str) -> bool {
        if plan.site != site {
            return false;
        }
        HITS.fetch_add(1, Ordering::SeqCst) >= plan.skip
    }

    /// Aborts the process if `site` is configured with the `abort` action. Called by
    /// [`crash_point!`](crate::crash_point).
    #[inline]
    pub fn fire(site: &str) {
        if let Some(plan) = plan()
            && plan.action == Action::Abort
            && should_fire(plan, site)
        {
            // A hard abort: no destructors, no buffered flush, so the on-disk state is
            // exactly what had reached the kernel at this line, which is the point.
            std::process::abort();
        }
    }

    /// Returns true if `site` is configured with the `abort` action and this call is the one that
    /// should fire. Lets a call site do something custom (for example write a torn record) and
    /// then abort, rather than the plain abort of [`crash_point!`](crate::crash_point).
    #[inline]
    pub fn armed(site: &str) -> bool {
        match plan() {
            Some(p) if p.action == Action::Abort => should_fire(p, site),
            _ => false,
        }
    }

    /// Returns an injected error if `site` is configured with an I/O fault action. Called by
    /// [`crash_io!`](crate::crash_io).
    #[inline]
    pub fn io_fault(site: &str) -> Option<io::Error> {
        let plan = plan()?;
        let kind = match plan.action {
            Action::Eio | Action::Enospc | Action::ShortWrite => plan.action,
            Action::Abort => return None,
        };
        if !should_fire(plan, site) {
            return None;
        }
        Some(match kind {
            Action::Eio => io::Error::from_raw_os_error(libc_eio()),
            Action::Enospc => io::Error::from_raw_os_error(libc_enospc()),
            // A short write surfaces as a write that made no progress. Modelled as an error so
            // the batch fails and rewinds; a genuine partial record on disk is covered instead
            // by an abort in the fsync window and by the dm-log-writes replay.
            Action::ShortWrite => io::Error::new(io::ErrorKind::WriteZero, "injected short write"),
            Action::Abort => unreachable!(),
        })
    }

    // The raw errnos, without pulling in the `libc` crate for two integers.
    fn libc_eio() -> i32 {
        5
    }
    fn libc_enospc() -> i32 {
        28
    }
}

#[cfg(feature = "enabled")]
pub use imp::{armed, fire, io_fault};

/// Aborts the process at `site` when configured with the `abort` action.
///
/// Expands to nothing (and generates no code) when the crate's `enabled` feature is off.
#[cfg(feature = "enabled")]
#[macro_export]
macro_rules! crash_point {
    ($site:literal) => {
        $crate::fire($site)
    };
}

/// See [`crash_point!`](crate::crash_point). This is the compiled-out form used when the feature is off.
#[cfg(not(feature = "enabled"))]
#[macro_export]
macro_rules! crash_point {
    ($site:literal) => {{}};
}

/// Returns an injected I/O error from the enclosing function when `site` is configured with an
/// `eio`, `enospc`, or `shortwrite` action. The enclosing function's error type must implement
/// `From<std::io::Error>`.
///
/// Expands to nothing when the crate's `enabled` feature is off.
#[cfg(feature = "enabled")]
#[macro_export]
macro_rules! crash_io {
    ($site:literal) => {
        if let ::core::option::Option::Some(err) = $crate::io_fault($site) {
            return ::core::result::Result::Err(::core::convert::From::from(err));
        }
    };
}

/// See [`crash_io!`](crate::crash_io). This is the compiled-out form used when the feature is off.
#[cfg(not(feature = "enabled"))]
#[macro_export]
macro_rules! crash_io {
    ($site:literal) => {{}};
}
