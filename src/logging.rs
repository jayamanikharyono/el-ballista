//! Process-wide logging setup.
//!
//! The crate emits `tracing` events with fields, inside spans that carry their context: `run`
//! (`job_id`, `run_id`) > `split` (`split_id`) > `scan` (`partition`). This module installs a
//! `tracing-subscriber` so they are written — to stderr always, and additionally appended to a
//! file when one is configured. Records that dependencies emit through the `log` facade
//! (DataFusion, Ballista) are bridged into the same output. Binaries and examples call
//! [`init_from_env_and_args`] once at startup; the library itself never installs a subscriber.
//!
//! Configuration comes from the environment and the command line, never the job JSON:
//!   * filter: `--log-level <off|error|warn|info|debug|trace>`, else `RUST_LOG` (a level, or
//!     `tracing` directives such as `warn,el_ballista=debug`), else `info`.
//!   * file:  `--log-file <path>`, else `EL_BALLISTA_LOG_FILE`, else none (stderr only).
//!
//! At `debug` (or a more verbose level) every generated SQL statement is emitted, so a
//! `--log-file` at debug captures each query as it is produced.

use std::io::IsTerminal;
use std::path::PathBuf;
use std::sync::Mutex;

/// Resolved logging options.
#[derive(Debug, Clone)]
pub struct LogOptions {
    /// What is emitted, as `tracing` filter directives: a level (`info`) or per-target
    /// directives (`warn,el_ballista=debug`).
    pub filter: String,
    /// Optional file to also write logs to (append). `None` means stderr only.
    pub file: Option<PathBuf>,
}

impl Default for LogOptions {
    fn default() -> Self {
        Self {
            filter: "info".to_string(),
            file: None,
        }
    }
}

impl LogOptions {
    /// Resolve options from the real `std::env::args()` and the process environment.
    /// Command-line flags win over environment variables.
    pub(crate) fn from_env_and_args() -> Self {
        let args: Vec<String> = std::env::args().collect();
        Self::resolve(&args, |k| std::env::var(k).ok())
    }

    /// Testable core: resolve from an explicit argv and an environment lookup.
    /// CLI flags take precedence over env vars; env vars over the built-in defaults. The
    /// `--log-level` flag takes a level; `RUST_LOG` also takes filter directives.
    pub(crate) fn resolve<F>(args: &[String], env: F) -> Self
    where
        F: Fn(&str) -> Option<String>,
    {
        let filter = flag_value(args, "--log-level")
            .and_then(|s| parse_level(&s))
            .or_else(|| {
                env("RUST_LOG")
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .map(|s| parse_level(&s).unwrap_or(s))
            })
            .unwrap_or_else(|| "info".to_string());

        let file = flag_value(args, "--log-file")
            .or_else(|| env("EL_BALLISTA_LOG_FILE"))
            .filter(|s| !s.is_empty())
            .map(PathBuf::from);

        Self { filter, file }
    }
}

/// A level name (case-insensitive), or a bare number `0..=5` for convenience (`4` == debug),
/// as the lowercase level directive.
fn parse_level(s: &str) -> Option<String> {
    let level = match s.trim().to_ascii_lowercase().as_str() {
        "off" | "0" => "off",
        "error" | "1" => "error",
        "warn" | "2" => "warn",
        "info" | "3" => "info",
        "debug" | "4" => "debug",
        "trace" | "5" => "trace",
        _ => return None,
    };
    Some(level.to_string())
}

/// Find `--flag value` or `--flag=value` in an argv slice. Returns the value if present.
fn flag_value(args: &[String], flag: &str) -> Option<String> {
    let eq_prefix = format!("{flag}=");
    let mut it = args.iter();
    while let Some(arg) = it.next() {
        if arg == flag {
            return it.next().cloned();
        }
        if let Some(rest) = arg.strip_prefix(&eq_prefix) {
            return Some(rest.to_string());
        }
    }
    None
}

/// Install the global subscriber using options resolved from the environment and argv.
///
/// Convenience wrapper over `init`. Call once at process start. A failure (for example
/// a second call, an invalid `RUST_LOG`, or an unwritable log file) is reported to stderr and
/// swallowed so it never aborts the program over logging alone.
///
/// # Examples
///
/// ```no_run
/// use el_ballista::logging;
///
/// fn main() {
///     // Honors RUST_LOG / --log-level and EL_BALLISTA_LOG_FILE / --log-file.
///     logging::init_from_env_and_args();
///     tracing::info!(job_id = "orders", "extraction starting");
/// }
/// ```
pub fn init_from_env_and_args() {
    let opts = LogOptions::from_env_and_args();
    if let Err(e) = init(&opts) {
        eprintln!("warning: could not initialize logging: {e}");
    }
}

/// Install the global subscriber for the given options.
///
/// Emits to stderr always and, when `opts.file` is set, additionally appends to that file
/// (creating parent directories as needed). Returns an error if a subscriber is already
/// installed, the filter does not parse, or the log file cannot be opened.
pub(crate) fn init(opts: &LogOptions) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use tracing_subscriber::prelude::*;
    use tracing_subscriber::{EnvFilter, fmt};

    let filter = EnvFilter::try_new(&opts.filter)?;
    let stderr = fmt::layer()
        .with_writer(std::io::stderr)
        .with_ansi(std::io::stderr().is_terminal());
    let file = match &opts.file {
        Some(path) => {
            if let Some(parent) = path.parent()
                && !parent.as_os_str().is_empty()
            {
                std::fs::create_dir_all(parent)?;
            }
            let file = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)?;
            Some(fmt::layer().with_ansi(false).with_writer(Mutex::new(file)))
        }
        None => None,
    };
    tracing_subscriber::registry()
        .with(filter)
        .with(stderr)
        .with(file)
        .try_init()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| s.to_string()).collect()
    }

    fn no_env(_: &str) -> Option<String> {
        None
    }

    #[test]
    fn defaults_to_info_stderr() {
        let o = LogOptions::resolve(&args(&["el-ballista", "run"]), no_env);
        assert_eq!(o.filter, "info");
        assert!(o.file.is_none());
    }

    #[test]
    fn cli_flags_win_over_env() {
        let env = |k: &str| match k {
            "RUST_LOG" => Some("warn".to_string()),
            "EL_BALLISTA_LOG_FILE" => Some("/from/env.log".to_string()),
            _ => None,
        };
        let o = LogOptions::resolve(
            &args(&[
                "el-ballista",
                "run",
                "--log-level",
                "debug",
                "--log-file",
                "/cli.log",
            ]),
            env,
        );
        assert_eq!(o.filter, "debug");
        assert_eq!(o.file, Some(PathBuf::from("/cli.log")));
    }

    #[test]
    fn env_used_when_no_flag() {
        let env = |k: &str| match k {
            "RUST_LOG" => Some("TRACE".to_string()),
            "EL_BALLISTA_LOG_FILE" => Some("/e.log".to_string()),
            _ => None,
        };
        let o = LogOptions::resolve(&args(&["el-ballista"]), env);
        assert_eq!(o.filter, "trace");
        assert_eq!(o.file, Some(PathBuf::from("/e.log")));
    }

    #[test]
    fn rust_log_directives_are_kept() {
        // Per-target directives used to fall back to `info` silently.
        let env = |k: &str| (k == "RUST_LOG").then(|| "warn,el_ballista=debug".to_string());
        let o = LogOptions::resolve(&args(&["el-ballista"]), env);
        assert_eq!(o.filter, "warn,el_ballista=debug");
        assert!(tracing_subscriber::EnvFilter::try_new(&o.filter).is_ok());
    }

    #[test]
    fn eq_form_and_numeric_level() {
        let o = LogOptions::resolve(
            &args(&["el-ballista", "--log-level=4", "--log-file=/a.log"]),
            no_env,
        );
        assert_eq!(o.filter, "debug");
        assert_eq!(o.file, Some(PathBuf::from("/a.log")));
    }

    #[test]
    fn empty_file_is_none() {
        let env = |k: &str| (k == "EL_BALLISTA_LOG_FILE").then(String::new);
        let o = LogOptions::resolve(&args(&["el-ballista"]), env);
        assert!(o.file.is_none());
    }
}
