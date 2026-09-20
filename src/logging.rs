//! Process-wide logging setup.
//!
//! The crate logs through the `log` facade everywhere (`log::debug!`, `log::info!`, …).
//! This module wires that facade to a concrete backend (`fern`) so records are actually
//! emitted — to stderr always, and additionally to a file when one is configured.
//! Binaries and examples call [`init_from_env_and_args`] once at startup; the library
//! itself never installs a logger.
//!
//! Configuration comes from the environment and the command line, never the job JSON:
//!   * level: `--log-level <off|error|warn|info|debug|trace>`, else `RUST_LOG`, else `info`.
//!   * file:  `--log-file <path>`, else `REL_LOG_FILE`, else none (stderr only).
//!
//! At `debug` (or a more verbose level) every generated SQL query is emitted: the query
//! builders and the execution plan log the final SQL text through `log::debug!`, so a
//! `--log-file` at debug captures each query as it is produced.

use std::path::PathBuf;
use std::str::FromStr;

use log::LevelFilter;

/// Resolved logging options.
#[derive(Debug, Clone)]
pub struct LogOptions {
    /// Maximum level that is emitted. Records more verbose than this are dropped.
    pub level: LevelFilter,
    /// Optional file to also write logs to (append). `None` means stderr only.
    pub file: Option<PathBuf>,
}

impl Default for LogOptions {
    fn default() -> Self {
        Self {
            level: LevelFilter::Info,
            file: None,
        }
    }
}

impl LogOptions {
    /// Resolve options from the real `std::env::args()` and the process environment.
    /// Command-line flags win over environment variables.
    pub fn from_env_and_args() -> Self {
        let args: Vec<String> = std::env::args().collect();
        Self::resolve(&args, |k| std::env::var(k).ok())
    }

    /// Testable core: resolve from an explicit argv and an environment lookup.
    /// CLI flags take precedence over env vars; env vars over the built-in defaults.
    pub fn resolve<F>(args: &[String], env: F) -> Self
    where
        F: Fn(&str) -> Option<String>,
    {
        let level = flag_value(args, "--log-level")
            .or_else(|| env("RUST_LOG"))
            .and_then(|s| parse_level(&s))
            .unwrap_or(LevelFilter::Info);

        let file = flag_value(args, "--log-file")
            .or_else(|| env("REL_LOG_FILE"))
            .filter(|s| !s.is_empty())
            .map(PathBuf::from);

        Self { level, file }
    }
}

/// Parse a level filter. Accepts the standard names (case-insensitive) via `log`'s own
/// parser, plus a bare number `0..=5` for convenience (`4` == debug).
fn parse_level(s: &str) -> Option<LevelFilter> {
    let t = s.trim();
    if let Ok(level) = LevelFilter::from_str(t) {
        return Some(level);
    }
    match t {
        "0" => Some(LevelFilter::Off),
        "1" => Some(LevelFilter::Error),
        "2" => Some(LevelFilter::Warn),
        "3" => Some(LevelFilter::Info),
        "4" => Some(LevelFilter::Debug),
        "5" => Some(LevelFilter::Trace),
        _ => None,
    }
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

/// Install the global logger using options resolved from the environment and argv.
///
/// Convenience wrapper over [`init`]. Call once at process start. A failure (for example
/// a second call, or an unwritable log file) is reported to stderr and swallowed so it
/// never aborts the program over logging alone.
pub fn init_from_env_and_args() {
    let opts = LogOptions::from_env_and_args();
    if let Err(e) = init(&opts) {
        eprintln!("warning: could not initialize logging: {e}");
    }
}

/// Install the global logger for the given options.
///
/// Emits to stderr always and, when `opts.file` is set, additionally appends to that file
/// (creating parent directories as needed). Returns an error if a logger is already
/// installed or the log file cannot be opened.
pub fn init(opts: &LogOptions) -> Result<(), fern::InitError> {
    let mut dispatch = fern::Dispatch::new()
        .level(opts.level)
        .format(|out, message, record| {
            out.finish(format_args!(
                "{} [{:<5}] {}: {}",
                chrono::Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ"),
                record.level(),
                record.target(),
                message
            ))
        })
        .chain(std::io::stderr());

    if let Some(path) = &opts.file {
        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent)?;
        }
        dispatch = dispatch.chain(fern::log_file(path)?);
    }

    dispatch.apply()?;
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
        let o = LogOptions::resolve(&args(&["rel", "run"]), no_env);
        assert_eq!(o.level, LevelFilter::Info);
        assert!(o.file.is_none());
    }

    #[test]
    fn cli_flags_win_over_env() {
        let env = |k: &str| match k {
            "RUST_LOG" => Some("warn".to_string()),
            "REL_LOG_FILE" => Some("/from/env.log".to_string()),
            _ => None,
        };
        let o = LogOptions::resolve(
            &args(&["rel", "run", "--log-level", "debug", "--log-file", "/cli.log"]),
            env,
        );
        assert_eq!(o.level, LevelFilter::Debug);
        assert_eq!(o.file, Some(PathBuf::from("/cli.log")));
    }

    #[test]
    fn env_used_when_no_flag() {
        let env = |k: &str| match k {
            "RUST_LOG" => Some("trace".to_string()),
            "REL_LOG_FILE" => Some("/e.log".to_string()),
            _ => None,
        };
        let o = LogOptions::resolve(&args(&["rel"]), env);
        assert_eq!(o.level, LevelFilter::Trace);
        assert_eq!(o.file, Some(PathBuf::from("/e.log")));
    }

    #[test]
    fn eq_form_and_numeric_level() {
        let o =
            LogOptions::resolve(&args(&["rel", "--log-level=4", "--log-file=/a.log"]), no_env);
        assert_eq!(o.level, LevelFilter::Debug);
        assert_eq!(o.file, Some(PathBuf::from("/a.log")));
    }

    #[test]
    fn empty_file_is_none() {
        let env = |k: &str| (k == "REL_LOG_FILE").then(String::new);
        let o = LogOptions::resolve(&args(&["rel"]), env);
        assert!(o.file.is_none());
    }
}
