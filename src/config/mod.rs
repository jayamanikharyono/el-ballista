//! Job and connection configuration.
//! config/mod.rs
//! Loads a job spec from JSON (see `extract.example.json`). Deliberately narrower than the
//! `extract.toml` shape in docs/architecture.md §6 — only the fields the code actually branches
//! on. Credentials are never inlined: the password is read from an environment variable named
//! in the file, per the doc's `dsn_env` convention.
//!
//! Strict by design: every struct is `deny_unknown_fields`, so a typo or a leftover block
//! (`"sink"`, `"watermark"`) is a load error instead of being silently ignored; enumerated
//! values (`parallel_scan.strategy`, `pushdown.policy`) are closed serde enums with lowercase
//! names; and `JobConfig::validate` runs on every construction path the library offers
//! (`from_file`, `PostgresConnector::from_config`, `Pipeline::from_config`).

use std::env;
use std::fs;
use std::path::Path;

use serde::{Deserialize, Deserializer, Serialize};

use crate::errors::AppError;

pub use crate::pushdown::PushdownPolicy;
pub use crate::types::JobId;
pub use crate::types::ParallelStrategy;

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SourceConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    /// Name of the environment variable holding the password.
    pub password_env: String,
    pub database: String,
    #[serde(default = "default_pool_max")]
    pub pool_max: u32,
    #[serde(default = "default_statement_timeout_ms")]
    pub statement_timeout_ms: u64,
    #[serde(default = "default_application_name")]
    pub application_name: String,
    #[serde(default = "default_schema")]
    pub schema: String,
}

fn default_pool_max() -> u32 {
    8
}

fn default_statement_timeout_ms() -> u64 {
    300_000
}

fn default_application_name() -> String {
    "el-ballista".to_string()
}

fn default_schema() -> String {
    "public".to_string()
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CheckpointConfig {
    /// Directory holding one JSON checkpoint file per job. File-based by design for now —
    /// see docs/phase-one-implementation-plan.md §4 for why a Postgres-backed store is deferred.
    #[serde(default = "default_checkpoint_dir")]
    pub dir: String,
    /// Background progress flush cadence for the driver-owned progress file. The extraction
    /// loop never blocks on these writes (fire-and-forget `try_send` + debounced writer).
    #[serde(default = "default_checkpoint_flush_secs")]
    pub flush_interval_secs: u64,
    /// Flush progress at least every this many rows even if the time interval has not elapsed.
    #[serde(default = "default_checkpoint_flush_rows")]
    pub flush_rows: u64,
    /// A job's lock file (held for the whole `run_with`) may be taken over by another run
    /// once its heartbeat is older than this. The holder refreshes the heartbeat every
    /// `lock_ttl_secs / 4`. Must be >= 4.
    #[serde(default = "default_lock_ttl_secs")]
    pub lock_ttl_secs: u64,
    /// Write a run report (`<dir>/runs/<job>/<run_id>.json`) for every `run_with` run.
    /// See `run_report`. Default `true`.
    #[serde(default = "default_true")]
    pub run_reports: bool,
    /// Also write run reports for diagnostic runs (`run()`, `el-ballista run`, `el-ballista distribute`).
    /// Default `false`: diagnostics leave no files behind.
    #[serde(default)]
    pub diagnostic_run_reports: bool,
}

fn default_true() -> bool {
    true
}

fn default_checkpoint_dir() -> String {
    ".checkpoints".to_string()
}

fn default_checkpoint_flush_secs() -> u64 {
    5
}

fn default_checkpoint_flush_rows() -> u64 {
    100_000
}

fn default_lock_ttl_secs() -> u64 {
    1800
}

impl Default for CheckpointConfig {
    fn default() -> Self {
        Self {
            dir: default_checkpoint_dir(),
            flush_interval_secs: default_checkpoint_flush_secs(),
            flush_rows: default_checkpoint_flush_rows(),
            lock_ttl_secs: default_lock_ttl_secs(),
            run_reports: true,
            diagnostic_run_reports: false,
        }
    }
}

/// docs/pushdown.md §4.3. Cost-based pushdown with per-source budgets and selectivity thresholds.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PushdownConfig {
    /// `always` | `never` | `cost_based` | `strict` | `hinted` (exact lowercase names;
    /// anything else is a load error, so a mistyped emergency `never` cannot fail open).
    #[serde(
        default = "default_pushdown_policy",
        deserialize_with = "deserialize_policy"
    )]
    pub policy: PushdownPolicy,
    #[serde(default)]
    pub deny: Vec<String>,
    /// `hinted` policy: predicates touching these columns are forced to the source
    /// (whenever they translate — hints never override correctness). Deny wins over push.
    #[serde(default)]
    pub push: Vec<String>,
    #[serde(default = "default_max_source_cost")]
    pub max_source_cost: u64,
    #[serde(default = "default_keep_threshold")]
    pub keep_threshold: f64,
    #[serde(default = "default_statistics_ttl_secs")]
    pub statistics_ttl_secs: u64,
}

fn default_pushdown_policy() -> PushdownPolicy {
    PushdownPolicy::CostBased
}

/// Canonical lowercase config name of a pushdown policy.
///
/// # Examples
///
/// ```
/// use el_ballista::config::{PushdownPolicy, policy_name};
///
/// assert_eq!(policy_name(PushdownPolicy::CostBased), "cost_based");
/// // Round-trips through the parser used for config files.
/// let name = policy_name(PushdownPolicy::Hinted);
/// assert_eq!(PushdownPolicy::parse(name), Ok(PushdownPolicy::Hinted));
/// ```
pub fn policy_name(policy: PushdownPolicy) -> &'static str {
    match policy {
        PushdownPolicy::Always => "always",
        PushdownPolicy::Never => "never",
        PushdownPolicy::CostBased => "cost_based",
        PushdownPolicy::Strict => "strict",
        PushdownPolicy::Hinted => "hinted",
    }
}

const POLICY_NAMES: &[&str] = &["always", "never", "cost_based", "strict", "hinted"];

fn deserialize_policy<'de, D: Deserializer<'de>>(d: D) -> Result<PushdownPolicy, D::Error> {
    let raw = String::deserialize(d)?;
    let policy = PushdownPolicy::parse(&raw)
        .ok()
        .filter(|p| policy_name(*p) == raw);
    policy.ok_or_else(|| serde::de::Error::unknown_variant(&raw, POLICY_NAMES))
}

fn default_max_source_cost() -> u64 {
    50_000
}

fn default_keep_threshold() -> f64 {
    0.30
}

fn default_statistics_ttl_secs() -> u64 {
    900
}

impl Default for PushdownConfig {
    fn default() -> Self {
        Self {
            policy: default_pushdown_policy(),
            deny: Vec::new(),
            push: Vec::new(),
            max_source_cost: default_max_source_cost(),
            keep_threshold: default_keep_threshold(),
            statistics_ttl_secs: default_statistics_ttl_secs(),
        }
    }
}

/// Phase 3: Execution configuration for streaming batches and memory management.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionConfig {
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    /// Byte cap per Arrow batch. Flushes early on wide rows (text/json/bytea) so peak
    /// memory stays bounded even when `batch_size` rows are very wide.
    #[serde(default = "default_max_batch_bytes")]
    pub max_batch_bytes: usize,
    /// Upper bound on concurrent single-node split scans (and consumer calls in
    /// `run_with`). Unset (the default) means the whole source budget, `source.pool_max`;
    /// an explicit value is still capped at it, so a scan never waits on a connection
    /// another scan of the same run holds.
    #[serde(default)]
    pub concurrent_partitions: Option<usize>,
    /// Use `COPY (SELECT …) TO STDOUT (FORMAT BINARY)` instead of cursor `FETCH`
    /// for full/keyset scans. Same rows, less per-row protocol overhead. Falls back
    /// to cursors automatically when the shape is unsupported (pushed filters with
    /// bound literals, unmapped types) — the fallback is logged, never silent.
    #[serde(default = "default_use_copy")]
    pub use_copy: bool,
    /// `statement_timeout` (ms) applied to each binary COPY scan instead of the session's
    /// `source.statement_timeout_ms`. A COPY is **one statement**: the timeout bounds the
    /// whole partition, including time the consumer spends applying backpressure. Unset
    /// (default) keeps the session timeout; `0` disables it for COPY scans. Cursor scans are
    /// unaffected (every FETCH is its own statement).
    #[serde(default)]
    pub copy_statement_timeout_ms: Option<u64>,
}

fn default_batch_size() -> usize {
    8192
}

fn default_max_batch_bytes() -> usize {
    16 * 1024 * 1024
}

fn default_use_copy() -> bool {
    false
}

impl Default for ExecutionConfig {
    fn default() -> Self {
        Self {
            batch_size: default_batch_size(),
            max_batch_bytes: default_max_batch_bytes(),
            concurrent_partitions: None,
            use_copy: default_use_copy(),
            copy_statement_timeout_ms: None,
        }
    }
}

/// Phase 4 (docs/roadmap.md): distributed execution over Ballista.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DistributedConfig {
    /// Remote scheduler endpoint (`http://host:port`), or empty for a standalone deployment
    /// where the scheduler and `parallel_workers` executors run in this process.
    #[serde(default = "default_scheduler_url")]
    pub scheduler_url: String,
    /// Number of executor processes (standalone mode) or expected workers (remote mode — used
    /// only to budget the per-process source connection pools; see docs/roadmap.md Phase 4).
    #[serde(default = "default_workers")]
    pub workers: usize,
    /// How many times a hung distributed job is cancelled and re-run before the extraction
    /// aborts with `DistributedJobAborted` (0 = abort on the first hang). A job counts as
    /// hung when a worker it started with stops heartbeating for `executor_timeout_secs` or
    /// is dropped by the scheduler, or when `job_timeout_secs` passes. Only a job that has
    /// not delivered any rows yet is re-run (Ballista delivers results after the job
    /// finishes, so that is the whole run phase); later, the hang is an error.
    #[serde(default = "default_max_retries")]
    pub max_retries: u32,
    /// A worker whose heartbeat has not advanced for this long counts as dead. Keep it well
    /// above the workers' `--heartbeat-secs` (default 5 s).
    #[serde(default = "default_executor_timeout_secs")]
    pub executor_timeout_secs: u64,
    /// Optional wall-clock limit per attempt of a distributed job; unset = no limit.
    #[serde(default)]
    pub job_timeout_secs: Option<u64>,
}

fn default_scheduler_url() -> String {
    String::new()
}

fn default_workers() -> usize {
    1
}

fn default_max_retries() -> u32 {
    2
}

fn default_executor_timeout_secs() -> u64 {
    30
}

impl Default for DistributedConfig {
    fn default() -> Self {
        Self {
            scheduler_url: default_scheduler_url(),
            workers: default_workers(),
            max_retries: default_max_retries(),
            executor_timeout_secs: default_executor_timeout_secs(),
            job_timeout_secs: None,
        }
    }
}

/// Parallel scan configuration.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ParallelScanConfig {
    /// `none` | `keyset` | `ctid` (lowercase; anything else is a load error).
    #[serde(default)]
    pub strategy: ParallelStrategy,
    #[serde(default = "default_parallel_partitions")]
    pub partitions: usize,
    #[serde(default = "default_partition_column")]
    pub partition_column: String,
}

fn default_parallel_partitions() -> usize {
    1
}

fn default_partition_column() -> String {
    "id".to_string()
}

impl Default for ParallelScanConfig {
    fn default() -> Self {
        Self {
            strategy: ParallelStrategy::None,
            partitions: default_parallel_partitions(),
            partition_column: default_partition_column(),
        }
    }
}

/// Comparison operator for a structured filter predicate. Symbolic spellings match
/// the CLI shorthand (`--filter "amount>=100"`), so both forms share one vocabulary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[non_exhaustive]
pub enum FilterOp {
    #[serde(rename = "=")]
    Eq,
    #[serde(rename = "!=")]
    NotEq,
    #[serde(rename = ">")]
    Gt,
    #[serde(rename = ">=")]
    GtEq,
    #[serde(rename = "<")]
    Lt,
    #[serde(rename = "<=")]
    LtEq,
    #[serde(rename = "is_null")]
    IsNull,
    #[serde(rename = "is_not_null")]
    IsNotNull,
}

impl FilterOp {
    /// The symbolic spelling used in JSON and the CLI shorthand.
    pub(crate) fn as_str(&self) -> &'static str {
        match self {
            FilterOp::Eq => "=",
            FilterOp::NotEq => "!=",
            FilterOp::Gt => ">",
            FilterOp::GtEq => ">=",
            FilterOp::Lt => "<",
            FilterOp::LtEq => "<=",
            FilterOp::IsNull => "is_null",
            FilterOp::IsNotNull => "is_not_null",
        }
    }
}

/// One caller-provided filter predicate in structured form.
///
/// ```json
/// { "column": "status", "op": "=", "value": "PAID" }
/// ```
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FilterSpec {
    pub column: String,
    pub op: FilterOp,
    /// JSON-native value: a number becomes an int/float literal, a boolean a boolean
    /// literal, a string a text literal (or a timestamp/date literal when the column
    /// type says so — see schema-aware lowering), null a NULL check. Nested
    /// arrays/objects are rejected. May be omitted for `is_null` / `is_not_null`.
    #[serde(default)]
    pub value: serde_json::Value,
}

/// One entry of [`JobConfig::filters`]: either the CLI-style shorthand
/// (`"status=PAID"`, handy for simple cases and `--filter` flags) or the
/// structured form above (the recommended JSON representation — typed values,
/// `is_null`, and timestamp coercion). Both lower to the same DataFusion predicate.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(untagged)]
#[non_exhaustive]
pub enum FilterInput {
    Shorthand(String),
    Structured(FilterSpec),
}

/// One AND-conjunct of [`JobConfig::filters`]: either a single predicate or an
/// OR-group written as an inner JSON array. The outer list is always ANDed;
/// an inner array is ORed, then ANDed with its siblings:
///
/// ```json
/// "filters": [
///   [{ "column": "status", "op": "=", "value": "PAID" },
///    { "column": "amount", "op": ">", "value": 100 }],
///   { "column": "updated_at", "op": ">=", "value": "2026-01-01T00:00:00Z" }
/// ]
/// ```
/// means `(status = 'PAID' OR amount > 100) AND updated_at >= ...`.
/// An empty inner array is rejected at validation (it would otherwise read as
/// "no constraint" or "no rows" depending on the reader — never guess).
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(untagged)]
#[non_exhaustive]
pub enum FilterEntry {
    Single(FilterInput),
    OrGroup(Vec<FilterInput>),
}

/// One extraction job. There is deliberately no `sink` block: this layer extracts to Arrow
/// and hands batches to the caller (`collect` / `stream` / `run_with`); a config that still
/// carries `"sink"` fails to load with an unknown-field error.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JobConfig {
    pub job_id: JobId,
    pub table: String,
    /// `None` means "all columns" (still resolved against the catalog schema).
    pub columns: Option<Vec<String>>,
    /// Caller-provided filter predicates, applied as the extraction filter.
    /// Empty means a full extraction (every row).
    /// The outer list is ANDed; an inner array entry is an OR-group
    /// (see [`FilterEntry`]).
    /// The orchestrator decides WHAT range to extract; the extraction layer decides HOW.
    #[serde(default)]
    pub filters: Vec<FilterEntry>,
    pub source: SourceConfig,
    #[serde(default)]
    pub checkpoint: CheckpointConfig,
    #[serde(default)]
    pub pushdown: PushdownConfig,
    #[serde(default)]
    pub parallel_scan: ParallelScanConfig,
    #[serde(default)]
    pub execution: ExecutionConfig,
    #[serde(default)]
    pub distributed: DistributedConfig,
}

impl JobConfig {
    /// The schema-qualified table name: `table` as written when it already contains a `.`,
    /// otherwise `source.schema` + `.` + `table`.
    ///
    /// # Examples
    ///
    /// ```
    /// use el_ballista::config::JobConfig;
    ///
    /// let config: JobConfig = serde_json::from_str(r#"{
    ///     "job_id": "orders", "table": "orders",
    ///     "source": {"host": "localhost", "port": 5432, "user": "etl",
    ///                "password_env": "EL_BALLISTA_DOCS_UNSET_PASSWORD", "database": "shop", "schema": "sales"}
    /// }"#)?;
    /// assert_eq!(config.resolved_table(), "sales.orders");
    /// # Ok::<(), serde_json::Error>(())
    /// ```
    pub fn resolved_table(&self) -> String {
        if self.table.contains('.') {
            self.table.clone()
        } else {
            format!("{}.{}", self.source.schema, self.table)
        }
    }

    /// Read, parse and validate a JSON job spec.
    ///
    /// Errors: [`AppError::ConfigRead`] when the file cannot be read, [`AppError::ConfigParse`]
    /// for malformed JSON or unknown fields (e.g. a `sink` block), and [`AppError::Config`] when
    /// a value fails validation (`batch_size = 0`, an empty OR-group, ...).
    ///
    /// # Examples
    ///
    /// ```
    /// use el_ballista::config::JobConfig;
    ///
    /// let config = JobConfig::from_file("examples/configs/full_extract.example.json")?;
    /// assert_eq!(config.job_id, "payment_full");
    /// assert_eq!(config.resolved_table(), "public.payment");
    /// # Ok::<(), el_ballista::errors::AppError>(())
    /// ```
    pub fn from_file(path: impl AsRef<Path>) -> Result<Self, AppError> {
        let path = path.as_ref();

        let text = fs::read_to_string(path).map_err(|source| AppError::ConfigRead {
            path: path.to_path_buf(),
            source,
        })?;

        let config: JobConfig =
            serde_json::from_str(&text).map_err(|source| AppError::ConfigParse {
                path: path.to_path_buf(),
                source,
            })?;

        config.validate()?;

        Ok(config)
    }

    /// Reject degenerate values that cause silent misbehavior downstream: a zero
    /// `batch_size` makes every FETCH return zero rows, and zero pools/workers/
    /// partitions divide budgets by zero or scan nothing. An empty OR-group
    /// (`"filters": [[]]`) is equally degenerate — it has no defined
    /// AND/OR meaning — so it fails here rather than as zero rows downstream.
    pub(crate) fn validate(&self) -> Result<(), AppError> {
        let mut bad = Vec::new();
        if self
            .filters
            .iter()
            .any(|e| matches!(e, FilterEntry::OrGroup(g) if g.is_empty()))
        {
            bad.push(
                "filters contains an empty OR-group (inner array must hold >= 1 predicate)"
                    .to_string(),
            );
        }
        if self.execution.batch_size < 1 {
            bad.push(format!(
                "execution.batch_size must be >= 1 (got {})",
                self.execution.batch_size
            ));
        }
        if self.execution.max_batch_bytes < 1 {
            bad.push(format!(
                "execution.max_batch_bytes must be >= 1 (got {})",
                self.execution.max_batch_bytes
            ));
        }
        if self.execution.concurrent_partitions == Some(0) {
            bad.push("execution.concurrent_partitions must be >= 1 (got 0)".to_string());
        }
        if self.parallel_scan.partitions < 1 {
            bad.push(format!(
                "parallel_scan.partitions must be >= 1 (got {})",
                self.parallel_scan.partitions
            ));
        }
        if self.distributed.workers < 1 {
            bad.push(format!(
                "distributed.workers must be >= 1 (got {})",
                self.distributed.workers
            ));
        }
        if self.distributed.executor_timeout_secs < 1 {
            bad.push("distributed.executor_timeout_secs must be >= 1 (got 0)".to_string());
        }
        if self.distributed.job_timeout_secs == Some(0) {
            bad.push(
                "distributed.job_timeout_secs must be >= 1 when set (got 0; omit it for no \
                 limit)"
                    .to_string(),
            );
        }
        if self.checkpoint.lock_ttl_secs < 4 {
            bad.push(format!(
                "checkpoint.lock_ttl_secs must be >= 4 (got {})",
                self.checkpoint.lock_ttl_secs
            ));
        }
        if self.parallel_scan.strategy == ParallelStrategy::Keyset
            && self.parallel_scan.partition_column.trim().is_empty()
        {
            bad.push("parallel_scan.partition_column must be set for strategy 'keyset'".into());
        }
        if self.table.trim().is_empty() {
            bad.push("table must not be empty".into());
        }
        if self.source.pool_max < 1 {
            bad.push(format!(
                "source.pool_max must be >= 1 (got {})",
                self.source.pool_max
            ));
        }
        if bad.is_empty() {
            Ok(())
        } else {
            Err(AppError::Config(format!(
                "invalid job spec: {}",
                bad.join("; ")
            )))
        }
    }

    /// Read the source password from the environment variable named by
    /// `source.password_env`. Errors with [`AppError::Config`] (naming the variable, never a
    /// value) when it is not set.
    ///
    /// # Examples
    ///
    /// ```
    /// use el_ballista::config::JobConfig;
    ///
    /// let config: JobConfig = serde_json::from_str(r#"{
    ///     "job_id": "orders", "table": "orders",
    ///     "source": {"host": "localhost", "port": 5432, "user": "etl",
    ///                "password_env": "EL_BALLISTA_DOCS_UNSET_PASSWORD", "database": "shop", "schema": "sales"}
    /// }"#)?;
    /// // `source.password_env` names an unset variable: an error, never an empty password.
    /// assert!(config.resolve_password().is_err());
    /// # Ok::<(), serde_json::Error>(())
    /// ```
    pub fn resolve_password(&self) -> Result<String, AppError> {
        env::var(&self.source.password_env).map_err(|_| {
            AppError::Config(format!(
                "environment variable '{}' is not set (source.password_env)",
                self.source.password_env
            ))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_file_example_config() {
        // The checked-in example spec must always parse: it is what the examples and the
        // runbook invoke. Catches renamed/removed serde fields.
        // Tests run with CWD at the crate root, where examples/ lives.
        let config = JobConfig::from_file("examples/configs/extract.example.json").unwrap();

        assert_eq!(config.job_id, "payment_extract");
        assert_eq!(config.table, "payment");
        assert_eq!(config.resolved_table(), "public.payment");
        assert_eq!(config.source.password_env, "PGPASSWORD");
        assert!(
            config
                .columns
                .as_ref()
                .unwrap()
                .contains(&"payment_date".to_string())
        );

        // Spec blocks absent from older files fall back to defaults.
        assert_eq!(config.pushdown.push, Vec::<String>::new());
        assert_eq!(config.distributed.workers, 2);
        assert_eq!(config.execution.batch_size, 8192);
        assert_eq!(config.execution.max_batch_bytes, 16 * 1024 * 1024);
        // Unset: the whole source budget (resolved against pool_max at run time).
        assert_eq!(config.execution.concurrent_partitions, None);
        assert!(!config.execution.use_copy);
        assert_eq!(config.checkpoint.flush_interval_secs, 5);
        assert_eq!(config.checkpoint.flush_rows, 100_000);
        assert_eq!(config.parallel_scan.partition_column, "payment_id");
    }

    #[test]
    fn test_from_file_missing_is_config_error() {
        let err = JobConfig::from_file("examples/configs/does-not-exist.json").unwrap_err();
        assert!(err.to_string().contains("cannot read"));
        // The io error is kept as the source, not flattened into the message.
        assert!(matches!(err, AppError::ConfigRead { .. }));
        assert!(std::error::Error::source(&err).is_some());
    }

    /// Parse a job spec from JSON text, the way `from_file` does.
    fn parse(text: &str) -> Result<JobConfig, serde_json::Error> {
        serde_json::from_str(text)
    }

    fn minimal_spec(extra: &str) -> String {
        format!(
            r#"{{"job_id": "j", "table": "t", "columns": null,
                "source": {{"host": "h", "port": 5432, "user": "u",
                            "password_env": "P", "database": "d"}}{extra}}}"#
        )
    }

    #[test]
    fn test_minimal_spec_parses() {
        let config = parse(&minimal_spec("")).unwrap();
        assert_eq!(config.job_id, "j");
        assert_eq!(config.parallel_scan.strategy, ParallelStrategy::None);
        assert_eq!(config.pushdown.policy, PushdownPolicy::CostBased);
        assert_eq!(config.checkpoint.lock_ttl_secs, 1800);
        config.validate().unwrap();
    }

    #[test]
    fn test_sink_block_is_an_unknown_field_error() {
        let err = parse(&minimal_spec(r#", "sink": {"path": "/tmp/x"}"#)).unwrap_err();
        assert!(err.to_string().contains("unknown field `sink`"), "{err}");
    }

    #[test]
    fn test_unknown_fields_rejected_in_every_block() {
        for extra in [
            r#", "watermark": {"column": "updated_at"}"#,
            r#", "execution": {"batch_sise": 10}"#,
            r#", "checkpoint": {"dri": "x"}"#,
            r#", "pushdown": {"polcy": "never"}"#,
            r#", "parallel_scan": {"partition": 2}"#,
            r#", "distributed": {"worker": 2}"#,
        ] {
            let err = parse(&minimal_spec(extra)).unwrap_err();
            assert!(err.to_string().contains("unknown field"), "{extra}: {err}");
        }
        let err = parse(
            r#"{"job_id": "j", "table": "t", "columns": null,
                "source": {"host": "h", "port": 5432, "user": "u", "password_env": "P",
                           "database": "d", "pasword": "x"}}"#,
        )
        .unwrap_err();
        assert!(err.to_string().contains("unknown field"), "{err}");
    }

    #[test]
    fn test_strategy_and_policy_are_closed_lowercase_enums() {
        let config = parse(&minimal_spec(
            r#", "parallel_scan": {"strategy": "keyset", "partitions": 4},
               "pushdown": {"policy": "never"}"#,
        ))
        .unwrap();
        assert_eq!(config.parallel_scan.strategy, ParallelStrategy::Keyset);
        assert_eq!(config.pushdown.policy, PushdownPolicy::Never);
        for bad in [
            r#", "parallel_scan": {"strategy": "Keyset"}"#,
            r#", "parallel_scan": {"strategy": "keyst"}"#,
            r#", "pushdown": {"policy": "NEVER"}"#,
            r#", "pushdown": {"policy": "nevr"}"#,
        ] {
            let err = parse(&minimal_spec(bad)).unwrap_err();
            assert!(err.to_string().contains("unknown variant"), "{bad}: {err}");
        }
        for policy in [
            PushdownPolicy::Always,
            PushdownPolicy::Never,
            PushdownPolicy::CostBased,
            PushdownPolicy::Strict,
            PushdownPolicy::Hinted,
        ] {
            let spec = minimal_spec(&format!(
                r#", "pushdown": {{"policy": "{}"}}"#,
                policy_name(policy)
            ));
            assert_eq!(parse(&spec).unwrap().pushdown.policy, policy);
        }
    }

    #[test]
    fn test_invalid_job_id_fails_at_load() {
        let spec = minimal_spec("").replace(r#""job_id": "j""#, r#""job_id": """#);
        let err = parse(&spec).unwrap_err();
        assert!(err.to_string().contains("job_id"), "{err}");
    }

    #[test]
    fn test_every_example_config_loads_and_validates() {
        let mut n = 0;
        for entry in fs::read_dir("examples/configs").unwrap() {
            let path = entry.unwrap().path();
            if path.extension().is_some_and(|e| e == "json") {
                let config = JobConfig::from_file(&path)
                    .unwrap_or_else(|e| panic!("{}: {e:?}", path.display()));
                config.validate().unwrap();
                n += 1;
            }
        }
        assert!(n >= 5);
    }

    #[test]
    fn test_from_file_bench_config() {
        // The benchmark job spec must parse too — a missing block here fails inside the
        // container at runtime, where the error is expensive to see.
        let config = JobConfig::from_file("benchmark/rust/bench-config.json").unwrap();
        assert_eq!(config.table, "orders");
        assert_eq!(config.source.host, "bench-pg");
        assert_eq!(config.execution.batch_size, 64000);
    }

    #[test]
    fn test_benchmark_generated_configs_parse_strictly() {
        // `benchmark/run.sh` writes `benchmark/bench-config-<scenario>.json` from one heredoc
        // on every run (generated artifacts). The scenario lives in the config — structured
        // `filters` + `columns`, never SQL — so every scenario, in each batch/COPY mode, must
        // load under the strict (deny_unknown_fields, no `sink`) job spec.
        let script = std::fs::read_to_string("benchmark/run.sh").unwrap();
        let start = script
            .find("cat > \"$BENCH_DIR/bench-config-$1.json\" <<EOF\n")
            .expect("run.sh generates bench-config-<scenario>.json from a heredoc");
        let body = &script[start..];
        let body = &body[body.find('\n').unwrap() + 1..];
        let template = &body[..body.find("\nEOF\n").expect("heredoc terminator")];
        let scenarios = [
            ("full", "[]", "null"),
            (
                "selective",
                r#"[{"column": "status", "op": "=", "value": "REFUNDED"}]"#,
                r#"["order_id", "amount", "status"]"#,
            ),
        ];
        for (scenario, filters, columns) in scenarios {
            for (batch_json, copy_json) in [
                ("", "\"use_copy\": true"),
                ("\"batch_size\": 64000,", "\"use_copy\": false"),
            ] {
                let text = template
                    .replace("$1", scenario)
                    .replace("$2", filters)
                    .replace("$3", columns)
                    .replace("$PARALLEL_STRATEGY", "keyset")
                    .replace("$RUST_PARTITIONS", "4")
                    .replace("$WORKERS", "3")
                    .replace("$POOL_MAX", "12")
                    .replace("${BATCH_JSON}", batch_json)
                    .replace("${COPY_JSON}", copy_json);
                assert!(!text.contains('$'), "unsubstituted variable in:\n{text}");
                let config = parse(&text).unwrap_or_else(|e| panic!("{e}\n{text}"));
                config.validate().unwrap();
                assert_eq!(config.parallel_scan.partitions, 4);
                assert_eq!(config.source.pool_max, 12);
                // run.sh retries a failed attempt on a fresh cluster itself; a re-run inside
                // the client would finish on fewer workers and skew the time.
                assert_eq!(config.distributed.max_retries, 0);
                match scenario {
                    "full" => {
                        assert!(config.filters.is_empty());
                        assert!(config.columns.is_none());
                    }
                    _ => {
                        assert!(matches!(
                            config.filters.as_slice(),
                            [FilterEntry::Single(FilterInput::Structured(f))]
                                if f.column == "status" && f.op == FilterOp::Eq
                        ));
                        assert_eq!(config.columns.as_ref().map(Vec::len), Some(3));
                    }
                }
            }
        }
    }

    #[test]
    fn test_from_file_full_config() {
        // The full-extraction spec must parse and validate (no watermark/incremental blocks).
        let config = JobConfig::from_file("examples/configs/full_extract.example.json").unwrap();
        assert_eq!(config.job_id, "payment_full");
        assert!(config.filters.is_empty());
        config.validate().unwrap();
    }

    #[test]
    fn test_example_config_carries_structured_filters() {
        // extract.example.json simulates an orchestrator-supplied slice of ANDed
        // structured predicates. (OR-groups like `[[A, B], C]` parse too — see
        // `test_filters_or_group_inner_array_means_or` — but this fixture stays
        // flat so the example remains the pure-AND case.)
        let config = JobConfig::from_file("examples/configs/extract.example.json").unwrap();
        assert_eq!(config.filters.len(), 4);
        for (entry, (column, op, value)) in config.filters.iter().zip([
            (
                "payment_date",
                FilterOp::GtEq,
                serde_json::json!("2007-04-06T00:00:00Z"),
            ),
            (
                "payment_date",
                FilterOp::Lt,
                serde_json::json!("2007-04-13T00:00:00Z"),
            ),
            ("customer_id", FilterOp::GtEq, serde_json::json!(300)),
            ("amount", FilterOp::Gt, serde_json::json!(5)),
        ]) {
            match entry {
                FilterEntry::Single(FilterInput::Structured(spec)) => {
                    assert_eq!(spec.column, column);
                    assert_eq!(spec.op, op);
                    assert_eq!(spec.value, value);
                }
                _ => panic!("expected single structured filter for {column}"),
            }
        }
        config.validate().unwrap();
    }

    fn filters_from_json(raw: &str) -> Vec<FilterEntry> {
        serde_json::from_str(raw).unwrap()
    }

    fn single_input(entry: &FilterEntry) -> &FilterInput {
        match entry {
            FilterEntry::Single(input) => input,
            FilterEntry::OrGroup(_) => panic!("expected single"),
        }
    }

    #[test]
    fn test_filters_accept_shorthand_and_structured_forms() {
        let filters =
            filters_from_json(r#"["status=PAID", {"column": "amount", "op": ">", "value": 100}]"#);
        assert_eq!(filters.len(), 2);
        assert!(matches!(
            single_input(&filters[0]),
            FilterInput::Shorthand(_)
        ));
        match single_input(&filters[1]) {
            FilterInput::Structured(spec) => {
                assert_eq!(spec.column, "amount");
                assert_eq!(spec.op, FilterOp::Gt);
                assert_eq!(spec.value, serde_json::json!(100));
            }
            FilterInput::Shorthand(_) => panic!("expected structured"),
        }
    }

    #[test]
    fn test_filters_or_group_inner_array_means_or() {
        // `[[A, B], C]` parses as OR-group + single: `(A OR B) AND C`.
        let filters = filters_from_json(
            r#"[[{"column": "status", "op": "=", "value": "PAID"},
                 {"column": "amount", "op": ">", "value": 100}],
                {"column": "user_id", "op": ">=", "value": 500}]"#,
        );
        assert_eq!(filters.len(), 2);
        match &filters[0] {
            FilterEntry::OrGroup(group) => assert_eq!(group.len(), 2),
            FilterEntry::Single(_) => panic!("expected OR-group"),
        }
        assert!(matches!(&filters[1], FilterEntry::Single(_)));
    }

    #[test]
    fn test_filters_empty_or_group_fails_validation() {
        let raw = r#"[[], {"column": "a", "op": "=", "value": 1}]"#;
        let filters: Vec<FilterEntry> = serde_json::from_str(raw).unwrap();
        let mut config = JobConfig::from_file("examples/configs/extract.example.json").unwrap();
        config.filters = filters;
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_filters_value_typing_follows_json() {
        let filters = filters_from_json(
            r#"[{"column": "a", "op": "=", "value": 100},
                {"column": "b", "op": "=", "value": 1.5},
                {"column": "c", "op": "=", "value": true},
                {"column": "d", "op": "=", "value": "PAID"},
                {"column": "e", "op": "=", "value": null}]"#,
        );
        assert_eq!(filters.len(), 5);
        for (filter, expected) in filters.iter().zip([
            serde_json::json!(100),
            serde_json::json!(1.5),
            serde_json::json!(true),
            serde_json::json!("PAID"),
            serde_json::json!(null),
        ]) {
            match single_input(filter) {
                FilterInput::Structured(spec) => assert_eq!(spec.value, expected),
                FilterInput::Shorthand(_) => panic!("expected structured"),
            }
        }
    }

    #[test]
    fn test_filters_is_null_needs_no_value() {
        let filters = filters_from_json(r#"[{"column": "deleted_at", "op": "is_null"}]"#);
        match single_input(&filters[0]) {
            FilterInput::Structured(spec) => {
                assert_eq!(spec.op, FilterOp::IsNull);
                assert_eq!(spec.value, serde_json::Value::Null);
            }
            FilterInput::Shorthand(_) => panic!("expected structured"),
        }
    }

    #[test]
    fn test_filters_unknown_op_fails_at_config_load() {
        let err = serde_json::from_str::<Vec<FilterEntry>>(
            r#"[{"column": "a", "op": "==", "value": 1}]"#,
        )
        .unwrap_err();
        // The untagged enum rejects the entry at load time (neither the
        // shorthand-string nor the structured variant matches).
        assert!(err.to_string().contains("did not match any variant"));
    }

    #[test]
    fn test_filter_op_symbols_round_trip() {
        for (op, symbol) in [
            (FilterOp::Eq, "="),
            (FilterOp::NotEq, "!="),
            (FilterOp::Gt, ">"),
            (FilterOp::GtEq, ">="),
            (FilterOp::Lt, "<"),
            (FilterOp::LtEq, "<="),
            (FilterOp::IsNull, "is_null"),
            (FilterOp::IsNotNull, "is_not_null"),
        ] {
            assert_eq!(op.as_str(), symbol);
            let back: FilterOp = serde_json::from_str(&format!("\"{symbol}\"")).unwrap();
            assert_eq!(back, op);
        }
    }

    #[test]
    fn test_validate_rejects_degenerate_values() {
        // Zero batch/partitions/workers/pool silently scan nothing or divide by zero.
        let mut config = JobConfig::from_file("examples/configs/extract.example.json").unwrap();
        config.validate().unwrap();

        config.execution.batch_size = 0;
        assert!(config.validate().is_err());
        config.execution.batch_size = 8192;

        config.parallel_scan.partitions = 0;
        assert!(config.validate().is_err());
        config.parallel_scan.partitions = 1;

        config.distributed.workers = 0;
        assert!(config.validate().is_err());
        config.distributed.workers = 2;

        config.source.pool_max = 0;
        assert!(config.validate().is_err());
        config.source.pool_max = 8;

        config.checkpoint.lock_ttl_secs = 1;
        assert!(config.validate().is_err());
        config.checkpoint.lock_ttl_secs = 1800;

        config.parallel_scan.strategy = ParallelStrategy::Keyset;
        config.parallel_scan.partition_column = " ".into();
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_resolved_table() {
        let mut config = JobConfig {
            job_id: JobId::new("test").unwrap(),
            table: "orders".to_string(),
            columns: None,
            filters: Vec::new(),
            source: SourceConfig {
                host: "localhost".to_string(),
                port: 5432,
                user: "postgres".to_string(),
                password_env: "TEST_PG_PASS".to_string(),
                database: "db".to_string(),
                pool_max: 4,
                statement_timeout_ms: 1000,
                application_name: "test".to_string(),
                schema: "public".to_string(),
            },
            checkpoint: CheckpointConfig::default(),
            pushdown: PushdownConfig::default(),
            parallel_scan: ParallelScanConfig::default(),
            execution: ExecutionConfig::default(),
            distributed: DistributedConfig::default(),
        };

        assert_eq!(config.resolved_table(), "public.orders");

        config.table = "custom.orders".to_string();
        assert_eq!(config.resolved_table(), "custom.orders");
    }

    #[test]
    fn test_resolve_password() {
        let config = JobConfig {
            job_id: JobId::new("test").unwrap(),
            table: "orders".to_string(),
            columns: None,
            filters: Vec::new(),
            source: SourceConfig {
                host: "localhost".to_string(),
                port: 5432,
                user: "postgres".to_string(),
                password_env: "EL_BALLISTA_TEST_ENV_NEVER_SET_7F3A".to_string(),
                database: "db".to_string(),
                pool_max: 4,
                statement_timeout_ms: 1000,
                application_name: "test".to_string(),
                schema: "public".to_string(),
            },
            checkpoint: CheckpointConfig::default(),
            pushdown: PushdownConfig::default(),
            parallel_scan: ParallelScanConfig::default(),
            execution: ExecutionConfig::default(),
            distributed: DistributedConfig::default(),
        };

        // No environment mutation — an unset name fails, an always-set one resolves.
        assert!(config.resolve_password().is_err());
        let mut set = config.clone();
        set.source.password_env = "PATH".to_string();
        assert_eq!(set.resolve_password().unwrap(), env::var("PATH").unwrap());
    }
}
