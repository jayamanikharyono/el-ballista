//! Job and connection configuration.
//! config/mod.rs
//! Loads a job spec from JSON (see `extract.example.json`). Deliberately narrower than the
//! `extract.toml` shape in docs/architecture.md §6 — only the fields Phase 1 actually branches
//! on. Credentials are never inlined: the password is read from an environment variable named
//! in the file, per the doc's `dsn_env` convention.

use std::env;
use std::fs;
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::errors::AppError;

#[derive(Debug, Clone, Deserialize, Serialize)]
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
    "rust-extract-layer".to_string()
}

fn default_schema() -> String {
    "public".to_string()
}

#[derive(Debug, Clone, Deserialize)]
pub struct SinkConfig {
    /// Local filesystem directory. Object-store URIs (gs://...) are a follow-up — see
    /// docs/phase-one-implementation-plan.md §7.
    pub path: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CheckpointConfig {
    /// Directory holding one JSON checkpoint file per job. File-based by design for now —
    /// see docs/phase-one-implementation-plan.md §4 for why a Postgres-backed store is deferred.
    #[serde(default = "default_checkpoint_dir")]
    pub dir: String,
}

fn default_checkpoint_dir() -> String {
    ".checkpoints".to_string()
}

impl Default for CheckpointConfig {
    fn default() -> Self {
        Self {
            dir: default_checkpoint_dir(),
        }
    }
}

/// docs/pushdown.md §4.3. Cost-based pushdown with per-source budgets and selectivity thresholds.
#[derive(Debug, Clone, Deserialize)]
pub struct PushdownConfig {
    #[serde(default = "default_pushdown_policy")]
    pub policy: String,
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

fn default_pushdown_policy() -> String {
    "cost_based".to_string()
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
pub struct ExecutionConfig {
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
}

fn default_batch_size() -> usize {
    8192
}

impl Default for ExecutionConfig {
    fn default() -> Self {
        Self {
            batch_size: default_batch_size(),
        }
    }
}

/// Phase 4 (docs/roadmap.md): distributed execution over Ballista.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DistributedConfig {
    /// Remote scheduler endpoint (`http://host:port`), or empty for a standalone deployment
    /// where the scheduler and `parallel_workers` executors run in this process.
    #[serde(default = "default_scheduler_url")]
    pub scheduler_url: String,
    /// Number of executor processes (standalone mode) or expected workers (remote mode — used
    /// only to budget the per-process source connection pools; see docs/roadmap.md Phase 4).
    #[serde(default = "default_workers")]
    pub workers: usize,
}

fn default_scheduler_url() -> String {
    String::new()
}

fn default_workers() -> usize {
    1
}

impl Default for DistributedConfig {
    fn default() -> Self {
        Self {
            scheduler_url: default_scheduler_url(),
            workers: default_workers(),
        }
    }
}

/// Parallel scan configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct ParallelScanConfig {
    #[serde(default = "default_parallel_strategy")]
    pub strategy: String,
    #[serde(default = "default_parallel_partitions")]
    pub partitions: usize,
    #[serde(default = "default_partition_column")]
    pub partition_column: String,
}

fn default_parallel_strategy() -> String {
    "none".to_string()
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
            strategy: default_parallel_strategy(),
            partitions: default_parallel_partitions(),
            partition_column: default_partition_column(),
        }
    }
}

/// Comparison operator for a structured filter predicate. Symbolic spellings match
/// the CLI shorthand (`--filter "amount>=100"`), so both forms share one vocabulary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
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
    pub fn as_str(&self) -> &'static str {
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
pub enum FilterEntry {
    Single(FilterInput),
    OrGroup(Vec<FilterInput>),
}

#[derive(Debug, Clone, Deserialize)]
pub struct JobConfig {
    pub job_id: String,
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
    pub sink: SinkConfig,
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
    pub fn resolved_table(&self) -> String {
        if self.table.contains('.') {
            self.table.clone()
        } else {
            format!("{}.{}", self.source.schema, self.table)
        }
    }

    pub fn from_file(path: impl AsRef<Path>) -> Result<Self, AppError> {
        let path = path.as_ref();

        let text = fs::read_to_string(path)
            .map_err(|e| AppError::Config(format!("cannot read {}: {e}", path.display())))?;

        let config: JobConfig = serde_json::from_str(&text)
            .map_err(|e| AppError::Config(format!("invalid config {}: {e}", path.display())))?;

        config.validate()?;

        Ok(config)
    }

    /// Reject degenerate values that cause silent misbehavior downstream: a zero
    /// `batch_size` makes every FETCH return zero rows, and zero pools/workers/
    /// partitions divide budgets by zero or scan nothing. An empty OR-group
    /// (`"filters": [[]]`) is equally degenerate — it has no defined
    /// AND/OR meaning — so it fails here rather than as zero rows downstream.
    pub fn validate(&self) -> Result<(), AppError> {
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

        assert_eq!(config.job_id, "orders_extract");
        assert_eq!(config.table, "orders");
        assert_eq!(config.resolved_table(), "public.orders");
        assert_eq!(config.source.password_env, "ORDERS_PG_PASSWORD");
        assert!(
            config
                .columns
                .as_ref()
                .unwrap()
                .contains(&"tags".to_string())
        );

        // Spec blocks absent from older files fall back to defaults.
        assert_eq!(config.pushdown.push, Vec::<String>::new());
        assert_eq!(config.distributed.workers, 2);
        assert_eq!(config.execution.batch_size, 8192);
        assert_eq!(config.parallel_scan.partition_column, "order_id");
    }

    #[test]
    fn test_from_file_missing_is_config_error() {
        let err = JobConfig::from_file("examples/configs/does-not-exist.json").unwrap_err();
        assert!(err.to_string().contains("cannot read"));
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
    fn test_from_file_full_config() {
        // The full-extraction spec must parse and validate (no watermark/incremental blocks).
        let config = JobConfig::from_file("examples/configs/full_extract.example.json").unwrap();
        assert_eq!(config.job_id, "orders_full");
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
            ("status", FilterOp::Eq, serde_json::json!("PAID")),
            ("amount", FilterOp::Gt, serde_json::json!(100)),
            (
                "updated_at",
                FilterOp::GtEq,
                serde_json::json!("2026-01-01T00:00:00Z"),
            ),
            ("user_id", FilterOp::GtEq, serde_json::json!(500)),
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
    }

    #[test]
    fn test_resolved_table() {
        let mut config = JobConfig {
            job_id: "test".to_string(),
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
            sink: SinkConfig {
                path: "./tmp".to_string(),
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
            job_id: "test".to_string(),
            table: "orders".to_string(),
            columns: None,
            filters: Vec::new(),
            source: SourceConfig {
                host: "localhost".to_string(),
                port: 5432,
                user: "postgres".to_string(),
                password_env: "REL_TEST_PASSWORD_ENV".to_string(),
                database: "db".to_string(),
                pool_max: 4,
                statement_timeout_ms: 1000,
                application_name: "test".to_string(),
                schema: "public".to_string(),
            },
            sink: SinkConfig {
                path: "./tmp".to_string(),
            },
            checkpoint: CheckpointConfig::default(),
            pushdown: PushdownConfig::default(),
            parallel_scan: ParallelScanConfig::default(),
            execution: ExecutionConfig::default(),
            distributed: DistributedConfig::default(),
        };

        unsafe {
            env::remove_var("REL_TEST_PASSWORD_ENV");
        }
        assert!(config.resolve_password().is_err());

        unsafe {
            env::set_var("REL_TEST_PASSWORD_ENV", "secret123");
        }
        assert_eq!(config.resolve_password().unwrap(), "secret123");
        unsafe {
            env::remove_var("REL_TEST_PASSWORD_ENV");
        }
    }
}
