//! End-to-end suite: Postgres → extractor → Arrow → Ballista → validation.
//!
//! The E2E tests run entirely in-process for the compute side: the Ballista
//! scheduler + executor run standalone in this same process
//! (`DistributedContext::standalone`, the same in-proc path `tests/pg_distributed.rs`
//! exercises). Postgres always comes from the Docker compose stack
//! (`tests/docker/compose.yaml` — `DATABASE_URL` overrides the default endpoint); no
//! embedded server, no external scheduler/workers, no silent skips.
//!
//! Filtering is caller-provided (full scan or explicit predicates) with direct SQL
//! as the oracle; split checkpointing is covered through `Pipeline::run` retry.
//!
//! ```bash
//! docker compose -f tests/docker/compose.yaml up -d --wait
//! cargo test --test e2e
//! # or: scripts/e2e.sh  (brings the stack up and down automatically)
//! ```

#[path = "common/mod.rs"]
mod common;

use common::{TEST_PASSWORD_ENV, TestDb};
use rust_ballista_extraction_layer::checkpoint::{
    CheckpointStore, JobKey, json_store::JsonCheckpointStore,
};
use std::sync::atomic::{AtomicU64, Ordering};

static JOB_COUNTER: AtomicU64 = AtomicU64::new(0);
use rust_ballista_extraction_layer::config::{
    CheckpointConfig, DistributedConfig, ExecutionConfig, FilterEntry, FilterInput, JobConfig,
    ParallelScanConfig, PushdownConfig, SinkConfig, SourceConfig,
};
use rust_ballista_extraction_layer::connector::postgres::PostgresConnector;
use rust_ballista_extraction_layer::distributed::DistributedContext;

struct E2E {
    db: TestDb,
}

impl E2E {
    async fn setup() -> Self {
        Self {
            db: TestDb::connect().await,
        }
    }

    fn job(&self, partitions: usize) -> JobConfig {
        // Unique pool per test invocation to avoid cross-test pool sharing when
        // 4 tests run sequentially in the same binary (global registry is per-process).
        // Budget = pool_max / workers, so step by workers*2 to guarantee unique budget.
        let n = JOB_COUNTER.fetch_add(1, Ordering::SeqCst);
        let pool_max = 32 + (n * 4) as u32;
        self.job_with_pool(partitions, pool_max)
    }

    fn job_with_pool(&self, partitions: usize, pool_max: u32) -> JobConfig {
        JobConfig {
            job_id: format!("e2e-{}", pool_max),
            table: "hostile".to_string(),
            columns: None,
            filters: Vec::new(),
            source: SourceConfig {
                host: self.db.host.clone(),
                port: self.db.port,
                user: self.db.user.clone(),
                password_env: TEST_PASSWORD_ENV.to_string(),
                database: self.db.database.clone(),
                pool_max,
                statement_timeout_ms: 300_000,
                application_name: format!("relex-e2e-{}", pool_max),
                schema: self.db.schema.clone(),
            },
            sink: SinkConfig {
                path: "/tmp/relex_e2e_sink".to_string(),
            },
            checkpoint: CheckpointConfig::default(),
            pushdown: PushdownConfig::default(),
            parallel_scan: ParallelScanConfig {
                strategy: "keyset".to_string(),
                partitions,
                partition_column: "id".to_string(),
            },
            execution: ExecutionConfig::default(),
            distributed: DistributedConfig {
                scheduler_url: String::new(),
                workers: 2,
            },
        }
    }

    /// In-process scheduler + executor (docs/roadmap.md Phase 4's "standalone" mode) — the
    /// whole plan-shipping path (codecs, partition distribution, budgeted pools) runs for
    /// real, with no separate scheduler/worker processes to stand up.
    async fn standalone(
        &self,
        partitions: usize,
    ) -> Result<DistributedContext, Box<dyn std::error::Error>> {
        let config = self.job(partitions);
        let ctx = DistributedContext::standalone(&config, partitions).await?;
        ctx.register_source(&config).await?;
        Ok(ctx)
    }

    /// Direct-from-Postgres expected ids for a predicate (the oracle).
    async fn expected_ids(&self, pred: &str) -> Result<Vec<i64>, Box<dyn std::error::Error>> {
        let sql = format!(
            "SELECT id FROM {}.hostile WHERE {} ORDER BY id",
            self.db.schema, pred
        );
        let rows: Vec<(i64,)> = sqlx::query_as(sqlx::AssertSqlSafe(sql.as_str()))
            .fetch_all(&self.db.pool)
            .await?;
        Ok(rows.into_iter().map(|r| r.0).collect())
    }

    async fn remote_ids(
        &self,
        ctx: &DistributedContext,
        select: &str,
    ) -> Result<Vec<i64>, Box<dyn std::error::Error>> {
        let batches = ctx.session.sql(select).await?.collect().await?;
        let mut ids = Vec::new();
        for b in &batches {
            ids.extend(common::int64_col(b, "id"));
        }
        ids.sort_unstable();
        Ok(ids)
    }
}

/// Always provisions a real, fully in-process E2E environment — never skips.
macro_rules! live {
    () => {
        E2E::setup().await
    };
}

#[tokio::test]
async fn e2e_full_extraction() -> Result<(), Box<dyn std::error::Error>> {
    let e = live!();
    let ctx = e.standalone(4).await?;
    let got = e.remote_ids(&ctx, "SELECT id FROM hostile").await?;
    assert_eq!(got, e.expected_ids("true").await?);
    assert_eq!(got.len(), 8);
    Ok(())
}

#[tokio::test]
async fn e2e_filtered_extraction() -> Result<(), Box<dyn std::error::Error>> {
    let e = live!();
    let ctx = e.standalone(2).await?;

    // Caller-provided range predicate: the extraction layer pushes it to the source,
    // and the result must match Postgres exactly (differential oracle).
    let got = e
        .remote_ids(&ctx, "SELECT id FROM hostile WHERE id > 4 AND id <= 8")
        .await?;
    assert_eq!(got, vec![5, 6, 7, 8]);
    assert_eq!(got, e.expected_ids("id > 4 AND id <= 8").await?);

    // Same range through the connector's filtered path (config filters → pushdown).
    let mut config = e.job(2);
    config.filters = vec![
        FilterEntry::Single(FilterInput::Shorthand("id>4".to_string())),
        FilterEntry::Single(FilterInput::Shorthand("id<=8".to_string())),
    ];
    let batches = PostgresConnector::from_config(config)
        .extract()
        .standalone()
        .collect()
        .await?;
    let mut ids = Vec::new();
    for b in &batches {
        ids.extend(common::int64_col(b, "id"));
    }
    ids.sort_unstable();
    assert_eq!(ids, got);
    Ok(())
}

#[tokio::test]
async fn e2e_split_checkpoint_retry_skips_completed() -> Result<(), Box<dyn std::error::Error>> {
    let e = live!();
    let mut config = e.job(2);
    let dir = std::env::temp_dir().join(format!(
        "relex_e2e_retry_{}_{}",
        std::process::id(),
        config.job_id
    ));
    let _ = std::fs::remove_dir_all(&dir);
    config.checkpoint.dir = dir.to_string_lossy().to_string();

    // First run completes and records per-split progress.
    let first = PostgresConnector::from_config(config.clone())
        .extract()
        .standalone()
        .run()
        .await?;
    assert_eq!(first.splits_completed, first.splits_total);
    assert!(first.splits_total >= 1);

    let store = JsonCheckpointStore::new(&dir)?;
    let key = JobKey::new(config.job_id.clone());
    let saved = store.read(&key).await?.expect("checkpoint recorded");
    assert!(saved.all_completed());

    // Second run skips every completed split and reports the same row count.
    let second = PostgresConnector::from_config(config)
        .extract()
        .standalone()
        .run()
        .await?;
    assert_eq!(second.splits_completed, second.splits_total);
    assert_eq!(second.rows_extracted, first.rows_extracted);

    let _ = std::fs::remove_dir_all(&dir);
    Ok(())
}

#[tokio::test]
async fn e2e_selective_extraction() -> Result<(), Box<dyn std::error::Error>> {
    let e = live!();
    let ctx = e.standalone(2).await?;
    // Narrow projection + pushed filter, checked against the database itself.
    let got = e
        .remote_ids(&ctx, "SELECT id FROM hostile WHERE nick = 'seven'")
        .await?;
    assert_eq!(got, e.expected_ids("nick = 'seven'").await?);
    assert_eq!(got, vec![7]);
    Ok(())
}

#[tokio::test]
async fn e2e_distributed_extraction() -> Result<(), Box<dyn std::error::Error>> {
    let e = live!();
    // Same result through a 4-way partitioned in-process scan as from Postgres directly:
    // distribution must not lose, duplicate, or corrupt rows.
    let ctx = e.standalone(4).await?;
    let got = e.remote_ids(&ctx, "SELECT id, amount FROM hostile").await?;
    assert_eq!(got, e.expected_ids("true").await?);
    Ok(())
}
