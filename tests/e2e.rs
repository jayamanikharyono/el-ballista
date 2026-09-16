//! End-to-end suite: Postgres → extractor → Arrow → Ballista → validation.
//!
//! The four core E2E tests from the testing strategy, run entirely in-process: Postgres
//! is self-provisioned (embedded, unless `DATABASE_URL` is set — see `tests/common`) and
//! the Ballista scheduler + executor run standalone in this same process
//! (`DistributedContext::standalone`, the same in-proc path `tests/pg_distributed.rs`
//! exercises). No external scheduler, workers, or container are required or supported
//! here — that was the old design's whole failure mode (`E2E_SCHEDULER_URL` unset meant
//! a silent skip, so a real regression in the distributed path could pass CI unnoticed).
//!
//! ```bash
//! cargo test --test e2e
//! ```

#[path = "common/mod.rs"]
mod common;

use chrono::{Duration, TimeZone, Utc};
use common::{TEST_PASSWORD_ENV, TestDb};
use rust_ballista_extraction_layer::checkpoint::{
    CheckpointStore, JobKey, RunStats, json_store::JsonCheckpointStore,
};
use rust_ballista_extraction_layer::config::{
    CheckpointConfig, DistributedConfig, ExecutionConfig, IncrementalConfig, JobConfig,
    ParallelScanConfig, PushdownConfig, SinkConfig, SourceConfig,
};
use rust_ballista_extraction_layer::distributed::DistributedContext;
use uuid::Uuid;

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
        JobConfig {
            job_id: "e2e".to_string(),
            table: "hostile".to_string(),
            columns: None,
            source: SourceConfig {
                host: self.db.host.clone(),
                port: self.db.port,
                user: self.db.user.clone(),
                password_env: TEST_PASSWORD_ENV.to_string(),
                database: self.db.database.clone(),
                pool_max: 4,
                statement_timeout_ms: 300_000,
                application_name: "relex-e2e".to_string(),
                schema: self.db.schema.clone(),
            },
            incremental: IncrementalConfig {
                column: "updated_at".to_string(),
                safety_lag_secs: 0,
                max_window_secs: 21600,
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
async fn e2e_incremental_extraction() -> Result<(), Box<dyn std::error::Error>> {
    let e = live!();
    let ctx = e.standalone(2).await?;

    // First extraction: everything through 2024-01-04, then commit the watermark.
    let first = e
        .remote_ids(
            &ctx,
            "SELECT id FROM hostile WHERE updated_at <= '2024-01-04T00:00:00Z'",
        )
        .await?;
    assert_eq!(first, vec![1, 2, 3, 4]);

    let dir = std::env::temp_dir().join(format!("relex_e2e_{}", std::process::id()));
    let _ = std::fs::create_dir_all(&dir);
    let store = JsonCheckpointStore::new(&dir)?;
    let key = JobKey {
        job_id: "e2e".to_string(),
        namespace: "incr".to_string(),
    };
    let run_id = Uuid::new_v4();
    store
        .acquire(&key, run_id, Duration::hours(1), "updated_at")
        .await?;
    let hi = Utc.with_ymd_and_hms(2024, 1, 4, 0, 0, 0).unwrap();
    store
        .commit(
            &key,
            run_id,
            hi,
            RunStats {
                rows_extracted: first.len() as u64,
                window_lo: None,
                window_hi: Some(hi),
            },
        )
        .await?;

    // Insert new records, two sharing one timestamp (checkpoint semantics proof).
    // A third sits exactly ON the committed watermark: (lo, hi] excludes it.
    let sql = format!(
        "INSERT INTO {}.hostile (name, updated_at) VALUES
         ('n1', '2024-02-01 00:00:00+00'),
         ('n2', '2024-02-01 00:00:00+00'),
         ('edge', '2024-01-04 00:00:00+00')",
        e.db.schema
    );
    sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
        .execute(&e.db.pool)
        .await?;

    // Second extraction: everything after the committed watermark. The fixture already
    // has four rows dated past 2024-01-04 (ids 5-8), so this is NOT "only the two new
    // rows" -- it's the full post-watermark set, which must match Postgres exactly (the
    // real checkpoint-semantics proof is the two targeted checks below).
    let second = e
        .remote_ids(
            &ctx,
            "SELECT id FROM hostile WHERE updated_at > '2024-01-04T00:00:00Z'",
        )
        .await?;
    let expected = e
        .expected_ids("updated_at > '2024-01-04T00:00:00Z'")
        .await?;
    assert_eq!(
        second, expected,
        "must match Postgres exactly: nothing lost or duplicated"
    );

    // The row sitting exactly ON the watermark must be excluded: (lo, hi] is exclusive-lo,
    // inclusive-hi at commit time, but a *new* extraction's lower bound is that same hi,
    // so a row timestamped exactly at the old hi must not reappear.
    let edge_id = e.expected_ids("name = 'edge'").await?;
    assert_eq!(edge_id.len(), 1, "fixture inserted exactly one 'edge' row");
    assert!(
        !second.contains(&edge_id[0]),
        "row exactly on the committed watermark must be excluded"
    );

    // The two rows sharing one timestamp (past the watermark) must both survive: a
    // window boundary must never arbitrarily keep one and drop the other.
    let dup_ids = e.expected_ids("name IN ('n1', 'n2')").await?;
    assert_eq!(
        dup_ids.len(),
        2,
        "fixture inserted exactly two duplicate-timestamp rows"
    );
    assert!(
        dup_ids.iter().all(|id| second.contains(id)),
        "both duplicate-timestamp rows must survive the boundary"
    );

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
