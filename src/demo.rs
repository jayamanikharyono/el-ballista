//! `el-ballista demo`: a short tour of the public API on the dvdrental demo database.
//! demo.rs
//! On `public.payment` it shows:
//! - a job config built in code (table, columns, a structured filter);
//! - each filter's pushdown decision (`PostgresConnector::explain_filters`);
//! - a standalone extraction streamed batch by batch, so memory stays bounded;
//! - DataFusion processing over the registered table (`register_table`): a filter and an
//!   aggregate, planned (and pushed down where exact) by DataFusion.
//!
//! It connects to `postgres@localhost:5432/test` (started by
//! `docker compose -f tests/docker/compose.yaml up -d --wait`) with the password from the
//! environment variable named by [`DEMO_PASSWORD_ENV`] (never a hard-coded password). Nothing
//! is written anywhere: this layer hands out Arrow batches.

/// Environment variable holding the demo database password.
pub const DEMO_PASSWORD_ENV: &str = "PGPASSWORD";

use datafusion::functions_aggregate::expr_fn::{count, sum};
use datafusion::prelude::*;
use futures::TryStreamExt;

use el_ballista::config::{
    CheckpointConfig, DistributedConfig, ExecutionConfig, FilterEntry, FilterInput, JobConfig,
    JobId, ParallelScanConfig, ParallelStrategy, PushdownConfig, PushdownPolicy, SourceConfig,
};
use el_ballista::connector::postgres::{PostgresConnector, register_table};
use el_ballista::errors::AppError;

/// The demo job: `public.payment`, five columns, `customer_id >= 300`, cost-based pushdown.
fn demo_config() -> Result<JobConfig, AppError> {
    Ok(JobConfig {
        job_id: JobId::new("demo_payment")?,
        table: "payment".to_string(),
        columns: Some(
            [
                "payment_id",
                "customer_id",
                "staff_id",
                "amount",
                "payment_date",
            ]
            .map(String::from)
            .to_vec(),
        ),
        filters: vec![FilterEntry::Single(FilterInput::Shorthand(
            "customer_id>=300".to_string(),
        ))],
        source: SourceConfig {
            host: "localhost".to_string(),
            port: 5432,
            user: "postgres".to_string(),
            password_env: DEMO_PASSWORD_ENV.to_string(),
            database: "test".to_string(),
            pool_max: 4,
            statement_timeout_ms: 30_000,
            application_name: "el-ballista-demo".to_string(),
            schema: "public".to_string(),
        },
        checkpoint: CheckpointConfig::default(),
        pushdown: PushdownConfig {
            policy: PushdownPolicy::CostBased,
            ..PushdownConfig::default()
        },
        parallel_scan: ParallelScanConfig {
            strategy: ParallelStrategy::None,
            partitions: 1,
            partition_column: "payment_id".to_string(),
        },
        execution: ExecutionConfig::default(),
        distributed: DistributedConfig::default(),
    })
}

pub(crate) async fn run() -> Result<(), AppError> {
    println!("\n=== el-ballista demo: public.payment on the dvdrental database ===\n");
    if std::env::var(DEMO_PASSWORD_ENV).is_err() {
        return Err(AppError::Config(format!(
            "`el-ballista demo` reads the database password from ${DEMO_PASSWORD_ENV}; set it first"
        )));
    }
    let config = demo_config()?;
    let connector = PostgresConnector::from_config(config.clone())?;

    println!("► Pushdown decisions (policy: cost_based)");
    for decision in connector.explain_filters().await? {
        println!(
            "  {} -> {:?} ({})",
            decision.filter, decision.pushdown, decision.reason
        );
    }

    println!("\n► Standalone extraction, streamed batch by batch");
    let mut stream = connector.extract().standalone().stream().await?;
    let (mut batches, mut rows) = (0usize, 0usize);
    while let Some(batch) = stream.try_next().await? {
        batches += 1;
        rows += batch.num_rows();
    }
    println!(
        "  {rows} row(s) with customer_id >= 300, in {batches} batch(es) of at most {} rows",
        config.execution.batch_size
    );

    println!("\n► DataFusion over the registered table: total amount per staff member");
    let ctx = SessionContext::new();
    register_table(&ctx, &config).await?;
    ctx.table(config.table.as_str())
        .await?
        .filter(col("customer_id").gt_eq(lit(300i64)))?
        .aggregate(
            vec![col("staff_id")],
            vec![
                sum(col("amount")).alias("total_amount"),
                count(col("payment_id")).alias("payments"),
            ],
        )?
        .sort(vec![col("staff_id").sort(true, false)])?
        .show()
        .await?;

    println!("\n=== done: nothing was written; this layer hands out Arrow batches ===\n");
    Ok(())
}
