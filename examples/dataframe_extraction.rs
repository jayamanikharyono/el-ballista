//! DataFrame front end: the same job spec `el-ballista run` uses, driven through
//! `ExtractContext` instead of the CLI.
//!
//!   cargo run --example dataframe_extraction -- <config.json>
//!
//! Registers the table with a cost-aware provider (statistics + EXPLAIN cache feed the
//! pushdown decisions) and collects the filtered projection as Arrow RecordBatches.
//! Filtering is caller-provided — full extraction or explicit predicates, never watermarks.

use datafusion::prelude::{col, lit};
use el_ballista::config::JobConfig;
use el_ballista::connector::postgres::ExtractContext;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Route the `log` facade to stderr (+ optional --log-file / EL_BALLISTA_LOG_FILE).
    // Set RUST_LOG=debug (or --log-level debug) to log every generated SQL query.
    el_ballista::logging::init_from_env_and_args();

    let config_path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "examples/configs/extract.example.json".to_string());
    let config = JobConfig::from_file(&config_path)?;

    let ctx = ExtractContext::from_config(config).await?;

    // Same builder shape as docs/roadmap.md Phase 2, minus watermarks.
    let batches = ctx
        .source("postgres", "public.payment")
        .await?
        .filter(col("customer_id").gt_eq(lit(300i64)))?
        .select(vec![col("payment_id"), col("amount")])?
        .with_column("amount_x2", col("amount") * lit(2.0))?
        .limit(0, Some(1000))?
        .collect()
        .await?;

    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    println!(
        "dataframe extraction: {rows} row(s) in {} batch(es)",
        batches.len()
    );

    // SQL entry point against the same registered sources.
    let sql_rows = ctx
        .sql("SELECT COUNT(*) AS n FROM public.payment")
        .await?
        .collect()
        .await?;
    println!("sql entry point: {} batch(es)", sql_rows.len());

    Ok(())
}
