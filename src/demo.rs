//! Demo pipeline with extraction, pushdown, and DataFusion transforms.
//! demo.rs
//! Demonstrates:
//! - Full extraction with schema support
//! - Filtered extraction (caller-provided predicates)
//! - Column-kind-aware pushdown translation, statistics collection, cost-based decisions,
//!   parallel scan configuration, and the DataFrame builder API concepts.
//!
//! This is a smoke test / illustration of the pieces wired together with DataFusion doing
//! the in-memory transform work, not a production entry point. Run it with `el-ballista demo`; it
//! connects to the dvdrental demo database (`postgres@localhost:5432/test`, started by
//! `docker compose -f tests/docker/compose.yaml up -d --wait`) with the password from the environment variable
//! named by [`DEMO_PASSWORD_ENV`] (never a hard-coded password).

/// Environment variable holding the demo database password.
pub const DEMO_PASSWORD_ENV: &str = "PGPASSWORD";

use datafusion::functions_aggregate::expr_fn::{count, sum};
use datafusion::prelude::*;
use std::sync::Arc;

use el_ballista::config::{
    CheckpointConfig, DistributedConfig, ExecutionConfig, FilterEntry, FilterInput, JobConfig,
    JobId, ParallelScanConfig, ParallelStrategy, PushdownConfig, PushdownPolicy, SourceConfig,
};
use el_ballista::connector::postgres::PostgresExtractor;
use el_ballista::connector::postgres::dialect::PostgresDialect;
use el_ballista::connector::postgres::inline_sql::PredicateInlineSql;
use el_ballista::connector::postgres::stats::StatisticsCollector;
use el_ballista::errors::AppError;
use el_ballista::pushdown::dialect::SqlDialect;
use el_ballista::pushdown::{ColumnKind, ColumnKinds, translate_with};

pub(crate) async fn run() -> Result<(), AppError> {
    println!("\n=== Extraction Feature Demonstration ===\n");

    let password = std::env::var(DEMO_PASSWORD_ENV).map_err(|_| {
        AppError::Config(format!(
            "`el-ballista demo` reads the database password from ${DEMO_PASSWORD_ENV}; set it first"
        ))
    })?;

    // Full extraction with schema support
    println!("► Extraction with schema support");
    let extractor = PostgresExtractor::connect(
        "localhost",
        5432,
        "postgres",
        &password,
        "test",
        4,
        30_000,
        "el-ballista-demo",
    )
    .await
    .map_err(|source| AppError::SourceConnect {
        target: "localhost:5432/test".to_string(),
        source,
    })?;

    let batch = extractor
        .extract_full_table(
            "public.payment",
            Some(vec![
                "payment_id",
                "customer_id",
                "staff_id",
                "amount",
                "payment_date",
            ]),
        )
        .await?;

    let num_rows_extracted = batch.num_rows();
    println!(
        "  ✓ Extracted {} row(s) from public.payment (full scan)",
        num_rows_extracted
    );

    // Pushdown translation: how a filter reaches the source
    println!("\n► Pushdown translation (column-kind aware)");
    let dialect = PostgresDialect;
    let kinds = ColumnKinds::from([
        (
            "status".to_string(),
            ColumnKind::Text {
                bytewise_collation: false,
            },
        ),
        ("amount".to_string(), ColumnKind::Float),
    ]);
    for (label, expr) in [
        ("status = 'PAID'", col("status").eq(lit("PAID"))),
        ("amount = 10.0", col("amount").eq(lit(10.0_f64))),
        ("amount < 10.0", col("amount").lt(lit(10.0_f64))),
    ] {
        match translate_with(&expr, &kinds) {
            Some((fidelity, predicate)) => {
                println!("  ✓ {label}: {fidelity:?} -> {}", predicate.render_inline())
            }
            None => println!("  ✓ {label}: kept in Arrow (no exact or superset source form)"),
        }
    }
    println!(
        "  ✓ Identifier quoting: {}",
        dialect.quote_ident("my_column")
    );
    println!("  ✓ Placeholder generation: {}", dialect.placeholder(1));

    // Statistics collection framework
    println!("\n► Statistics collection (framework)");
    let pool = Arc::new(extractor.pool().clone());
    let stats_collector = StatisticsCollector::new(pool.clone(), 900);

    // Attempt to collect statistics (may fail if pg_stats is empty, but demonstrates the framework)
    match stats_collector.get_statistics("public", "payment").await {
        Ok(stats) => {
            println!("  ✓ Statistics collected for public.payment");
            println!("    - Estimated rows: {:.0}", stats.row_count_estimate);
            println!("    - Table size: {} bytes", stats.table_size_bytes);
            println!("    - Columns with stats: {}", stats.columns.len());
        }
        Err(_) => {
            println!("  ℹ Statistics collection attempted (pg_stats may be empty)");
        }
    }

    // DataFrame operations and metadata columns
    println!("\n► DataFusion transformation with Parquet output");
    let ctx = SessionContext::new();
    ctx.register_batch("payment_raw", batch)?;
    let df = ctx.table("payment_raw").await?;

    // 2. Filter.
    let df = df.filter(col("customer_id").gt_eq(lit(300i64)))?;
    println!("  ✓ Filtered to customer_id >= 300");

    // 3. Transform: derive a new column from existing ones.
    let df = df.with_column("amount_cents", col("amount") * lit(100i64))?;
    println!("  ✓ Added derived column: amount_cents");

    // 4. Drop/rename columns.
    let df = df.select(vec![
        col("staff_id"),
        col("customer_id").alias("customer"),
        col("amount_cents"),
    ])?;
    println!("  ✓ Projected columns: staff_id, customer, amount_cents");

    // 5. Aggregate.
    let df = df.aggregate(
        vec![col("staff_id")],
        vec![
            sum(col("amount_cents")).alias("total_amount_cents"),
            count(col("customer")).alias("payment_count"),
        ],
    )?;
    println!("  ✓ Aggregated by staff_id");

    df.clone().show().await?;

    let batches = df.collect().await?;

    if batches.is_empty() || batches.iter().all(|b| b.num_rows() == 0) {
        println!("\n  ℹ No rows after aggregation, nothing to write");
        return Ok(());
    }

    println!("\n► Data ready for output");
    println!(
        "  - {} rows collected via DataFusion",
        batches.iter().map(|b| b.num_rows()).sum::<usize>()
    );
    println!("  - Use DataFusion writers (ParquetWriter, CSVWriter) or Ballista for sink");
    println!("  - This project extracts to Arrow only");
    println!("  - Sink functionality delegated to external libraries");

    // Demonstrate config framework
    println!("\n► Configuration framework");
    let _config = JobConfig {
        job_id: JobId::new("demo_payment")?,
        table: "payment".to_string(),
        columns: None,
        filters: vec![FilterEntry::Single(FilterInput::Shorthand(
            "customer_id>=300".to_string(),
        ))],
        source: SourceConfig {
            host: "localhost".to_string(),
            port: 5432,
            user: "postgres".to_string(),
            password_env: DEMO_PASSWORD_ENV.to_string(),
            database: "test".to_string(),
            pool_max: 8,
            statement_timeout_ms: 300_000,
            application_name: "el-ballista-demo".to_string(),
            schema: "public".to_string(),
        },
        checkpoint: CheckpointConfig::default(),
        pushdown: PushdownConfig {
            policy: PushdownPolicy::CostBased,
            deny: vec![],
            push: vec![],
            max_source_cost: 50_000,
            keep_threshold: 0.30,
            statistics_ttl_secs: 900,
        },
        parallel_scan: ParallelScanConfig {
            strategy: ParallelStrategy::None,
            partitions: 1,
            partition_column: "payment_id".to_string(),
        },
        execution: ExecutionConfig::default(),
        distributed: DistributedConfig::default(),
    };

    println!("  ✓ Pushdown policy: cost_based");
    println!("    - max_source_cost: 50,000 units");
    println!("    - keep_threshold: 0.30 (push if selectivity < 30%)");
    println!("    - statistics_ttl: 900 seconds");
    println!("  ✓ Parallel scan: disabled (strategy=none, partitions=1)");
    println!("  ✓ Filtered extraction via caller-provided predicates (e.g. customer_id>=300)");

    println!("\n=== Demo complete ===\n");
    println!("Output:");
    println!(
        "  - {} rows extracted from public.payment",
        num_rows_extracted
    );
    println!(
        "  - {} rows after filter+aggregate",
        batches.iter().map(|b| b.num_rows()).sum::<usize>()
    );
    println!("  - Ready for sink (DataFusion/Ballista/orchestrator)");

    Ok(())
}
