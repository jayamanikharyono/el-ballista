//! Demo pipeline with extraction, pushdown, and DataFusion transforms.
//! demo.rs
//! Demonstrates:
//! - Full extraction with schema support
//! - Filtered extraction (caller-provided predicates)
//! - Column-kind-aware pushdown translation, statistics collection, cost-based decisions,
//!   parallel scan configuration, and the DataFrame builder API concepts.
//!
//! This is a smoke test / illustration of the pieces wired together with DataFusion doing
//! the in-memory transform work, not a production entry point. Run it with `rel demo`; it
//! connects to `postgres@localhost:5432/app` with the password from the environment variable
//! named by [`DEMO_PASSWORD_ENV`] (never a hard-coded password).

/// Environment variable holding the demo database password.
pub const DEMO_PASSWORD_ENV: &str = "PGPASSWORD";

use datafusion::functions_aggregate::expr_fn::{count, sum};
use datafusion::prelude::*;
use std::sync::Arc;

use rust_ballista_extraction_layer::config::{
    CheckpointConfig, DistributedConfig, ExecutionConfig, FilterEntry, FilterInput, JobConfig,
    JobId, ParallelScanConfig, ParallelStrategy, PushdownConfig, PushdownPolicy, SourceConfig,
};
use rust_ballista_extraction_layer::connector::postgres::PostgresExtractor;
use rust_ballista_extraction_layer::connector::postgres::dialect::PostgresDialect;
use rust_ballista_extraction_layer::connector::postgres::inline_sql::PredicateInlineSql;
use rust_ballista_extraction_layer::connector::postgres::stats::StatisticsCollector;
use rust_ballista_extraction_layer::errors::AppError;
use rust_ballista_extraction_layer::pushdown::dialect::SqlDialect;
use rust_ballista_extraction_layer::pushdown::{ColumnKind, ColumnKinds, translate_with};

pub(crate) async fn run() -> Result<(), AppError> {
    println!("\n=== Extraction Feature Demonstration ===\n");

    let password = std::env::var(DEMO_PASSWORD_ENV).map_err(|_| {
        AppError::Config(format!(
            "`rel demo` reads the database password from ${DEMO_PASSWORD_ENV}; set it first"
        ))
    })?;

    // Full extraction with schema support
    println!("► Extraction with schema support");
    let extractor = PostgresExtractor::connect(
        "localhost",
        5432,
        "postgres",
        &password,
        "app",
        4,
        30_000,
        "rust-extract-layer-demo",
    )
    .await
    .map_err(|source| AppError::SourceConnect {
        target: "localhost:5432/app".to_string(),
        source,
    })?;

    let batch = extractor
        .extract_full_table(
            "public.orders", // Now uses explicit schema
            Some(vec![
                "order_id",
                "user_id",
                "status",
                "amount",
                "created_at",
                "updated_at",
            ]),
        )
        .await?;

    let num_rows_extracted = batch.num_rows();
    println!(
        "  ✓ Extracted {} row(s) from public.orders (full scan)",
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
    match stats_collector.get_statistics("public", "orders").await {
        Ok(stats) => {
            println!("  ✓ Statistics collected for public.orders");
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
    ctx.register_batch("orders_raw", batch)?;
    let df = ctx.table("orders_raw").await?;

    // 2. Filter.
    let df = df.filter(col("status").eq(lit("PAID")))?;
    println!("  ✓ Filtered to status='PAID'");

    // 3. Transform: derive a new column from existing ones.
    let df = df.with_column("amount_usd", col("amount") * lit(1.0_f64 / 15_800.0))?;
    println!("  ✓ Added derived column: amount_usd");

    // 4. Drop/rename columns.
    let df = df.select(vec![
        col("status"),
        col("user_id").alias("customer_id"),
        col("amount_usd"),
    ])?;
    println!("  ✓ Projected columns: status, customer_id, amount_usd");

    // 5. Aggregate.
    let df = df.aggregate(
        vec![col("status")],
        vec![
            sum(col("amount_usd")).alias("total_amount_usd"),
            count(col("customer_id")).alias("order_count"),
        ],
    )?;
    println!("  ✓ Aggregated by status");

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
        job_id: JobId::new("demo_orders")?,
        table: "orders".to_string(),
        columns: None,
        filters: vec![FilterEntry::Single(FilterInput::Shorthand(
            "status=PAID".to_string(),
        ))],
        source: SourceConfig {
            host: "localhost".to_string(),
            port: 5432,
            user: "postgres".to_string(),
            password_env: DEMO_PASSWORD_ENV.to_string(),
            database: "app".to_string(),
            pool_max: 8,
            statement_timeout_ms: 300_000,
            application_name: "rust-extract-layer".to_string(),
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
            partition_column: "order_id".to_string(),
        },
        execution: ExecutionConfig::default(),
        distributed: DistributedConfig::default(),
    };

    println!("  ✓ Pushdown policy: cost_based");
    println!("    - max_source_cost: 50,000 units");
    println!("    - keep_threshold: 0.30 (push if selectivity < 30%)");
    println!("    - statistics_ttl: 900 seconds");
    println!("  ✓ Parallel scan: disabled (strategy=none, partitions=1)");
    println!("  ✓ Filtered extraction via caller-provided predicates (e.g. status=PAID)");

    println!("\n=== Demo complete ===\n");
    println!("Output:");
    println!(
        "  - {} rows extracted from public.orders",
        num_rows_extracted
    );
    println!(
        "  - {} rows after filter+aggregate",
        batches.iter().map(|b| b.num_rows()).sum::<usize>()
    );
    println!("  - Ready for sink (DataFusion/Ballista/orchestrator)");

    Ok(())
}
