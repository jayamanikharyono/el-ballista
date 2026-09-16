//! Demo pipeline with Phase 1 and Phase 2 features.
//! demo.rs
//! Demonstrates:
//! - Phase 1: extraction, filtering, aggregation, Parquet output with metadata columns.
//! - Phase 2: collation-aware pushdown, statistics collection, cost-based decision framework,
//!   parallel scan configuration, and the DataFrame builder API concepts.
//!
//! This is a smoke test / illustration of the pieces wired together with DataFusion doing
//! the in-memory transform work, not a production entry point.

use chrono::{DateTime, Utc};
use datafusion::functions_aggregate::expr_fn::{count, sum};
use datafusion::prelude::*;
use std::sync::Arc;

use rust_ballista_extraction_layer::config::{
    CheckpointConfig, DistributedConfig, ExecutionConfig, IncrementalConfig, JobConfig,
    ParallelScanConfig, PushdownConfig, SinkConfig, SourceConfig,
};
use rust_ballista_extraction_layer::connector::postgres::PostgresExtractor;
use rust_ballista_extraction_layer::errors::AppError;
use rust_ballista_extraction_layer::pushdown::dialect::{PostgresDialect, SqlDialect};
use rust_ballista_extraction_layer::pushdown::stats::StatisticsCollector;
use rust_ballista_extraction_layer::types::ColumnMetadata;

pub async fn run() -> Result<(), AppError> {
    println!("\n=== Phase 1 & Phase 2 Feature Demonstration ===\n");

    // Phase 1: Extract with schema support
    println!("► Phase 1: Extraction with schema support");
    let extractor = PostgresExtractor::connect(
        "localhost",
        5432,
        "postgres",
        "postgres",
        "app",
        4,
        30_000,
        "rust-extract-layer-demo",
    )
    .await?;

    let lo = DateTime::<Utc>::from_timestamp(0, 0).unwrap();
    let hi = Utc::now();

    let batch = extractor
        .extract_incremental_window(
            "public.orders", // Now uses explicit schema
            Some(vec![
                "order_id",
                "user_id",
                "status",
                "amount",
                "created_at",
                "updated_at",
            ]),
            "updated_at",
            lo,
            hi,
        )
        .await?;

    let num_rows_extracted = batch.num_rows();
    println!(
        "  ✓ Extracted {} row(s) from public.orders",
        num_rows_extracted
    );

    // Phase 2: Demonstrate SqlDialect (collation-aware fidelity)
    println!("\n► Phase 2: SqlDialect and collation-aware fidelity");
    let dialect = PostgresDialect;

    let text_col_c = ColumnMetadata {
        column_name: "status".to_string(),
        data_type: "text".to_string(),
        is_nullable: true,
        numeric_precision: None,
        numeric_scale: None,
        udt_name: None,
        collation_name: Some("C".to_string()),
    };

    let text_col_citext = ColumnMetadata {
        column_name: "email".to_string(),
        data_type: "citext".to_string(),
        is_nullable: true,
        numeric_precision: None,
        numeric_scale: None,
        udt_name: None,
        collation_name: Some("en_US".to_string()),
    };

    let fidelity_c = dialect.column_literal_fidelity(&text_col_c, true, false);
    let fidelity_citext = dialect.column_literal_fidelity(&text_col_citext, true, false);

    println!(
        "  ✓ C-collation string comparison: {:?} (Exact)",
        fidelity_c
    );
    println!(
        "  ✓ citext string comparison: {:?} (Inexact)",
        fidelity_citext
    );
    println!(
        "  ✓ Identifier quoting: {}",
        dialect.quote_ident("my_column")
    );
    println!("  ✓ Placeholder generation: {}", dialect.placeholder(1));

    // Phase 2: Statistics collection framework
    println!("\n► Phase 2: Statistics collection (framework)");
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

    // Phase 1 & 2: DataFrame operations and metadata columns
    println!("\n► Phase 1 & 2: DataFusion transformation with Parquet output");
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

    println!("\n► Phase 1 & 2: Data ready for output");
    println!(
        "  - {} rows collected via DataFusion",
        batches.iter().map(|b| b.num_rows()).sum::<usize>()
    );
    println!("  - Use DataFusion writers (ParquetWriter, CSVWriter) or Ballista for sink");
    println!("  - This project extracts to Arrow only");
    println!("  - Sink functionality delegated to external libraries");

    // Phase 2: Demonstrate config framework
    println!("\n► Phase 2: Configuration framework");
    let _config = JobConfig {
        job_id: "demo_orders".to_string(),
        table: "orders".to_string(),
        columns: None,
        source: SourceConfig {
            host: "localhost".to_string(),
            port: 5432,
            user: "postgres".to_string(),
            password_env: "PGPASSWORD".to_string(),
            database: "app".to_string(),
            pool_max: 8,
            statement_timeout_ms: 300_000,
            application_name: "rust-extract-layer".to_string(),
            schema: "public".to_string(),
        },
        incremental: IncrementalConfig {
            column: "updated_at".to_string(),
            safety_lag_secs: 300,
            max_window_secs: 6 * 3600,
        },
        sink: SinkConfig {
            path: "./demo_output".to_string(),
        },
        checkpoint: CheckpointConfig::default(),
        pushdown: PushdownConfig {
            policy: "cost_based".to_string(),
            deny: vec![],
            push: vec![],
            max_source_cost: 50_000,
            keep_threshold: 0.30,
            statistics_ttl_secs: 900,
        },
        parallel_scan: ParallelScanConfig {
            strategy: "none".to_string(),
            partitions: 1,
            partition_column: "order_id".to_string(),
        },
        execution: ExecutionConfig { batch_size: 8192 },
        distributed: DistributedConfig::default(),
    };

    println!("  ✓ Pushdown policy: cost_based");
    println!("    - max_source_cost: 50,000 units");
    println!("    - keep_threshold: 0.30 (push if selectivity < 30%)");
    println!("    - statistics_ttl: 900 seconds");
    println!("  ✓ Parallel scan: disabled (strategy=none, partitions=1)");
    println!("    - Ready for Phase 2.5: keyset or ctid strategies");

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
