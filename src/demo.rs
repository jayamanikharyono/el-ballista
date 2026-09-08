//! Demo pipeline.
//! demo.rs
//! Runs extraction -> filter -> transform -> drop/rename columns -> aggregate -> write-to-file
//! against a real Postgres table, without going through the checkpoint-driven CLI path
//! (`crate::cli`). This is a smoke test / illustration of the pieces wired together with
//! DataFusion doing the in-memory transform work, not a production entry point — it hardcodes
//! connection details the way the original `main.rs` demo did, and doesn't touch the checkpoint
//! store, so running it repeatedly always re-reads the same "everything so far" window rather
//! than advancing incrementally. `rel run --config <path>` (see `crate::cli`) is the real,
//! checkpoint-driven path.

use chrono::{DateTime, Utc};
use datafusion::functions_aggregate::expr_fn::{count, sum};
use datafusion::prelude::*;

use crate::errors::AppError;
use crate::extractor::postgres::PostgresExtractor;
use crate::sink;

pub async fn run() -> Result<(), AppError> {
    // 1. Extract. A wide-open window (epoch -> now) since there's no checkpoint driving this —
    // see the module note above.
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
            "orders",
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

    println!("extracted {} row(s)", batch.num_rows());

    let ctx = SessionContext::new();
    ctx.register_batch("orders_raw", batch)?;
    let df = ctx.table("orders_raw").await?;

    // 2. Filter.
    let df = df.filter(col("status").eq(lit("PAID")))?;

    // 3. Transform: derive a new column from existing ones.
    let df = df.with_column("amount_usd", col("amount") * lit(1.0_f64 / 15_800.0))?;

    // 4. Drop/rename columns: `select` with an explicit expression list drops anything not
    // named and renames via `.alias(...)` in the same pass — order_id/amount/created_at/
    // updated_at are dropped here, user_id is renamed to customer_id.
    let df = df.select(vec![
        col("status"),
        col("user_id").alias("customer_id"),
        col("amount_usd"),
    ])?;

    // 5. Aggregate.
    let df = df.aggregate(
        vec![col("status")],
        vec![
            sum(col("amount_usd")).alias("total_amount_usd"),
            count(col("customer_id")).alias("order_count"),
        ],
    )?;

    df.clone().show().await?;

    let batches = df.collect().await?;

    if batches.is_empty() || batches.iter().all(|b| b.num_rows() == 0) {
        println!("no rows after aggregation, nothing to write");
        return Ok(());
    }

    let schema = batches[0].schema();

    let window_dir = sink::write_window("./demo_output", schema, &batches, lo, hi)?;

    println!("wrote aggregated output to {}", window_dir.display());

    Ok(())
}
