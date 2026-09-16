//! End-to-End Example (distributed full load).
//!
//! Complete extraction → transformation → Parquet pipeline where the scan itself runs on
//! the Ballista cluster: each executor reads only its own keyset partition of the table,
//! opening just its `pool_max / workers` share of source connections.
//!
//! Unlike the incremental job specs, this loads the **entire table** (no watermark window)
//! — the full-load counterpart to `distributed_extraction.rs`.
//!
//! Pipeline:
//! ```text
//! PostgreSQL ──► Ballista scan (one keyset partition per executor)
//!     ↓
//! DataFusion SQL (filter + transform, planned once, executed distributed)
//!     ↓
//! collected Arrow RecordBatches ──► Parquet file
//! ```
//!
//! Usage:
//! ```bash
//! # standalone (scheduler + in-process executor, no cluster needed):
//! cargo run --example end_to_end -- <config.json> [workers]
//!
//! # remote (against a running `rel scheduler` + `rel worker` cluster):
//! cargo run --example end_to_end -- <config.json> [workers] <scheduler-url>
//! ```
//!
//! The password comes from the config's `password_env` variable — export it first,
//! e.g. `export ORDERS_PG_PASSWORD=...`.

use arrow::datatypes::SchemaRef;
use chrono::Utc;
use parquet::arrow::ArrowWriter;
use rust_ballista_extraction_layer::config::JobConfig;
use rust_ballista_extraction_layer::distributed::DistributedContext;
use std::fs;
use std::fs::File;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("═══════════════════════════════════════════════════════════");
    println!("  End-to-End Example (distributed full load)");
    println!("  Full Scan → Transformation → Parquet Pipeline");
    println!("═══════════════════════════════════════════════════════════\n");

    // 1. Setup
    println!("► Step 1: Initialize");
    fs::create_dir_all("output")?;
    fs::create_dir_all(".checkpoints")?;

    let config_path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "examples/configs/extract.example.json".to_string());
    let workers: usize = std::env::args()
        .nth(2)
        .map(|s| s.parse().unwrap())
        .unwrap_or(2);
    let scheduler_url = std::env::args().nth(3);

    let config = JobConfig::from_file(&config_path)?;
    println!("  ✓ Output directory: output/");
    println!("  ✓ Checkpoint store: .checkpoints/");
    println!("  ✓ Job spec: {config_path} (workers={workers})");

    // 2. Connect to the Ballista cluster and register the source.
    println!("\n► Step 2: Connect via distributed context");
    let ctx = match scheduler_url.as_deref() {
        Some(url) => DistributedContext::remote(&config, url, workers).await?,
        None => DistributedContext::standalone(&config, workers).await?,
    };
    ctx.register_source(&config).await?;

    println!(
        "  ✓ Scheduler: {}",
        scheduler_url
            .as_deref()
            .unwrap_or("in-process (standalone)")
    );
    println!("  ✓ Table registered: {}", config.resolved_table());
    println!("  ✓ Scan splits into {workers} keyset partition(s)");

    // 3. Full load: no watermark filter — the whole table, one partition per executor.
    // The scan below (step 4) reads every row; the watermark machinery is bypassed
    // entirely, so this is a full load rather than an incremental window.
    println!("\n► Step 3: Full-load scan (no incremental window)");
    println!("  ✓ No watermark filter — every row, one keyset partition per executor");

    // 4. Apply transformations via SQL (planned once, executed distributed).
    println!("\n► Step 4: Apply SQL transformations");

    println!("  Query:");
    println!("    SELECT order_id, amount, status");
    println!("    FROM {}", config.table);
    println!("    WHERE status = 'PAID'");

    let table_name = config.table.clone();
    let result_df = ctx
        .session
        .sql(&format!(
            "SELECT order_id, amount, status FROM {table_name} \
             WHERE status = 'PAID' ORDER BY order_id"
        ))
        .await?;

    println!("  ✓ SQL transformation planned for distributed execution");

    // 5. Collect results and show sample.
    //
    // The scan + sort + filter run distributed on the cluster; `collect` streams the
    // resulting Arrow batches back to this process. That split is deliberate, not just
    // convenient: see step 6.
    println!("\n► Step 5: Collect transformation results");

    let schema: SchemaRef = result_df.schema().inner().clone();
    let results = result_df.collect().await?;
    let result_rows: usize = results.iter().map(|b| b.num_rows()).sum();

    println!("  Transformed rows: {}", result_rows);
    println!("  Batches: {}", results.len());

    if !results.is_empty() {
        println!("  Sample (first batch):");
        for (i, batch) in results.iter().take(1).enumerate() {
            println!("    Batch {}: {} rows", i, batch.num_rows());
        }
    }

    // 6. Write results to Parquet — locally, with the `parquet` crate's ArrowWriter.
    //
    // Why not `DataFrame::write_parquet`? That plans a `CopyToExec` node, and in
    // DataFusion 54 the *physical* CopyTo node has no protobuf mapping, so a remote
    // scheduler cannot ship it to executors at all. And even if it could, the file
    // would land on whichever worker executed the write — not here. The honest split,
    // and this project's sink stance, is: compute distributed, sink local.
    println!("\n► Step 6: Write results to Parquet (local sink)");

    let output_path = "output/end_to_end_results.parquet";

    let file = File::create(output_path)?;
    let mut writer = ArrowWriter::try_new(file, schema, None)?;
    for batch in &results {
        writer.write(batch)?;
    }
    writer.close()?;

    let file_size = fs::metadata(output_path)?.len();
    println!("  ✓ Parquet file written: {}", output_path);
    println!("  ✓ File size: {} bytes", file_size);

    // 7. Save checkpoint
    println!("\n► Step 7: Save execution checkpoint");

    let checkpoint = serde_json::json!({
        "pipeline": "end_to_end_distributed_full_load",
        "table": config.resolved_table(),
        "mode": "full_load",
        "workers": workers,
        "scheduler": scheduler_url,
        "rows_transformed": result_rows,
        "output_file": output_path,
        "execution_time": Utc::now().to_rfc3339(),
    });

    let checkpoint_path = ".checkpoints/end_to_end__default.json";
    fs::write(checkpoint_path, checkpoint.to_string())?;
    println!("  ✓ Checkpoint saved: {}", checkpoint_path);

    // 8. Pipeline summary
    println!("\n► Step 8: Pipeline summary");
    println!("  Extraction:");
    println!("    • Source: {}", config.resolved_table());
    println!("    • Mode: full load (no window), {workers} keyset partition(s)");
    println!("    • Engine: Ballista cluster");

    println!("  Transformation:");
    println!("    • Engine: DataFusion (distributed)");
    println!("    • Filter: status = 'PAID'");
    println!("    • Rows after filter: {}", result_rows);

    println!("  Output:");
    println!("    • Format: Apache Parquet");
    println!("    • File: {}", output_path);
    println!("    • Size: {} bytes", file_size);

    // 9. Verification
    println!("\n► Step 9: Verification");
    println!(
        "  ✓ Full table scanned across {} executor partition(s)",
        workers
    );
    println!("  ✓ DataFusion used for SQL transformations");
    println!("  ✓ Results written to Parquet");
    println!("  ✓ Checkpoint created for pipeline state");
    println!("  ✓ Complete pipeline executed successfully");

    println!("\n═══════════════════════════════════════════════════════════");
    println!("  ✓ End-to-end distributed pipeline example complete");
    println!("═══════════════════════════════════════════════════════════");

    Ok(())
}
