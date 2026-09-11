//! End-to-End Example
//! 
//! Demonstrates a complete extraction and transformation pipeline using the extraction layer.
//!
//! This example validates:
//! - Extraction layer data extraction
//! - DataFusion integration
//! - SQL transformations on extracted data
//! - Parquet output writing
//! - Complete pipeline workflow
//!
//! Pipeline:
//! ```text
//! PostgreSQL Database
//!     ↓ (extraction layer)
//! Arrow RecordBatch
//!     ↓ (register in DataFusion)
//! DataFusion Context
//!     ↓ (SQL query: filter + transform)
//! Results
//!     ↓ (write_parquet)
//! Parquet File
//! ```
//!
//! Usage:
//! ```bash
//! cargo run --example end_to_end
//! ```

use rust_ballista_extraction_layer::connector::postgres::PostgresExtractor;
use chrono::{Utc, Duration};
use std::fs;
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("═══════════════════════════════════════════════════════════");
    println!("  End-to-End Example");
    println!("  Complete Extraction → Transformation → Parquet Pipeline");
    println!("═══════════════════════════════════════════════════════════\n");
    
    // 1. Setup
    println!("► Step 1: Initialize");
    fs::create_dir_all("output")?;
    fs::create_dir_all(".checkpoints")?;
    println!("  ✓ Output directory: output/");
    println!("  ✓ Checkpoint store: .checkpoints/");
    
    // 2. Connect to PostgreSQL using extraction layer
    println!("\n► Step 2: Connect to PostgreSQL via extraction layer");
    
    let extractor = PostgresExtractor::connect(
        "localhost",
        5432,
        "postgres",
        "postgres",
        "app",
        5,
        30000,
        "end_to_end_example",
    ).await?;
    
    println!("  ✓ Connected to database");
    println!("  ✓ Extraction layer initialized");
    
    // 3. Define extraction parameters
    println!("\n► Step 3: Define extraction window");
    
    let table_name = "public.orders";
    let columns = Some(vec!["order_id", "amount", "status", "updated_at"]);
    let timestamp_column = "updated_at";
    
    // Extract last 24 hours of data
    let now = Utc::now();
    let safety_lag = Duration::minutes(5);
    let window_duration = Duration::hours(24);
    
    let window_end = now - safety_lag;
    let window_start = window_end - window_duration;
    
    println!("  Table: {}", table_name);
    println!("  Window: {} to {}", 
        window_start.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        window_end.to_rfc3339_opts(chrono::SecondsFormat::Secs, true));
    println!("  Duration: {} hours", window_duration.num_hours());
    
    // 4. Extract data from PostgreSQL using extraction layer
    println!("\n► Step 4: Extract data via extraction layer");
    println!("  Calling: extractor.extract_incremental_window(...)");
    
    let batch = extractor.extract_incremental_window(
        table_name,
        columns.clone(),
        timestamp_column,
        window_start,
        window_end,
    ).await?;
    
    let row_count = batch.num_rows();
    println!("  ✓ Extracted {} rows", row_count);
    println!("  ✓ Schema: {} columns", batch.num_columns());
    
    // 5. Register batch in DataFusion context
    println!("\n► Step 5: Load into DataFusion");
    
    let ctx = SessionContext::new();
    ctx.register_batch("orders", batch.clone())?;
    
    println!("  ✓ Registered table: orders");
    println!("  ✓ {} rows available for transformation", row_count);
    
    // 6. Apply transformations via SQL
    println!("\n► Step 6: Apply SQL transformations");
    
    println!("  Query:");
    println!("    SELECT order_id, amount, status");
    println!("    FROM orders");
    println!("    WHERE status = 'PAID'");
    
    let result_df = ctx.sql(
        "SELECT order_id, amount, status 
         FROM orders 
         WHERE status = 'PAID' 
         ORDER BY order_id"
    ).await?;
    
    println!("  ✓ SQL transformation executed");
    
    // 7. Collect results and show sample
    println!("\n► Step 7: Collect transformation results");
    
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
    
    // 8. Write results to Parquet
    println!("\n► Step 8: Write results to Parquet");
    
    let output_path = "output/end_to_end_results.parquet";
    
    // Need to query again for writing to parquet
    let write_df = ctx.sql(
        "SELECT order_id, amount, status 
         FROM orders 
         WHERE status = 'PAID' 
         ORDER BY order_id"
    ).await?;
    
    write_df.write_parquet(output_path, Default::default(), None).await?;
    
    let file_size = fs::metadata(output_path)?.len();
    println!("  ✓ Parquet file written: {}", output_path);
    println!("  ✓ File size: {} bytes", file_size);
    
    // 9. Save checkpoint
    println!("\n► Step 9: Save execution checkpoint");
    
    let checkpoint = serde_json::json!({
        "pipeline": "end_to_end",
        "table": table_name,
        "extraction_start": window_start.to_rfc3339(),
        "extraction_end": window_end.to_rfc3339(),
        "rows_extracted": row_count,
        "rows_transformed": result_rows,
        "output_file": output_path,
        "execution_time": Utc::now().to_rfc3339(),
    });
    
    let checkpoint_path = ".checkpoints/end_to_end__default.json";
    fs::write(checkpoint_path, checkpoint.to_string())?;
    println!("  ✓ Checkpoint saved: {}", checkpoint_path);
    
    // 10. Pipeline summary
    println!("\n► Step 10: Pipeline summary");
    println!("  Extraction:");
    println!("    • Source: {}", table_name);
    println!("    • Rows extracted: {}", row_count);
    println!("    • Method: Extraction layer");
    
    println!("  Transformation:");
    println!("    • Engine: DataFusion");
    println!("    • Filter: status = 'PAID'");
    println!("    • Rows after filter: {}", result_rows);
    
    println!("  Output:");
    println!("    • Format: Apache Parquet");
    println!("    • File: {}", output_path);
    println!("    • Size: {} bytes", file_size);
    
    // 11. Verification
    println!("\n► Step 11: Verification");
    println!("  ✓ Extraction layer used for data retrieval");
    println!("  ✓ DataFusion used for SQL transformations");
    println!("  ✓ Results written to Parquet");
    println!("  ✓ Checkpoint created for pipeline state");
    println!("  ✓ Complete pipeline executed successfully");
    
    println!("\n═══════════════════════════════════════════════════════════");
    println!("  ✓ End-to-end pipeline example complete");
    println!("═══════════════════════════════════════════════════════════");
    
    Ok(())
}
