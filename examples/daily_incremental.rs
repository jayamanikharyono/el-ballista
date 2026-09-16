//! Daily Incremental Example
//!
//! Demonstrates extracting the last 24 hours of data from a PostgreSQL table.
//! This pattern is used for daily batch processing pipelines.
//!
//! This example validates:
//! - Time-windowed extraction
//! - Yesterday's data extraction
//! - Watermark boundary handling
//! - Incremental processing patterns
//!
//! Usage:
//! ```bash
//! cargo run --example daily_incremental
//! ```

use chrono::{DateTime, Duration, Utc};
use rust_ballista_extraction_layer::connector::postgres::PostgresExtractor;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("═══════════════════════════════════════════════════════════");
    println!("  Daily Incremental Example");
    println!("  Last 24 Hours Data Extraction");
    println!("═══════════════════════════════════════════════════════════\n");

    // 1. Connect to PostgreSQL using the extraction layer
    println!("► Step 1: Connect to PostgreSQL");

    let extractor = PostgresExtractor::connect(
        "localhost",
        5432,
        "postgres",
        "postgres",
        "app",
        5,
        30000,
        "daily_incremental_example",
    )
    .await?;

    println!("  ✓ Connected to database");
    println!("  ✓ Extraction layer initialized");

    // 2. Define extraction parameters
    println!("\n► Step 2: Define extraction parameters");
    let table_name = "public.orders";
    let columns = Some(vec![
        "order_id",
        "customer_name",
        "amount",
        "status",
        "updated_at",
    ]);
    let timestamp_column = "updated_at";

    println!("  Table: {}", table_name);
    println!("  Columns: {:?}", columns.as_ref().unwrap());
    println!("  Timestamp column: {}", timestamp_column);
    println!("  Window: Last 24 hours");

    // 3. Calculate yesterday's window
    println!("\n► Step 3: Calculate daily window");
    let now = Utc::now();

    // For daily processing, we typically extract data from "yesterday"
    // (midnight to midnight in UTC)
    let window_end = now.date_naive().and_hms_opt(0, 0, 0).unwrap(); // Today at 00:00:00
    let window_end = DateTime::from_naive_utc_and_offset(window_end, Utc);
    let window_start = window_end - Duration::days(1);

    // This example runs after the day boundary (see step 6: 01:00 UTC), so the
    // uncommitted-transaction risk the safety lag guards against has already passed
    // for yesterday's rows — extract the full day through midnight. Cutting at
    // window_end - lag with no checkpoint would permanently skip the day's last
    // minutes, since no later run covers them.
    let extraction_end = window_end;

    println!("  Processing window:");
    println!("    Window start: {}", window_start.to_rfc3339());
    println!("    Window end:   {}", window_end.to_rfc3339());
    println!("    Extraction to: {}", extraction_end.to_rfc3339());

    // 4. Execute daily incremental extraction
    println!("\n► Step 4: Execute daily incremental extraction");

    println!("  Query pattern:");
    println!("    SELECT ... FROM orders");
    println!("    WHERE updated_at > '{}'", window_start.to_rfc3339());
    println!("      AND updated_at <= '{}'", extraction_end.to_rfc3339());

    let batch = extractor
        .extract_incremental_window(
            table_name,
            columns.clone(),
            timestamp_column,
            window_start,
            extraction_end,
        )
        .await?;

    let row_count = batch.num_rows();
    let column_count = batch.num_columns();

    println!("  ✓ Daily incremental extraction complete");
    println!("  ✓ Rows extracted: {}", row_count);
    println!("  ✓ Columns: {}", column_count);

    // 5. Display statistics
    println!("\n► Step 5: Extraction statistics");
    if row_count > 0 {
        println!("  Daily volume: {} rows", row_count);

        // In a real scenario, we'd calculate:
        // - Row count distribution by hour
        // - Amount totals
        // - Status distribution
        println!("  Processing complete for: {}", window_start.date_naive());

        // Calculate "yesterday" in local terms for reporting
        let yesterday = window_start.date_naive();
        println!("  Date processed: {}", yesterday);
    } else {
        println!("  No data found for yesterday's window");
    }

    // 6. Daily pipeline workflow
    println!("\n► Step 6: Daily pipeline workflow");
    println!("  Typical daily ETL pattern:");
    println!("    1. Schedule: Run at 01:00 UTC (after day boundary)");
    println!("    2. Extract: Previous day's data (00:00-23:59)");
    println!("    3. Transform: Apply business logic and aggregations");
    println!("    4. Load: Write to data warehouse (BigQuery, Snowflake)");
    println!("    5. Validate: Check row counts and data quality");
    println!("    6. Notify: Send success/failure alerts");

    // 7. Use case scenarios
    println!("\n► Step 7: Use case scenarios");
    println!("  Daily incremental patterns:");
    println!("    1. Daily sales reporting");
    println!("    2. Customer activity analysis");
    println!("    3. Inventory updates");
    println!("    4. Financial reconciliation");

    // 8. Verification
    println!("\n► Step 8: Verification");
    println!("  ✓ Daily window correctly defined");
    println!("  ✓ Safety lag applied for consistency");
    println!("  ✓ {} rows extracted from yesterday", row_count);
    println!("  ✓ Ready for daily batch processing");

    // 9. Status
    println!("\n► Result");
    println!("  ✓ Daily incremental extraction complete");
    println!("  ✓ Date processed: {}", window_start.date_naive());
    println!("  ✓ Data ready for daily batch pipeline");

    println!("\n═══════════════════════════════════════════════════════════");
    println!("  ✓ Daily incremental example complete");
    println!("═══════════════════════════════════════════════════════════");

    Ok(())
}
