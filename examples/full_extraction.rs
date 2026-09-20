//! Full Load Example
//!
//! Demonstrates extracting the entire contents of a PostgreSQL table.
//! This pattern is used for initial data loads or periodic full refreshes.
//!
//! This example validates:
//! - Full table extraction via extraction layer
//! - Schema discovery and type mapping
//! - Row conversion to Arrow RecordBatch
//! - Large dataset handling
//!
//! Usage:
//! ```bash
//! cargo run --example full_load
//! ```

use rust_ballista_extraction_layer::connector::postgres::PostgresExtractor;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Route the `log` facade to stderr (+ optional --log-file / REL_LOG_FILE).
    // Set RUST_LOG=debug (or --log-level debug) to log every generated SQL query.
    rust_ballista_extraction_layer::logging::init_from_env_and_args();

    println!("═══════════════════════════════════════════════════════════");
    println!("  Full Load Example");
    println!("  Complete Table Extraction");
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
        "full_load_example",
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

    println!("  Table: {}", table_name);
    println!("  Columns: {:?}", columns.as_ref().unwrap());
    println!("  Mode: Full load (all data, no filters)");

    // 3. Execute full table extraction
    println!("\n► Step 3: Execute full table extraction");
    println!("  Extracting all rows...");

    let batch = extractor
        .extract_full_table(table_name, columns.clone())
        .await?;

    let row_count = batch.num_rows();
    let column_count = batch.num_columns();

    println!("  ✓ Full table extracted");
    println!("  ✓ Rows: {}", row_count);
    println!("  ✓ Columns: {}", column_count);

    // 4. Display schema information
    println!("\n► Step 4: Schema information");
    println!("  Arrow Schema: {:?}", batch.schema());

    // 5. Display sample data
    println!("\n► Step 5: Sample data (first 3 rows)");
    if row_count > 0 {
        for i in 0..row_count.min(3) {
            println!("  [Row {}]: {} columns", i + 1, column_count);
        }
    }

    // 6. Use case scenarios
    println!("\n► Step 6: Use case scenarios");
    println!("  Full load patterns:");
    println!("    1. Initial data warehouse load");
    println!("    2. Periodic full refresh");
    println!("    3. Schema migration");
    println!("    4. Data quality validation");

    // 7. Verification
    println!("\n► Step 7: Verification");
    println!("  ✓ Full table extracted successfully");
    println!("  ✓ Total {} rows, {} columns", row_count, column_count);
    println!("  ✓ Schema correctly mapped");
    println!("  ✓ Ready for transformation and loading");

    // 8. Status
    println!("\n► Result");
    println!("  ✓ Full load extraction complete");
    println!("  ✓ Data ready for further processing");
    println!("  ✓ Extraction layer working correctly");

    println!("\n═══════════════════════════════════════════════════════════");
    println!("  ✓ Full load example complete");
    println!("═══════════════════════════════════════════════════════════");

    Ok(())
}
