//! Filtered Extraction Example
//!
//! Demonstrates filtered extraction with caller-provided predicates: the
//! orchestrator decides WHAT range to extract (here a one-week `payment_date` window plus a
//! customer range and an amount predicate), and the extraction layer decides HOW to extract it efficiently
//! (pushing the predicates to the source through DataFusion pushdown).
//!
//! This is how incremental and backfill use cases are expressed without any
//! watermark or backfill machinery in the extraction layer: an incremental job
//! passes a time-range predicate, a backfill passes a historical range.
//!
//! Usage:
//! ```bash
//! cargo run --example filtered_extraction
//! ```

use el_ballista::config::JobConfig;
use el_ballista::connector::postgres::PostgresConnector;
use futures::TryStreamExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Route the `log` facade to stderr (+ optional --log-file / EL_BALLISTA_LOG_FILE).
    // Set RUST_LOG=debug (or --log-level debug) to log every generated SQL query.
    el_ballista::logging::init_from_env_and_args();

    println!("═══════════════════════════════════════════════════════════");
    println!("  Filtered Extraction Example");
    println!("  Caller-provided predicates → source pushdown");
    println!("═══════════════════════════════════════════════════════════\n");

    // 1. Load the job spec. Its `filters` are the orchestrator-supplied slice —
    // an incremental-style time window plus two more predicates, in structured
    // form (typed values, no re-parsing):
    //   { "column": "payment_date", "op": ">=", "value": "2007-04-06T00:00:00Z" }
    // No watermark state, no backfill orchestration — just filters.
    println!("► Step 1: Job spec + caller-provided filters");
    let config = JobConfig::from_file("examples/configs/extract.example.json")?;
    println!("  Table: {}", config.resolved_table());
    println!("  Filters: {} (decisions below)", config.filters.len());

    // 2. Preview pushdown decisions before extracting.
    println!("\n► Step 2: Preview pushdown decisions");
    let connector = PostgresConnector::from_config(config)?;
    for decision in connector.pipeline().explain_filters().await? {
        println!(
            "  filter {:<12} -> pushed_to_source={} ({})",
            decision.filter, decision.pushed_to_source, decision.reason
        );
    }

    // 3. Extract through the connector (pushdown decides source vs Arrow).
    println!("\n► Step 3: Extract with pushdown");
    let batches = connector.extract().standalone().collect().await?;
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    println!(
        "  ✓ Filtered extraction complete: {} row(s) in {} batch(es)",
        rows,
        batches.len()
    );

    // 4. Operational run with split checkpointing: every split's stream goes to the
    // consumer, and a split is recorded completed only after the consumer returns Ok.
    // (Here it just counts; a real consumer writes each split somewhere durable.)
    println!("\n► Step 4: Operational run (split checkpointing)");
    let outcome = connector
        .extract()
        .standalone()
        .run_with(|split, mut stream| async move {
            let mut rows = 0usize;
            while let Some(batch) = stream.try_next().await? {
                rows += batch.num_rows();
            }
            println!("    {}: {rows} row(s) consumed", split.split_id);
            Ok(())
        })
        .await?;
    println!(
        "  ✓ Run outcome: {} row(s) delivered, splits {}/{} ({} skipped from an earlier run)",
        outcome.rows_delivered,
        outcome.splits_completed,
        outcome.splits_total,
        outcome.splits_skipped
    );

    println!("\n═══════════════════════════════════════════════════════════");
    println!("  ✓ Filtered extraction example complete");
    println!("═══════════════════════════════════════════════════════════");

    Ok(())
}
