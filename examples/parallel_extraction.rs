//! Parallel extraction: the job's table split into keyset partitions, scanned concurrently.
//!
//! `parallel_scan` (set here in code on top of the job config) splits the table into
//! `partitions` ranges of `partition_column`, from one `MIN`/`MAX` read. The first range also
//! holds every row whose key is NULL and the last one is open-ended, so the partitions cover
//! every row exactly once at the moment they are planned. Partitions are separate statements
//! with separate snapshots: rows that change their key during the scan can be missed or seen
//! twice (see the README's isolation notes). At most `pool_max` partitions query the source
//! at once.
//!
//! `run_with` hands each partition (split) to the consumer below with its bounds; this
//! example just counts rows. The checkpoint goes to a fresh temporary directory, so every
//! run extracts every split.
//!
//! ```bash
//! PGPASSWORD=... cargo run --example parallel_extraction -- [config.json] [partitions]
//! # defaults: examples/configs/full_extract.dvd_rental.json, 4
//! ```

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use el_ballista::config::JobConfig;
use el_ballista::connector::postgres::{PostgresConnector, SplitInfo, close_pools};
use futures::TryStreamExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    el_ballista::logging::init_from_env_and_args();

    let mut args = std::env::args().skip(1);
    let config_path = args
        .next()
        .unwrap_or_else(|| "examples/configs/full_extract.dvd_rental.json".to_string());
    let partitions: usize = match args.next() {
        Some(n) => n.parse()?,
        None => 4,
    };

    let mut config = JobConfig::from_file(&config_path)?;
    config.parallel_scan.strategy = "keyset".parse()?;
    config.parallel_scan.partitions = partitions;
    config.checkpoint.dir = std::env::temp_dir()
        .join(format!(
            "el-ballista-parallel-example-{}",
            std::process::id()
        ))
        .to_string_lossy()
        .into_owned();
    let checkpoint_dir = config.checkpoint.dir.clone();
    println!(
        "► {}.{} in {partitions} keyset partition(s) on {} (at most {} scanning at once)",
        config.source.schema,
        config.table,
        config.parallel_scan.partition_column,
        config.source.pool_max
    );

    let connector = PostgresConnector::from_config(config)?;
    let total = Arc::new(AtomicUsize::new(0));
    let outcome = connector
        .extract()
        .standalone()
        .run_with(|split: SplitInfo, mut stream| {
            let total = Arc::clone(&total);
            async move {
                let mut rows = 0usize;
                while let Some(batch) = stream.try_next().await? {
                    rows += batch.num_rows();
                }
                total.fetch_add(rows, Ordering::Relaxed);
                let range = match &split.bounds {
                    Some(b) => format!("lo={:?} hi={:?}", b.lo, b.hi),
                    None => "whole table".to_string(),
                };
                println!(
                    "  ✓ {} ({}/{}): {rows} rows, {range}",
                    split.split_id,
                    split.index + 1,
                    split.total
                );
                Ok(())
            }
        })
        .await?;

    println!(
        "  ✓ {} rows extracted across all partitions ({} split(s))",
        total.load(Ordering::Relaxed),
        outcome.splits_total
    );
    let _ = std::fs::remove_dir_all(&checkpoint_dir);
    close_pools().await;
    Ok(())
}
