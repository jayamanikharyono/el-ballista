//! Benchmark: initial loads through plain DataFusion (standalone) or a Ballista cluster.
//!
//! Fair-comparison counterpart to `benchmark/spark/load.py`. Scans the configured table,
//! writes one Snappy Parquet file, prints a JSON summary for `benchmark/run.sh`.
//!
//!   cargo run --release --example bench_full_load -- \
//!       <config.json> <workers> <output.parquet> [scenario_label]
//!
//! **What** is extracted comes only from the job config file — the table, the structured
//! `filters` and the `columns` projection (`benchmark/run.sh` writes one config per scenario).
//! **How** it is extracted is only the connector API (`PostgresConnector::extract()`). The
//! harness takes no filter/projection arguments, builds no SQL and never touches the
//! provider directly; `scenario_label` is just a name for the summary. Deployment comes from
//! `BENCH_SCHEDULER_URL`:
//! - empty/unset: `.standalone().stream()` — plain DataFusion in one process, no Ballista.
//!   The process uses the whole `pool_max`; at most `execution.concurrent_partitions`
//!   (default `pool_max`) partitions scan at once, so buffered batches stay bounded by that
//!   concurrency, not by the partition count.
//! - `http://host:port`: `.distributed().scheduler(url).workers(n).stream()` on a
//!   `rel scheduler` + `rel worker` deployment; every worker process budgets
//!   `pool_max / workers` connections.
//!
//! - Config without `filters`/`columns`: full load (every column, every row).
//! - Config with them: selective load, e.g.
//!   `"filters": [{"column": "status", "op": "=", "value": "REFUNDED"}]`,
//!   `"columns": ["order_id", "amount", "status"]`. Give both engines the same predicate and
//!   projection or the comparison is meaningless.
//!
//! Timed section: end to end, from this program's entry point (before the config is read)
//! to the last byte of the Parquet file written — the same span the Spark side times.
//! Printed split in two:
//! - `scan_ms`: config load, schema discovery, split planning, scan and transport
//!   (everything that is not Parquet encode)
//! - `write_ms`: cumulative Parquet encode time across batches
//!
//! Batches stream straight into the writer (`execute_stream`), so memory stays O(batch)
//! at any row count — collecting everything first OOM-kills small hosts past a few
//! million rows (exit 137 with no error message).

use std::fs::File;
use std::time::Instant;

use arrow::datatypes::SchemaRef;
use futures::StreamExt;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use rust_ballista_extraction_layer::config::JobConfig;
use rust_ballista_extraction_layer::connector::postgres::PostgresConnector;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Timed section = end to end, the same span as the Spark side: from this entry point
    // (before the config is read) to the last Parquet byte written.
    let t_scan = Instant::now();
    let t_start_epoch_ms = epoch_ms();
    // Route the `log` facade to stderr (+ optional --log-file / REL_LOG_FILE).
    // Set RUST_LOG=debug (or --log-level debug) to log every generated SQL query.
    rust_ballista_extraction_layer::logging::init_from_env_and_args();

    let config_path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "benchmark/rust/bench-config.json".to_string());
    let workers: usize = std::env::args()
        .nth(2)
        .map(|s| s.parse().unwrap())
        .unwrap_or(2);
    let output_path = std::env::args()
        .nth(3)
        .unwrap_or_else(|| "output/bench_full_load.parquet".to_string());
    let scenario = std::env::args()
        .nth(4)
        .unwrap_or_else(|| "full".to_string());

    let config = JobConfig::from_file(&config_path)?;
    let table = config.table.clone();

    // Create the output file FIRST, before minutes of scanning: a broken /output mount
    // (stale bind under churn) then fails in seconds with the path attached, instead of
    // surfacing as a bare Os error after the whole scan. Retried because bind mounts over
    // network filesystems (virtiofs) can flap ENOENT transiently between container start
    // and first write — an immediate second attempt distinguishes that from a truly
    // missing directory.
    let output_file = {
        let mut attempt = 0;
        loop {
            attempt += 1;
            match File::create(&output_path) {
                Ok(file) => break file,
                Err(e) if attempt < 3 => {
                    eprintln!("create {output_path} attempt {attempt}/3 failed ({e}); retrying...");
                    std::thread::sleep(std::time::Duration::from_secs(2));
                }
                Err(e) => return Err(format!("create output file {output_path}: {e}").into()),
            }
        }
    };

    // Recorded as-is in the summary: the config is the whole definition of the scenario.
    let filters = serde_json::to_value(&config.filters)?;
    let projection = config
        .columns
        .as_ref()
        .map_or_else(|| "*".to_string(), |c| c.join(","));
    let use_copy = config.execution.use_copy;
    let connector = PostgresConnector::from_config(config)?;

    // Standalone: `.standalone()` — plain DataFusion in this process, whole pool_max.
    // Remote: `.distributed()` on the `rel scheduler` + `rel worker` cluster.
    let scheduler_url = std::env::var("BENCH_SCHEDULER_URL")
        .ok()
        .filter(|s| !s.is_empty());
    let engine = if scheduler_url.is_some() {
        "rust-ballista-remote"
    } else {
        "rust-datafusion-standalone"
    };

    let mut stream = match scheduler_url.as_deref() {
        Some(url) => {
            connector
                .extract()
                .distributed()
                .scheduler(url)
                .workers(workers)
                .stream()
                .await?
        }
        None => connector.extract().standalone().stream().await?,
    };
    let schema: SchemaRef = stream.schema();

    // Stream batches straight into the Parquet writer: memory stays O(batch) no matter
    // how many rows the table holds. Collecting everything first (the naive alternative)
    // OOM-kills this container past a few million rows — exit 137, no error message.
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();
    let mut writer = ArrowWriter::try_new(output_file, schema, Some(props))?;

    let mut rows = 0usize;
    let mut n_batches = 0usize;
    let mut write_ns: u128 = 0;
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        rows += batch.num_rows();
        n_batches += 1;
        let t = Instant::now();
        writer.write(&batch)?;
        write_ns += t.elapsed().as_nanos();
    }
    writer.close()?;
    let elapsed_ms = t_scan.elapsed().as_millis();
    let t_end_epoch_ms = epoch_ms();
    // scan_ms = everything that is not pure Parquet encode (scan, transport, planning);
    // write_ms = cumulative encode time. They partition elapsed by construction.
    let write_ms = write_ns / 1_000_000;
    let scan_ms = elapsed_ms.saturating_sub(write_ms);

    let output_bytes = std::fs::metadata(&output_path)
        .map(|m| m.len())
        .map_err(|e| format!("stat output file {output_path}: {e}"))?;

    println!(
        "{}",
        serde_json::json!({
            "engine": engine,
            "scenario": scenario,
            "table": table,
            "filters": filters,
            "projection": projection,
            "use_copy": use_copy,
            "workers": workers,
            "rows": rows,
            "batches": n_batches,
            "scan_ms": scan_ms,
            "write_ms": write_ms,
            "elapsed_ms": scan_ms + write_ms,
            // Timed-section bounds (wall clock, epoch ms): run.sh restricts CPU/memory
            // statistics to this window so they describe the same work as elapsed_ms.
            "t_start_epoch_ms": t_start_epoch_ms,
            "t_end_epoch_ms": t_end_epoch_ms,
            // Exact cgroup memory high-water mark of this container (includes page cache;
            // the figure a --memory limit / OOM kill applies to). Null outside cgroup v2.
            "mem_peak_bytes": cgroup_memory_peak(),
            "output_bytes": output_bytes,
            "output": output_path,
        })
    );

    Ok(())
}

/// Wall clock in epoch milliseconds (0 if the clock is before 1970).
fn epoch_ms() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0)
}

/// `/sys/fs/cgroup/memory.peak` of this container (cgroup v2, kernel >= 5.19), if readable.
fn cgroup_memory_peak() -> Option<u64> {
    std::fs::read_to_string("/sys/fs/cgroup/memory.peak")
        .ok()
        .and_then(|s| s.trim().parse().ok())
}
