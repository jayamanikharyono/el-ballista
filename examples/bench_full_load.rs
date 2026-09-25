//! Benchmark: initial loads through standalone Ballista.
//!
//! Fair-comparison counterpart to `benchmark/spark/load.py`. Scans the configured table,
//! writes one Snappy Parquet file, prints a JSON summary for `benchmark/run.sh`.
//!
//!   cargo run --release --example bench_full_load -- \
//!       <config.json> <workers> <output.parquet> [filter_sql] [columns_csv] [scenario]
//!
//! Deployment comes from `BENCH_SCHEDULER_URL`: empty/unset runs standalone (scheduler +
//! in-process executor, no cluster needed); set to `http://host:port` to fan out over a
//! `rel scheduler` + `rel worker` deployment instead. Same code path either way —
//! standalone only removes the network.
//!
//! - No filter/columns: full load (`SELECT *`), the entire row.
//! - With filter/columns: selective load, e.g. `status = 'REFUNDED'`,
//!   `order_id,amount,status` — same predicate and projection must be given to both
//!   engines or the comparison is meaningless.
//!
//! Timed sections (printed in the summary):
//! - `scan_ms`: scan + transport + planning (everything that is not Parquet encode)
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
use rust_ballista_extraction_layer::connector::postgres::distributed::DistributedContext;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Route the `log` facade to stderr (+ optional --log-file / REL_LOG_FILE).
    // Set RUST_LOG=debug (or --log-level debug) to log every generated SQL query.
    rust_ballista_extraction_layer::logging::init_from_env_and_args();

    let config_path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "examples/configs/extract.example.json".to_string());
    let workers: usize = std::env::args()
        .nth(2)
        .map(|s| s.parse().unwrap())
        .unwrap_or(2);
    let output_path = std::env::args()
        .nth(3)
        .unwrap_or_else(|| "output/bench_full_load.parquet".to_string());
    let filter_sql = std::env::args().nth(4).filter(|s| !s.is_empty());
    let columns_csv = std::env::args().nth(5).filter(|s| !s.is_empty());
    let scenario = std::env::args()
        .nth(6)
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

    let projection = columns_csv.as_deref().unwrap_or("*");
    let mut sql = format!("SELECT {projection} FROM {table}");
    if let Some(filter) = filter_sql.as_deref() {
        sql.push_str(" WHERE ");
        sql.push_str(filter);
    }

    // Standalone: scheduler + in-process executor, no external cluster needed. The scan
    // still fans out over `workers` keyset partitions with budgeted pools — the same code
    // path as the remote deployment, minus the network.
    let scheduler_url = std::env::var("BENCH_SCHEDULER_URL")
        .ok()
        .filter(|s| !s.is_empty());
    let deployment = if scheduler_url.is_some() {
        "remote"
    } else {
        "standalone"
    };

    let ctx = match scheduler_url.as_deref() {
        Some(url) => DistributedContext::remote(&config, url, workers).await?,
        None => DistributedContext::standalone(&config, workers).await?,
    };
    ctx.register_source(&config).await?;

    let df = ctx.session.sql(&sql).await?;
    let schema: SchemaRef = df.schema().inner().clone();

    // Stream batches straight into the Parquet writer: memory stays O(batch) no matter
    // how many rows the table holds. Collecting everything first (the naive alternative)
    // OOM-kills this container past a few million rows — exit 137, no error message.
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();
    let mut writer = ArrowWriter::try_new(output_file, schema, Some(props))?;

    let t_scan = Instant::now();
    let t_start_epoch_ms = epoch_ms();
    let mut stream = df.execute_stream().await?;
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
            "engine": format!("rust-ballista-{deployment}"),
            "scenario": scenario,
            "table": table,
            "filter": filter_sql,
            "projection": projection,
            "use_copy": config.execution.use_copy,
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
