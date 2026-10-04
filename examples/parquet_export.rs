//! Parquet export through the checkpointed API: one Parquet file per split, with a run report.
//!
//! ```bash
//! cargo run --release --example parquet_export -- [config.json] [output_dir]
//! # defaults: examples/configs/full_extract.example.json, output
//! ```
//!
//! Each split's stream is written to `<output_dir>/<job_id>/<split_id>.parquet` (via a temp
//! file renamed into place, so a split's file is either complete or absent). A split is
//! recorded completed only after its file is in place, so re-running the same job after a
//! failure rewrites only the unfinished splits. Every run also leaves a run report in
//! `<checkpoint.dir>/runs/<job_id>/<run_id>.json` (`el-ballista runs show --config <config>`).
//!
//! The layer itself never writes data: this example is the consumer, which is the caller's job.

use std::fs::File;
use std::path::PathBuf;

use el_ballista::connector::postgres::{PostgresConnector, SplitInfo};
use futures::TryStreamExt;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Route the `log` facade to stderr (+ optional --log-file / EL_BALLISTA_LOG_FILE).
    el_ballista::logging::init_from_env_and_args();

    let mut args = std::env::args().skip(1);
    let config_path = args
        .next()
        .unwrap_or_else(|| "examples/configs/full_extract.example.json".to_string());
    let output_dir = PathBuf::from(args.next().unwrap_or_else(|| "output".to_string()));

    let connector = PostgresConnector::from_config_file(&config_path)?;
    let job_id = connector.config().job_id.to_string();
    let out = output_dir.join(&job_id);
    std::fs::create_dir_all(&out)?;

    let outcome = connector
        .extract()
        .standalone()
        .run_with(|split: SplitInfo, mut stream| {
            let out = out.clone();
            async move {
                let path = out.join(format!("{}.parquet", split.split_id));
                let tmp = out.join(format!("{}.parquet.tmp", split.split_id));
                let props = WriterProperties::builder()
                    .set_compression(Compression::SNAPPY)
                    .build();
                let mut writer =
                    ArrowWriter::try_new(File::create(&tmp)?, stream.schema(), Some(props))?;
                while let Some(batch) = stream.try_next().await? {
                    writer.write(&batch)?;
                }
                writer.close()?;
                std::fs::rename(&tmp, &path)?;
                Ok(())
            }
        })
        .await?;

    println!(
        "job '{job_id}': {} row(s) written to {} ({} of {} split(s) this run, {} skipped from an earlier run)",
        outcome.rows_delivered,
        out.display(),
        outcome.splits_completed - outcome.splits_skipped,
        outcome.splits_total,
        outcome.splits_skipped,
    );
    match &outcome.report_path {
        Some(path) => println!("run {}: report {}", outcome.run_id, path.display()),
        None => println!(
            "run {}: no report (checkpoint.run_reports is off)",
            outcome.run_id
        ),
    }
    Ok(())
}
