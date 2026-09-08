//! Parquet sink.
//! sink/mod.rs
//! Writes one run's batch to a deterministic local-filesystem path and finalizes the window
//! with a `_SUCCESS` marker, per docs/incremental-extraction.md §5. GCS/object-store output is a
//! follow-up — see docs/phase-one-implementation-plan.md §7 — this only writes local disk.

use std::fs::{self, File};
use std::path::PathBuf;
use std::sync::Arc;

use arrow::array::{Date32Array, StringArray, TimestampMicrosecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, NaiveDate, Utc};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

use crate::errors::AppError;

/// Appends the four metadata columns docs/architecture.md §4 specifies on every extracted
/// batch: `_extracted_at` (wall-clock read time), `_extracted_date` (sink partition key),
/// `_source` (connector reference name), and `_watermark_hi` (the upper bound of this run's
/// window). All four are non-nullable — every row in a batch shares the same run metadata.
pub fn with_metadata_columns(
    batch: &RecordBatch,
    source_ref: &str,
    extracted_at: DateTime<Utc>,
    watermark_hi: DateTime<Utc>,
) -> Result<RecordBatch, AppError> {
    let num_rows = batch.num_rows();

    let mut fields: Vec<Field> = batch
        .schema()
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect();
    let mut columns = batch.columns().to_vec();

    fields.push(Field::new(
        "_extracted_at",
        DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        false,
    ));
    columns.push(Arc::new(
        TimestampMicrosecondArray::from(vec![extracted_at.timestamp_micros(); num_rows])
            .with_timezone("UTC"),
    ));

    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    let extracted_date_days = extracted_at
        .date_naive()
        .signed_duration_since(epoch)
        .num_days() as i32;

    fields.push(Field::new("_extracted_date", DataType::Date32, false));
    columns.push(Arc::new(Date32Array::from(vec![
        extracted_date_days;
        num_rows
    ])));

    fields.push(Field::new("_source", DataType::Utf8, false));
    columns.push(Arc::new(StringArray::from(vec![source_ref; num_rows])));

    fields.push(Field::new(
        "_watermark_hi",
        DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        false,
    ));
    columns.push(Arc::new(
        TimestampMicrosecondArray::from(vec![watermark_hi.timestamp_micros(); num_rows])
            .with_timezone("UTC"),
    ));

    let schema = Arc::new(Schema::new(fields));

    RecordBatch::try_new(schema, columns)
        .map_err(|e| AppError::Sink(format!("cannot append metadata columns: {e}")))
}

/// Writes `batches` to
/// `<base_dir>/_extracted_date=YYYY-MM-DD/w=<lo_epoch>-<hi_epoch>/part-00000.parquet` and
/// finalizes the window directory with a `_SUCCESS` marker, per
/// docs/incremental-extraction.md §5. Because the path is derived only from `lo`/`hi`, retrying
/// the same window after a crash overwrites the same objects rather than creating duplicates —
/// that determinism is what makes the commit protocol safe, not anything in this function
/// itself, so callers must not vary `lo`/`hi` across retries of what is logically the same run.
pub fn write_window(
    base_dir: &str,
    schema: Arc<Schema>,
    batches: &[RecordBatch],
    lo: DateTime<Utc>,
    hi: DateTime<Utc>,
) -> Result<PathBuf, AppError> {
    let extracted_date = hi.format("%Y-%m-%d").to_string();

    let window_dir = PathBuf::from(base_dir)
        .join(format!("_extracted_date={extracted_date}"))
        .join(format!("w={}-{}", lo.timestamp(), hi.timestamp()));

    fs::create_dir_all(&window_dir)
        .map_err(|e| AppError::Sink(format!("cannot create {}: {e}", window_dir.display())))?;

    let part_path = window_dir.join("part-00000.parquet");

    let file = File::create(&part_path)
        .map_err(|e| AppError::Sink(format!("cannot create {}: {e}", part_path.display())))?;

    let props = WriterProperties::builder().build();

    let mut writer = ArrowWriter::try_new(file, schema, Some(props))
        .map_err(|e| AppError::Sink(format!("cannot open parquet writer: {e}")))?;

    for batch in batches {
        writer
            .write(batch)
            .map_err(|e| AppError::Sink(format!("cannot write batch: {e}")))?;
    }

    writer
        .close()
        .map_err(|e| AppError::Sink(format!("cannot finalize parquet file: {e}")))?;

    // Naming the expected object count lets a future reader/loader ignore an incomplete window
    // directory (docs/incremental-extraction.md §5). We only ever write one part file today.
    let success_marker = window_dir.join("_SUCCESS");
    fs::write(&success_marker, r#"{"object_count":1}"#)
        .map_err(|e| AppError::Sink(format!("cannot write {}: {e}", success_marker.display())))?;

    Ok(window_dir)
}
