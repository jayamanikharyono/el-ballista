//! MySQL prototype suite — schema/type/extraction coverage plus the Arrow→DataFusion e2e
//! checks, in one file (the former `mysql_integration.rs` + `mysql_e2e.rs`, consolidated).
//!
//! A real MySQL is always provided by the compose stack via `tests/common/mysql.rs`
//! (`MYSQL_URL` or the compose default), so these tests never skip — a harness failure is a
//! hard failure, not a silent pass. Prototype scope: MySQL has no pushdown/
//! distributed path yet, so "e2e" means "MySQL → typed Arrow → DataFusion can query it".

#[path = "common/mysql.rs"]
mod mysql_common;

use arrow::array::Array;
use arrow::datatypes::{DataType, TimeUnit};
use datafusion::prelude::SessionContext;
use mysql_common::MySqlTestDb;
use rust_ballista_extraction_layer::connector::mysql::MysqlExtractor;
use rust_ballista_extraction_layer::connector::mysql::{
    dialect::MysqlDialect, schema_reader::MysqlSchemaReader, type_mapper::mysql_type_to_arrow,
};

/// Connect an extractor to the per-test database (all tests share this boilerplate).
async fn extractor(db: &MySqlTestDb) -> MysqlExtractor {
    MysqlExtractor::connect(&db.host, db.port, &db.user, &db.password, &db.database, 4)
        .await
        .expect("MysqlExtractor::connect")
}

#[tokio::test]
async fn schema_reads_hostile() {
    let db = MySqlTestDb::connect().await;
    let reader = MysqlSchemaReader::new(&db.pool, db.database.clone());
    let meta = reader
        .get_table_metadata(&db.table())
        .await
        .expect("get_table_metadata");

    // Hostile has 22 columns including the PK + updated_at
    assert!(
        meta.columns.len() >= 20,
        "expected >=20 columns, got {}: {:?}",
        meta.columns.len(),
        meta.columns
            .iter()
            .map(|c| &c.column_name)
            .collect::<Vec<_>>()
    );
    // Spot-check a few MySQL types are present
    assert!(meta.columns.iter().any(|c| c.column_name == "name"));
    assert!(meta.columns.iter().any(|c| c.column_name == "amount"));
    assert!(meta.columns.iter().any(|c| c.column_name == "feeling"));
}

#[tokio::test]
async fn type_mapper_covers_hostile() {
    // Pure unit, no DB — but validates the intended Arrow mapping for MySQL types
    assert_eq!(mysql_type_to_arrow("bigint"), DataType::Int64);
    assert_eq!(mysql_type_to_arrow("varchar"), DataType::Utf8);
    assert_eq!(mysql_type_to_arrow("decimal"), DataType::Utf8);
    assert_eq!(
        mysql_type_to_arrow("datetime"),
        DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None)
    );
    assert_eq!(mysql_type_to_arrow("json"), DataType::Utf8);
    // Unknown falls back to Utf8 (prototype behavior)
    assert_eq!(mysql_type_to_arrow("geometry"), DataType::Utf8);
}

/// Full extract: 8 rows, columns decode to their **typed** Arrow types (not blanket `Utf8`),
/// values survive the typed decode, and the batch is valid Arrow DataFusion can ingest.
#[tokio::test]
async fn full_extract_typed_schema_values_and_datafusion_ingest() {
    let db = MySqlTestDb::connect().await;
    let ex = extractor(&db).await;

    let batch = ex
        .extract_full_table(&db.table(), None)
        .await
        .expect("extract_full_table");

    assert_eq!(batch.num_rows(), 8, "hostile fixture has 8 rows");

    // Typed decode: representative columns map to their Arrow types.
    let dt = |name: &str| {
        batch
            .schema()
            .field_with_name(name)
            .unwrap()
            .data_type()
            .clone()
    };
    assert_eq!(dt("id"), DataType::Int64);
    assert_eq!(dt("tiny"), DataType::Int8); // width preserved (not collapsed to Int64)
    assert_eq!(dt("small"), DataType::Int16);
    assert_eq!(dt("count"), DataType::Int32);
    assert_eq!(dt("f"), DataType::Float32); // 32-bit float preserved
    assert_eq!(dt("name"), DataType::Utf8);
    assert_eq!(dt("ratio"), DataType::Float64);
    assert_eq!(dt("day"), DataType::Date32);
    assert_eq!(
        dt("updated_at"),
        DataType::Timestamp(TimeUnit::Microsecond, None)
    );
    assert_eq!(dt("flag"), DataType::Boolean);
    assert_eq!(dt("payload"), DataType::Binary);
    assert_eq!(dt("amount"), DataType::Decimal128(12, 2)); // exact decimal, like Postgres
    assert_eq!(dt("meta"), DataType::Utf8); // json kept as text

    // Values survive the typed decode.
    let name = batch
        .column(batch.schema().index_of("name").unwrap())
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap();
    assert_eq!(name.value(0), "Zürich");
    let nick = batch
        .column(batch.schema().index_of("nick").unwrap())
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap();
    assert!(nick.is_null(4)); // 'nulls' row (name is the literal 'nulls', nick is NULL)
    // `amount DECIMAL(12,2)` = 123.45 → Decimal128 unscaled 12345 at scale 2.
    let amount = batch
        .column(batch.schema().index_of("amount").unwrap())
        .as_any()
        .downcast_ref::<arrow::array::Decimal128Array>()
        .unwrap();
    assert_eq!(amount.value(0), 12345);

    // DataFusion can ingest the typed batch (proves Arrow validity, not just row count).
    let ctx = SessionContext::new();
    let df = ctx.read_batch(batch.clone()).unwrap();
    let rows: usize = df
        .collect()
        .await
        .unwrap()
        .iter()
        .map(|b| b.num_rows())
        .sum();
    assert_eq!(rows, 8);
}

#[tokio::test]
async fn null_and_empty_string_distinction() {
    let db = MySqlTestDb::connect().await;
    let ex = extractor(&db).await;
    let batch = ex.extract_full_table(&db.table(), None).await.unwrap();

    let name_idx = batch.schema().index_of("name").unwrap();
    let nick_idx = batch.schema().index_of("nick").unwrap();
    let name_arr = batch
        .column(name_idx)
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap();
    let nick_arr = batch
        .column(nick_idx)
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap();

    // Row 1: '' (empty) vs Row 4: NULL — must be distinct
    assert_eq!(name_arr.value(1), "");
    assert!(nick_arr.is_null(4));
    assert_eq!(nick_arr.value(1), ""); // empty string, not null
}

#[tokio::test]
async fn projection_returns_requested_columns() {
    let db = MySqlTestDb::connect().await;
    let ex = extractor(&db).await;
    let batch = ex
        .extract_full_table(&db.table(), Some(vec!["id", "name"]))
        .await
        .unwrap();
    assert_eq!(batch.num_columns(), 2);
    assert_eq!(batch.schema().field(0).name(), "id");
    assert_eq!(batch.schema().field(1).name(), "name");
}

/// Empty table: extraction yields 0 rows but preserves the schema, and DataFusion can still
/// plan over the empty batch.
#[tokio::test]
async fn empty_table_yields_valid_empty_arrow() {
    let db = MySqlTestDb::connect().await;
    let empty_table = format!("{}.hostile_empty", db.database);
    // Each identifier quoted separately: one backtick pair around `db.table` is a single
    // identifier containing a dot, not a qualified name.
    sqlx::query(sqlx::AssertSqlSafe(
        format!(
            "CREATE TABLE `{}`.`hostile_empty` LIKE `{}`.`hostile`",
            db.database, db.database
        )
        .as_str(),
    ))
    .execute(&db.pool)
    .await
    .unwrap();

    let ex = extractor(&db).await;
    let batch = ex.extract_full_table(&empty_table, None).await.unwrap();
    assert_eq!(batch.num_rows(), 0);
    assert!(batch.schema().fields().len() > 10);

    let ctx = SessionContext::new();
    ctx.register_batch("empty_hostile", batch).unwrap();
    let out = ctx
        .sql("SELECT * FROM empty_hostile")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(out.iter().map(|b| b.num_rows()).sum::<usize>(), 0);

    sqlx::query(sqlx::AssertSqlSafe(
        format!("DROP TABLE `{}`.`hostile_empty`", db.database).as_str(),
    ))
    .execute(&db.pool)
    .await
    .unwrap();
}

/// Filtering happens *in DataFusion*, not MySQL (the prototype has no pushdown).
#[tokio::test]
async fn datafusion_filter_over_extract() {
    let db = MySqlTestDb::connect().await;
    let ex = extractor(&db).await;
    let batch = ex.extract_full_table(&db.table(), None).await.unwrap();

    let ctx = SessionContext::new();
    ctx.register_batch("hostile", batch).unwrap();
    let rows: usize = ctx
        .sql("SELECT id, name FROM hostile WHERE name = 'Zürich'")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap()
        .iter()
        .map(|b| b.num_rows())
        .sum();
    assert_eq!(rows, 1);
}

/// Filter in DataFusion over the typed `updated_at` timestamp (caller-provided range).
/// Rows are dated 2024-01-01..08; `> 2024-01-04` keeps 4.
#[tokio::test]
async fn datafusion_filtered_range_simulation() {
    let db = MySqlTestDb::connect().await;
    let ex = extractor(&db).await;
    let batch = ex.extract_full_table(&db.table(), None).await.unwrap();

    let ctx = SessionContext::new();
    ctx.register_batch("hostile", batch).unwrap();
    let out = ctx
        .sql("SELECT COUNT(*) as cnt FROM hostile WHERE updated_at > CAST('2024-01-04' AS TIMESTAMP)")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let cnt = out[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap();
    assert_eq!(cnt.value(0), 4);
}

#[tokio::test]
async fn dialect_quote_and_placeholder() {
    use rust_ballista_extraction_layer::pushdown::dialect::SqlDialect;
    let d = MysqlDialect;
    assert_eq!(d.quote_ident("order"), "`order`");
    assert_eq!(d.quote_ident("a`b"), "`a``b`");
    assert_eq!(d.placeholder(1), "?");
    assert_eq!(d.placeholder(5), "?");
}
