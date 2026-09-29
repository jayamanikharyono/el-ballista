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
use el_ballista::connector::mysql::{
    MysqlError, MysqlExtractor, dialect::MysqlDialect, schema_reader::MysqlSchemaReader,
    type_mapper::arrow_type_for,
};
use el_ballista::types::ColumnMetadata;
use mysql_common::MySqlTestDb;

/// Number of columns in the MySQL hostile fixture (`tests/common/mysql.rs`).
const HOSTILE_COLUMNS: usize = 23;

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

    // Hostile has exactly 23 columns including the PK + updated_at, in DDL order.
    assert_eq!(
        meta.columns.len(),
        HOSTILE_COLUMNS,
        "columns: {:?}",
        meta.columns
            .iter()
            .map(|c| &c.column_name)
            .collect::<Vec<_>>()
    );
    assert_eq!(meta.columns[0].column_name, "id");
    assert_eq!(meta.columns[HOSTILE_COLUMNS - 1].column_name, "updated_at");
    // COLUMN_TYPE is carried (the type mapper needs it for BOOLEAN / unsigned / bit(n)).
    let is_bool = meta
        .columns
        .iter()
        .find(|c| c.column_name == "is_bool")
        .expect("is_bool column");
    assert_eq!(is_bool.udt_name.as_deref(), Some("tinyint(1)"));
    // Spot-check a few MySQL types are present
    assert!(meta.columns.iter().any(|c| c.column_name == "name"));
    assert!(meta.columns.iter().any(|c| c.column_name == "amount"));
    assert!(meta.columns.iter().any(|c| c.column_name == "feeling"));
}

#[tokio::test]
async fn type_mapper_covers_hostile() {
    // Pure unit, no DB — the same mapping the extractor decodes through.
    let ty = |data_type: &str, column_type: &str| {
        arrow_type_for(&ColumnMetadata {
            column_name: "c".to_string(),
            data_type: data_type.to_string(),
            is_nullable: true,
            numeric_precision: None,
            numeric_scale: None,
            udt_name: Some(column_type.to_string()),
            collation_name: None,
        })
        .unwrap()
    };
    assert_eq!(ty("bigint", "bigint"), DataType::Int64);
    assert_eq!(ty("bigint", "bigint unsigned"), DataType::UInt64);
    assert_eq!(ty("tinyint", "tinyint(1)"), DataType::Boolean);
    assert_eq!(ty("varchar", "varchar(20)"), DataType::Utf8);
    assert_eq!(ty("decimal", "decimal(12,2)"), DataType::Decimal128(12, 2));
    assert_eq!(
        ty("datetime", "datetime(6)"),
        DataType::Timestamp(TimeUnit::Microsecond, None)
    );
    assert_eq!(ty("json", "json"), DataType::Utf8);
    // Unknown falls back to Utf8 (prototype behavior)
    assert_eq!(ty("geometry", "geometry"), DataType::Utf8);
}

/// Full extract: 8 rows, columns decode to their **typed** Arrow types (not blanket `Utf8`),
/// values survive the typed decode, and the batch is valid Arrow DataFusion can ingest.
#[tokio::test]
async fn full_extract_typed_schema_values_and_datafusion_ingest() {
    let db = MySqlTestDb::connect().await;
    let ex = extractor(&db).await;

    // No ORDER BY in extraction: sort by id before asserting per-row positions.
    let batch = mysql_common::sorted_by(
        &ex.extract_full_table(&db.table(), None)
            .await
            .expect("extract_full_table"),
        "id",
    );

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
    assert_eq!(dt("is_bool"), DataType::Boolean); // BOOLEAN = tinyint(1)
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
    // No ORDER BY in extraction: sort by id before asserting per-row positions.
    let batch = mysql_common::sorted_by(
        &ex.extract_full_table(&db.table(), None).await.unwrap(),
        "id",
    );

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
    assert_eq!(batch.schema().fields().len(), HOSTILE_COLUMNS);

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
    use el_ballista::pushdown::dialect::SqlDialect;
    let d = MysqlDialect;
    assert_eq!(d.quote_ident("order"), "`order`");
    assert_eq!(d.quote_ident("a`b"), "`a``b`");
    assert_eq!(d.placeholder(1), "?");
    assert_eq!(d.placeholder(5), "?");
}

/// Streaming extract: `batch_size` bounds every batch, the tail is included, and the batches
/// union to exactly the materialized extract (metamorphic oracle), with one schema throughout.
#[tokio::test]
async fn streamed_batches_are_bounded_and_union_to_full() {
    let db = MySqlTestDb::connect().await;
    let ex = extractor(&db).await;
    let mut batches = Vec::new();
    let schema = ex
        .extract_full_table_for_each_batch(&db.table(), None, 3, |b| {
            batches.push(b);
            Ok(())
        })
        .await
        .expect("streamed extract");
    let mut sizes: Vec<usize> = batches.iter().map(|b| b.num_rows()).collect();
    sizes.sort_unstable();
    assert_eq!(sizes, vec![2, 3, 3]);
    assert!(batches.iter().all(|b| b.schema() == schema));

    let streamed = mysql_common::sorted_by(
        &arrow::compute::concat_batches(&schema, &batches).unwrap(),
        "id",
    );
    let full = mysql_common::sorted_by(
        &ex.extract_full_table(&db.table(), None).await.unwrap(),
        "id",
    );
    assert_eq!(streamed, full);

    // Empty table: callback never runs, schema still returned.
    sqlx::query(sqlx::AssertSqlSafe(
        format!("CREATE TABLE `{0}`.`e` LIKE `{0}`.`hostile`", db.database).as_str(),
    ))
    .execute(&db.pool)
    .await
    .unwrap();
    let mut calls = 0;
    let schema = ex
        .extract_full_table_for_each_batch(&format!("{}.e", db.database), None, 3, |_| {
            calls += 1;
            Ok(())
        })
        .await
        .unwrap();
    assert_eq!(calls, 0);
    assert_eq!(schema.fields().len(), HOSTILE_COLUMNS);

    // batch_size 0 is rejected, not "zero rows".
    let err = ex
        .extract_full_table_for_each_batch(&db.table(), None, 0, |_| Ok(()))
        .await
        .unwrap_err();
    assert!(matches!(err, MysqlError::InvalidBatchSize), "{err}");
    db.cleanup().await;
}

/// Unknown table / unknown projection column are typed errors, not `SELECT  FROM` or a silently
/// narrower batch.
#[tokio::test]
async fn unknown_table_and_column_are_typed_errors() {
    let db = MySqlTestDb::connect().await;
    let ex = extractor(&db).await;
    let err = ex
        .extract_full_table("no_such_table", None)
        .await
        .unwrap_err();
    assert!(
        matches!(&err, MysqlError::TableNotFound { table, .. } if table == "no_such_table"),
        "{err}"
    );
    let err = ex
        .extract_full_table(&db.table(), Some(vec!["id", "nmae"]))
        .await
        .unwrap_err();
    assert!(
        matches!(&err, MysqlError::UnknownColumns { missing, .. } if missing == &vec!["nmae".to_string()]),
        "{err}"
    );
    db.cleanup().await;
}

/// Unsigned / boolean / YEAR / TIME / BIT edges, oracle = trivial (the literal values inserted): unsigned maxima never wrap,
/// BOOLEAN is Boolean, YEAR is Int16, TIME is a signed Duration(µs), BIT(n) is UInt64.
#[tokio::test]
async fn unsigned_bool_year_time_bit_decode_losslessly() {
    use arrow::array::{
        BooleanArray, DurationMicrosecondArray, Int16Array, Int32Array, Int64Array, UInt64Array,
    };
    let db = MySqlTestDb::connect().await;
    for sql in [
        "CREATE TABLE edges (id INT PRIMARY KEY, tu TINYINT UNSIGNED, su SMALLINT UNSIGNED, \
         mu MEDIUMINT UNSIGNED, iu INT UNSIGNED, bu BIGINT UNSIGNED, b BOOLEAN, y YEAR, \
         tm TIME(6), b8 BIT(8))",
        "INSERT INTO edges VALUES \
         (1, 255, 65535, 16777215, 4294967295, 18446744073709551615, TRUE, 2155, '838:59:59', b'11111111'), \
         (2, 0, 0, 0, 0, 0, FALSE, 1901, '-12:34:56.5', b'00000001'), \
         (3, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)",
    ] {
        sqlx::query(sqlx::AssertSqlSafe(sql))
            .execute(&db.pool)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
    let ex = extractor(&db).await;
    let b = mysql_common::sorted_by(&ex.extract_full_table("edges", None).await.unwrap(), "id");
    macro_rules! col {
        ($name:literal, $ty:ty) => {
            b.column(b.schema().index_of($name).unwrap())
                .as_any()
                .downcast_ref::<$ty>()
                .unwrap_or_else(|| {
                    panic!(
                        "{} has type {}",
                        $name,
                        b.schema().field_with_name($name).unwrap().data_type()
                    )
                })
        };
    }
    let tu = col!("tu", Int16Array);
    assert_eq!((tu.value(0), tu.value(1), tu.is_null(2)), (255, 0, true));
    let su = col!("su", Int32Array);
    assert_eq!((su.value(0), su.value(1)), (65_535, 0));
    let mu = col!("mu", Int32Array);
    assert_eq!(mu.value(0), 16_777_215);
    let iu = col!("iu", Int64Array);
    assert_eq!(iu.value(0), 4_294_967_295);
    let bu = col!("bu", UInt64Array);
    assert_eq!(
        (bu.value(0), bu.value(1), bu.is_null(2)),
        (u64::MAX, 0, true)
    );
    let bo = col!("b", BooleanArray);
    assert_eq!(
        (bo.value(0), bo.value(1), bo.is_null(2)),
        (true, false, true)
    );
    let y = col!("y", Int16Array);
    assert_eq!((y.value(0), y.value(1), y.is_null(2)), (2155, 1901, true));
    let tm = col!("tm", DurationMicrosecondArray);
    assert_eq!(
        tm.value(0),
        838 * 3_600_000_000 + 59 * 60_000_000 + 59_000_000
    );
    assert_eq!(
        tm.value(1),
        -(12 * 3_600_000_000 + 34 * 60_000_000 + 56_500_000)
    );
    assert!(tm.is_null(2));
    let b8 = col!("b8", UInt64Array);
    assert_eq!((b8.value(0), b8.value(1)), (255, 1));

    // A BOOLEAN holding 2 cannot be represented as Arrow Boolean: typed error, not `true`.
    sqlx::query("INSERT INTO edges (id, b) VALUES (4, 2)")
        .execute(&db.pool)
        .await
        .unwrap();
    let err = ex
        .extract_full_table("edges", Some(vec!["id", "b"]))
        .await
        .unwrap_err();
    assert!(
        matches!(&err, MysqlError::NotBoolean { column } if column == "b"),
        "{err}"
    );
    db.cleanup().await;
}

/// Credentials are passed field by field, not interpolated into a URL: a password with URL
/// metacharacters (`/ # ? @ :`) connects.
#[tokio::test]
async fn password_with_url_metacharacters_connects() {
    let db = MySqlTestDb::connect().await;
    let user = format!("u_{}", db.database);
    let password = "p/a#s?s@w:rd%";
    for sql in [
        format!("CREATE USER '{user}'@'%' IDENTIFIED BY '{password}'"),
        format!("GRANT SELECT ON `{}`.* TO '{user}'@'%'", db.database),
    ] {
        sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
            .execute(&db.pool)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
    let result = MysqlExtractor::connect(&db.host, db.port, &user, password, &db.database, 1).await;
    let rows = match &result {
        Ok(ex) => ex
            .extract_full_table(&db.table(), Some(vec!["id"]))
            .await
            .map(|b| b.num_rows())
            .map_err(|e| e.to_string()),
        Err(e) => Err(e.to_string()),
    };
    drop(result);
    let drop_user = format!("DROP USER '{user}'@'%'");
    sqlx::query(sqlx::AssertSqlSafe(drop_user.as_str()))
        .execute(&db.pool)
        .await
        .unwrap();
    assert_eq!(rows, Ok(8));
    db.cleanup().await;
}

/// MySQL zero dates and ENUM/SET decoding. Oracles: trivial (hand-written expected
/// values) and the typed error for the zero date. sqlx itself decodes `0000-00-00` as NULL;
/// the connector must not let that pass as a real NULL.
#[tokio::test]
async fn zero_dates_error_and_enum_set_decode_as_text() {
    use arrow::array::{Array, StringArray};
    let db = MySqlTestDb::connect().await;
    for sql in [
        "SET SESSION sql_mode = ''",
        "CREATE TABLE zd (id INT PRIMARY KEY, d DATE, dt DATETIME, e ENUM('sad','ok'), \
         s SET('red','green','blue'))",
        "INSERT INTO zd VALUES (1, '2024-01-02', '2024-01-02 03:04:05', 'ok', 'red,blue'), \
         (2, NULL, NULL, NULL, ''), (3, '2024-01-03', '2024-01-03 00:00:00', 'sad', NULL)",
    ] {
        sqlx::query(sqlx::AssertSqlSafe(sql))
            .execute(&db.pool)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
    let ex = extractor(&db).await;
    let b = mysql_common::sorted_by(&ex.extract_full_table("zd", None).await.unwrap(), "id");
    let text = |name: &str| {
        b.column(b.schema().index_of(name).unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap_or_else(|| panic!("{name} should decode as Utf8"))
            .clone()
    };
    let e = text("e");
    assert_eq!((e.value(0), e.is_null(1), e.value(2)), ("ok", true, "sad"));
    let s = text("s");
    assert_eq!(
        (s.value(0), s.value(1), s.is_null(2)),
        ("red,blue", "", true)
    );
    let d = b.column(b.schema().index_of("d").unwrap());
    assert!(d.is_null(1) && !d.is_null(0), "a real NULL stays NULL");

    // A zero date must fail loudly instead of becoming NULL (DATE and DATETIME).
    for (col, sql) in [
        ("d", "INSERT INTO zd (id, d) VALUES (4, '0000-00-00')"),
        (
            "dt",
            "UPDATE zd SET d = NULL, dt = '0000-00-00 00:00:00' WHERE id = 4",
        ),
    ] {
        let mut conn = db.pool.acquire().await.unwrap();
        sqlx::query("SET SESSION sql_mode = ''")
            .execute(&mut *conn)
            .await
            .unwrap();
        sqlx::query(sqlx::AssertSqlSafe(sql))
            .execute(&mut *conn)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
        drop(conn);
        let err = ex.extract_full_table("zd", None).await.unwrap_err();
        assert!(
            matches!(&err, MysqlError::ZeroDate { column } if column == col),
            "{col}: {err}"
        );
    }
    db.cleanup().await;
}
