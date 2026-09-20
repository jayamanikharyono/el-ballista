//! Exhaustive extraction matrix: every dvdrental table, extracted through the connector, checked
//! against the same data queried directly from the database, on BOTH engines, plus cross-engine.
//!
//! Three oracle layers per (table, column):
//!   1. same-engine: connector extraction == direct `SELECT CAST(col AS text/CHAR)` on that engine
//!      — proves the connector reads/decodes exactly what the DB holds.
//!   2. cross-engine: Postgres connector == MySQL connector — proves the shared dvdrental rows are
//!      identical in both engines.
//!   3. (in `dvdrental_cross_engine.rs`) absolute row counts.
//!
//! Plus the Postgres-only methods vs direct SQL: incremental window and keyset partitioning.
//!
//! Comparison method: every cell — from Arrow and from the DB's `CAST(... AS text)` — is reduced to
//! ONE canonical string by [`canon`], then per-column value multisets are sorted and compared. The
//! same `canon` runs on all sides, so decimal scale (`100.00` vs `100`) and timestamp precision
//! (`…:57.62` vs `…:57.620000`) converge; genuinely equal values compare equal regardless of how
//! each engine renders them.
//!
//! Structurally-incomparable columns are excluded per table (see `MATRIX`), and why:
//!   * `film.special_features` — Postgres `text[]` vs MySQL text; different shapes.
//!   * `film.fulltext` — Postgres `tsvector` (the connector can't decode it) ; MySQL text.
//!   * `film.release_year` / `film.rating` — Postgres domain/enum decode is out of scope here.
//!   * `staff.picture` — still excluded here because the `CAST(... AS text/CHAR)` oracle cannot
//!     render binary identically on both engines. DB-level bytes DO match now (the seed UNHEX-decodes
//!     the COPY `\\x<hex>` text — see `gen_mysql_seed.py`); parity is covered by the picture test in
//!     `dvdrental_cross_engine.rs`, which compares Arrow `Binary` on both sides.
//!   * `customer.activebool` / `staff.active` — Postgres `bool` (`true`/`false`) vs MySQL
//!     `tinyint` (`1`/`0`); not comparable without a bool mapping.
//!
//! Everything else on all 15 tables is compared cell-for-cell.
//!
//! Requires the compose stack up (`docker compose -f tests/docker/compose.yaml up -d --wait`).

use arrow::array::{
    Array, Date32Array, Decimal128Array, Float32Array, Float64Array, Int16Array, Int32Array,
    Int64Array, Int8Array, StringArray, TimestampMicrosecondArray,
};
use arrow::array::ArrayRef;
use arrow::datatypes::{DataType, TimeUnit};
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, TimeZone, Utc};
use rust_ballista_extraction_layer::connector::mysql::MysqlExtractor;
use rust_ballista_extraction_layer::connector::postgres::PostgresExtractor;
use sqlx::mysql::MySqlPoolOptions;
use sqlx::postgres::PgPoolOptions;
use sqlx::{MySqlPool, PgPool, Row};

const PG_URL_DEFAULT: &str = "postgres://postgres:postgres@127.0.0.1:5432/test";
const MY_URL_DEFAULT: &str = "mysql://root:password@127.0.0.1:3306/test";

/// A table and the columns compared cell-for-cell (incomparable columns omitted — see module docs).
struct Table {
    name: &'static str,
    cols: &'static [&'static str],
}

const MATRIX: &[Table] = &[
    Table { name: "actor", cols: &["actor_id", "first_name", "last_name", "last_update"] },
    Table { name: "address", cols: &["address_id", "address", "address2", "district", "city_id", "postal_code", "phone", "last_update"] },
    Table { name: "category", cols: &["category_id", "name", "last_update"] },
    Table { name: "city", cols: &["city_id", "city", "country_id", "last_update"] },
    Table { name: "country", cols: &["country_id", "country", "last_update"] },
    Table { name: "customer", cols: &["customer_id", "store_id", "first_name", "last_name", "email", "address_id", "create_date", "last_update"] },
    Table { name: "film", cols: &["film_id", "title", "description", "language_id", "rental_duration", "rental_rate", "length", "replacement_cost", "last_update"] },
    Table { name: "film_actor", cols: &["actor_id", "film_id", "last_update"] },
    Table { name: "film_category", cols: &["film_id", "category_id", "last_update"] },
    Table { name: "inventory", cols: &["inventory_id", "film_id", "store_id", "last_update"] },
    Table { name: "language", cols: &["language_id", "name", "last_update"] },
    Table { name: "payment", cols: &["payment_id", "customer_id", "staff_id", "rental_id", "amount", "payment_date"] },
    Table { name: "rental", cols: &["rental_id", "rental_date", "inventory_id", "customer_id", "return_date", "staff_id", "last_update"] },
    Table { name: "staff", cols: &["staff_id", "first_name", "last_name", "address_id", "email", "store_id", "username", "password", "last_update"] },
    Table { name: "store", cols: &["store_id", "manager_staff_id", "address_id", "last_update"] },
];

// ---- connection helpers ----

fn pg_url() -> String {
    std::env::var("DATABASE_URL").ok().filter(|u| !u.trim().is_empty()).unwrap_or_else(|| PG_URL_DEFAULT.to_string())
}
fn my_url() -> String {
    std::env::var("MYSQL_URL").ok().or_else(|| std::env::var("DATABASE_URL_MYSQL").ok()).filter(|u| !u.trim().is_empty()).unwrap_or_else(|| MY_URL_DEFAULT.to_string())
}
fn parse(url: &str, default_port: u16) -> (String, u16, String, String, String) {
    let rest = url.split_once("://").map(|(_, r)| r).unwrap_or_else(|| panic!("bad url {url:?}"));
    let (auth, rest) = rest.split_once('@').unwrap_or_else(|| panic!("bad url {url:?}"));
    let (user, password) = auth.split_once(':').unwrap_or((auth, ""));
    let (hostport, database) = rest.split_once('/').unwrap_or_else(|| panic!("bad url {url:?}"));
    let database = database.split('?').next().unwrap_or(database);
    let (host, port) = match hostport.split_once(':') {
        Some((h, p)) => (h.to_string(), p.parse().unwrap_or(default_port)),
        None => (hostport.to_string(), default_port),
    };
    (host, port, user.to_string(), password.to_string(), database.to_string())
}
async fn pg_pool() -> PgPool {
    PgPoolOptions::new().max_connections(4).connect(&pg_url()).await.unwrap_or_else(|e| {
        panic!("cannot connect to Postgres ({e}). Is the compose stack up? `docker compose -f tests/docker/compose.yaml up -d --wait`.")
    })
}
async fn my_pool() -> MySqlPool {
    MySqlPoolOptions::new().max_connections(4).connect(&my_url()).await.unwrap_or_else(|e| {
        panic!("cannot connect to MySQL ({e}). Is the compose stack up? `docker compose -f tests/docker/compose.yaml up -d --wait`.")
    })
}
async fn pg_extractor() -> PostgresExtractor {
    let (h, p, u, pw, db) = parse(&pg_url(), 5432);
    PostgresExtractor::connect(&h, p, &u, &pw, &db, 4, 30_000, "rbel-matrix").await.unwrap_or_else(|e| panic!("PostgresExtractor::connect: {e}"))
}
async fn my_extractor() -> (MysqlExtractor, String) {
    let (h, p, u, pw, db) = parse(&my_url(), 3306);
    let ex = MysqlExtractor::connect(&h, p, &u, &pw, &db, 4)
        .await
        .unwrap_or_else(|e| panic!("MysqlExtractor::connect: {e}"));
    (ex, db)
}

// ---- canonicalization ----

/// One canonical string per value, applied identically on every side. Trims trailing spaces
/// first (`bpchar`/`CHAR(n)` pads to length on store while `CAST(... AS text/CHAR)` trims, so the
/// padded connector value and the trimmed oracle text converge), then trailing zeros in a
/// fractional part (and a bare trailing dot), which converges decimal scale and timestamp precision
/// across Arrow rendering and each engine's `CAST(... AS text)`; leaves other values alone.
fn canon(s: &str) -> String {
    let s = s.trim_end_matches(' ');
    if s.contains('.') {
        let t = s.trim_end_matches('0');
        let t = t.trim_end_matches('.');
        return t.to_string();
    }
    s.to_string()
}

fn decimal_str(v: i128, scale: i8) -> String {
    if scale <= 0 {
        return v.to_string();
    }
    let s = scale as usize;
    let neg = v < 0;
    let mag = v.unsigned_abs().to_string();
    let mag = if mag.len() <= s {
        format!("{:0>width$}", mag, width = s + 1)
    } else {
        mag
    };
    let (int_part, frac_part) = mag.split_at(mag.len() - s);
    let body = format!("{int_part}.{frac_part}");
    if neg { format!("-{body}") } else { body }
}

/// Render one Arrow cell to its raw string (pre-`canon`). Only the types used by `MATRIX` columns
/// are handled; anything else means an excluded column slipped in.
fn arrow_cell(col: &ArrayRef, i: usize) -> Option<String> {
    if col.is_null(i) {
        return None;
    }
    let s = match col.data_type() {
        DataType::Int8 => col.as_any().downcast_ref::<Int8Array>().unwrap().value(i).to_string(),
        DataType::Int16 => col.as_any().downcast_ref::<Int16Array>().unwrap().value(i).to_string(),
        DataType::Int32 => col.as_any().downcast_ref::<Int32Array>().unwrap().value(i).to_string(),
        DataType::Int64 => col.as_any().downcast_ref::<Int64Array>().unwrap().value(i).to_string(),
        DataType::Float32 => col.as_any().downcast_ref::<Float32Array>().unwrap().value(i).to_string(),
        DataType::Float64 => col.as_any().downcast_ref::<Float64Array>().unwrap().value(i).to_string(),
        DataType::Utf8 => col.as_any().downcast_ref::<StringArray>().unwrap().value(i).to_string(),
        DataType::Date32 => {
            let days = col.as_any().downcast_ref::<Date32Array>().unwrap().value(i);
            let d = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap() + chrono::Duration::days(days as i64);
            d.format("%Y-%m-%d").to_string()
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            let us = col.as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap().value(i);
            DateTime::from_timestamp_micros(us).unwrap().naive_utc().format("%Y-%m-%d %H:%M:%S%.6f").to_string()
        }
        DataType::Decimal128(_, scale) => {
            let v = col.as_any().downcast_ref::<Decimal128Array>().unwrap().value(i);
            decimal_str(v, *scale)
        }
        other => panic!("matrix: unexpected Arrow type {other:?} (excluded column leaked in?)"),
    };
    Some(canon(&s))
}

/// A connector-extracted column as a sorted multiset of canonical strings.
fn conn_col(batch: &RecordBatch, col: &str) -> Vec<Option<String>> {
    let idx = batch.schema().index_of(col).unwrap_or_else(|_| panic!("column {col} in batch"));
    let arr = batch.column(idx);
    let mut v: Vec<Option<String>> = (0..arr.len()).map(|i| arrow_cell(arr, i)).collect();
    v.sort();
    v
}

/// The same column read directly from Postgres via `CAST(... AS text)`, canonicalized and sorted.
async fn pg_col(pool: &PgPool, table: &str, col: &str) -> Vec<Option<String>> {
    let sql = format!("SELECT CAST(\"{col}\" AS text) FROM public.\"{table}\"");
    let rows = sqlx::query(sqlx::AssertSqlSafe(sql)).fetch_all(pool).await.unwrap_or_else(|e| panic!("pg direct {table}.{col}: {e}"));
    let mut v: Vec<Option<String>> = rows.iter().map(|r| r.try_get::<Option<String>, _>(0).unwrap().map(|s| canon(&s))).collect();
    v.sort();
    v
}

/// The same column read directly from MySQL via `CAST(... AS CHAR)`, canonicalized and sorted.
async fn my_col(pool: &MySqlPool, db: &str, table: &str, col: &str) -> Vec<Option<String>> {
    let sql = format!("SELECT CAST(`{col}` AS CHAR) FROM `{db}`.`{table}`");
    let rows = sqlx::query(sqlx::AssertSqlSafe(sql)).fetch_all(pool).await.unwrap_or_else(|e| panic!("mysql direct {table}.{col}: {e}"));
    let mut v: Vec<Option<String>> = rows.iter().map(|r| r.try_get::<Option<String>, _>(0).unwrap().map(|s| canon(&s))).collect();
    v.sort();
    v
}

#[tokio::test]
async fn full_extraction_matches_direct_sql_on_both_engines_and_cross_engine() {
    let pgp = pg_pool().await;
    let myp = my_pool().await;
    let pgx = pg_extractor().await;
    let (myx, db) = my_extractor().await;

    for t in MATRIX {
        let cols: Vec<&str> = t.cols.to_vec();
        let pg_batch = pgx
            .extract_full_table(&format!("public.{}", t.name), Some(cols.clone()))
            .await
            .unwrap_or_else(|e| panic!("PG extract {}: {e}", t.name));
        let my_batch = myx
            .extract_full_table(&format!("{db}.{}", t.name), Some(cols.clone()))
            .await
            .unwrap_or_else(|e| panic!("MySQL extract {}: {e}", t.name));

        for col in t.cols {
            let pg_conn = conn_col(&pg_batch, col);
            let my_conn = conn_col(&my_batch, col);
            let pg_sql = pg_col(&pgp, t.name, col).await;
            let my_sql = my_col(&myp, &db, t.name, col).await;

            // Layer 1: each connector matches its own DB queried directly.
            assert_eq!(pg_conn, pg_sql, "PG connector != PG direct SQL for {}.{}", t.name, col);
            assert_eq!(my_conn, my_sql, "MySQL connector != MySQL direct SQL for {}.{}", t.name, col);
            // Layer 2: the two connectors agree on the shared dataset.
            assert_eq!(pg_conn, my_conn, "PG != MySQL for {}.{}", t.name, col);
        }
    }
}

#[tokio::test]
async fn pg_incremental_window_matches_direct_sql() {
    let pgp = pg_pool().await;
    let pgx = pg_extractor().await;

    // A day-wide window over `rental.rental_date`; the connector uses the half-open `(lo, hi]`.
    let lo: DateTime<Utc> = Utc.with_ymd_and_hms(2005, 6, 15, 0, 0, 0).unwrap();
    let hi: DateTime<Utc> = Utc.with_ymd_and_hms(2005, 6, 16, 0, 0, 0).unwrap();

    let batch = pgx
        .extract_incremental_window(
            "public.rental",
            Some(vec!["rental_id", "rental_date"]),
            "rental_date",
            lo,
            hi,
        )
        .await
        .expect("incremental window");
    let conn_ids = conn_col(&batch, "rental_id");

    let rows = sqlx::query(
        "SELECT CAST(rental_id AS text) FROM public.rental WHERE rental_date > $1 AND rental_date <= $2",
    )
    .bind(lo)
    .bind(hi)
    .fetch_all(&pgp)
    .await
    .expect("direct window query");
    let mut sql_ids: Vec<Option<String>> = rows.iter().map(|r| r.try_get::<Option<String>, _>(0).unwrap().map(|s| canon(&s))).collect();
    sql_ids.sort();

    assert!(!conn_ids.is_empty(), "window should not be empty (fixture has June 2005 rentals)");
    assert_eq!(conn_ids, sql_ids, "incremental window extraction != direct SQL over the same window");
}

#[tokio::test]
async fn pg_keyset_partitions_union_equals_full() {
    let pgx = pg_extractor().await;

    // Metamorphic oracle: two keyset partitions tiling the id space must union to the full scan
    // (exercises the keyset predicate without hand-reproducing its exact boundary).
    let full = pgx
        .extract_full_table("public.actor", Some(vec!["actor_id"]))
        .await
        .expect("full actor");
    let a = pgx
        .extract_keyset_partition("public.actor", Some(vec!["actor_id"]), "actor_id", 0, 100)
        .await
        .expect("keyset a");
    let b = pgx
        .extract_keyset_partition("public.actor", Some(vec!["actor_id"]), "actor_id", 100, 1_000_000)
        .await
        .expect("keyset b");

    let mut union: Vec<Option<String>> = conn_col(&a, "actor_id");
    union.extend(conn_col(&b, "actor_id"));
    union.sort();

    let full_ids = conn_col(&full, "actor_id");
    assert_eq!(union, full_ids, "keyset partitions union must equal the full actor scan");
}
