//! Pushdown differential: `always` vs `never` return identical rows.
//!
//! The single highest-value integration test (testing-plan.md Phase C.2): any fidelity
//! lie — `Exact` that isn't, or `Inexact` whose source form returns *fewer* rows than Arrow —
//! shows up as a row mismatch.
//!
//! Oracle (AGENTS.md §7): **differential** — the same SQL with pushdown policy `always`
//! against `never` (all filtering in Arrow). A second, local check asserts which cases are
//! actually pushed under `always`, so a passing differential cannot be vacuous.
//!
//! Run: `cargo test --test pg_pushdown -- --test-threads=1` (requires the compose stack up;
//! `DATABASE_URL` overrides the default endpoint).

#[path = "common/mod.rs"]
mod common;

use std::sync::Arc;

use common::{TEST_PASSWORD_ENV, TestCluster, TestDb};
use datafusion::physical_plan::displayable;
use datafusion::prelude::SessionContext;
use el_ballista::config::{
    CheckpointConfig, DistributedConfig, ExecutionConfig, JobConfig, ParallelScanConfig,
    PushdownConfig, SourceConfig,
};
use el_ballista::connector::postgres::PostgresTableProvider;
use el_ballista::connector::postgres::distributed::DistributedContext;
use el_ballista::connector::postgres::distributed::connection::PostgresConnectionDescriptor;
use el_ballista::connector::postgres::register_table;
use el_ballista::pushdown::PushdownPolicy;
use el_ballista::pushdown::cost_model::CostParams;

type R<T = ()> = Result<T, Box<dyn std::error::Error>>;

/// Always uses a real database (the compose stack, unless `DATABASE_URL` is set) — never
/// skips. Kept as a macro only so call sites (`let db = live!();`) didn't need to change.
macro_rules! live {
    () => {
        TestDb::connect().await
    };
}

fn job_for(db: &TestDb, table: &str, policy: &str) -> JobConfig {
    JobConfig {
        job_id: "pushdown_diff".to_string().parse().unwrap(),
        table: table.to_string(),
        columns: None,
        filters: Vec::new(),
        source: SourceConfig {
            host: db.host.clone(),
            port: db.port,
            user: db.user.clone(),
            password_env: TEST_PASSWORD_ENV.to_string(),
            database: db.database.clone(),
            pool_max: 4,
            statement_timeout_ms: 300_000,
            application_name: "relex-test".to_string(),
            schema: db.schema.clone(),
        },
        checkpoint: CheckpointConfig::default(),
        pushdown: PushdownConfig {
            policy: policy.parse().unwrap(),
            ..Default::default()
        },
        parallel_scan: ParallelScanConfig {
            strategy: "keyset".parse().unwrap(),
            partitions: 2,
            partition_column: "id".to_string(),
        },
        execution: ExecutionConfig::default(),
        distributed: DistributedConfig {
            scheduler_url: String::new(),
            workers: 2,
            ..DistributedConfig::default()
        },
    }
}

/// `SELECT id FROM <table> WHERE <filter>` through plain DataFusion (`register_table`).
async fn ids_where(db: &TestDb, table: &str, policy: &str, filter: &str) -> R<Vec<i64>> {
    let config = job_for(db, table, policy);
    let ctx = SessionContext::new();
    register_table(&ctx, &config).await?;
    let df = ctx
        .sql(&format!("SELECT id FROM {table} WHERE {filter}"))
        .await?;
    let batches = df.collect().await?;
    let mut ids = Vec::new();
    for b in &batches {
        ids.extend(common::int64_col(b, "id"));
    }
    ids.sort_unstable();
    Ok(ids)
}

/// A context on `cluster` (two workers) with `table` registered under `policy`: pushed
/// filters travel through the plan codecs to the worker processes.
async fn distributed_ctx(
    cluster: &TestCluster,
    db: &TestDb,
    table: &str,
    policy: &str,
) -> R<DistributedContext> {
    let config = job_for(db, table, policy);
    let ctx = DistributedContext::remote(&config, &cluster.url, 2).await?;
    ctx.register_source(&config).await?;
    Ok(ctx)
}

async fn ids_in(ctx: &SessionContext, table: &str, filter: &str) -> R<Vec<i64>> {
    let batches = ctx
        .sql(&format!("SELECT id FROM {table} WHERE {filter}"))
        .await?
        .collect()
        .await?;
    let mut ids = Vec::new();
    for b in &batches {
        ids.extend(common::int64_col(b, "id"));
    }
    ids.sort_unstable();
    Ok(ids)
}

/// A local (non-distributed) context with `table` registered under policy `always`, for
/// inspecting what the physical plan pushes.
async fn local_always_ctx(db: &TestDb, table: &str) -> R<SessionContext> {
    let config = job_for(db, table, "always");
    let provider = PostgresTableProvider::new(
        PostgresConnectionDescriptor::from_config(&config.source, 1),
        &config.resolved_table(),
        PushdownPolicy::Always,
        Vec::new(),
        Vec::new(),
        CostParams::default(),
        config.pushdown.statistics_ttl_secs,
        config.execution.batch_size,
    )
    .await?;
    let ctx = SessionContext::new();
    ctx.register_table(table, Arc::new(provider))?;
    Ok(ctx)
}

/// How many filters the physical plan pushes into the Postgres scan: the non-vacuity check
/// for the differential.
async fn pushed_filter_count(ctx: &SessionContext, table: &str, filter: &str) -> R<usize> {
    let plan = ctx
        .sql(&format!("SELECT id FROM {table} WHERE {filter}"))
        .await?
        .create_physical_plan()
        .await?;
    let text = displayable(plan.as_ref()).indent(true).to_string();
    let count = text
        .split("pushed_filters=")
        .nth(1)
        .and_then(|rest| rest.split(|c: char| !c.is_ascii_digit()).next())
        .and_then(|n| n.parse::<usize>().ok())
        .ok_or_else(|| format!("no Postgres scan in plan:\n{text}"))?;
    Ok(count)
}

#[tokio::test]
async fn pushdown_matches_no_pushdown() -> R {
    let db = live!();
    // Indexed-PK equality: pushed under `always`, kept under `never`.
    let pushed = ids_where(&db, "hostile", "always", "id = 3").await?;
    let kept = ids_where(&db, "hostile", "never", "id = 3").await?;
    assert_eq!(pushed, vec![3]);
    assert_eq!(pushed, kept);

    // OR mixing an indexed and an unindexed column: must agree either way
    // (regression cover for the cost-model branch rule).
    let pushed = ids_where(&db, "hostile", "always", "id = 1 OR nick = 'seven'").await?;
    let kept = ids_where(&db, "hostile", "never", "id = 1 OR nick = 'seven'").await?;
    assert_eq!(pushed, vec![1, 7]);
    assert_eq!(pushed, kept);
    Ok(())
}

/// One differential row per hazard: collation-sensitive text, `NOT`, float `-0.0`/`NaN`,
/// `IS NULL` precedence, enums under `OR`, uuid/jsonb text comparisons, and dates.
#[tokio::test]
async fn pushdown_differential_table() -> R {
    let db = live!();
    let s = db.schema.clone();
    let setup = [
        format!(
            "CREATE COLLATION {s}.ci (provider = icu, locale = 'und-u-ks-level2', \
             deterministic = false)"
        ),
        format!(
            r#"CREATE TABLE {s}.pd (
                id bigint PRIMARY KEY,
                name text COLLATE "en-US-x-icu",
                ci_name text COLLATE {s}.ci,
                x double precision,
                flag boolean,
                feeling {s}.mood,
                uid uuid,
                meta jsonb,
                note text,
                d date
            )"#
        ),
        format!(r#"CREATE INDEX pd_name_idx ON {s}.pd (name)"#),
        format!(
            r#"INSERT INTO {s}.pd VALUES
            (1, 'B', 'FOO', '-0',  true,  'sad',      '123e4567-e89b-12d3-a456-426614174000', '"x"', 'a',     '2024-02-29'),
            (2, 'a', 'bar', 1.0,   false, 'ok',       '123e4567-e89b-12d3-a456-426614174001', '1',   NULL,    '1970-01-01'),
            (3, 'c', 'foo', -1.0,  NULL,  'ecstatic', '123e4567-e89b-12d3-a456-426614174002', NULL,  'b',     '2024-01-01'),
            (4, NULL, NULL, NULL,  NULL,  NULL,       NULL,                                   'true', E'c\\d', NULL),
            (5, 'Ä', 'Foo', 'NaN', true,  'sad',      NULL,                                   '"y"', '',      '9999-12-31')"#
        ),
        format!("ANALYZE {s}.pd"),
    ];
    for sql in &setup {
        sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
            .execute(&db.pool)
            .await
            .map_err(|e| format!("setup failed: {sql}: {e}"))?;
    }

    // (filter, pushed under `always`?)
    let cases: &[(&str, bool)] = &[
        // Text range under an ICU collation ('B' < 'a' bytewise, not under ICU).
        ("name < 'a'", true),
        ("name >= 'B'", true),
        ("name <> 'a'", true),
        // NOT / equality under a nondeterministic (case-insensitive) collation.
        ("NOT (ci_name = 'foo')", true),
        ("ci_name = 'foo'", true),
        // -0.0 and NaN under float comparisons.
        ("x < 0.0", false),
        ("x <> 0.0", false),
        ("x = 0.0", true),
        ("NOT (x = 0.0)", false),
        // `(NOT flag) IS NULL` must keep its grouping.
        ("(NOT flag) IS NULL", true),
        ("NOT (flag IS NULL)", true),
        ("(NOT flag) IS NOT NULL", true),
        // Enum comparisons under OR / NOT / ranges.
        ("feeling = 'sad' OR feeling = 'ecstatic'", true),
        ("NOT (feeling = 'ok')", true),
        ("feeling < 'p'", true),
        // uuid / jsonb compared through their text form (=, <> only).
        ("uid = '123e4567-e89b-12d3-a456-426614174001'", true),
        ("uid <> '123e4567-e89b-12d3-a456-426614174001'", true),
        ("uid < '123e4567-e89b-12d3-a456-426614174001'", false),
        (r#"meta = '"x"'"#, true),
        // IS NULL on nullable columns of several kinds.
        ("name IS NULL", true),
        ("note IS NOT NULL", true),
        ("uid IS NULL", true),
        // Backslash literal (bound parameter).
        (r"note = 'c\d'", true),
        // Dates: every operator, NOT, a two-sided window, leap day, epoch, year 9999 and NULL.
        ("d >= '2024-01-01'", true),
        ("d < '2024-01-01'", true),
        ("d = '2024-02-29'", true),
        ("d <> '2024-02-29'", true),
        ("d <= '9999-12-31'", true),
        ("NOT (d = '1970-01-01')", true),
        ("d >= '2024-01-01' AND d < '2024-03-01'", true),
        ("d IS NULL", true),
        // A cast on the column is DataFusion's job, never pushed.
        ("CAST(d AS VARCHAR) = '2024-02-29'", false),
    ];

    // One cluster, one context per policy for the whole table.
    let cluster = TestCluster::start(2, 2).await;
    let never = distributed_ctx(&cluster, &db, "pd", "never").await?;
    let always = distributed_ctx(&cluster, &db, "pd", "always").await?;
    let local = local_always_ctx(&db, "pd").await?;

    let mut failures = Vec::new();
    for (filter, expect_pushed) in cases {
        let kept = ids_in(&never.session, "pd", filter).await?;
        let pushed = ids_in(&always.session, "pd", filter).await?;
        let n = pushed_filter_count(&local, "pd", filter).await?;
        println!("{filter:<50} never={kept:?} always={pushed:?} pushed_filters={n}");
        if pushed != kept {
            failures.push(format!(
                "`{filter}`: always={pushed:?} never={kept:?} (pushdown changed the answer)"
            ));
        }
        if (n > 0) != *expect_pushed {
            failures.push(format!(
                "`{filter}`: expected pushed={expect_pushed}, plan pushed {n} filter(s)"
            ));
        }
    }
    assert!(failures.is_empty(), "failures:\n{}", failures.join("\n"));
    Ok(())
}

/// Provider statistics refresh lazily once older than `statistics_ttl_secs`. Oracle:
/// the provider's own decision before vs after `ANALYZE` + one scan (TTL 0).
#[tokio::test]
async fn cost_statistics_refresh_after_ttl() -> R {
    use datafusion::prelude::{col, lit};
    use el_ballista::pushdown::Decision;

    let db = live!();
    let s = db.schema.clone();
    for sql in [
        format!("CREATE TABLE {s}.st (id bigint PRIMARY KEY, v bigint)"),
        format!("INSERT INTO {s}.st SELECT g, g FROM generate_series(1, 2000) g"),
    ] {
        sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
            .execute(&db.pool)
            .await?;
    }
    let config = job_for(&db, "st", "cost_based");
    let provider = PostgresTableProvider::new(
        PostgresConnectionDescriptor::from_config(&config.source, 1),
        &config.resolved_table(),
        PushdownPolicy::CostBased,
        Vec::new(),
        Vec::new(),
        CostParams::default(),
        0, // statistics TTL: stale immediately
        config.execution.batch_size,
    )
    .await?;
    let filter = col("v").eq(lit(5i64));
    // Never analyzed: no column statistics, unindexed column -> keep.
    assert!(matches!(provider.decide_cost(&filter), Decision::Keep));

    let sql = format!("ANALYZE {s}.st");
    sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
        .execute(&db.pool)
        .await?;
    // A scan (of anything) refreshes the stale snapshot for later decisions.
    let ctx = SessionContext::new();
    let provider = Arc::new(provider);
    ctx.register_table("st", provider.clone())?;
    ctx.sql("SELECT id FROM st WHERE id = 1")
        .await?
        .collect()
        .await?;
    // Now n_distinct(v) = 2000: selective and cheap -> push.
    assert!(
        matches!(provider.decide_cost(&filter), Decision::Push { .. }),
        "stale statistics were not refreshed: {}",
        provider.explain_decision(&filter)
    );
    Ok(())
}

/// Incremental-load windows on unindexed temporal columns (timestamptz, timestamp, date):
/// judged alone, one side of a one-day window keeps half the table and stays in Arrow; judged
/// with its sibling (as DataFusion hands them over), the pair is a 0.5% window estimated from
/// the column's histogram / most-common values and both sides push. Oracles: the provider's
/// own decisions, the physical plan's pushed-filter count, and a `never` differential.
#[tokio::test]
async fn range_windows_push_on_histogram_estimates() -> R {
    use datafusion::datasource::TableProvider;
    use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
    use datafusion::prelude::{col, lit};
    use datafusion::scalar::ScalarValue;

    let db = live!();
    let s = db.schema.clone();
    for sql in [
        format!(
            "CREATE TABLE {s}.ev (id bigint PRIMARY KEY, ts timestamptz NOT NULL, \
             naive timestamp NOT NULL, d date NOT NULL)"
        ),
        // 40k rows over 200 days (200 rows a day), no index on the temporal columns.
        format!(
            "INSERT INTO {s}.ev SELECT g, \
               timestamptz '2026-01-01 00:00:00+00' + (g % 200) * interval '1 day' \
                 + (g % 1440) * interval '1 minute', \
               timestamp '2026-01-01 00:00:00' + (g % 200) * interval '1 day' \
                 + (g % 1440) * interval '1 minute', \
               date '2026-01-01' + (g % 200) \
             FROM generate_series(1, 40000) g"
        ),
        format!("ANALYZE {s}.ev"),
    ] {
        sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
            .execute(&db.pool)
            .await
            .map_err(|e| format!("setup failed: {sql}: {e}"))?;
    }

    let config = job_for(&db, "ev", "cost_based");
    let provider = Arc::new(
        PostgresTableProvider::new(
            PostgresConnectionDescriptor::from_config(&config.source, 1),
            &config.resolved_table(),
            PushdownPolicy::CostBased,
            Vec::new(),
            Vec::new(),
            CostParams::default(),
            config.pushdown.statistics_ttl_secs,
            config.execution.batch_size,
        )
        .await?,
    );

    // Day 100 of 200: 2026-04-11 .. 2026-04-12.
    let day =
        |d: i64| chrono::NaiveDate::from_ymd_opt(2026, 1, 1).unwrap() + chrono::Duration::days(d);
    let micros = |d: i64| {
        day(d)
            .and_hms_opt(0, 0, 0)
            .unwrap()
            .and_utc()
            .timestamp_micros()
    };
    let days_since_epoch =
        |d: i64| (day(d) - chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()).num_days() as i32;
    let windows: Vec<(&str, Expr, Expr)> = vec![
        (
            "ts",
            col("ts").gt_eq(lit(ScalarValue::TimestampMicrosecond(
                Some(micros(100)),
                Some("UTC".into()),
            ))),
            col("ts").lt(lit(ScalarValue::TimestampMicrosecond(
                Some(micros(101)),
                Some("UTC".into()),
            ))),
        ),
        (
            "naive",
            col("naive").gt_eq(lit(ScalarValue::TimestampMicrosecond(
                Some(micros(100)),
                None,
            ))),
            col("naive").lt(lit(ScalarValue::TimestampMicrosecond(
                Some(micros(101)),
                None,
            ))),
        ),
        (
            "d",
            col("d").gt_eq(lit(ScalarValue::Date32(Some(days_since_epoch(100))))),
            col("d").lt(lit(ScalarValue::Date32(Some(days_since_epoch(101))))),
        ),
    ];

    let mut failures = Vec::new();
    for (name, lo, hi) in &windows {
        let alone = provider.explain_decision(lo);
        let verdicts = provider.supports_filters_pushdown(&[lo, hi])?;
        let reasons = provider.explain_decisions(&[lo, hi]);
        println!("{name}: alone={alone}\n  together={reasons:?}");
        if !alone.starts_with("KEEP") {
            failures.push(format!("{name}: lower bound alone should keep: {alone}"));
        }
        if verdicts
            != [
                TableProviderFilterPushDown::Exact,
                TableProviderFilterPushDown::Exact,
            ]
        {
            failures.push(format!(
                "{name}: window should push exactly: {verdicts:?} {reasons:?}"
            ));
        }
        if !reasons
            .iter()
            .all(|r| r.contains("window of 2 range filters"))
        {
            failures.push(format!(
                "{name}: reasons should name the window: {reasons:?}"
            ));
        }
    }

    // End to end through SQL: both sides reach the scan, and the rows match `never`.
    let ctx = SessionContext::new();
    ctx.register_table("ev", provider.clone())?;
    for filter in [
        "ts >= TIMESTAMP '2026-04-11 00:00:00' AND ts < TIMESTAMP '2026-04-12 00:00:00'",
        "naive >= TIMESTAMP '2026-04-11 00:00:00' AND naive < TIMESTAMP '2026-04-12 00:00:00'",
        "d >= '2026-04-11' AND d < '2026-04-12'",
    ] {
        let pushed = pushed_filter_count(&ctx, "ev", filter).await?;
        let got = ids_in(&ctx, "ev", filter).await?;
        let kept = ids_where(&db, "ev", "never", filter).await?;
        println!("{filter}: pushed_filters={pushed} rows={}", got.len());
        if pushed != 2 {
            failures.push(format!(
                "`{filter}`: expected 2 pushed filters, got {pushed}"
            ));
        }
        if got != kept || got.len() != 200 {
            failures.push(format!(
                "`{filter}`: {} rows pushed vs {} kept (want 200 each)",
                got.len(),
                kept.len()
            ));
        }
    }
    assert!(failures.is_empty(), "failures:\n{}", failures.join("\n"));
    Ok(())
}
