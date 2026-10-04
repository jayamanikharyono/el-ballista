//! Property-based pushdown differential (the "gold standard" oracle in AGENTS.md §7).
//!
//! Oracle: **differential** — for randomly generated predicates over a hostile little table
//! (ICU- and case-insensitively-collated text, `-0.0`/`NaN`/NULL floats, integer extremes,
//! booleans, an enum), the rows returned with pushdown policy `always` must equal the rows
//! returned with `never` (every filter evaluated by DataFusion over Arrow). Any predicate the
//! translator labels `Exact`/`Inexact` but that the source evaluates differently shows up as
//! a mismatch, printed with the SQL that produced it (not shrunk: the check runs async
//! against the database, see below).
//!
//! Deterministic: a fixed RNG seed, so a failure reproduces run-to-run.
//! Run: `cargo test --test pg_pushdown_prop -- --test-threads=1` (needs the compose stack).

#[path = "common/mod.rs"]
mod common;

use std::sync::Arc;

use common::{TEST_PASSWORD_ENV, TestDb};
use datafusion::prelude::SessionContext;
use el_ballista::config::{
    CheckpointConfig, DistributedConfig, ExecutionConfig, JobConfig, ParallelScanConfig,
    PushdownConfig, SourceConfig,
};
use el_ballista::connector::postgres::PostgresTableProvider;
use el_ballista::pushdown::PushdownPolicy;
use proptest::prelude::*;
use proptest::strategy::ValueTree;
use proptest::test_runner::{Config, RngAlgorithm, TestRng, TestRunner};

type R<T = ()> = Result<T, Box<dyn std::error::Error>>;

const CASES: u32 = 96;

fn job(db: &TestDb) -> JobConfig {
    JobConfig {
        job_id: "pushdown_prop".parse().unwrap(),
        table: "p".to_string(),
        columns: None,
        filters: Vec::new(),
        source: SourceConfig {
            host: db.host.clone(),
            port: db.port,
            user: db.user.clone(),
            password_env: TEST_PASSWORD_ENV.to_string(),
            database: db.database.clone(),
            pool_max: 4,
            statement_timeout_ms: 60_000,
            application_name: "relex-prop".to_string(),
            schema: db.schema.clone(),
        },
        checkpoint: CheckpointConfig::default(),
        pushdown: PushdownConfig::default(),
        parallel_scan: ParallelScanConfig::default(),
        execution: ExecutionConfig::default(),
        distributed: DistributedConfig::default(),
    }
}

async fn ctx_with(db: &TestDb, policy: PushdownPolicy) -> R<SessionContext> {
    let mut config = job(db);
    config.pushdown.policy = policy;
    let provider = PostgresTableProvider::from_config(&config).await?;
    let ctx = SessionContext::new();
    ctx.register_table("p", Arc::new(provider))?;
    Ok(ctx)
}

/// Whether the physical plan pushes any filter into the Postgres scan (non-vacuity check).
async fn pushes_something(ctx: &SessionContext, filter: &str) -> bool {
    let Ok(df) = ctx.sql(&format!("SELECT id FROM p WHERE {filter}")).await else {
        return false;
    };
    let Ok(plan) = df.create_physical_plan().await else {
        return false;
    };
    let text = datafusion::physical_plan::displayable(plan.as_ref())
        .indent(true)
        .to_string();
    text.split("pushed_filters=")
        .nth(1)
        .and_then(|rest| rest.split(|c: char| !c.is_ascii_digit()).next())
        .and_then(|n| n.parse::<usize>().ok())
        .is_some_and(|n| n > 0)
}

async fn ids(ctx: &SessionContext, filter: &str) -> Result<Vec<i64>, String> {
    let batches = ctx
        .sql(&format!("SELECT id FROM p WHERE {filter}"))
        .await
        .map_err(|e| e.to_string())?
        .collect()
        .await
        .map_err(|e| e.to_string())?;
    let mut out = Vec::new();
    for b in &batches {
        out.extend(common::int64_col(b, "id"));
    }
    out.sort_unstable();
    Ok(out)
}

/// A random boolean SQL predicate (DataFusion dialect) over table `p`.
fn predicate() -> impl Strategy<Value = String> {
    let text_lit = prop::sample::select(vec![
        "'a'", "'A'", "'B'", "'foo'", "'FOO'", "''", "'ä'", "'Z'", "'b '",
    ]);
    let text_col = prop::sample::select(vec!["icu", "ci", "plain"]);
    let float_lit = prop::sample::select(vec!["0.0", "-0.0", "1.5", "-1.5", "1e300"]);
    let int_lit = prop::sample::select(vec![
        "0",
        "-1",
        "5",
        "9223372036854775807",
        "-9223372036854775807",
    ]);
    let enum_lit = prop::sample::select(vec!["'sad'", "'ok'", "'ecstatic'"]);
    let op = prop::sample::select(vec!["=", "<>", "<", "<=", ">", ">="]);
    let any_col = prop::sample::select(vec!["icu", "ci", "plain", "x", "n", "flag", "mood"]);

    let leaf = prop_oneof![
        (text_col, op.clone(), text_lit).prop_map(|(c, o, l)| format!("{c} {o} {l}")),
        (op.clone(), float_lit).prop_map(|(o, l)| format!("x {o} {l}")),
        (op.clone(), int_lit).prop_map(|(o, l)| format!("n {o} {l}")),
        (op.clone(), enum_lit).prop_map(|(o, l)| format!("mood {o} {l}")),
        prop::sample::select(vec!["flag", "NOT flag", "flag = true", "flag <> false"])
            .prop_map(str::to_string),
        any_col.clone().prop_map(|c| format!("{c} IS NULL")),
        any_col.prop_map(|c| format!("{c} IS NOT NULL")),
    ];
    leaf.prop_recursive(3, 16, 2, |inner| {
        prop_oneof![
            inner.clone().prop_map(|p| format!("NOT ({p})")),
            inner.clone().prop_map(|p| format!("({p}) IS NULL")),
            (inner.clone(), inner.clone()).prop_map(|(a, b)| format!("({a}) AND ({b})")),
            (inner.clone(), inner).prop_map(|(a, b)| format!("({a}) OR ({b})")),
        ]
    })
}

#[tokio::test]
async fn random_predicates_agree_with_and_without_pushdown() -> R {
    let db = TestDb::connect().await;
    let s = &db.schema;
    for sql in [
        format!(
            "CREATE COLLATION {s}.ci (provider = icu, locale = 'und-u-ks-level2', deterministic = false)"
        ),
        // `{s}.mood` ('sad', 'ok', 'ecstatic', …) comes with the hostile fixture.
        format!(
            "CREATE TABLE {s}.p (id bigint PRIMARY KEY, icu text COLLATE \"en-US-x-icu\", \
             ci text COLLATE {s}.ci, plain text, x double precision, n bigint, flag boolean, \
             mood {s}.mood)"
        ),
        format!(
            "INSERT INTO {s}.p VALUES
             (1, 'a', 'foo', 'a', 0.0, 0, true, 'sad'),
             (2, 'B', 'FOO', 'B', '-0', -1, false, 'ok'),
             (3, 'A', 'Foo', 'A', 1.5, 5, NULL, 'ecstatic'),
             (4, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
             (5, 'ä', 'bar', 'ä', 'NaN', 9223372036854775807, true, 'sad'),
             (6, '', '', '', '-Infinity', -9223372036854775807, false, 'ok'),
             (7, 'Z', 'b ', 'b ', 1e300, 7, true, 'ecstatic'),
             (8, 'foo', 'ä', 'foo', -1.5, -5, false, NULL)"
        ),
        format!("ANALYZE {s}.p"),
    ] {
        sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
            .execute(&db.pool)
            .await?;
    }
    let always = ctx_with(&db, PushdownPolicy::Always).await?;
    let never = ctx_with(&db, PushdownPolicy::Never).await?;

    // Generate the whole (deterministic) case list first, then evaluate each against the
    // database. proptest's shrinking needs a sync closure, so on a mismatch we report the
    // failing predicate as-is (it is already small: depth <= 3).
    let mut runner = TestRunner::new_with_rng(
        Config {
            cases: CASES,
            ..Config::default()
        },
        TestRng::from_seed(RngAlgorithm::ChaCha, &[7u8; 32]),
    );
    let strategy = predicate();
    let mut mismatches = Vec::new();
    let mut pushed_cases = 0u32;
    for _ in 0..CASES {
        let filter = strategy
            .new_tree(&mut runner)
            .map_err(|e| format!("generate: {e}"))?
            .current();
        if pushes_something(&always, &filter).await {
            pushed_cases += 1;
        }
        let kept = ids(&never, &filter).await;
        let pushed = ids(&always, &filter).await;
        if kept != pushed {
            mismatches.push(format!(
                "{filter}\n    never:  {kept:?}\n    always: {pushed:?}"
            ));
        }
    }
    db.cleanup().await;
    println!("{pushed_cases} of {CASES} generated predicates pushed at least one filter");
    assert!(
        pushed_cases >= CASES / 4,
        "only {pushed_cases} of {CASES} cases pushed anything: the differential is near-vacuous"
    );
    assert!(
        mismatches.is_empty(),
        "{} of {CASES} predicates disagree:\n  {}",
        mismatches.len(),
        mismatches.join("\n  ")
    );
    Ok(())
}
