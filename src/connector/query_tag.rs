//! Debug SQL comment tags: a leading `/* ... */` comment identifying which pipeline and
//! run produced a query, so it can be recognized directly on the Postgres instance —
//! `pg_stat_activity`, `pg_stat_statements`, server logs — without cross-referencing
//! application logs. Backend-neutral (see `connector/mod.rs`'s SPI doc comment): any
//! connector can prepend one of these to whatever SQL it builds.
//!
//! Rendered shape (single line, sqlcommenter-adjacent but hand-rolled — no new dependency):
//!
//! ```text
//! /* rust-extract query_id=q_1a2b3c4d pipeline=orders_incremental run_id=r_9f8e7d6c strategy=incremental partition=7/23 */
//! ```
//!
//! `query_id` is fresh per query (one [`QueryTag`] per statement); `pipeline` and `run_id`
//! are session-scoped (one [`QuerySession`] per extractor connection / plan construction,
//! reused by every query issued through it) so grepping logs for one `run_id` shows every
//! statement from one run.

use uuid::Uuid;

/// A pipeline + run identity shared by every query issued through one session (one
/// `PostgresExtractor` connection, or one `PostgresExecutionPlan`). `run_id` is either a
/// fresh id generated once for the session, or the caller's real checkpoint `run_id` when
/// one exists — either way it stays fixed for the session's lifetime, so every query it
/// tags carries the same value.
#[derive(Debug, Clone)]
pub struct QuerySession {
    pipeline: String,
    run_id: String,
}

impl QuerySession {
    /// A fresh session with a generated `run_id`. Used whenever no checkpointed run is in
    /// progress (ad-hoc `rel plan`, the demo pipeline, table-scan sessions) — `pipeline` is
    /// typically the job's `application_name`, which is already threaded through config for
    /// an unrelated reason (identifying the Postgres connection) and doubles as a stable
    /// pipeline label here.
    pub fn new(pipeline: impl Into<String>) -> Self {
        Self {
            pipeline: sanitize(&pipeline.into()),
            run_id: fresh_run_id(),
        }
    }

    /// A session tied to a real checkpoint run: reuses the caller's actual `run_id` so the
    /// SQL comment matches the checkpoint file / application logs for that run, rather than
    /// inventing an unrelated one.
    pub fn for_run(pipeline: impl Into<String>, run_id: Uuid) -> Self {
        Self {
            pipeline: sanitize(&pipeline.into()),
            run_id: format!("r_{}", &run_id.simple().to_string()[..8]),
        }
    }

    /// Reconstruct a session from an already-rendered `run_id` string (e.g. read back off
    /// a serialized/deserialized execution plan) — used so a plan built on the scheduler and
    /// executed elsewhere tags every partition's query with the *same* run_id, rather than
    /// generating a new one per process.
    pub fn from_parts(pipeline: impl Into<String>, run_id: impl Into<String>) -> Self {
        Self {
            pipeline: sanitize(&pipeline.into()),
            run_id: run_id.into(),
        }
    }

    pub fn run_id(&self) -> &str {
        &self.run_id
    }

    pub fn pipeline(&self) -> &str {
        &self.pipeline
    }

    /// Build the tag for one query: a fresh `query_id`, this session's `pipeline`/`run_id`,
    /// and the caller-supplied `strategy` (e.g. "full", "incremental", "cursor", "keyset").
    pub fn tag(&self, strategy: impl Into<String>) -> QueryTag {
        QueryTag {
            query_id: format!("q_{}", short_id()),
            pipeline: self.pipeline.clone(),
            run_id: self.run_id.clone(),
            strategy: sanitize(&strategy.into()),
            partition: None,
        }
    }
}

/// One query's rendered debug identity. Build via [`QuerySession::tag`].
#[derive(Debug, Clone)]
pub struct QueryTag {
    query_id: String,
    pipeline: String,
    run_id: String,
    strategy: String,
    partition: Option<(usize, usize)>,
}

impl QueryTag {
    /// Record which partition (1-based) of how many this query covers, for parallel/keyset
    /// or ctid-split scans. Omitted from the rendered comment for unpartitioned queries.
    pub fn with_partition(mut self, index_one_based: usize, total: usize) -> Self {
        self.partition = Some((index_one_based, total));
        self
    }

    /// Render as a single-line SQL comment, safe to prepend to any generated statement.
    /// Every field is either program-generated (`query_id`, a generated `run_id`) or a
    /// short config identifier already sanitized in [`QuerySession::new`] — never raw user
    /// input — but `sanitize` runs again defensively so a surprising config value (an
    /// `application_name` containing `*/`) can never break out of the comment early.
    pub fn render(&self) -> String {
        let mut out = format!(
            "/* rust-extract query_id={} pipeline={} run_id={} strategy={}",
            sanitize(&self.query_id),
            sanitize(&self.pipeline),
            sanitize(&self.run_id),
            sanitize(&self.strategy),
        );
        if let Some((index, total)) = self.partition {
            out.push_str(&format!(" partition={index}/{total}"));
        }
        out.push_str(" */ ");
        out
    }
}

/// Strip anything that could end the comment early or break it across lines. None of the
/// inputs here are expected to contain these characters, but the comment is prepended to
/// real SQL sent to the server, so this is enforced rather than assumed.
fn sanitize(s: &str) -> String {
    s.replace("*/", "").replace(['\n', '\r'], " ")
}

fn short_id() -> String {
    Uuid::new_v4().simple().to_string()[..8].to_string()
}

/// A fresh, correctly-prefixed run id string, for callers that need to generate one to
/// pass around (e.g. across a serialization boundary) before a [`QuerySession`] exists —
/// see `PostgresExecutionPlan::try_new`'s `run_id` parameter.
pub fn fresh_run_id() -> String {
    format!("r_{}", short_id())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_render_contains_all_fields_in_order() {
        let session = QuerySession::from_parts("orders_incremental", "r_9f8e7d6c");
        let tag = session.tag("incremental");
        let rendered = tag.render();
        assert!(rendered.starts_with("/* rust-extract query_id=q_"));
        assert!(rendered.contains("pipeline=orders_incremental"));
        assert!(rendered.contains("run_id=r_9f8e7d6c"));
        assert!(rendered.contains("strategy=incremental"));
        assert!(rendered.trim_end().ends_with("*/"));
        assert!(
            !rendered.contains("partition="),
            "unpartitioned tag must omit partition"
        );
    }

    #[test]
    fn test_with_partition_appends_index_and_total() {
        let session = QuerySession::from_parts("orders_incremental", "r_9f8e7d6c");
        let tag = session.tag("keyset").with_partition(7, 23);
        assert!(tag.render().contains("partition=7/23"));
    }

    #[test]
    fn test_each_tag_from_a_session_gets_a_fresh_query_id_but_shares_run_id() {
        let session = QuerySession::from_parts("p", "r_fixed");
        let a = session.tag("full").render();
        let b = session.tag("full").render();
        assert_ne!(
            a, b,
            "two calls must not render identically (query_id must differ)"
        );
        assert!(a.contains("run_id=r_fixed") && b.contains("run_id=r_fixed"));
    }

    #[test]
    fn test_for_run_uses_the_given_uuid_not_a_random_one() {
        let run_id = Uuid::parse_str("11111111-2222-3333-4444-555555555555").unwrap();
        let session = QuerySession::for_run("p", run_id);
        // First 8 hex chars of the simple (no-dashes) representation, deterministically --
        // not a freshly generated random id.
        assert_eq!(session.run_id(), "r_11111111");
    }

    #[test]
    fn test_sanitize_prevents_early_comment_termination() {
        // Devil's advocate: a pipeline/strategy value that itself contains "*/" must never
        // be able to close the SQL comment early and inject trailing text as live SQL.
        let session = QuerySession::new("evil*/ DROP TABLE users; --");
        let rendered = session.tag("full").render();
        assert!(
            !rendered.contains("*/ DROP"),
            "must not allow early comment close: {rendered}"
        );
        // The comment must still open and close exactly once, at the start and end.
        assert_eq!(rendered.matches("/*").count(), 1);
        assert_eq!(rendered.matches("*/").count(), 1);
    }

    #[test]
    fn test_sanitize_strips_newlines() {
        let session = QuerySession::from_parts("p\nwith\rnewlines", "r_x");
        let rendered = session.tag("full").render();
        assert!(!rendered.contains('\n') && !rendered.contains('\r'));
    }
}
