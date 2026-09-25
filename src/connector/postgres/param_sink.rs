//! Postgres [`SqlSink`] — binds a rendered predicate straight into a sqlx `QueryBuilder<Postgres>`.
//! Backend-specific (`push_bind` appends `$n` at render position); the generic sink interface and
//! parameter IR live in [`crate::pushdown`].
//!
//! [`SqlSink`]: crate::pushdown::SqlSink

use sqlx::{Postgres, QueryBuilder};

use crate::pushdown::{SqlParam, SqlSink};

/// [`SqlSink`](crate::pushdown::SqlSink) that binds straight into a Postgres query: `push_bind`
/// appends `$n` at the current position, so text and numbering stay aligned by construction.
pub struct PgParamSink<'q> {
    query: &'q mut QueryBuilder<Postgres>,
}

impl<'q> PgParamSink<'q> {
    pub(crate) fn new(query: &'q mut QueryBuilder<Postgres>) -> Self {
        Self { query }
    }
}

impl SqlSink for PgParamSink<'_> {
    fn push_sql(&mut self, sql: &str) {
        self.query.push(sql);
    }

    fn push_param(&mut self, param: SqlParam) {
        match param {
            SqlParam::Bool(v) => self.query.push_bind(v),
            SqlParam::Int(v) => self.query.push_bind(v),
            SqlParam::Float(v) => self.query.push_bind(v),
            SqlParam::Text(v) => self.query.push_bind(v),
            SqlParam::Timestamp(v) => self.query.push_bind(v),
        };
    }
}
