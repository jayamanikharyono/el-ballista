//! PostgreSQL pushdown semantics: the Postgres implementation of the shared [`SqlDialect`]
//! trait, and the mapping from Postgres column types to engine-neutral [`ColumnKind`]s that
//! drives translation fidelity.
//!
//! [`SqlDialect`]: crate::pushdown::dialect::SqlDialect

use std::collections::HashSet;

use crate::pushdown::dialect::SqlDialect;
use crate::pushdown::{CastType, Collation, ColumnKind, ColumnKinds};
use crate::types::{ColumnMetadata, TableMetadata};

/// PostgreSQL SQL dialect.
pub struct PostgresDialect;

impl SqlDialect for PostgresDialect {
    fn quote_ident(&self, name: &str) -> String {
        format!("\"{}\"", name.replace('"', "\"\""))
    }

    fn placeholder(&self, param_index: usize) -> String {
        format!("${}", param_index)
    }

    fn cast_type_name(&self, to: CastType) -> &'static str {
        match to {
            CastType::Text => "text",
        }
    }

    fn collation_name(&self, collation: Collation) -> &'static str {
        match collation {
            // The "C" collation compares strcmp-style on the encoded bytes; on a UTF-8
            // database that is Rust/Arrow `str` ordering.
            Collation::Binary => "\"C\"",
        }
    }
}

/// Classify one Postgres column (from `information_schema.columns`) for translation.
///
/// - integer types, `boolean`, timestamps, `date`, floats: their primitive kinds;
/// - `text` / `character varying`: [`ColumnKind::Text`], compared under `COLLATE "C"`;
///   `bytewise_collation` is true only for an explicit `C`/`POSIX` column collation (unknown
///   or database-default collations are never assumed byte-wise);
/// - `character(n)`: [`ColumnKind::Opaque`] — `bpchar` comparisons ignore trailing blanks, so
///   no comparison is exact or a superset of Arrow's on the padded value;
/// - enum (`is_enum`, from `pg_enum`): [`ColumnKind::Label`];
/// - `uuid`, `json`, `jsonb`, and other `USER-DEFINED` types (incl. `citext`), which the
///   extractor emits as their text form: [`ColumnKind::TextCast`];
/// - everything else (numeric, bytea, arrays, ...): [`ColumnKind::Opaque`].
///
/// # Examples
/// ```
/// use el_ballista::connector::postgres::dialect::column_kind;
/// use el_ballista::pushdown::ColumnKind;
/// use el_ballista::types::ColumnMetadata;
/// let uuid = ColumnMetadata {
///     column_name: "u".into(), data_type: "uuid".into(), is_nullable: true,
///     numeric_precision: None, numeric_scale: None, udt_name: Some("uuid".into()),
///     collation_name: None,
/// };
/// assert_eq!(column_kind(&uuid, false), ColumnKind::TextCast);
/// ```
pub fn column_kind(column: &ColumnMetadata, is_enum: bool) -> ColumnKind {
    match column.data_type.as_str() {
        "smallint" | "integer" | "bigint" => ColumnKind::Integer,
        "boolean" => ColumnKind::Boolean,
        "timestamp with time zone" | "timestamp without time zone" => ColumnKind::Timestamp,
        "date" => ColumnKind::Date,
        "real" | "double precision" => ColumnKind::Float,
        "text" | "character varying" => ColumnKind::Text {
            bytewise_collation: matches!(column.collation_name.as_deref(), Some("C" | "POSIX")),
        },
        "USER-DEFINED" if is_enum => ColumnKind::Label,
        "uuid" | "json" | "jsonb" | "USER-DEFINED" => ColumnKind::TextCast,
        _ => ColumnKind::Opaque,
    }
}

/// Column kinds for every column of a table. `enum_columns` are the true enums (`pg_enum`).
///
/// `server_utf8` is whether the server encoding is UTF8. `COLLATE "C"` compares the
/// server-encoded bytes, which equals Arrow's (UTF-8) string order only under UTF8; under any
/// other encoding text and enum-label columns are downgraded to [`ColumnKind::TextCast`]
/// (equality/inequality only — byte equality is encoding-independent), so no ordering
/// comparison is pushed.
///
/// # Examples
/// ```
/// use std::collections::HashSet;
/// use el_ballista::connector::postgres::dialect::column_kinds;
/// use el_ballista::types::TableMetadata;
/// let table = TableMetadata { schema_name: "public".into(), table_name: "t".into(), columns: vec![] };
/// assert!(column_kinds(&table, &HashSet::new(), true).is_empty());
/// ```
pub fn column_kinds(
    table: &TableMetadata,
    enum_columns: &HashSet<String>,
    server_utf8: bool,
) -> ColumnKinds {
    table
        .columns
        .iter()
        .map(|c| {
            let kind = match column_kind(c, enum_columns.contains(&c.column_name)) {
                ColumnKind::Text { .. } | ColumnKind::Label if !server_utf8 => ColumnKind::TextCast,
                kind => kind,
            };
            (c.column_name.clone(), kind)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn non_utf8_server_downgrades_text_ordering() {
        // COLLATE "C" orders server-encoded bytes, equal to Arrow's UTF-8 order only on
        // a UTF8 server; elsewhere only equality may push.
        let table = TableMetadata {
            schema_name: "public".into(),
            table_name: "t".into(),
            columns: vec![
                col("text", None),
                col("USER-DEFINED", None),
                col("bigint", None),
            ],
        };
        let mut t = table.clone();
        t.columns[1].column_name = "mood".into();
        t.columns[2].column_name = "id".into();
        let enums: HashSet<String> = ["mood".to_string()].into();
        let utf8 = column_kinds(&t, &enums, true);
        let latin1 = column_kinds(&t, &enums, false);
        assert!(matches!(utf8["test_col"], ColumnKind::Text { .. }));
        assert!(matches!(utf8["mood"], ColumnKind::Label));
        assert!(matches!(latin1["test_col"], ColumnKind::TextCast));
        assert!(matches!(latin1["mood"], ColumnKind::TextCast));
        assert!(matches!(latin1["id"], ColumnKind::Integer));
    }

    fn col(data_type: &str, collation: Option<&str>) -> ColumnMetadata {
        ColumnMetadata {
            column_name: "test_col".to_string(),
            data_type: data_type.to_string(),
            is_nullable: true,
            numeric_precision: None,
            numeric_scale: None,
            udt_name: None,
            collation_name: collation.map(String::from),
        }
    }

    #[test]
    fn test_quote_ident() {
        let dialect = PostgresDialect;
        assert_eq!(dialect.quote_ident("simple"), r#""simple""#);
        assert_eq!(dialect.quote_ident(r#"with"quote"#), r#""with""quote""#);
    }

    #[test]
    fn test_placeholder() {
        let dialect = PostgresDialect;
        assert_eq!(dialect.placeholder(1), "$1");
        assert_eq!(dialect.placeholder(42), "$42");
    }

    #[test]
    fn test_column_kind_mapping() {
        assert_eq!(
            column_kind(&col("integer", None), false),
            ColumnKind::Integer
        );
        assert_eq!(
            column_kind(&col("boolean", None), false),
            ColumnKind::Boolean
        );
        assert_eq!(
            column_kind(&col("double precision", None), false),
            ColumnKind::Float
        );
        assert_eq!(
            column_kind(&col("timestamp without time zone", None), false),
            ColumnKind::Timestamp
        );
        assert_eq!(
            column_kind(&col("text", Some("C")), false),
            ColumnKind::Text {
                bytewise_collation: true
            }
        );
        assert_eq!(
            column_kind(&col("character varying", Some("en-US-x-icu")), false),
            ColumnKind::Text {
                bytewise_collation: false
            }
        );
        assert_eq!(
            column_kind(&col("text", None), false),
            ColumnKind::Text {
                bytewise_collation: false
            }
        );
        assert_eq!(
            column_kind(&col("character", None), false),
            ColumnKind::Opaque
        );
        assert_eq!(column_kind(&col("uuid", None), false), ColumnKind::TextCast);
        assert_eq!(
            column_kind(&col("jsonb", None), false),
            ColumnKind::TextCast
        );
        assert_eq!(
            column_kind(&col("USER-DEFINED", None), true),
            ColumnKind::Label
        );
        assert_eq!(
            column_kind(&col("USER-DEFINED", None), false),
            ColumnKind::TextCast
        );
        assert_eq!(
            column_kind(&col("numeric", None), false),
            ColumnKind::Opaque
        );
        assert_eq!(column_kind(&col("bytea", None), false), ColumnKind::Opaque);
    }
}
