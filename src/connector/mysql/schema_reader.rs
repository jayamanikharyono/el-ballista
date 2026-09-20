//! MySQL schema reader (prototype) — reads column metadata from `information_schema`, producing
//! the SHARED [`TableMetadata`]/[`ColumnMetadata`] (the same types the Postgres connector uses,
//! which is one of the abstractions this prototype validates).

use sqlx::MySqlPool;

use crate::types::{ColumnMetadata, TableMetadata};

/// One `information_schema.COLUMNS` row, decoded as strings (robust across MySQL versions/types).
#[derive(sqlx::FromRow)]
struct InformationSchemaColumn {
    column_name: String,
    data_type: String,
    is_nullable: String,
    column_type: Option<String>,
    collation_name: Option<String>,
}

pub struct MysqlSchemaReader<'a> {
    pool: &'a MySqlPool,
    default_schema: String,
}

impl<'a> MysqlSchemaReader<'a> {
    pub fn new(pool: &'a MySqlPool, default_schema: impl Into<String>) -> Self {
        Self {
            pool,
            default_schema: default_schema.into(),
        }
    }

    pub async fn get_table_metadata(&self, table_name: &str) -> Result<TableMetadata, sqlx::Error> {
        let (schema, table) = match table_name.split_once('.') {
            Some((s, t)) => (s.to_string(), t.to_string()),
            None => (self.default_schema.clone(), table_name.to_string()),
        };

        // MySQL uses `?` placeholders; information_schema columns are aliased to the shared
        // ColumnMetadata field names.
        let rows = sqlx::query_as::<_, InformationSchemaColumn>(
            r#"
            SELECT
                COLUMN_NAME    AS column_name,
                DATA_TYPE      AS data_type,
                IS_NULLABLE    AS is_nullable,
                COLUMN_TYPE    AS column_type,
                COLLATION_NAME AS collation_name
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
            ORDER BY ORDINAL_POSITION
            "#,
        )
        .bind(&schema)
        .bind(&table)
        .fetch_all(self.pool)
        .await?;

        let columns = rows
            .into_iter()
            .map(|r| ColumnMetadata {
                column_name: r.column_name,
                data_type: r.data_type,
                is_nullable: r.is_nullable.eq_ignore_ascii_case("YES"),
                numeric_precision: None,
                numeric_scale: None,
                udt_name: r.column_type,
                collation_name: r.collation_name,
            })
            .collect();

        Ok(TableMetadata {
            schema_name: schema,
            table_name: table,
            columns,
        })
    }
}
