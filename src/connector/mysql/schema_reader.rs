//! MySQL schema reader (prototype) — reads column metadata from `information_schema`, producing
//! the SHARED [`TableMetadata`]/[`ColumnMetadata`] (the same types the Postgres connector uses,
//! which is one of the abstractions this prototype validates).

use sqlx::MySqlPool;

use super::error::MysqlError;
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

/// Reads table metadata from `information_schema` for one MySQL connection pool.
pub struct MysqlSchemaReader<'a> {
    pool: &'a MySqlPool,
    default_schema: String,
}

impl<'a> MysqlSchemaReader<'a> {
    /// A reader over `pool`; unqualified table names resolve in `default_schema`.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
    /// # use el_ballista::connector::mysql::MysqlExtractor;
    /// use el_ballista::connector::mysql::schema_reader::MysqlSchemaReader;
    ///
    /// # let password = std::env::var("MYSQL_PASSWORD")?;
    /// # let ex = MysqlExtractor::connect("127.0.0.1", 3306, "root", &password, "sakila", 4).await?;
    /// let reader = MysqlSchemaReader::new(ex.pool(), "sakila");
    /// let meta = reader.get_table_metadata("actor").await?; // or "sakila.actor"
    /// for c in &meta.columns {
    ///     println!("{} {} nullable={}", c.column_name, c.data_type, c.is_nullable);
    /// }
    /// # Ok(()) }
    /// ```
    pub fn new(pool: &'a MySqlPool, default_schema: impl Into<String>) -> Self {
        Self {
            pool,
            default_schema: default_schema.into(),
        }
    }

    /// Column metadata for `table_name` (`table` or `schema.table`), in ordinal order.
    ///
    /// `COLUMN_TYPE` is carried in [`ColumnMetadata::udt_name`]: the type mapper needs it to tell
    /// `tinyint(1)` (BOOLEAN), `unsigned`, `bit(n)` and `decimal(p,s)` apart.
    ///
    /// # Errors
    ///
    /// [`MysqlError::TableNotFound`] when `information_schema` lists no columns for the table
    /// (missing table, or no privilege on any column) — never an empty `TableMetadata`, which
    /// would otherwise render as `SELECT  FROM …`. [`MysqlError::Source`] on query failure.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
    /// # use el_ballista::connector::mysql::MysqlExtractor;
    /// use el_ballista::connector::mysql::schema_reader::MysqlSchemaReader;
    ///
    /// # let password = std::env::var("MYSQL_PASSWORD")?;
    /// # let ex = MysqlExtractor::connect("127.0.0.1", 3306, "root", &password, "sakila", 4).await?;
    /// let reader = MysqlSchemaReader::new(ex.pool(), "sakila");
    /// let meta = reader.get_table_metadata("actor").await?; // or "sakila.actor"
    /// for c in &meta.columns {
    ///     println!("{} {} nullable={}", c.column_name, c.data_type, c.is_nullable);
    /// }
    /// # Ok(()) }
    /// ```
    pub async fn get_table_metadata(&self, table_name: &str) -> Result<TableMetadata, MysqlError> {
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

        if rows.is_empty() {
            return Err(MysqlError::TableNotFound { schema, table });
        }

        let columns = rows
            .into_iter()
            .map(|r| ColumnMetadata {
                column_name: r.column_name,
                data_type: r.data_type,
                is_nullable: r.is_nullable.eq_ignore_ascii_case("YES"),
                numeric_precision: None,
                numeric_scale: None,
                udt_name: r.column_type,
                // TODO(mysql-pushdown): collation is read but not yet used anywhere; text
                // pushdown must consult it (MySQL defaults are case/accent-insensitive).
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
