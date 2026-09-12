use std::fmt;
use serde::{Deserialize, Serialize};
use sqlx::FromRow;

#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct ColumnMetadata {
    pub column_name: String,
    pub data_type: String,
    pub is_nullable: bool,
    pub numeric_precision: Option<i32>,
    pub numeric_scale: Option<i32>,
    pub udt_name: Option<String>,
    #[sqlx(default)]
    pub collation_name: Option<String>,
}

impl fmt::Display for ColumnMetadata {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} {}{}",
            self.column_name,
            self.data_type,
            if self.is_nullable {
                " NULL"
            } else {
                " NOT NULL"
            }
        )
    }
}