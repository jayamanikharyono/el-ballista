use crate::types::ColumnMetadata;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableMetadata {
    pub schema_name: String,
    pub table_name: String,
    pub columns: Vec<ColumnMetadata>,
}

/// A projection that does not match the table: a typo'd column name or an index past the
/// end. Projections are never silently narrowed — `["id", "amout"]` must not quietly
/// return one column.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ProjectionError {
    #[error("unknown column '{column}' in {table}")]
    UnknownColumn { column: String, table: String },

    #[error("column index {index} out of range for {table} ({column_count} columns)")]
    IndexOutOfRange {
        index: usize,
        column_count: usize,
        table: String,
    },
}

impl fmt::Display for TableMetadata {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "{}.{}", self.schema_name, self.table_name)?;

        for (i, column) in self.columns.iter().enumerate() {
            let prefix = if i == self.columns.len() - 1 {
                "└──"
            } else {
                "├──"
            };

            writeln!(f, "{prefix} {column}")?;
        }

        Ok(())
    }
}

impl TableMetadata {
    fn qualified_name(&self) -> String {
        format!("{}.{}", self.schema_name, self.table_name)
    }

    /// Narrow to the named columns, in the order given. `None` keeps every column.
    ///
    /// Errors with [`ProjectionError::UnknownColumn`] naming the first name that is not a
    /// column of this table (names are matched exactly, as `information_schema` reports
    /// them).
    ///
    /// # Examples
    ///
    /// ```
    /// use rust_ballista_extraction_layer::types::{ColumnMetadata, TableMetadata};
    ///
    /// let col = |n: &str| ColumnMetadata {
    ///     column_name: n.into(),
    ///     data_type: "bigint".into(),
    ///     is_nullable: false,
    ///     numeric_precision: None,
    ///     numeric_scale: None,
    ///     udt_name: None,
    ///     collation_name: None,
    /// };
    /// let t = TableMetadata {
    ///     schema_name: "public".into(),
    ///     table_name: "t".into(),
    ///     columns: vec![col("id"), col("amount")],
    /// };
    /// assert_eq!(t.select_columns(Some(&["amount"])).unwrap().columns.len(), 1);
    /// assert!(t.select_columns(Some(&["id", "amout"])).is_err());
    /// ```
    pub fn select_columns(
        &self,
        column_names: Option<&[&str]>,
    ) -> Result<TableMetadata, ProjectionError> {
        match column_names {
            Some(names) => {
                let columns = names
                    .iter()
                    .map(|name| {
                        self.columns
                            .iter()
                            .find(|column| column.column_name == *name)
                            .cloned()
                            .ok_or_else(|| ProjectionError::UnknownColumn {
                                column: (*name).to_string(),
                                table: self.qualified_name(),
                            })
                    })
                    .collect::<Result<Vec<_>, _>>()?;

                Ok(TableMetadata {
                    schema_name: self.schema_name.clone(),
                    table_name: self.table_name.clone(),
                    columns,
                })
            }

            None => Ok(self.clone()),
        }
    }

    /// Select columns by catalog index — what DataFusion's projection pushdown hands a
    /// `TableProvider` (`Option<&Vec<usize>>` into the table's full column list), as opposed to
    /// `select_columns`'s by-name lookup used by the checkpoint-driven CLI path.
    ///
    /// Errors with [`ProjectionError::IndexOutOfRange`] for an index past the end.
    ///
    /// # Examples
    ///
    /// ```
    /// use rust_ballista_extraction_layer::types::TableMetadata;
    ///
    /// let t = TableMetadata {
    ///     schema_name: "public".into(),
    ///     table_name: "t".into(),
    ///     columns: vec![],
    /// };
    /// assert!(t.select_indices(&[0]).is_err());
    /// assert!(t.select_indices(&[]).unwrap().columns.is_empty());
    /// ```
    pub fn select_indices(&self, indices: &[usize]) -> Result<TableMetadata, ProjectionError> {
        let columns = indices
            .iter()
            .map(|&i| {
                self.columns
                    .get(i)
                    .cloned()
                    .ok_or_else(|| ProjectionError::IndexOutOfRange {
                        index: i,
                        column_count: self.columns.len(),
                        table: self.qualified_name(),
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(TableMetadata {
            schema_name: self.schema_name.clone(),
            table_name: self.table_name.clone(),
            columns,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn table() -> TableMetadata {
        let col = |n: &str| ColumnMetadata {
            column_name: n.to_string(),
            data_type: "bigint".to_string(),
            is_nullable: true,
            numeric_precision: None,
            numeric_scale: None,
            udt_name: None,
            collation_name: None,
        };
        TableMetadata {
            schema_name: "public".to_string(),
            table_name: "t".to_string(),
            columns: vec![col("id"), col("amount"), col("name")],
        }
    }

    #[test]
    fn select_columns_keeps_requested_order() {
        let t = table().select_columns(Some(&["name", "id"])).unwrap();
        let names: Vec<_> = t.columns.iter().map(|c| c.column_name.as_str()).collect();
        assert_eq!(names, ["name", "id"]);
        assert_eq!(table().select_columns(None).unwrap().columns.len(), 3);
    }

    #[test]
    fn select_columns_rejects_typo_naming_it() {
        let err = table().select_columns(Some(&["id", "amout"])).unwrap_err();
        assert_eq!(
            err,
            ProjectionError::UnknownColumn {
                column: "amout".to_string(),
                table: "public.t".to_string()
            }
        );
        assert!(err.to_string().contains("amout"));
    }

    #[test]
    fn select_indices_rejects_out_of_range() {
        assert_eq!(table().select_indices(&[2, 0]).unwrap().columns.len(), 2);
        let err = table().select_indices(&[0, 3]).unwrap_err();
        assert!(matches!(
            err,
            ProjectionError::IndexOutOfRange {
                index: 3,
                column_count: 3,
                ..
            }
        ));
    }
}
