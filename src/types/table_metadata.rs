use crate::types::ColumnMetadata;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableMetadata {
    pub schema_name: String,
    pub table_name: String,
    pub columns: Vec<ColumnMetadata>,
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
    pub fn select_columns(&self, column_names: Option<&[&str]>) -> TableMetadata {
        match column_names {
            Some(names) => {
                let columns = names
                    .iter()
                    .filter_map(|name| {
                        self.columns
                            .iter()
                            .find(|column| column.column_name == *name)
                            .cloned()
                    })
                    .collect();

                TableMetadata {
                    schema_name: self.schema_name.clone(),
                    table_name: self.table_name.clone(),
                    columns,
                }
            }

            None => self.clone(),
        }
    }

    /// Select columns by catalog index — what DataFusion's projection pushdown hands a
    /// `TableProvider` (`Option<&Vec<usize>>` into the table's full column list), as opposed to
    /// `select_columns`'s by-name lookup used by the checkpoint-driven CLI path.
    pub fn select_indices(&self, indices: &[usize]) -> TableMetadata {
        let columns = indices
            .iter()
            .filter_map(|&i| self.columns.get(i).cloned())
            .collect();

        TableMetadata {
            schema_name: self.schema_name.clone(),
            table_name: self.table_name.clone(),
            columns,
        }
    }
}
