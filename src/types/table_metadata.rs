use std::fmt;
use crate::types::ColumnMetadata;

#[derive(Debug, Clone)]
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
    pub fn select_columns(
        &self,
        column_names: Option<&[&str]>,
    ) -> TableMetadata {
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
}