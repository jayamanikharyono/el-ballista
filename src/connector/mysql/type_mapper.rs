//! MySQL type -> Arrow type mapping (prototype).
//!
//! Documents the intended Arrow representation per MySQL `information_schema.DATA_TYPE`. The
//! prototype extractor does not yet decode to these types (it materializes `Utf8` via `CAST` —
//! see [`super::extractor`]); this function is the seam the typed-decode step builds on.

use arrow::datatypes::{DataType, TimeUnit};

/// Map a MySQL `DATA_TYPE` (catalog name, e.g. `bigint`, `varchar`, `datetime`) to the Arrow
/// [`DataType`] we intend to materialize it as. Unknown types fall back to `Utf8`.
pub fn mysql_type_to_arrow(data_type: &str) -> DataType {
    match data_type.to_ascii_lowercase().as_str() {
        "tinyint" | "smallint" | "mediumint" | "int" | "integer" | "bigint" | "year" => {
            DataType::Int64
        }
        "float" | "double" | "real" => DataType::Float64,
        // Keep exact decimals as text until a decimal decode path exists.
        "decimal" | "numeric" => DataType::Utf8,
        "bit" | "bool" | "boolean" => DataType::Boolean,
        "date" => DataType::Date32,
        "datetime" | "timestamp" => DataType::Timestamp(TimeUnit::Microsecond, None),
        "char" | "varchar" | "text" | "tinytext" | "mediumtext" | "longtext" | "enum" | "set"
        | "json" => DataType::Utf8,
        "binary" | "varbinary" | "blob" | "tinyblob" | "mediumblob" | "longblob" => {
            DataType::Binary
        }
        _ => DataType::Utf8,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_common_types() {
        assert_eq!(mysql_type_to_arrow("bigint"), DataType::Int64);
        assert_eq!(mysql_type_to_arrow("INT"), DataType::Int64);
        assert_eq!(mysql_type_to_arrow("varchar"), DataType::Utf8);
        assert_eq!(mysql_type_to_arrow("double"), DataType::Float64);
        assert_eq!(
            mysql_type_to_arrow("datetime"),
            DataType::Timestamp(TimeUnit::Microsecond, None)
        );
        assert_eq!(mysql_type_to_arrow("date"), DataType::Date32);
    }

    #[test]
    fn unknown_falls_back_to_utf8() {
        assert_eq!(mysql_type_to_arrow("geometry"), DataType::Utf8);
    }
}
