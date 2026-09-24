//! PostgreSQL → Arrow type mapping.
//! extractor/postgres/arrow_type_mapper.rs
//! Maps PostgreSQL column metadata to Arrow [`DataType`] values.

use arrow::datatypes::{DataType, Field, TimeUnit};
use std::sync::Arc;

use bigdecimal::{BigDecimal, ToPrimitive};

use crate::connector::errors::ExtractorError;
use crate::types::ColumnMetadata;

/// Convert a `BigDecimal` to the unscaled `i128` Arrow `Decimal128` expects.
/// `123.45` at scale 2 → `12345`, `1200` at scale -2 → `12`.
/// Text is captured before the move so error messages still show the original value.
pub fn decimal_to_unscaled(decimal: BigDecimal, scale: i64) -> Result<i128, ExtractorError> {
    let text = decimal.to_string();
    let unscaled = decimal * BigDecimal::from(10).powi(scale);
    unscaled.to_i128().ok_or_else(|| {
        ExtractorError::Internal(format!("numeric value cannot fit into i128: {}", text))
    })
}

/// Convert PostgreSQL **binary** `numeric` wire bytes to the unscaled `i128` Arrow
/// `Decimal128` expects — the `COPY (…​) TO STDOUT (FORMAT BINARY)` counterpart of
/// [`decimal_to_unscaled`].
///
/// Layout (all big-endian): `ndigits: u16`, `weight: i16`, `sign: u16`
/// (`0x0000` positive, `0x4000` negative, `0xC000` NaN), `dscale: u16` (ignored —
/// the digit groups are exact), then `ndigits × u16` base-10000 digits with
/// `value = sign × Σ dᵢ × 10000^(weight−i)`.
///
/// The target is `trunc(value × 10^scale)`, computed as an exact integer rational so
/// no float rounding can diverge from the cursor path (which truncates toward zero
/// via `BigDecimal::to_i128`). Overflow and NaN are errors, matching the cursor path.
pub fn numeric_bytes_to_unscaled(raw: &[u8], scale: i64) -> Result<i128, ExtractorError> {
    let corrupt = |why: &str| ExtractorError::Internal(format!("corrupt binary numeric: {why}"));
    if raw.len() < 8 {
        return Err(corrupt("short header"));
    }
    let ndigits = u16::from_be_bytes([raw[0], raw[1]]) as usize;
    let weight = i16::from_be_bytes([raw[2], raw[3]]) as i64;
    let sign = u16::from_be_bytes([raw[4], raw[5]]);
    if raw.len() != 8 + ndigits * 2 {
        return Err(corrupt("length mismatch"));
    }
    let neg = match sign {
        0x0000 => false,
        0x4000 => true,
        0xC000 => return Err(corrupt("NaN has no Decimal128 representation")),
        _ => return Err(corrupt("unknown sign")),
    };
    // Target: trunc(Σ dᵢ × 10^(scale + 4×(weight−i))). Fold around the minimum
    // exponent so every term is an exact integer: num / 10^(−e_min).
    let mut e_min: i64 = 0;
    let mut first = true;
    for i in 0..ndigits {
        let e = scale
            .checked_add(4 * (weight - i as i64))
            .ok_or_else(|| corrupt("exponent overflow"))?;
        if first || e < e_min {
            e_min = e;
            first = false;
        }
    }
    if first {
        return Ok(0); // ndigits == 0: numeric zero.
    }
    let mut num: i128 = 0;
    let (groups, rest) = raw[8..].as_chunks::<2>();
    debug_assert!(rest.is_empty(), "numeric length pre-validated");
    for (i, chunk) in groups.iter().enumerate() {
        let d = u16::from_be_bytes([chunk[0], chunk[1]]);
        if d > 9999 {
            return Err(corrupt("digit group out of range"));
        }
        let d = d as i128;
        let e = scale
            .checked_add(4 * (weight - i as i64))
            .ok_or_else(|| corrupt("exponent overflow"))?;
        let shift = (e - e_min) as u32;
        let term = d
            .checked_mul(checked_pow10(shift).ok_or_else(|| corrupt("value overflows i128"))?)
            .ok_or_else(|| corrupt("value overflows i128"))?;
        num = num
            .checked_add(term)
            .ok_or_else(|| corrupt("value overflows i128"))?;
    }
    let mut unscaled = if e_min >= 0 {
        let factor = checked_pow10(e_min as u32).ok_or_else(|| corrupt("value overflows i128"))?;
        num.checked_mul(factor)
            .ok_or_else(|| corrupt("value overflows i128"))?
    } else {
        let denom =
            checked_pow10((-e_min) as u32).ok_or_else(|| corrupt("value overflows i128"))?;
        num / denom // Truncates toward zero — matches `BigDecimal::to_i128`.
    };
    if neg {
        unscaled = unscaled
            .checked_neg()
            .ok_or_else(|| corrupt("value overflows i128"))?;
    }
    Ok(unscaled)
}

/// 10^exp, or `None` on overflow. Exponents here are small in practice (a couple of
/// dozen); the loop breaks fast on huge ones via `checked_mul`.
fn checked_pow10(mut exp: u32) -> Option<i128> {
    let mut acc: i128 = 1;
    while exp > 0 {
        acc = acc.checked_mul(10)?;
        exp -= 1;
    }
    Some(acc)
}

pub struct ArrowTypeMapper;

impl ArrowTypeMapper {
    pub fn map(column: &ColumnMetadata) -> Result<DataType, ExtractorError> {
        match column.data_type.as_str() {
            "smallint" => Ok(DataType::Int16),
            "integer" => Ok(DataType::Int32),
            "bigint" => Ok(DataType::Int64),

            "real" => Ok(DataType::Float32),
            "double precision" => Ok(DataType::Float64),

            "boolean" => Ok(DataType::Boolean),

            "text" | "character varying" | "character" => Ok(DataType::Utf8),

            "date" => Ok(DataType::Date32),

            "timestamp without time zone" => Ok(DataType::Timestamp(TimeUnit::Microsecond, None)),

            "timestamp with time zone" => Ok(DataType::Timestamp(
                TimeUnit::Microsecond,
                Some("UTC".into()),
            )),

            "bytea" => Ok(DataType::Binary),

            "numeric" => {
                let precision_raw = column.numeric_precision.unwrap_or(38);
                let scale_raw = column.numeric_scale.unwrap_or(10);
                if !(1..=38).contains(&precision_raw) {
                    return Err(ExtractorError::Internal(format!(
                        "numeric precision {} out of Decimal128 range 1..38 for column '{}'",
                        precision_raw, column.column_name
                    )));
                }
                if !(-127..=127).contains(&scale_raw) {
                    return Err(ExtractorError::Internal(format!(
                        "numeric scale {} out of i8 range for column '{}'",
                        scale_raw, column.column_name
                    )));
                }
                // Decimal128 precision is u8 but Postgres allows >38; clamp report above.
                let precision = precision_raw as u8;
                let scale = scale_raw as i8;
                Ok(DataType::Decimal128(precision, scale))
            }

            "json" | "jsonb" => Ok(DataType::Utf8),
            "uuid" => Ok(DataType::Utf8),

            // PostgreSQL ENUM / custom types.
            // The extractor casts these values to TEXT before decoding.
            "USER-DEFINED" => Ok(DataType::Utf8),

            "ARRAY" => Self::map_array(column),

            other => Err(ExtractorError::UnsupportedType(other.to_string())),
        }
    }

    fn map_array(column_metadata: &ColumnMetadata) -> Result<DataType, ExtractorError> {
        match column_metadata.udt_name.as_deref() {
            Some("_text") => Ok(DataType::List(Arc::new(Field::new(
                "item",
                DataType::Utf8,
                true,
            )))),

            other => Err(ExtractorError::UnsupportedType(format!(
                "array element type {:?} for column '{}' (only text[] arrays are supported)",
                other, column_metadata.column_name
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn col(data_type: &str, udt_name: Option<&str>) -> ColumnMetadata {
        ColumnMetadata {
            column_name: "test_col".to_string(),
            data_type: data_type.to_string(),
            is_nullable: true,
            numeric_precision: Some(20),
            numeric_scale: Some(4),
            udt_name: udt_name.map(String::from),
            collation_name: None,
        }
    }

    #[test]
    fn test_mappings() {
        assert_eq!(
            ArrowTypeMapper::map(&col("smallint", None)).unwrap(),
            DataType::Int16
        );
        assert_eq!(
            ArrowTypeMapper::map(&col("integer", None)).unwrap(),
            DataType::Int32
        );
        assert_eq!(
            ArrowTypeMapper::map(&col("bigint", None)).unwrap(),
            DataType::Int64
        );
        assert_eq!(
            ArrowTypeMapper::map(&col("real", None)).unwrap(),
            DataType::Float32
        );
        assert_eq!(
            ArrowTypeMapper::map(&col("double precision", None)).unwrap(),
            DataType::Float64
        );
        assert_eq!(
            ArrowTypeMapper::map(&col("boolean", None)).unwrap(),
            DataType::Boolean
        );
        assert_eq!(
            ArrowTypeMapper::map(&col("text", None)).unwrap(),
            DataType::Utf8
        );
        assert_eq!(
            ArrowTypeMapper::map(&col("date", None)).unwrap(),
            DataType::Date32
        );
        assert_eq!(
            ArrowTypeMapper::map(&col("bytea", None)).unwrap(),
            DataType::Binary
        );
        assert_eq!(
            ArrowTypeMapper::map(&col("json", None)).unwrap(),
            DataType::Utf8
        );
        assert_eq!(
            ArrowTypeMapper::map(&col("jsonb", None)).unwrap(),
            DataType::Utf8
        );
        assert_eq!(
            ArrowTypeMapper::map(&col("uuid", None)).unwrap(),
            DataType::Utf8
        );
        assert_eq!(
            ArrowTypeMapper::map(&col("numeric", None)).unwrap(),
            DataType::Decimal128(20, 4)
        );
        assert_eq!(
            ArrowTypeMapper::map(&col("ARRAY", Some("_text"))).unwrap(),
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)))
        );
        assert!(ArrowTypeMapper::map(&col("unsupported_type", None)).is_err());
        assert!(ArrowTypeMapper::map(&col("ARRAY", Some("_int4"))).is_err());
    }

    /// Encode value groups the way PostgreSQL does: `value = sign × Σ dᵢ × 10000^(weight−i)`.
    fn numeric_wire(digits: &[u16], weight: i16, neg: bool) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&(digits.len() as u16).to_be_bytes());
        out.extend_from_slice(&weight.to_be_bytes());
        out.extend_from_slice(&(if neg { 0x4000u16 } else { 0x0000u16 }).to_be_bytes());
        out.extend_from_slice(&0u16.to_be_bytes()); // dscale (ignored)
        for d in digits {
            out.extend_from_slice(&d.to_be_bytes());
        }
        out
    }

    #[test]
    fn test_numeric_bytes_match_decimal_semantics() {
        // Same cases as decimal_to_unscaled, through the binary layout.
        assert_eq!(
            numeric_bytes_to_unscaled(&numeric_wire(&[123, 4500], 0, false), 2).unwrap(),
            12345
        );
        assert_eq!(
            numeric_bytes_to_unscaled(&numeric_wire(&[123, 4500], 0, false), 0).unwrap(),
            123
        );
        assert_eq!(
            numeric_bytes_to_unscaled(&numeric_wire(&[7, 5000], 0, true), 1).unwrap(),
            -75
        );
        assert_eq!(
            numeric_bytes_to_unscaled(&numeric_wire(&[], 0, false), 2).unwrap(),
            0
        );
        // 1200 at scale -2 -> 12 (digits [1200], weight 0).
        assert_eq!(
            numeric_bytes_to_unscaled(&numeric_wire(&[1200], 0, false), -2).unwrap(),
            12
        );
        // 3.141592653589793 at scale 15 -> full precision preserved.
        assert_eq!(
            numeric_bytes_to_unscaled(&numeric_wire(&[3, 1415, 9265, 3589, 7930], 0, false), 15)
                .unwrap(),
            3141592653589793
        );
    }

    #[test]
    fn test_numeric_bytes_truncates_like_cursor_path() {
        // 1.999 at scale 0 truncates toward zero (matches BigDecimal::to_i128).
        assert_eq!(
            numeric_bytes_to_unscaled(&numeric_wire(&[1, 9990], 0, false), 0).unwrap(),
            1
        );
        assert_eq!(
            numeric_bytes_to_unscaled(&numeric_wire(&[1, 9990], 0, true), 0).unwrap(),
            -1
        );
    }

    #[test]
    fn test_numeric_bytes_rejects_nan_and_garbage() {
        let mut nan = numeric_wire(&[], 0, false);
        nan[4] = 0xC0; // sign = NaN
        assert!(numeric_bytes_to_unscaled(&nan, 2).is_err());
        assert!(numeric_bytes_to_unscaled(&[0u8; 7], 2).is_err());
        assert!(numeric_bytes_to_unscaled(&[0u8; 10], 2).is_err()); // length mismatch
    }
}
