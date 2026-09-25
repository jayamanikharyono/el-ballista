//! PostgreSQL → Arrow type mapping.
//! extractor/postgres/arrow_type_mapper.rs
//! Maps PostgreSQL column metadata to Arrow [`DataType`] values.

use arrow::datatypes::{DataType, Field, TimeUnit};
use std::sync::Arc;

use crate::connector::errors::ExtractorError;
use crate::types::ColumnMetadata;

/// Decimal128 scale used for unconstrained `numeric` columns (no typmod): Postgres
/// accepts any scale there, so values with more fractional digits than this are
/// rejected with a typed error at decode time instead of being truncated.
pub const UNCONSTRAINED_NUMERIC_SCALE: i8 = 10;

/// Decimal128 precision used for unconstrained `numeric` columns.
pub const UNCONSTRAINED_NUMERIC_PRECISION: u8 = 38;

/// `10^i` for `i in 0..=38` — every power a `Decimal128` can need, computed at compile time.
const POW10: [i128; 39] = {
    let mut table = [1i128; 39];
    let mut i = 1;
    while i < 39 {
        table[i] = table[i - 1] * 10;
        i += 1;
    }
    table
};

/// Why a binary `numeric` value has no exact `Decimal128(precision, scale)` form.
/// Callers attach the column name (see `ExtractorError::UnsupportedValue`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NumericDecodeError {
    /// Malformed wire bytes (never produced by a healthy server).
    Corrupt(&'static str),
    /// `NaN` has no `Decimal128` representation.
    NaN,
    /// `Infinity` / `-Infinity` (PostgreSQL 14+) have no `Decimal128` representation.
    Infinity,
    /// Non-zero digits beyond the column's scale: storing it would silently truncate.
    ExceedsScale,
    /// More significant digits than the column's precision (or than `i128`).
    ExceedsPrecision,
}

impl std::fmt::Display for NumericDecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Corrupt(why) => write!(f, "corrupt binary numeric: {why}"),
            Self::NaN => f.write_str("numeric NaN"),
            Self::Infinity => f.write_str("numeric infinity"),
            Self::ExceedsScale => {
                f.write_str("non-zero digits beyond the target scale (would truncate)")
            }
            Self::ExceedsPrecision => f.write_str("more digits than the target precision"),
        }
    }
}

/// Convert PostgreSQL **binary** `numeric` bytes (the format both the cursor path —
/// sqlx requests binary results — and `COPY … (FORMAT BINARY)` deliver) to the unscaled
/// `i128` of `Decimal128(precision, scale)`.
///
/// Layout (all big-endian): `ndigits: u16`, `weight: i16`, `sign: u16`
/// (`0x0000` positive, `0x4000` negative, `0xC000` NaN, `0xD000`/`0xF000` ±Infinity),
/// `dscale: u16` (display only — ignored), then `ndigits × u16` base-10000 digits with
/// `value = sign × Σ dᵢ × 10000^(weight−i)`.
///
/// Exact integer arithmetic, no allocation, no string round-trip: each digit group is
/// shifted by a table power of ten. The result is `value × 10^scale`, which must be an
/// integer (else [`NumericDecodeError::ExceedsScale`] — never a silent truncation) with
/// at most `precision` digits (else [`NumericDecodeError::ExceedsPrecision`]).
///
/// # Examples
///
/// ```
/// use rust_ballista_extraction_layer::connector::postgres::arrow_type_mapper::numeric_bytes_to_unscaled;
///
/// // 123.45: ndigits=2, weight=0, sign=+, dscale=2, digits [123, 4500].
/// let raw = [0, 2, 0, 0, 0, 0, 0, 2, 0, 123, 0x11, 0x94];
/// assert_eq!(numeric_bytes_to_unscaled(&raw, 12, 2), Ok(12345));
/// assert!(numeric_bytes_to_unscaled(&raw, 12, 1).is_err()); // would truncate
/// ```
pub fn numeric_bytes_to_unscaled(
    raw: &[u8],
    precision: u8,
    scale: i8,
) -> Result<i128, NumericDecodeError> {
    use NumericDecodeError::{Corrupt, ExceedsPrecision, ExceedsScale};

    let header: &[u8; 8] = raw.first_chunk::<8>().ok_or(Corrupt("short header"))?;
    let ndigits = usize::from(u16::from_be_bytes([header[0], header[1]]));
    let weight = i64::from(i16::from_be_bytes([header[2], header[3]]));
    let sign = u16::from_be_bytes([header[4], header[5]]);
    let neg = match sign {
        0x0000 => false,
        0x4000 => true,
        0xC000 => return Err(NumericDecodeError::NaN),
        0xD000 | 0xF000 => return Err(NumericDecodeError::Infinity),
        _ => return Err(Corrupt("unknown sign word")),
    };
    let digits = &raw[8..];
    if digits.len() != ndigits * 2 {
        return Err(Corrupt("length mismatch"));
    }
    let precision = usize::from(precision).min(38);
    let scale = i64::from(scale);

    let mut acc: i128 = 0;
    let (groups, _) = digits.as_chunks::<2>();
    for (i, group) in groups.iter().enumerate() {
        let d = u16::from_be_bytes(*group);
        if d > 9999 {
            return Err(Corrupt("digit group out of range"));
        }
        if d == 0 {
            continue;
        }
        let d = i128::from(d);
        // Decimal exponent of this group's units digit in the *unscaled* result.
        // `i` < 65536 and |weight| < 32768, so this cannot overflow i64.
        let exp = scale + 4 * (weight - i as i64);
        let term = if exp >= 0 {
            let exp = usize::try_from(exp).map_err(|_| ExceedsPrecision)?;
            let factor = *POW10.get(exp).ok_or(ExceedsPrecision)?;
            d.checked_mul(factor).ok_or(ExceedsPrecision)?
        } else {
            // Group straddles (or lies entirely below) the scale boundary: its low
            // digits must all be zero, or we would drop them.
            let drop = usize::try_from(-exp).map_err(|_| ExceedsScale)?;
            if drop >= 4 {
                return Err(ExceedsScale);
            }
            let div = POW10[drop];
            if d % div != 0 {
                return Err(ExceedsScale);
            }
            d / div
        };
        acc = acc.checked_add(term).ok_or(ExceedsPrecision)?;
    }
    if acc >= POW10[precision] {
        return Err(ExceedsPrecision);
    }
    Ok(if neg { -acc } else { acc })
}

pub struct ArrowTypeMapper;

impl ArrowTypeMapper {
    pub(crate) fn map(column: &ColumnMetadata) -> Result<DataType, ExtractorError> {
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
                // Unconstrained `numeric` (no typmod) reports NULL precision/scale:
                // Decimal128(38, 10), with over-scale values rejected at decode time.
                let precision_raw = column
                    .numeric_precision
                    .unwrap_or(i32::from(UNCONSTRAINED_NUMERIC_PRECISION));
                let scale_raw = column
                    .numeric_scale
                    .unwrap_or(i32::from(UNCONSTRAINED_NUMERIC_SCALE));
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
                let precision = u8::try_from(precision_raw).map_err(|_| {
                    ExtractorError::Internal(format!("numeric precision {precision_raw}"))
                })?;
                let scale = i8::try_from(scale_raw)
                    .map_err(|_| ExtractorError::Internal(format!("numeric scale {scale_raw}")))?;
                Ok(DataType::Decimal128(precision, scale))
            }

            // Selected as `::text` (see `PostgresQueryBuilder::push_columns`) so the cursor
            // and COPY paths both receive Postgres' own text rendering, byte for byte.
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

    fn special(sign: u16) -> Vec<u8> {
        let mut out = numeric_wire(&[], 0, false);
        out[4..6].copy_from_slice(&sign.to_be_bytes());
        out
    }

    #[test]
    fn test_numeric_bytes_exact_values() {
        let n = |d: &[u16], w, neg, p, s| numeric_bytes_to_unscaled(&numeric_wire(d, w, neg), p, s);
        assert_eq!(n(&[123, 4500], 0, false, 12, 2), Ok(12345));
        assert_eq!(n(&[7, 5000], 0, true, 12, 1), Ok(-75));
        assert_eq!(n(&[], 0, false, 12, 2), Ok(0));
        // 1200 at scale -2 -> 12.
        assert_eq!(n(&[1200], 0, false, 10, -2), Ok(12));
        // 3.141592653589793 at scale 15 -> full precision preserved.
        assert_eq!(
            n(&[3, 1415, 9265, 3589, 7930], 0, false, 30, 15),
            Ok(3141592653589793)
        );
        // 10000.5 = [1, 0, 5000] weight 1.
        assert_eq!(n(&[1, 0, 5000], 1, false, 10, 1), Ok(100005));
        // 0.0001 = [1] weight -1 at scale 4.
        assert_eq!(n(&[1], -1, false, 10, 4), Ok(1));
    }

    #[test]
    fn test_numeric_bytes_never_truncates() {
        // 1.999 at scale 0 used to truncate to 1: now a typed error.
        let v = numeric_wire(&[1, 9990], 0, false);
        assert_eq!(
            numeric_bytes_to_unscaled(&v, 38, 0),
            Err(NumericDecodeError::ExceedsScale)
        );
        // Unconstrained default scale 10: 1.123456789012 has 12 fractional digits.
        let v = numeric_wire(&[1, 1234, 5678, 9012], 0, false);
        assert_eq!(
            numeric_bytes_to_unscaled(
                &v,
                UNCONSTRAINED_NUMERIC_PRECISION,
                UNCONSTRAINED_NUMERIC_SCALE
            ),
            Err(NumericDecodeError::ExceedsScale)
        );
        // Trailing zeros beyond scale are fine: 1.5000 at scale 1.
        let v = numeric_wire(&[1, 5000], 0, false);
        assert_eq!(numeric_bytes_to_unscaled(&v, 5, 1), Ok(15));
        // A digit group entirely below the scale that is non-zero.
        let v = numeric_wire(&[1, 0, 1], 0, false);
        assert_eq!(
            numeric_bytes_to_unscaled(&v, 38, 4),
            Err(NumericDecodeError::ExceedsScale)
        );
    }

    #[test]
    fn test_numeric_bytes_precision_limits() {
        // 10^28 has 29 integer digits: does not fit Decimal128(38, 10).
        let v = numeric_wire(&[1], 7, false);
        assert_eq!(
            numeric_bytes_to_unscaled(&v, 38, 10),
            Err(NumericDecodeError::ExceedsPrecision)
        );
        // 10^27 (28 digits) fits exactly at the limit.
        let v = numeric_wire(&[1000], 6, false);
        assert_eq!(numeric_bytes_to_unscaled(&v, 38, 10), Ok(POW10[37]));
        // Constrained precision: 1000.00 does not fit numeric(5,2).
        let v = numeric_wire(&[1000], 0, false);
        assert_eq!(
            numeric_bytes_to_unscaled(&v, 5, 2),
            Err(NumericDecodeError::ExceedsPrecision)
        );
        // Huge weight never panics.
        let v = numeric_wire(&[9999], i16::MAX, true);
        assert_eq!(
            numeric_bytes_to_unscaled(&v, 38, 10),
            Err(NumericDecodeError::ExceedsPrecision)
        );
        let v = numeric_wire(&[9999], i16::MIN, false);
        assert_eq!(
            numeric_bytes_to_unscaled(&v, 38, 10),
            Err(NumericDecodeError::ExceedsScale)
        );
    }

    #[test]
    fn test_numeric_bytes_rejects_nan_infinity_and_garbage() {
        assert_eq!(
            numeric_bytes_to_unscaled(&special(0xC000), 38, 10),
            Err(NumericDecodeError::NaN)
        );
        assert_eq!(
            numeric_bytes_to_unscaled(&special(0xD000), 38, 10),
            Err(NumericDecodeError::Infinity)
        );
        assert_eq!(
            numeric_bytes_to_unscaled(&special(0xF000), 38, 10),
            Err(NumericDecodeError::Infinity)
        );
        assert!(numeric_bytes_to_unscaled(&[0u8; 7], 38, 2).is_err());
        assert!(numeric_bytes_to_unscaled(&[0u8; 10], 38, 2).is_err()); // length mismatch
        let mut bad = numeric_wire(&[10000], 0, false);
        assert!(numeric_bytes_to_unscaled(&bad, 38, 2).is_err());
        bad[4] = 0x12;
        assert!(numeric_bytes_to_unscaled(&bad, 38, 2).is_err());
    }

    #[test]
    fn test_unconstrained_numeric_maps_to_default() {
        let mut c = col("numeric", None);
        c.numeric_precision = None;
        c.numeric_scale = None;
        assert_eq!(
            ArrowTypeMapper::map(&c).unwrap(),
            DataType::Decimal128(38, 10)
        );
    }
}
