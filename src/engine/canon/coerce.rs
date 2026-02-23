use num_bigint::BigInt;
use serde_json::{Map, Number, Value};

use crate::util::time::normalize_rfc3339_utc;

const MAX_SAFE_NUMERIC_STRING_LEN: usize = 512;
const MAX_SAFE_DECIMAL_EXPONENT: i32 = 2048;

/// Recursively coerce scalar string values into typed JSON values.
pub fn coerce_value(value: Value, normalize_time: bool) -> Value {
    match value {
        Value::Object(map) => Value::Object(
            map.into_iter()
                .map(|(key, child)| (key, coerce_value(child, normalize_time)))
                .collect::<Map<String, Value>>(),
        ),
        Value::Array(items) => Value::Array(
            items
                .into_iter()
                .map(|item| coerce_value(item, normalize_time))
                .collect(),
        ),
        Value::String(text) => coerce_string(text, normalize_time),
        primitive => primitive,
    }
}

fn coerce_string(text: String, normalize_time: bool) -> Value {
    match text.as_str() {
        "true" => Value::Bool(true),
        "false" => Value::Bool(false),
        _ => {
            if let Some(number) = parse_json_number(text.as_str()) {
                return Value::Number(number);
            }
            if normalize_time {
                if let Some(normalized) = normalize_rfc3339_utc(text.as_str()) {
                    return Value::String(normalized);
                }
            }
            Value::String(text)
        }
    }
}

fn parse_json_number(input: &str) -> Option<Number> {
    if let Ok(parsed) = input.parse::<i64>() {
        return Some(Number::from(parsed));
    }
    if let Ok(parsed) = input.parse::<u64>() {
        return Some(Number::from(parsed));
    }
    if is_integer_literal(input) {
        return None;
    }
    let parsed = input.parse::<f64>().ok()?;
    if !is_exact_f64_literal(input, parsed) {
        return None;
    }
    Number::from_f64(parsed)
}

fn is_integer_literal(input: &str) -> bool {
    let bytes = input.as_bytes();
    if bytes.is_empty() {
        return false;
    }
    let mut index = 0;
    if bytes[index] == b'-' {
        index += 1;
        if index == bytes.len() {
            return false;
        }
    }
    bytes[index..].iter().all(|byte| byte.is_ascii_digit())
}

fn is_exact_f64_literal(input: &str, parsed: f64) -> bool {
    let Some((decimal_numerator, decimal_denominator)) = parse_decimal_rational(input) else {
        return false;
    };
    let Some((float_numerator, float_denominator)) = f64_rational(parsed) else {
        return false;
    };
    decimal_numerator * float_denominator == float_numerator * decimal_denominator
}

fn parse_decimal_rational(input: &str) -> Option<(BigInt, BigInt)> {
    if input.len() > MAX_SAFE_NUMERIC_STRING_LEN {
        return None;
    }

    let bytes = input.as_bytes();
    let mut index = 0usize;
    let mut negative = false;
    if bytes.get(index) == Some(&b'-') {
        negative = true;
        index += 1;
    }

    let mut digits = String::new();
    let mut has_digit = false;
    while let Some(byte) = bytes.get(index) {
        if byte.is_ascii_digit() {
            digits.push(char::from(*byte));
            has_digit = true;
            index += 1;
            continue;
        }
        break;
    }

    let mut fractional_digits: i32 = 0;
    if bytes.get(index) == Some(&b'.') {
        index += 1;
        while let Some(byte) = bytes.get(index) {
            if byte.is_ascii_digit() {
                digits.push(char::from(*byte));
                has_digit = true;
                fractional_digits = fractional_digits.checked_add(1)?;
                index += 1;
                continue;
            }
            break;
        }
    }

    if !has_digit {
        return None;
    }

    let mut exponent: i32 = 0;
    if matches!(bytes.get(index), Some(b'e' | b'E')) {
        index += 1;
        let mut exponent_negative = false;
        if matches!(bytes.get(index), Some(b'+' | b'-')) {
            exponent_negative = bytes[index] == b'-';
            index += 1;
        }
        let exponent_start = index;
        while let Some(byte) = bytes.get(index) {
            if byte.is_ascii_digit() {
                exponent = exponent
                    .checked_mul(10)?
                    .checked_add(i32::from(*byte - b'0'))?;
                index += 1;
                continue;
            }
            break;
        }
        if exponent_start == index {
            return None;
        }
        if exponent_negative {
            exponent = -exponent;
        }
    }

    if index != bytes.len() {
        return None;
    }

    let exponent10 = exponent.checked_sub(fractional_digits)?;
    if exponent10.abs() > MAX_SAFE_DECIMAL_EXPONENT {
        return None;
    }

    let normalized_digits = digits.trim_start_matches('0');
    let digits = if normalized_digits.is_empty() {
        "0"
    } else {
        normalized_digits
    };

    let mut numerator = BigInt::parse_bytes(digits.as_bytes(), 10)?;
    if negative {
        numerator = -numerator;
    }
    if exponent10 >= 0 {
        numerator *= BigInt::from(10u8).pow(exponent10 as u32);
        return Some((numerator, BigInt::from(1u8)));
    }

    let denominator = BigInt::from(10u8).pow((-exponent10) as u32);
    Some((numerator, denominator))
}

fn f64_rational(value: f64) -> Option<(BigInt, BigInt)> {
    if !value.is_finite() {
        return None;
    }
    if value == 0.0 {
        return Some((BigInt::from(0u8), BigInt::from(1u8)));
    }

    let bits = value.to_bits();
    let negative = (bits >> 63) == 1;
    let exponent_bits = ((bits >> 52) & 0x7ff) as i32;
    let fraction_bits = bits & ((1u64 << 52) - 1);

    let (mantissa, exponent2) = if exponent_bits == 0 {
        (fraction_bits, 1 - 1023 - 52)
    } else {
        (fraction_bits | (1u64 << 52), exponent_bits - 1023 - 52)
    };

    let mut numerator = BigInt::from(mantissa);
    let mut denominator = BigInt::from(1u8);
    if exponent2 >= 0 {
        numerator <<= exponent2 as usize;
    } else {
        denominator <<= (-exponent2) as usize;
    }

    if negative {
        numerator = -numerator;
    }
    Some((numerator, denominator))
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::coerce_value;

    #[test]
    fn coerces_booleans_and_numbers_recursively() {
        let input = json!({
            "bools": ["true", "false"],
            "numbers": {
                "i": "42",
                "f": "3.5",
                "raw": "007x"
            }
        });
        let actual = coerce_value(input, false);
        assert_eq!(
            actual,
            json!({
                "bools": [true, false],
                "numbers": {
                    "i": 42,
                    "f": 3.5,
                    "raw": "007x"
                }
            })
        );
    }

    #[test]
    fn normalizes_rfc3339_when_enabled() {
        let input = json!("2026-02-23T20:15:30+09:00");
        let actual = coerce_value(input, true);
        assert_eq!(actual, json!("2026-02-23T11:15:30Z"));
    }

    #[test]
    fn keeps_rfc3339_string_when_disabled() {
        let input = json!("2026-02-23T20:15:30+09:00");
        let actual = coerce_value(input.clone(), false);
        assert_eq!(actual, input);
    }

    #[test]
    fn keeps_large_or_precision_sensitive_numbers_as_strings() {
        let input = json!({
            "safe_float": "3.5",
            "precise_float": "0.10000000000000001",
            "too_large_integer": "18446744073709551616"
        });
        let actual = coerce_value(input, false);
        assert_eq!(
            actual,
            json!({
                "safe_float": 3.5,
                "precise_float": "0.10000000000000001",
                "too_large_integer": "18446744073709551616"
            })
        );
    }

    #[test]
    fn preserves_fractional_seconds_when_normalizing_rfc3339() {
        let input = json!("2026-02-23T20:15:30.123456+09:00");
        let actual = coerce_value(input, true);
        assert_eq!(actual, json!("2026-02-23T11:15:30.123456Z"));
    }
}
