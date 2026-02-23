use serde_json::{Map, Number, Value};

use crate::util::time::normalize_rfc3339_utc;

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
    let parsed = input.parse::<f64>().ok()?;
    Number::from_f64(parsed)
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
}
