pub mod coerce;
pub mod normalize;

use serde_json::Value;

/// Canonicalization options shared by `canon` command and engine layers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CanonOptions {
    /// Sort object keys lexicographically. If `false`, preserve input key order.
    pub sort_keys: bool,
    /// Normalize RFC3339 timestamps to UTC (`Z`) when enabled.
    pub normalize_time: bool,
}

impl Default for CanonOptions {
    fn default() -> Self {
        Self {
            sort_keys: true,
            normalize_time: false,
        }
    }
}

/// Canonicalize a sequence of JSON values deterministically.
pub fn canonicalize_values(values: Vec<Value>, options: CanonOptions) -> Vec<Value> {
    values
        .into_iter()
        .map(|value| canonicalize_value(value, options))
        .collect()
}

/// Canonicalize one JSON value by applying coercion then normalization.
pub fn canonicalize_value(value: Value, options: CanonOptions) -> Value {
    let coerced = coerce::coerce_value(value, options.normalize_time);
    normalize::normalize_value(coerced, options.sort_keys)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{CanonOptions, canonicalize_value};

    #[test]
    fn canonicalizes_with_defaults() {
        let input = json!({
            "z": "false",
            "a": {
                "num": "1",
                "time": "2026-02-23T20:15:30+09:00"
            }
        });
        let actual = canonicalize_value(input, CanonOptions::default());
        assert_eq!(
            actual,
            json!({
                "a": {
                    "num": 1,
                    "time": "2026-02-23T20:15:30+09:00"
                },
                "z": false
            })
        );
    }

    #[test]
    fn canonicalization_is_deterministic() {
        let input = json!({
            "b": {"z": "2", "a": "1"},
            "a": [{"k":"false", "d":"3.5"}]
        });
        let options = CanonOptions {
            sort_keys: true,
            normalize_time: true,
        };
        let once = canonicalize_value(input.clone(), options);
        let twice = canonicalize_value(once.clone(), options);
        assert_eq!(once, twice);
    }
}
