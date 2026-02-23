use std::cmp::Ordering;
use std::collections::BTreeSet;

use serde_json::{Value, json};
use thiserror::Error;

use crate::domain::rules::{AssertReport, AssertRules, MismatchEntry};

#[derive(Debug, Error)]
pub enum AssertValidationError {
    #[error("{0}")]
    InputUsage(String),
    #[error("{0}")]
    Internal(String),
}

pub fn validate(
    values: &[Value],
    rules: &AssertRules,
) -> Result<AssertReport, AssertValidationError> {
    validate_rules(rules)?;

    let mut mismatches = Vec::new();
    validate_count(values, rules, &mut mismatches);

    let required_keys: BTreeSet<String> = rules.required_keys.iter().cloned().collect();

    for (index, row) in values.iter().enumerate() {
        for key in &required_keys {
            if get_value_at_path(row, key).is_none() {
                mismatches.push(MismatchEntry {
                    path: row_path(index, key),
                    reason: "missing_key".to_string(),
                    actual: Value::Null,
                    expected: Value::String("present".to_string()),
                });
            }
        }

        for (path, expected_type) in &rules.types {
            match get_value_at_path(row, path) {
                Some(actual) => {
                    if !expected_type.matches(actual) {
                        mismatches.push(MismatchEntry {
                            path: row_path(index, path),
                            reason: "type_mismatch".to_string(),
                            actual: Value::String(json_type_name(actual).to_string()),
                            expected: Value::String(expected_type.as_str().to_string()),
                        });
                    }
                }
                None => mismatches.push(MismatchEntry {
                    path: row_path(index, path),
                    reason: "missing_key".to_string(),
                    actual: Value::Null,
                    expected: Value::String(expected_type.as_str().to_string()),
                }),
            }
        }

        for (path, range) in &rules.ranges {
            match get_value_at_path(row, path) {
                Some(actual) => {
                    let Some(number) = actual.as_number() else {
                        mismatches.push(MismatchEntry {
                            path: row_path(index, path),
                            reason: "not_numeric".to_string(),
                            actual: Value::String(json_type_name(actual).to_string()),
                            expected: json!({
                                "type": "number",
                                "min": range.min,
                                "max": range.max
                            }),
                        });
                        continue;
                    };

                    if let Some(min) = &range.min {
                        if compare_numbers(number, min) == Ordering::Less {
                            mismatches.push(MismatchEntry {
                                path: row_path(index, path),
                                reason: "below_min".to_string(),
                                actual: actual.clone(),
                                expected: Value::Number(min.clone()),
                            });
                        }
                    }
                    if let Some(max) = &range.max {
                        if compare_numbers(number, max) == Ordering::Greater {
                            mismatches.push(MismatchEntry {
                                path: row_path(index, path),
                                reason: "above_max".to_string(),
                                actual: actual.clone(),
                                expected: Value::Number(max.clone()),
                            });
                        }
                    }
                }
                None => mismatches.push(MismatchEntry {
                    path: row_path(index, path),
                    reason: "missing_key".to_string(),
                    actual: Value::Null,
                    expected: json!({
                        "min": range.min,
                        "max": range.max
                    }),
                }),
            }
        }
    }

    sort_mismatches(&mut mismatches);

    Ok(AssertReport {
        matched: mismatches.is_empty(),
        mismatch_count: mismatches.len(),
        mismatches,
    })
}

fn validate_rules(rules: &AssertRules) -> Result<(), AssertValidationError> {
    if let (Some(min), Some(max)) = (rules.count.min, rules.count.max)
        && min > max
    {
        return Err(AssertValidationError::InputUsage(
            "count.min must be <= count.max".to_string(),
        ));
    }

    for path in &rules.required_keys {
        validate_path(path)?;
    }

    for path in rules.types.keys() {
        validate_path(path)?;
    }

    for (path, range) in &rules.ranges {
        validate_path(path)?;
        if let (Some(min), Some(max)) = (&range.min, &range.max)
            && compare_numbers(min, max) == Ordering::Greater
        {
            return Err(AssertValidationError::InputUsage(format!(
                "ranges.{path}.min must be <= ranges.{path}.max"
            )));
        }
    }

    Ok(())
}

fn validate_count(values: &[Value], rules: &AssertRules, mismatches: &mut Vec<MismatchEntry>) {
    let actual_len = values.len();
    if let Some(min) = rules.count.min
        && actual_len < min
    {
        mismatches.push(MismatchEntry {
            path: "$".to_string(),
            reason: "below_min_count".to_string(),
            actual: json!(actual_len),
            expected: json!(min),
        });
    }

    if let Some(max) = rules.count.max
        && actual_len > max
    {
        mismatches.push(MismatchEntry {
            path: "$".to_string(),
            reason: "above_max_count".to_string(),
            actual: json!(actual_len),
            expected: json!(max),
        });
    }
}

fn validate_path(path: &str) -> Result<(), AssertValidationError> {
    if path.is_empty() {
        return Err(AssertValidationError::InputUsage(
            "rule paths must not be empty".to_string(),
        ));
    }
    if path.split('.').any(|segment| segment.is_empty()) {
        return Err(AssertValidationError::InputUsage(format!(
            "invalid rule path `{path}`"
        )));
    }
    Ok(())
}

fn row_path(index: usize, path: &str) -> String {
    if path.is_empty() {
        format!("$[{index}]")
    } else {
        format!("$[{index}].{path}")
    }
}

fn get_value_at_path<'a>(root: &'a Value, path: &str) -> Option<&'a Value> {
    let mut current = root;
    for segment in path.split('.') {
        match current {
            Value::Object(map) => {
                current = map.get(segment)?;
            }
            _ => return None,
        }
    }
    Some(current)
}

fn json_type_name(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(n) if n.is_i64() || n.is_u64() => "integer",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

fn compare_numbers(left: &serde_json::Number, right: &serde_json::Number) -> Ordering {
    if let (Some(left), Some(right)) = (left.as_i64(), right.as_i64()) {
        return left.cmp(&right);
    }

    if let (Some(left), Some(right)) = (left.as_u64(), right.as_u64()) {
        return left.cmp(&right);
    }

    if let (Some(left), Some(right)) = (left.as_i64(), right.as_u64()) {
        return if left.is_negative() {
            Ordering::Less
        } else {
            u64::try_from(left)
                .expect("non-negative i64 always fits into u64")
                .cmp(&right)
        };
    }

    if let (Some(left), Some(right)) = (left.as_u64(), right.as_i64()) {
        return if right.is_negative() {
            Ordering::Greater
        } else {
            left.cmp(&u64::try_from(right).expect("non-negative i64 always fits into u64"))
        };
    }

    left.as_f64()
        .and_then(|left| right.as_f64().and_then(|right| left.partial_cmp(&right)))
        .expect("serde_json::Number is always finite")
}

fn sort_mismatches(mismatches: &mut [MismatchEntry]) {
    mismatches.sort_by(|left, right| {
        let left_key = (
            left.path.clone(),
            left.reason.clone(),
            stable_value_key(&left.actual),
            stable_value_key(&left.expected),
        );
        let right_key = (
            right.path.clone(),
            right.reason.clone(),
            stable_value_key(&right.actual),
            stable_value_key(&right.expected),
        );
        left_key.cmp(&right_key)
    });
}

fn stable_value_key(value: &Value) -> String {
    serde_json::to_string(value).unwrap_or_else(|_| "<serialization-error>".to_string())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use serde_json::{Number, json};

    use crate::domain::rules::{AssertRules, CountRule, NumericRangeRule, RuleType};

    use super::{AssertValidationError, validate};

    #[test]
    fn validates_count_and_field_rules() {
        let values = vec![json!({"id": 1, "score": 1.5})];
        let mut types = BTreeMap::new();
        types.insert("id".to_string(), RuleType::Integer);
        let mut ranges = BTreeMap::new();
        ranges.insert(
            "score".to_string(),
            NumericRangeRule {
                min: Some(float_number(1.0)),
                max: Some(float_number(2.0)),
            },
        );
        let rules = AssertRules {
            required_keys: vec!["id".to_string(), "score".to_string()],
            types,
            count: CountRule {
                min: Some(1),
                max: Some(1),
            },
            ranges,
        };

        let report = validate(&values, &rules).expect("valid result");
        assert!(report.matched);
        assert_eq!(report.mismatch_count, 0);
    }

    #[test]
    fn reports_type_and_range_mismatches() {
        let values = vec![json!({"id": "1", "score": 5.0})];
        let mut types = BTreeMap::new();
        types.insert("id".to_string(), RuleType::Integer);
        let mut ranges = BTreeMap::new();
        ranges.insert(
            "score".to_string(),
            NumericRangeRule {
                min: Some(float_number(1.0)),
                max: Some(float_number(3.0)),
            },
        );
        let rules = AssertRules {
            required_keys: vec![],
            types,
            count: CountRule::default(),
            ranges,
        };

        let report = validate(&values, &rules).expect("validation report");
        assert!(!report.matched);
        assert_eq!(report.mismatch_count, 2);
        assert_eq!(report.mismatches[0].reason, "type_mismatch");
        assert_eq!(report.mismatches[1].reason, "above_max");
    }

    #[test]
    fn rejects_invalid_rule_bounds() {
        let rules = AssertRules {
            required_keys: vec![],
            types: BTreeMap::new(),
            count: CountRule {
                min: Some(3),
                max: Some(1),
            },
            ranges: BTreeMap::new(),
        };

        let err = validate(&[], &rules).expect_err("must fail");
        match err {
            AssertValidationError::InputUsage(message) => {
                assert!(message.contains("count.min"));
            }
            other => panic!("unexpected error: {other}"),
        }
    }

    #[test]
    fn compares_large_integer_ranges_exactly() {
        let values = vec![json!({"value": 9_007_199_254_740_993u64})];
        let mut ranges = BTreeMap::new();
        ranges.insert(
            "value".to_string(),
            NumericRangeRule {
                min: None,
                max: Some(Number::from(9_007_199_254_740_992u64)),
            },
        );
        let rules = AssertRules {
            required_keys: vec![],
            types: BTreeMap::new(),
            count: CountRule::default(),
            ranges,
        };

        let report = validate(&values, &rules).expect("validation report");
        assert!(!report.matched);
        assert_eq!(report.mismatch_count, 1);
        assert_eq!(report.mismatches[0].reason, "above_max");
        assert_eq!(report.mismatches[0].path, "$[0].value");
    }

    fn float_number(value: f64) -> Number {
        Number::from_f64(value).expect("finite float")
    }
}
