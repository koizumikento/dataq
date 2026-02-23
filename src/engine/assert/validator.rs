use std::cmp::Ordering;

use regex::Regex;
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

struct CompiledPattern {
    path: String,
    source: String,
    regex: Regex,
}

pub fn validate(
    values: &[Value],
    rules: &AssertRules,
) -> Result<AssertReport, AssertValidationError> {
    let compiled_patterns = validate_rules(rules)?;

    let required_keys = sorted_unique_paths(&rules.required_keys);
    let forbid_keys = sorted_unique_paths(&rules.forbid_keys);

    let mut mismatches = Vec::new();
    validate_count(values, rules, &mut mismatches);

    for (index, row) in values.iter().enumerate() {
        validate_required_keys(index, row, &required_keys, &mut mismatches);
        validate_forbid_keys(index, row, &forbid_keys, &mut mismatches);
        validate_types(index, row, rules, &mut mismatches);
        validate_nullable(index, row, rules, &mut mismatches);
        validate_enum(index, row, rules, &mut mismatches);
        validate_patterns(index, row, rules, &compiled_patterns, &mut mismatches);
        validate_ranges(index, row, rules, &mut mismatches);
    }

    Ok(AssertReport {
        matched: mismatches.is_empty(),
        mismatch_count: mismatches.len(),
        mismatches,
    })
}

fn validate_rules(rules: &AssertRules) -> Result<Vec<CompiledPattern>, AssertValidationError> {
    if let (Some(min), Some(max)) = (rules.count.min, rules.count.max)
        && min > max
    {
        return Err(AssertValidationError::InputUsage(
            "count.min must be <= count.max".to_string(),
        ));
    }

    for path in sorted_unique_paths(&rules.required_keys) {
        validate_path(&path)?;
    }

    for path in sorted_unique_paths(&rules.forbid_keys) {
        validate_path(&path)?;
    }

    for path in rules.types.keys() {
        validate_path(path)?;
    }

    for path in rules.nullable.keys() {
        validate_path(path)?;
    }

    for path in rules.enum_values.keys() {
        validate_path(path)?;
    }

    let mut compiled_patterns = Vec::new();
    for (path, pattern) in &rules.patterns {
        validate_path(path)?;
        let regex = Regex::new(pattern).map_err(|err| {
            AssertValidationError::InputUsage(format!("invalid pattern for pattern.{path}: {err}"))
        })?;
        compiled_patterns.push(CompiledPattern {
            path: path.clone(),
            source: pattern.clone(),
            regex,
        });
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

    Ok(compiled_patterns)
}

fn validate_count(values: &[Value], rules: &AssertRules, mismatches: &mut Vec<MismatchEntry>) {
    let actual_len = values.len();
    if let Some(min) = rules.count.min
        && actual_len < min
    {
        push_mismatch(
            mismatches,
            "$".to_string(),
            "count",
            "below_min_count",
            json!(actual_len),
            json!(min),
        );
    }

    if let Some(max) = rules.count.max
        && actual_len > max
    {
        push_mismatch(
            mismatches,
            "$".to_string(),
            "count",
            "above_max_count",
            json!(actual_len),
            json!(max),
        );
    }
}

fn validate_required_keys(
    index: usize,
    row: &Value,
    required_keys: &[String],
    mismatches: &mut Vec<MismatchEntry>,
) {
    for key in required_keys {
        if get_value_at_path(row, key).is_none() {
            push_mismatch(
                mismatches,
                row_path(index, key),
                "required_keys",
                "missing_key",
                Value::Null,
                Value::String("present".to_string()),
            );
        }
    }
}

fn validate_forbid_keys(
    index: usize,
    row: &Value,
    forbid_keys: &[String],
    mismatches: &mut Vec<MismatchEntry>,
) {
    for key in forbid_keys {
        if let Some(actual) = get_value_at_path(row, key) {
            push_mismatch(
                mismatches,
                row_path(index, key),
                "forbid_keys",
                "forbidden_key",
                actual.clone(),
                Value::String("absent".to_string()),
            );
        }
    }
}

fn validate_types(
    index: usize,
    row: &Value,
    rules: &AssertRules,
    mismatches: &mut Vec<MismatchEntry>,
) {
    for (path, expected_type) in &rules.types {
        match get_value_at_path(row, path) {
            Some(actual) => {
                if actual.is_null() && is_nullable(rules, path) {
                    continue;
                }
                if !expected_type.matches(actual) {
                    push_mismatch(
                        mismatches,
                        row_path(index, path),
                        "types",
                        "type_mismatch",
                        Value::String(json_type_name(actual).to_string()),
                        Value::String(expected_type.as_str().to_string()),
                    );
                }
            }
            None => push_mismatch(
                mismatches,
                row_path(index, path),
                "types",
                "missing_key",
                Value::Null,
                Value::String(expected_type.as_str().to_string()),
            ),
        }
    }
}

fn validate_nullable(
    index: usize,
    row: &Value,
    rules: &AssertRules,
    mismatches: &mut Vec<MismatchEntry>,
) {
    for (path, allow_null) in &rules.nullable {
        if *allow_null {
            continue;
        }
        if let Some(actual) = get_value_at_path(row, path)
            && actual.is_null()
        {
            push_mismatch(
                mismatches,
                row_path(index, path),
                "nullable",
                "null_not_allowed",
                Value::Null,
                Value::Bool(false),
            );
        }
    }
}

fn validate_enum(
    index: usize,
    row: &Value,
    rules: &AssertRules,
    mismatches: &mut Vec<MismatchEntry>,
) {
    for (path, allowed_values) in &rules.enum_values {
        match get_value_at_path(row, path) {
            Some(actual) => {
                if actual.is_null() && is_nullable(rules, path) {
                    continue;
                }
                if !allowed_values.iter().any(|allowed| allowed == actual) {
                    push_mismatch(
                        mismatches,
                        row_path(index, path),
                        "enum",
                        "enum_mismatch",
                        actual.clone(),
                        Value::Array(allowed_values.clone()),
                    );
                }
            }
            None => push_mismatch(
                mismatches,
                row_path(index, path),
                "enum",
                "missing_key",
                Value::Null,
                Value::Array(allowed_values.clone()),
            ),
        }
    }
}

fn validate_patterns(
    index: usize,
    row: &Value,
    rules: &AssertRules,
    compiled_patterns: &[CompiledPattern],
    mismatches: &mut Vec<MismatchEntry>,
) {
    for compiled in compiled_patterns {
        match get_value_at_path(row, &compiled.path) {
            Some(actual) => {
                if actual.is_null() && is_nullable(rules, &compiled.path) {
                    continue;
                }

                let Some(actual_string) = actual.as_str() else {
                    push_mismatch(
                        mismatches,
                        row_path(index, &compiled.path),
                        "pattern",
                        "pattern_not_string",
                        Value::String(json_type_name(actual).to_string()),
                        Value::String(compiled.source.clone()),
                    );
                    continue;
                };

                if !compiled.regex.is_match(actual_string) {
                    push_mismatch(
                        mismatches,
                        row_path(index, &compiled.path),
                        "pattern",
                        "pattern_mismatch",
                        Value::String(actual_string.to_string()),
                        Value::String(compiled.source.clone()),
                    );
                }
            }
            None => push_mismatch(
                mismatches,
                row_path(index, &compiled.path),
                "pattern",
                "missing_key",
                Value::Null,
                Value::String(compiled.source.clone()),
            ),
        }
    }
}

fn validate_ranges(
    index: usize,
    row: &Value,
    rules: &AssertRules,
    mismatches: &mut Vec<MismatchEntry>,
) {
    for (path, range) in &rules.ranges {
        match get_value_at_path(row, path) {
            Some(actual) => {
                if actual.is_null() && is_nullable(rules, path) {
                    continue;
                }

                let Some(number) = actual.as_number() else {
                    push_mismatch(
                        mismatches,
                        row_path(index, path),
                        "ranges",
                        "not_numeric",
                        Value::String(json_type_name(actual).to_string()),
                        json!({
                            "type": "number",
                            "min": range.min,
                            "max": range.max
                        }),
                    );
                    continue;
                };

                if let Some(min) = &range.min
                    && compare_numbers(number, min) == Ordering::Less
                {
                    push_mismatch(
                        mismatches,
                        row_path(index, path),
                        "ranges",
                        "below_min",
                        actual.clone(),
                        Value::Number(min.clone()),
                    );
                }
                if let Some(max) = &range.max
                    && compare_numbers(number, max) == Ordering::Greater
                {
                    push_mismatch(
                        mismatches,
                        row_path(index, path),
                        "ranges",
                        "above_max",
                        actual.clone(),
                        Value::Number(max.clone()),
                    );
                }
            }
            None => push_mismatch(
                mismatches,
                row_path(index, path),
                "ranges",
                "missing_key",
                Value::Null,
                json!({
                    "min": range.min,
                    "max": range.max
                }),
            ),
        }
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

fn is_nullable(rules: &AssertRules, path: &str) -> bool {
    rules.nullable.get(path).copied().unwrap_or(false)
}

fn sorted_unique_paths(paths: &[String]) -> Vec<String> {
    let mut sorted = paths.to_vec();
    sorted.sort();
    sorted.dedup();
    sorted
}

fn push_mismatch(
    mismatches: &mut Vec<MismatchEntry>,
    path: String,
    rule_kind: &str,
    reason: &str,
    actual: Value,
    expected: Value,
) {
    mismatches.push(MismatchEntry {
        path,
        rule_kind: rule_kind.to_string(),
        reason: reason.to_string(),
        actual,
        expected,
    });
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
            ..AssertRules::default()
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
            ..AssertRules::default()
        };

        let report = validate(&values, &rules).expect("validation report");
        assert!(!report.matched);
        assert_eq!(report.mismatch_count, 2);
        assert_eq!(report.mismatches[0].reason, "type_mismatch");
        assert_eq!(report.mismatches[0].rule_kind, "types");
        assert_eq!(report.mismatches[1].reason, "above_max");
        assert_eq!(report.mismatches[1].rule_kind, "ranges");
    }

    #[test]
    fn validates_enum_pattern_forbid_keys_and_nullable() {
        let values = vec![json!({
            "status": "pending",
            "name": "User-1",
            "meta": {"blocked": true},
            "optional": null
        })];

        let mut enum_values = BTreeMap::new();
        enum_values.insert("status".to_string(), vec![json!("open"), json!("closed")]);

        let mut patterns = BTreeMap::new();
        patterns.insert("name".to_string(), "^[a-z]+_[0-9]+$".to_string());

        let mut nullable = BTreeMap::new();
        nullable.insert("optional".to_string(), true);

        let rules = AssertRules {
            forbid_keys: vec!["meta.blocked".to_string()],
            nullable,
            enum_values,
            patterns,
            ..AssertRules::default()
        };

        let report = validate(&values, &rules).expect("validation report");
        assert!(!report.matched);
        assert_eq!(report.mismatch_count, 3);

        assert_eq!(report.mismatches[0].rule_kind, "forbid_keys");
        assert_eq!(report.mismatches[0].reason, "forbidden_key");
        assert_eq!(report.mismatches[1].rule_kind, "enum");
        assert_eq!(report.mismatches[1].reason, "enum_mismatch");
        assert_eq!(report.mismatches[2].rule_kind, "pattern");
        assert_eq!(report.mismatches[2].reason, "pattern_mismatch");
    }

    #[test]
    fn nullable_true_allows_null_across_other_rules() {
        let values = vec![json!({
            "nickname": null,
            "score": null,
            "status": null
        })];

        let mut types = BTreeMap::new();
        types.insert("nickname".to_string(), RuleType::String);

        let mut ranges = BTreeMap::new();
        ranges.insert(
            "score".to_string(),
            NumericRangeRule {
                min: Some(float_number(0.0)),
                max: Some(float_number(10.0)),
            },
        );

        let mut enum_values = BTreeMap::new();
        enum_values.insert("status".to_string(), vec![json!("ok")]);

        let mut patterns = BTreeMap::new();
        patterns.insert("nickname".to_string(), "^[a-z]+$".to_string());

        let mut nullable = BTreeMap::new();
        nullable.insert("nickname".to_string(), true);
        nullable.insert("score".to_string(), true);
        nullable.insert("status".to_string(), true);

        let rules = AssertRules {
            types,
            nullable,
            enum_values,
            patterns,
            ranges,
            ..AssertRules::default()
        };

        let report = validate(&values, &rules).expect("validation report");
        assert!(report.matched);
        assert_eq!(report.mismatch_count, 0);
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
            ..AssertRules::default()
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
    fn rejects_invalid_pattern() {
        let mut patterns = BTreeMap::new();
        patterns.insert("name".to_string(), "[a-z".to_string());

        let rules = AssertRules {
            patterns,
            ..AssertRules::default()
        };

        let err = validate(&[], &rules).expect_err("must fail");
        match err {
            AssertValidationError::InputUsage(message) => {
                assert!(message.contains("invalid pattern"));
                assert!(message.contains("pattern.name"));
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
            ..AssertRules::default()
        };

        let report = validate(&values, &rules).expect("validation report");
        assert!(!report.matched);
        assert_eq!(report.mismatch_count, 1);
        assert_eq!(report.mismatches[0].reason, "above_max");
        assert_eq!(report.mismatches[0].rule_kind, "ranges");
        assert_eq!(report.mismatches[0].path, "$[0].value");
    }

    fn float_number(value: f64) -> Number {
        Number::from_f64(value).expect("finite float")
    }
}
