use std::cmp::Ordering;

use serde_json::Value;
use thiserror::Error;

use crate::adapters::mlr;
use crate::util::sort::sort_value_keys;

/// Join strategy for `join` command.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinHow {
    Inner,
    Left,
}

/// Domain errors for deterministic join execution.
#[derive(Debug, Error)]
pub enum JoinError {
    #[error("left input row {index} must be an object")]
    LeftRowNotObject { index: usize },
    #[error("right input row {index} must be an object")]
    RightRowNotObject { index: usize },
    #[error("left input row {index} is missing join key `{key}`")]
    LeftMissingKey { index: usize, key: String },
    #[error("right input row {index} is missing join key `{key}`")]
    RightMissingKey { index: usize, key: String },
    #[error("mlr join failed: {0}")]
    Mlr(#[from] mlr::MlrError),
}

/// Executes `mlr`-backed join and applies deterministic output ordering.
pub fn join_values(
    left: &[Value],
    right: &[Value],
    on: &str,
    how: JoinHow,
) -> Result<Vec<Value>, JoinError> {
    validate_rows(left, on, "left")?;
    validate_rows(right, on, "right")?;

    let join_how = match how {
        JoinHow::Inner => mlr::MlrJoinHow::Inner,
        JoinHow::Left => mlr::MlrJoinHow::Left,
    };
    let rows = mlr::join_rows(left, right, on, join_how)?;
    Ok(deterministic_rows(rows, on))
}

fn validate_rows(values: &[Value], on: &str, side: &'static str) -> Result<(), JoinError> {
    for (index, value) in values.iter().enumerate() {
        let Some(map) = value.as_object() else {
            return Err(match side {
                "left" => JoinError::LeftRowNotObject { index },
                _ => JoinError::RightRowNotObject { index },
            });
        };

        if !map.contains_key(on) {
            return Err(match side {
                "left" => JoinError::LeftMissingKey {
                    index,
                    key: on.to_string(),
                },
                _ => JoinError::RightMissingKey {
                    index,
                    key: on.to_string(),
                },
            });
        }
    }
    Ok(())
}

fn deterministic_rows(mut rows: Vec<Value>, key_field: &str) -> Vec<Value> {
    rows.sort_by(|left, right| compare_rows(left, right, key_field));
    rows.into_iter().map(|row| sort_value_keys(&row)).collect()
}

fn compare_rows(left: &Value, right: &Value, key_field: &str) -> Ordering {
    let left_key = key_field_literal(left, key_field);
    let right_key = key_field_literal(right, key_field);
    left_key
        .cmp(&right_key)
        .then_with(|| canonical_row_literal(left).cmp(&canonical_row_literal(right)))
}

fn key_field_literal(value: &Value, key_field: &str) -> String {
    match value {
        Value::Object(map) => map
            .get(key_field)
            .map(value_literal)
            .unwrap_or_else(|| "null".to_string()),
        _ => "null".to_string(),
    }
}

fn canonical_row_literal(value: &Value) -> String {
    value_literal(&sort_value_keys(value))
}

fn value_literal(value: &Value) -> String {
    serde_json::to_string(value).unwrap_or_else(|_| "null".to_string())
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{JoinError, JoinHow, join_values};

    #[test]
    fn left_missing_key_is_input_error() {
        let left = vec![json!({"id": 1}), json!({"name": "missing"})];
        let right = vec![json!({"id": 1})];

        let err = join_values(&left, &right, "id", JoinHow::Inner).expect_err("missing key");
        assert!(matches!(err, JoinError::LeftMissingKey { index: 1, .. }));
    }

    #[test]
    fn right_non_object_is_input_error() {
        let left = vec![json!({"id": 1})];
        let right = vec![json!("oops")];

        let err = join_values(&left, &right, "id", JoinHow::Left).expect_err("non object");
        assert!(matches!(err, JoinError::RightRowNotObject { index: 0 }));
    }
}
