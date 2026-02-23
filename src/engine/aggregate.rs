use std::cmp::Ordering;

use serde_json::Value;
use thiserror::Error;

use crate::adapters::mlr;
use crate::util::sort::sort_value_keys;

/// Aggregate metric for `aggregate` command.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregateMetric {
    Count,
    Sum,
    Avg,
}

impl AggregateMetric {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Count => "count",
            Self::Sum => "sum",
            Self::Avg => "avg",
        }
    }
}

/// Domain errors for deterministic aggregate execution.
#[derive(Debug, Error)]
pub enum AggregateError {
    #[error("input row {index} must be an object")]
    RowNotObject { index: usize },
    #[error("input row {index} is missing group key `{key}`")]
    MissingGroupKey { index: usize, key: String },
    #[error("input row {index} is missing target key `{key}`")]
    MissingTargetKey { index: usize, key: String },
    #[error("input row {index} has non-numeric target `{key}` for metric `{metric}`")]
    NonNumericTarget {
        index: usize,
        key: String,
        metric: String,
    },
    #[error("mlr aggregate failed: {0}")]
    Mlr(#[from] mlr::MlrError),
}

/// Executes `mlr`-backed aggregation and applies deterministic output ordering.
pub fn aggregate_values(
    values: &[Value],
    group_by: &str,
    metric: AggregateMetric,
    target: &str,
) -> Result<Vec<Value>, AggregateError> {
    validate_rows(values, group_by, target, metric)?;

    let mlr_metric = match metric {
        AggregateMetric::Count => mlr::MlrAggregateMetric::Count,
        AggregateMetric::Sum => mlr::MlrAggregateMetric::Sum,
        AggregateMetric::Avg => mlr::MlrAggregateMetric::Avg,
    };
    let rows = mlr::aggregate_rows(values, group_by, mlr_metric, target)?;
    Ok(deterministic_rows(rows, group_by))
}

fn validate_rows(
    values: &[Value],
    group_by: &str,
    target: &str,
    metric: AggregateMetric,
) -> Result<(), AggregateError> {
    for (index, value) in values.iter().enumerate() {
        let Some(map) = value.as_object() else {
            return Err(AggregateError::RowNotObject { index });
        };

        if !map.contains_key(group_by) {
            return Err(AggregateError::MissingGroupKey {
                index,
                key: group_by.to_string(),
            });
        }
        if !map.contains_key(target) {
            return Err(AggregateError::MissingTargetKey {
                index,
                key: target.to_string(),
            });
        }

        if matches!(metric, AggregateMetric::Sum | AggregateMetric::Avg)
            && map.get(target).and_then(Value::as_f64).is_none()
        {
            return Err(AggregateError::NonNumericTarget {
                index,
                key: target.to_string(),
                metric: metric.as_str().to_string(),
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

    use super::{AggregateError, AggregateMetric, aggregate_values};

    #[test]
    fn missing_group_key_is_input_error() {
        let values = vec![
            json!({"region": "tokyo", "price": 10}),
            json!({"price": 20}),
        ];

        let err = aggregate_values(&values, "region", AggregateMetric::Count, "price")
            .expect_err("missing group key");
        assert!(matches!(
            err,
            AggregateError::MissingGroupKey { index: 1, .. }
        ));
    }

    #[test]
    fn sum_requires_numeric_target() {
        let values = vec![json!({"region": "tokyo", "price": "10"})];

        let err = aggregate_values(&values, "region", AggregateMetric::Sum, "price")
            .expect_err("non numeric target");
        assert!(matches!(
            err,
            AggregateError::NonNumericTarget { index: 0, .. }
        ));
    }
}
