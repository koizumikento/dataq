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

/// Row ordering selector for aggregate output.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregateSortBy {
    Group,
    Metric,
}

/// Sort direction for aggregate output.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregateOrder {
    Asc,
    Desc,
}

/// Optional aggregate output controls applied after `mlr` aggregation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AggregateOptions {
    pub sort_by: AggregateSortBy,
    pub order: AggregateOrder,
    pub limit: Option<usize>,
}

impl Default for AggregateOptions {
    fn default() -> Self {
        Self {
            sort_by: AggregateSortBy::Group,
            order: AggregateOrder::Asc,
            limit: None,
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
    options: AggregateOptions,
) -> Result<Vec<Value>, AggregateError> {
    validate_rows(values, group_by, target, metric)?;

    let mlr_metric = match metric {
        AggregateMetric::Count => mlr::MlrAggregateMetric::Count,
        AggregateMetric::Sum => mlr::MlrAggregateMetric::Sum,
        AggregateMetric::Avg => mlr::MlrAggregateMetric::Avg,
    };
    let rows = mlr::aggregate_rows(values, group_by, mlr_metric, target)?;
    Ok(deterministic_rows(rows, group_by, metric, options))
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

fn deterministic_rows(
    rows: Vec<Value>,
    key_field: &str,
    metric: AggregateMetric,
    options: AggregateOptions,
) -> Vec<Value> {
    let mut rows = sort_aggregate_rows(rows, key_field, metric, options);
    if let Some(limit) = options.limit {
        rows.truncate(limit);
    }
    rows.into_iter().map(|row| sort_value_keys(&row)).collect()
}

fn sort_aggregate_rows(
    mut rows: Vec<Value>,
    key_field: &str,
    metric: AggregateMetric,
    options: AggregateOptions,
) -> Vec<Value> {
    rows.sort_by(|left, right| match options.sort_by {
        AggregateSortBy::Group => compare_rows_by_group(left, right, key_field, options.order),
        AggregateSortBy::Metric => {
            compare_rows_by_metric(left, right, key_field, metric, options.order)
        }
    });
    rows
}

fn compare_rows_by_group(
    left: &Value,
    right: &Value,
    key_field: &str,
    order: AggregateOrder,
) -> Ordering {
    let left_key = key_field_literal(left, key_field);
    let right_key = key_field_literal(right, key_field);
    compare_with_order(left_key.cmp(&right_key), order)
        .then_with(|| canonical_row_literal(left).cmp(&canonical_row_literal(right)))
}

fn compare_rows_by_metric(
    left: &Value,
    right: &Value,
    key_field: &str,
    metric: AggregateMetric,
    order: AggregateOrder,
) -> Ordering {
    compare_with_order(
        metric_value(left, metric).total_cmp(&metric_value(right, metric)),
        order,
    )
    .then_with(|| key_field_literal(left, key_field).cmp(&key_field_literal(right, key_field)))
    .then_with(|| canonical_row_literal(left).cmp(&canonical_row_literal(right)))
}

fn compare_with_order(ordering: Ordering, order: AggregateOrder) -> Ordering {
    match order {
        AggregateOrder::Asc => ordering,
        AggregateOrder::Desc => ordering.reverse(),
    }
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

fn metric_value(value: &Value, metric: AggregateMetric) -> f64 {
    match value {
        Value::Object(map) => map
            .get(metric.as_str())
            .and_then(Value::as_f64)
            .unwrap_or(f64::NAN),
        _ => f64::NAN,
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

    use super::{
        AggregateError, AggregateMetric, AggregateOptions, AggregateOrder, AggregateSortBy,
        aggregate_values, deterministic_rows,
    };

    #[test]
    fn missing_group_key_is_input_error() {
        let values = vec![
            json!({"region": "tokyo", "price": 10}),
            json!({"price": 20}),
        ];

        let err = aggregate_values(
            &values,
            "region",
            AggregateMetric::Count,
            "price",
            AggregateOptions::default(),
        )
        .expect_err("missing group key");
        assert!(matches!(
            err,
            AggregateError::MissingGroupKey { index: 1, .. }
        ));
    }

    #[test]
    fn sum_requires_numeric_target() {
        let values = vec![json!({"region": "tokyo", "price": "10"})];

        let err = aggregate_values(
            &values,
            "region",
            AggregateMetric::Sum,
            "price",
            AggregateOptions::default(),
        )
        .expect_err("non numeric target");
        assert!(matches!(
            err,
            AggregateError::NonNumericTarget { index: 0, .. }
        ));
    }

    #[test]
    fn default_options_preserve_group_ascending_order() {
        let rows = vec![
            json!({"team": "b", "sum": 7.0}),
            json!({"team": "a", "sum": 15.0}),
        ];

        assert_eq!(
            deterministic_rows(
                rows,
                "team",
                AggregateMetric::Sum,
                AggregateOptions::default()
            ),
            vec![
                json!({"sum": 15.0, "team": "a"}),
                json!({"sum": 7.0, "team": "b"})
            ]
        );
    }

    #[test]
    fn metric_desc_limit_uses_group_literal_tie_breaker_ascending() {
        let rows = vec![
            json!({"team": "c", "sum": 15.0}),
            json!({"team": "a", "sum": 15.0}),
            json!({"team": "b", "sum": 7.0}),
        ];

        assert_eq!(
            deterministic_rows(
                rows,
                "team",
                AggregateMetric::Sum,
                AggregateOptions {
                    sort_by: AggregateSortBy::Metric,
                    order: AggregateOrder::Desc,
                    limit: Some(2),
                },
            ),
            vec![
                json!({"sum": 15.0, "team": "a"}),
                json!({"sum": 15.0, "team": "c"})
            ]
        );
    }

    #[test]
    fn group_desc_reverses_group_comparison_only() {
        let rows = vec![
            json!({"team": "a", "sum": 9.0}),
            json!({"team": "b", "sum": 7.0}),
            json!({"team": "a", "sum": 8.0}),
        ];

        assert_eq!(
            deterministic_rows(
                rows,
                "team",
                AggregateMetric::Sum,
                AggregateOptions {
                    sort_by: AggregateSortBy::Group,
                    order: AggregateOrder::Desc,
                    limit: None,
                },
            ),
            vec![
                json!({"sum": 7.0, "team": "b"}),
                json!({"sum": 8.0, "team": "a"}),
                json!({"sum": 9.0, "team": "a"}),
            ]
        );
    }

    #[test]
    fn limit_zero_returns_empty_array() {
        let rows = vec![json!({"team": "a", "count": 1})];

        assert_eq!(
            deterministic_rows(
                rows,
                "team",
                AggregateMetric::Count,
                AggregateOptions {
                    limit: Some(0),
                    ..AggregateOptions::default()
                },
            ),
            Vec::<serde_json::Value>::new()
        );
    }
}
