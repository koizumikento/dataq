use std::cmp::Ordering;

use serde_json::{Number, Value};
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
    compare_with_order(compare_metric_values(left, right, metric), order)
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

fn compare_metric_values(left: &Value, right: &Value, metric: AggregateMetric) -> Ordering {
    let left = metric_number(left, metric);
    let right = metric_number(right, metric);

    match (left, right) {
        (Some(left), Some(right)) => compare_numbers(left, right),
        (Some(_), None) => Ordering::Greater,
        (None, Some(_)) => Ordering::Less,
        (None, None) => Ordering::Equal,
    }
}

fn metric_number(value: &Value, metric: AggregateMetric) -> Option<&Number> {
    match value {
        Value::Object(map) => map.get(metric.as_str()).and_then(Value::as_number),
        _ => None,
    }
}

fn compare_numbers(left: &Number, right: &Number) -> Ordering {
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
            (left as u64).cmp(&right)
        };
    }
    if let (Some(left), Some(right)) = (left.as_u64(), right.as_i64()) {
        return if right.is_negative() {
            Ordering::Greater
        } else {
            left.cmp(&(right as u64))
        };
    }

    match (left.as_f64(), right.as_f64()) {
        (Some(left), Some(right)) => left.total_cmp(&right),
        _ => left.to_string().cmp(&right.to_string()),
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
        aggregate_values, compare_numbers, deterministic_rows,
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
    fn metric_sort_orders_adjacent_large_integer_sums_exactly() {
        let rows = vec![
            json!({"team": "middle", "sum": 9_007_199_254_740_992_u64}),
            json!({"team": "highest", "sum": 9_007_199_254_740_993_u64}),
            json!({"team": "lowest", "sum": 9_007_199_254_740_991_u64}),
        ];

        assert_eq!(
            deterministic_rows(
                rows.clone(),
                "team",
                AggregateMetric::Sum,
                AggregateOptions {
                    sort_by: AggregateSortBy::Metric,
                    order: AggregateOrder::Asc,
                    limit: None,
                },
            ),
            vec![
                json!({"sum": 9_007_199_254_740_991_u64, "team": "lowest"}),
                json!({"sum": 9_007_199_254_740_992_u64, "team": "middle"}),
                json!({"sum": 9_007_199_254_740_993_u64, "team": "highest"}),
            ]
        );
        assert_eq!(
            deterministic_rows(
                rows.clone(),
                "team",
                AggregateMetric::Sum,
                AggregateOptions {
                    sort_by: AggregateSortBy::Metric,
                    order: AggregateOrder::Desc,
                    limit: None,
                },
            ),
            vec![
                json!({"sum": 9_007_199_254_740_993_u64, "team": "highest"}),
                json!({"sum": 9_007_199_254_740_992_u64, "team": "middle"}),
                json!({"sum": 9_007_199_254_740_991_u64, "team": "lowest"}),
            ]
        );
        assert_eq!(
            deterministic_rows(
                rows,
                "team",
                AggregateMetric::Sum,
                AggregateOptions {
                    sort_by: AggregateSortBy::Metric,
                    order: AggregateOrder::Desc,
                    limit: Some(1),
                },
            ),
            vec![json!({"sum": 9_007_199_254_740_993_u64, "team": "highest"})]
        );
    }

    #[test]
    fn numeric_comparison_handles_signed_unsigned_and_fractional_values() {
        let negative = serde_json::Number::from(-1);
        let largest_unsigned = serde_json::Number::from(u64::MAX);
        let fraction = serde_json::Number::from_f64(1.5).expect("finite number");
        let two = serde_json::Number::from(2);

        assert_eq!(
            compare_numbers(&negative, &largest_unsigned),
            std::cmp::Ordering::Less
        );
        assert_eq!(
            compare_numbers(&largest_unsigned, &negative),
            std::cmp::Ordering::Greater
        );
        assert_eq!(compare_numbers(&fraction, &two), std::cmp::Ordering::Less);
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
