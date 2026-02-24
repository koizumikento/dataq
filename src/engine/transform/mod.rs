use std::cmp::Ordering;

use serde_json::{Number, Value};
use thiserror::Error;

use crate::adapters::{jq, mlr};
use crate::util::sort::sort_value_keys;

/// Execution result for the fixed `jq -> mlr` rowset transform.
#[derive(Debug, Clone, PartialEq)]
pub struct TransformRowsetExecution {
    pub rows: Vec<Value>,
    pub jq_input_records: usize,
    pub jq_output_records: usize,
    pub mlr_output_records: usize,
}

/// Domain errors for transform rowset execution.
#[derive(Debug, Error)]
pub enum TransformRowsetError {
    #[error("`--jq-filter` cannot be empty")]
    InvalidJqFilter,
    #[error("`--mlr` requires at least one argument")]
    InvalidMlrArgs,
    #[error("jq stage failed: {source}")]
    Jq {
        input_records: usize,
        #[source]
        source: jq::JqError,
    },
    #[error("mlr stage failed: {source}")]
    Mlr {
        jq_input_records: usize,
        jq_output_records: usize,
        #[source]
        source: mlr::MlrError,
    },
}

/// Executes fixed stage order `jq -> mlr` with deterministic ordering.
pub fn execute_rowset(
    values: &[Value],
    jq_filter: &str,
    mlr_args: &[String],
) -> Result<TransformRowsetExecution, TransformRowsetError> {
    if jq_filter.trim().is_empty() {
        return Err(TransformRowsetError::InvalidJqFilter);
    }
    if mlr_args.is_empty() {
        return Err(TransformRowsetError::InvalidMlrArgs);
    }

    let jq_input = values.len();
    let jq_rows =
        jq::run_custom_filter(values, jq_filter).map_err(|source| TransformRowsetError::Jq {
            input_records: jq_input,
            source,
        })?;
    let jq_rows = deterministic_rows(jq_rows);
    let jq_output = jq_rows.len();

    let mlr_rows =
        mlr::run_verbs(&jq_rows, mlr_args).map_err(|source| TransformRowsetError::Mlr {
            jq_input_records: jq_input,
            jq_output_records: jq_output,
            source,
        })?;
    let mlr_rows: Vec<Value> = mlr_rows
        .into_iter()
        .map(canonicalize_float_values)
        .collect();
    let rows = deterministic_rows(mlr_rows);
    let mlr_output = rows.len();

    Ok(TransformRowsetExecution {
        rows,
        jq_input_records: jq_input,
        jq_output_records: jq_output,
        mlr_output_records: mlr_output,
    })
}

fn deterministic_rows(mut rows: Vec<Value>) -> Vec<Value> {
    rows.sort_by(compare_rows);
    rows.into_iter()
        .map(|row| sort_value_keys(&row))
        .collect::<Vec<Value>>()
}

fn compare_rows(left: &Value, right: &Value) -> Ordering {
    canonical_row_literal(left).cmp(&canonical_row_literal(right))
}

fn canonical_row_literal(value: &Value) -> String {
    serde_json::to_string(&sort_value_keys(value)).unwrap_or_else(|_| "null".to_string())
}

fn canonicalize_float_values(value: Value) -> Value {
    match value {
        Value::Array(items) => Value::Array(
            items
                .into_iter()
                .map(canonicalize_float_values)
                .collect::<Vec<Value>>(),
        ),
        Value::Object(map) => Value::Object(
            map.into_iter()
                .map(|(key, value)| (key, canonicalize_float_values(value)))
                .collect(),
        ),
        Value::String(text) => parse_float_literal(text.as_str())
            .map(Value::Number)
            .unwrap_or(Value::String(text)),
        Value::Number(number) => {
            let original = number.clone();
            Value::Number(canonicalize_float_number(number).unwrap_or(original))
        }
        other => other,
    }
}

fn canonicalize_float_number(number: Number) -> Option<Number> {
    if number.is_i64() || number.is_u64() {
        return Some(number);
    }
    number.as_f64().and_then(Number::from_f64)
}

fn parse_float_literal(text: &str) -> Option<Number> {
    if !text.contains('.') && !text.contains('e') && !text.contains('E') {
        return None;
    }
    text.parse::<f64>().ok().and_then(Number::from_f64)
}
