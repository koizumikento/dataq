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

/// Domain errors for transform SQL execution through a DuckDB adapter hook.
#[derive(Debug, Error)]
pub enum TransformSqlError {
    #[error("`--query` cannot be empty")]
    InvalidSql,
    #[error("failed to transform rowset with duckdb: {0}")]
    Duckdb(String),
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
    let rows = normalize_output_rows(mlr_rows);
    let mlr_output = rows.len();

    Ok(TransformRowsetExecution {
        rows,
        jq_input_records: jq_input,
        jq_output_records: jq_output,
        mlr_output_records: mlr_output,
    })
}

/// Executes SQL rowset transform through an injected DuckDB adapter hook.
pub fn execute_sql_with_duckdb_hook<F, E>(
    values: &[Value],
    sql: &str,
    execute_duckdb: F,
) -> Result<Vec<Value>, TransformSqlError>
where
    F: FnOnce(&[Value], &str) -> Result<Vec<Value>, E>,
    E: std::fmt::Display,
{
    if sql.trim().is_empty() {
        return Err(TransformSqlError::InvalidSql);
    }

    let rows = execute_duckdb(values, sql)
        .map_err(|source| TransformSqlError::Duckdb(source.to_string()))?;
    Ok(normalize_sql_output_rows(rows))
}

fn normalize_output_rows(rows: Vec<Value>) -> Vec<Value> {
    let rows = rows
        .into_iter()
        .map(canonicalize_float_values)
        .collect::<Vec<Value>>();
    deterministic_rows(rows)
}

fn normalize_sql_output_rows(rows: Vec<Value>) -> Vec<Value> {
    rows.into_iter()
        .map(canonicalize_float_values)
        .map(|row| sort_value_keys(&row))
        .collect()
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

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::execute_sql_with_duckdb_hook;

    #[test]
    fn sql_output_preserves_duckdb_row_order_while_canonicalizing_rows() {
        let rows = execute_sql_with_duckdb_hook(&[], "select * from input", |_rows, _sql| {
            Ok::<_, String>(vec![
                json!({"z": 1.50, "a": {"y": 2.0, "b": 1}}),
                json!({"z": 1.0, "a": {"y": 4.50, "b": 3}}),
            ])
        })
        .expect("transform sql rows");

        assert_eq!(
            serde_json::to_string(&rows).expect("serialize rows"),
            r#"[{"a":{"b":1,"y":2.0},"z":1.5},{"a":{"b":3,"y":4.5},"z":1.0}]"#
        );
    }
}
