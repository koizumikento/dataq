use std::cmp::Ordering;
use std::io::Write;
use std::process::{Child, Command, Stdio};

use serde_json::Value;
use tempfile::NamedTempFile;
use thiserror::Error;

use crate::util::sort::sort_value_keys;

#[derive(Debug, Error)]
pub enum MlrError {
    #[error("invalid mlr arguments: {0}")]
    InvalidArguments(String),
    #[error("`mlr` is not available in PATH")]
    Unavailable,
    #[error("failed to spawn mlr: {0}")]
    Spawn(std::io::Error),
    #[error("failed to write mlr stdin: {0}")]
    Stdin(std::io::Error),
    #[error("mlr execution failed: {0}")]
    Execution(String),
    #[error("mlr output is not valid JSON: {0}")]
    Parse(serde_json::Error),
    #[error("mlr output must be a JSON array")]
    OutputShape,
    #[error("mlr output row {index} must be an object")]
    OutputRowShape { index: usize },
    #[error("mlr output row {index} is missing field `{field}`")]
    OutputFieldMissing { index: usize, field: String },
    #[error("mlr output row {index} has non-numeric field `{field}`")]
    OutputFieldNotNumeric { index: usize, field: String },
    #[error("failed to serialize mlr input: {0}")]
    Serialize(serde_json::Error),
    #[error("failed to create temporary mlr input file: {0}")]
    TempFile(std::io::Error),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MlrJoinHow {
    Inner,
    Left,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MlrAggregateMetric {
    Count,
    Sum,
    Avg,
}

impl MlrAggregateMetric {
    fn action(self) -> &'static str {
        match self {
            Self::Count => "count",
            Self::Sum => "sum",
            Self::Avg => "mean",
        }
    }

    fn source_field_suffix(self) -> &'static str {
        match self {
            Self::Count => "count",
            Self::Sum => "sum",
            Self::Avg => "mean",
        }
    }

    fn output_field(self) -> &'static str {
        match self {
            Self::Count => "count",
            Self::Sum => "sum",
            Self::Avg => "avg",
        }
    }
}

pub fn sort_github_actions_jobs(values: &[Value]) -> Result<Vec<Value>, MlrError> {
    run_sort(values, "job_id")
}

pub fn sort_gitlab_ci_jobs(values: &[Value]) -> Result<Vec<Value>, MlrError> {
    run_sort(values, "job_name")
}

pub fn sort_generic_map_jobs(values: &[Value]) -> Result<Vec<Value>, MlrError> {
    run_sort(values, "job_name")
}

pub fn join_rows(
    left: &[Value],
    right: &[Value],
    on: &str,
    how: MlrJoinHow,
) -> Result<Vec<Value>, MlrError> {
    let mlr_bin = resolve_mlr_bin();
    join_rows_with_bin(left, right, on, how, &mlr_bin)
}

pub fn aggregate_rows(
    values: &[Value],
    group_by: &str,
    metric: MlrAggregateMetric,
    target: &str,
) -> Result<Vec<Value>, MlrError> {
    let mlr_bin = resolve_mlr_bin();
    aggregate_rows_with_bin(values, group_by, metric, target, &mlr_bin)
}

pub fn run_verbs(values: &[Value], verbs: &[String]) -> Result<Vec<Value>, MlrError> {
    if verbs.is_empty() {
        return Err(MlrError::InvalidArguments(
            "`--mlr` requires at least one argument".to_string(),
        ));
    }
    if verbs.iter().any(|arg| arg.trim().is_empty()) {
        return Err(MlrError::InvalidArguments(
            "`--mlr` arguments cannot be empty".to_string(),
        ));
    }

    let mut args = vec!["--ijson".to_string(), "--ojson".to_string()];
    args.extend(verbs.iter().cloned());
    let mlr_bin = resolve_mlr_bin();
    run_mlr_with_stdin_values(values, &args, &mlr_bin)
}

fn resolve_mlr_bin() -> String {
    std::env::var("DATAQ_MLR_BIN").unwrap_or_else(|_| "mlr".to_string())
}

fn run_sort(values: &[Value], key_field: &str) -> Result<Vec<Value>, MlrError> {
    let mlr_bin = resolve_mlr_bin();
    run_sort_with_bin(values, key_field, &mlr_bin)
}

fn run_sort_with_bin(values: &[Value], key_field: &str, bin: &str) -> Result<Vec<Value>, MlrError> {
    let args = vec![
        "--ijson".to_string(),
        "--ojson".to_string(),
        "sort".to_string(),
        "-f".to_string(),
        key_field.to_string(),
    ];
    let rows = run_mlr_with_stdin_values(values, &args, bin)?;
    Ok(deterministic_sort_rows(rows, key_field))
}

fn join_rows_with_bin(
    left: &[Value],
    right: &[Value],
    on: &str,
    how: MlrJoinHow,
    bin: &str,
) -> Result<Vec<Value>, MlrError> {
    let left_file = write_temp_values_file(left)?;

    let mut args = vec![
        "--ijson".to_string(),
        "--ojson".to_string(),
        "join".to_string(),
        "-j".to_string(),
        on.to_string(),
        "-f".to_string(),
        left_file.path().to_string_lossy().into_owned(),
    ];
    if matches!(how, MlrJoinHow::Left) {
        args.push("--ul".to_string());
    }

    run_mlr_with_stdin_values(right, &args, bin)
}

fn aggregate_rows_with_bin(
    values: &[Value],
    group_by: &str,
    metric: MlrAggregateMetric,
    target: &str,
    bin: &str,
) -> Result<Vec<Value>, MlrError> {
    let args = vec![
        "--ijson".to_string(),
        "--ojson".to_string(),
        "stats1".to_string(),
        "-a".to_string(),
        metric.action().to_string(),
        "-f".to_string(),
        target.to_string(),
        "-g".to_string(),
        group_by.to_string(),
    ];

    let rows = run_mlr_with_stdin_values(values, &args, bin)?;
    normalize_aggregate_rows(rows, metric, target)
}

fn run_mlr_with_stdin_values(
    values: &[Value],
    args: &[String],
    bin: &str,
) -> Result<Vec<Value>, MlrError> {
    let input = serde_json::to_vec(values).map_err(MlrError::Serialize)?;
    let mut child = spawn_mlr(bin, args)?;

    if let Some(stdin) = child.stdin.as_mut() {
        if let Err(err) = stdin.write_all(&input) {
            if err.kind() != std::io::ErrorKind::BrokenPipe {
                return Err(MlrError::Stdin(err));
            }
        }
    } else {
        return Err(MlrError::Execution(
            "mlr stdin was not piped as expected".to_string(),
        ));
    }

    wait_and_collect_rows(child)
}

fn spawn_mlr(bin: &str, args: &[String]) -> Result<Child, MlrError> {
    match Command::new(bin)
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
    {
        Ok(child) => Ok(child),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Err(MlrError::Unavailable),
        Err(err) => Err(MlrError::Spawn(err)),
    }
}

fn wait_and_collect_rows(child: Child) -> Result<Vec<Value>, MlrError> {
    let output = child.wait_with_output().map_err(MlrError::Spawn)?;
    if !output.status.success() {
        let stderr = String::from_utf8(output.stderr)
            .unwrap_or_else(|_| "failed to decode mlr stderr".to_string());
        return Err(MlrError::Execution(stderr.trim().to_string()));
    }

    let parsed: Value = serde_json::from_slice(&output.stdout).map_err(MlrError::Parse)?;
    match parsed {
        Value::Array(rows) => Ok(rows),
        _ => Err(MlrError::OutputShape),
    }
}

fn write_temp_values_file(values: &[Value]) -> Result<NamedTempFile, MlrError> {
    let mut file = NamedTempFile::new().map_err(MlrError::TempFile)?;
    serde_json::to_writer(file.as_file_mut(), values).map_err(MlrError::Serialize)?;
    file.as_file_mut().flush().map_err(MlrError::TempFile)?;
    Ok(file)
}

fn normalize_aggregate_rows(
    rows: Vec<Value>,
    metric: MlrAggregateMetric,
    target: &str,
) -> Result<Vec<Value>, MlrError> {
    let source_field = format!("{}_{}", target, metric.source_field_suffix());
    let output_field = metric.output_field().to_string();

    let mut out = Vec::with_capacity(rows.len());
    for (index, row) in rows.into_iter().enumerate() {
        let Some(mut map) = row.as_object().cloned() else {
            return Err(MlrError::OutputRowShape { index });
        };
        let metric_value =
            map.remove(&source_field)
                .ok_or_else(|| MlrError::OutputFieldMissing {
                    index,
                    field: source_field.clone(),
                })?;

        let normalized_value = normalize_metric_value(index, &output_field, metric, metric_value)?;
        map.insert(output_field.clone(), normalized_value);
        out.push(Value::Object(map));
    }

    Ok(out)
}

fn normalize_metric_value(
    index: usize,
    field: &str,
    metric: MlrAggregateMetric,
    value: Value,
) -> Result<Value, MlrError> {
    match metric {
        MlrAggregateMetric::Count => normalize_integer_value(index, field, value),
        MlrAggregateMetric::Sum | MlrAggregateMetric::Avg => {
            normalize_float_value(index, field, value)
        }
    }
}

fn normalize_integer_value(index: usize, field: &str, value: Value) -> Result<Value, MlrError> {
    if let Some(number) = value.as_i64() {
        return Ok(Value::from(number));
    }
    if let Some(number) = value.as_u64() {
        return Ok(Value::from(number));
    }
    if let Some(number) = value.as_f64() {
        let rounded = number.round();
        if (number - rounded).abs() < f64::EPSILON {
            return Ok(Value::from(rounded as i64));
        }
    }
    if let Some(text) = value.as_str() {
        if let Ok(parsed) = text.parse::<i64>() {
            return Ok(Value::from(parsed));
        }
    }

    Err(MlrError::OutputFieldNotNumeric {
        index,
        field: field.to_string(),
    })
}

fn normalize_float_value(index: usize, field: &str, value: Value) -> Result<Value, MlrError> {
    if let Some(number) = value.as_f64() {
        return serde_json::Number::from_f64(number)
            .map(Value::Number)
            .ok_or_else(|| MlrError::OutputFieldNotNumeric {
                index,
                field: field.to_string(),
            });
    }
    if let Some(text) = value.as_str() {
        if let Ok(parsed) = text.parse::<f64>() {
            return serde_json::Number::from_f64(parsed)
                .map(Value::Number)
                .ok_or_else(|| MlrError::OutputFieldNotNumeric {
                    index,
                    field: field.to_string(),
                });
        }
    }

    Err(MlrError::OutputFieldNotNumeric {
        index,
        field: field.to_string(),
    })
}

fn deterministic_sort_rows(mut rows: Vec<Value>, key_field: &str) -> Vec<Value> {
    rows.sort_by(|left, right| compare_rows(left, right, key_field));
    rows
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
            .map(|v| {
                serde_json::to_string(&sort_value_keys(v)).unwrap_or_else(|_| "null".to_string())
            })
            .unwrap_or_else(|| "null".to_string()),
        _ => "null".to_string(),
    }
}

fn canonical_row_literal(value: &Value) -> String {
    serde_json::to_string(&sort_value_keys(value)).unwrap_or_else(|_| "null".to_string())
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;

    use super::{
        MlrAggregateMetric, MlrError, MlrJoinHow, aggregate_rows_with_bin, join_rows_with_bin,
        run_sort_with_bin,
    };

    #[test]
    fn maps_unavailable_binary_to_unavailable_error() {
        let err = run_sort_with_bin(&[], "job_id", "/definitely-missing/mlr")
            .expect_err("missing binary should fail");
        assert!(matches!(err, MlrError::Unavailable));
    }

    #[test]
    fn maps_invalid_json_output_to_parse_error() {
        let dir = tempfile::tempdir().expect("tempdir");
        let bin = write_test_script(
            dir.path().join("fake-mlr"),
            "cat >/dev/null\nprintf 'not-json'",
        );

        let err = run_sort_with_bin(&[], "job_id", bin.to_str().expect("utf8 path"))
            .expect_err("invalid JSON should fail");
        assert!(matches!(err, MlrError::Parse(_)));
    }

    #[test]
    fn maps_non_zero_exit_to_execution_error() {
        let dir = tempfile::tempdir().expect("tempdir");
        let bin = write_test_script(
            dir.path().join("fake-mlr"),
            "cat >/dev/null\necho 'mlr failed in test' 1>&2\nexit 7",
        );

        let err = run_sort_with_bin(&[], "job_id", bin.to_str().expect("utf8 path"))
            .expect_err("non-zero should fail");
        assert!(matches!(err, MlrError::Execution(_)));
    }

    #[test]
    fn join_uses_explicit_argument_list() {
        let dir = tempfile::tempdir().expect("tempdir");
        let bin = write_test_script(
            dir.path().join("fake-mlr"),
            r#"
for arg in "$@"; do
  if [ "$arg" = "join" ]; then found_join=1; fi
  if [ "$arg" = "-j" ]; then found_j=1; fi
  if [ "$arg" = "-f" ]; then found_f=1; fi
done
if [ -z "$found_join" ] || [ -z "$found_j" ] || [ -z "$found_f" ]; then
  echo 'missing join args' 1>&2
  exit 9
fi
cat >/dev/null
printf '[{"id":1}]'
"#,
        );

        let rows = join_rows_with_bin(
            &[serde_json::json!({"id":1})],
            &[serde_json::json!({"id":1})],
            "id",
            MlrJoinHow::Inner,
            bin.to_str().expect("utf8 path"),
        )
        .expect("join should succeed");
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn left_join_wires_user_left_rows_to_mlr_left_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let bin = write_test_script(
            dir.path().join("fake-mlr"),
            r#"
left_file=""
capture_next=0
for arg in "$@"; do
  if [ "$capture_next" = "1" ]; then
    left_file="$arg"
    capture_next=0
    continue
  fi
  if [ "$arg" = "-f" ]; then
    capture_next=1
    continue
  fi
  if [ "$arg" = "--ul" ]; then
    saw_left=1
  fi
done

if [ -z "$left_file" ]; then
  echo 'missing -f value' 1>&2
  exit 9
fi
if ! grep -q '"left_marker":"L"' "$left_file"; then
  echo 'left file was not wired from user-left input' 1>&2
  exit 9
fi

stdin_payload="$(cat)"
if ! printf '%s' "$stdin_payload" | grep -q '"right_marker":"R"'; then
  echo 'stdin was not wired from user-right input' 1>&2
  exit 9
fi

if [ -n "$saw_left" ]; then
  printf '[{"id":1,"left_marker":"L","right_marker":"R"},{"id":2,"left_marker":"L","right_marker":null}]'
  exit 0
fi

printf '[{"id":1,"left_marker":"L","right_marker":"R"}]'
"#,
        );

        let rows = join_rows_with_bin(
            &[
                serde_json::json!({"id":1,"left_marker":"L"}),
                serde_json::json!({"id":2,"left_marker":"L"}),
            ],
            &[serde_json::json!({"id":1,"right_marker":"R"})],
            "id",
            MlrJoinHow::Left,
            bin.to_str().expect("utf8 path"),
        )
        .expect("left join should succeed");

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[1]["right_marker"], serde_json::Value::Null);
    }

    #[test]
    fn aggregate_normalizes_metric_field_names() {
        let dir = tempfile::tempdir().expect("tempdir");
        let bin = write_test_script(
            dir.path().join("fake-mlr"),
            r#"cat >/dev/null
printf '[{"region":"apac","price_mean":"12.5"}]'"#,
        );

        let rows = aggregate_rows_with_bin(
            &[serde_json::json!({"region":"apac","price":12.5})],
            "region",
            MlrAggregateMetric::Avg,
            "price",
            bin.to_str().expect("utf8 path"),
        )
        .expect("aggregate should succeed");
        assert_eq!(rows[0]["avg"], serde_json::json!(12.5));
    }

    fn write_test_script(path: PathBuf, body: &str) -> PathBuf {
        let script = format!("#!/bin/sh\n{body}\n");
        fs::write(&path, script).expect("write script");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let permissions = fs::Permissions::from_mode(0o755);
            fs::set_permissions(&path, permissions).expect("chmod");
        }
        path
    }
}
