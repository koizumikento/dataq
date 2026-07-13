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
    #[error(
        "mlr output row {index} field `{field}` has syntactically integral value `{value}` outside the supported JSON integer range (i64/u64)"
    )]
    OutputIntegerOutOfRange {
        index: usize,
        field: String,
        value: String,
    },
    #[error("failed to inspect mlr output number representation: {0}")]
    OutputRepresentation(String),
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

    let stdout = run_mlr_with_stdin_values_output(values, &args, bin)?;
    if matches!(metric, MlrAggregateMetric::Sum) {
        let source_field = format!("{}_{}", target, metric.source_field_suffix());
        reject_out_of_range_integral_field_tokens(&stdout, &source_field)?;
    }
    let rows = parse_mlr_rows(&stdout)?;
    normalize_aggregate_rows(rows, metric, target)
}

fn run_mlr_with_stdin_values(
    values: &[Value],
    args: &[String],
    bin: &str,
) -> Result<Vec<Value>, MlrError> {
    let stdout = run_mlr_with_stdin_values_output(values, args, bin)?;
    parse_mlr_rows(&stdout)
}

fn run_mlr_with_stdin_values_output(
    values: &[Value],
    args: &[String],
    bin: &str,
) -> Result<Vec<u8>, MlrError> {
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

    wait_and_collect_stdout(child)
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

fn wait_and_collect_stdout(child: Child) -> Result<Vec<u8>, MlrError> {
    let output = child.wait_with_output().map_err(MlrError::Spawn)?;
    if !output.status.success() {
        let stderr = String::from_utf8(output.stderr)
            .unwrap_or_else(|_| "failed to decode mlr stderr".to_string());
        return Err(MlrError::Execution(stderr.trim().to_string()));
    }

    Ok(output.stdout)
}

fn parse_mlr_rows(stdout: &[u8]) -> Result<Vec<Value>, MlrError> {
    let parsed: Value = serde_json::from_slice(stdout).map_err(MlrError::Parse)?;
    match parsed {
        Value::Array(rows) => Ok(rows),
        _ => Err(MlrError::OutputShape),
    }
}

fn reject_out_of_range_integral_field_tokens(stdout: &[u8], field: &str) -> Result<(), MlrError> {
    // `serde_json::Number` stores integer tokens beyond `u64` as `f64`. Inspect the
    // aggregate field in the original stdout before parsing can discard its lexeme.
    let tokens =
        top_level_object_field_tokens(stdout, field).map_err(MlrError::OutputRepresentation)?;
    for (index, token) in tokens {
        if is_json_integral_token(&token)
            && token.parse::<i64>().is_err()
            && token.parse::<u64>().is_err()
        {
            return Err(MlrError::OutputIntegerOutOfRange {
                index,
                field: field.to_string(),
                value: token,
            });
        }
    }
    Ok(())
}

fn top_level_object_field_tokens(
    stdout: &[u8],
    field: &str,
) -> Result<Vec<(usize, String)>, String> {
    let mut tokens = Vec::new();
    for (row_index, row) in json_container_items(stdout, b'[', b']')?
        .into_iter()
        .enumerate()
    {
        let row = trim_json_whitespace(row);
        if row.first() != Some(&b'{') {
            continue;
        }

        let mut found = None;
        for member in json_container_items(row, b'{', b'}')? {
            let (raw_key, raw_value) = object_member_parts(member)?;
            let key: String = serde_json::from_slice(trim_json_whitespace(raw_key))
                .map_err(|error| format!("failed to decode object key: {error}"))?;
            if key == field {
                found = Some(
                    std::str::from_utf8(trim_json_whitespace(raw_value))
                        .map_err(|error| format!("field token is not UTF-8: {error}"))?
                        .to_string(),
                );
            }
        }
        if let Some(token) = found {
            tokens.push((row_index, token));
        }
    }
    Ok(tokens)
}

fn json_container_items(input: &[u8], open: u8, close: u8) -> Result<Vec<&[u8]>, String> {
    let input = trim_json_whitespace(input);
    if input.first() != Some(&open) || input.last() != Some(&close) {
        return Err(format!(
            "expected JSON container delimited by `{}` and `{}`",
            char::from(open),
            char::from(close)
        ));
    }

    let content = &input[1..input.len() - 1];
    if trim_json_whitespace(content).is_empty() {
        return Ok(Vec::new());
    }

    let mut items = Vec::new();
    let mut start = 0_usize;
    let mut closing_stack = Vec::new();
    let mut in_string = false;
    let mut escaped = false;
    for (index, byte) in content.iter().copied().enumerate() {
        if in_string {
            if escaped {
                escaped = false;
            } else if byte == b'\\' {
                escaped = true;
            } else if byte == b'"' {
                in_string = false;
            }
            continue;
        }

        match byte {
            b'"' => in_string = true,
            b'{' => closing_stack.push(b'}'),
            b'[' => closing_stack.push(b']'),
            b'}' | b']' => {
                if closing_stack.pop() != Some(byte) {
                    return Err(format!("mismatched JSON delimiter at byte {index}"));
                }
            }
            b',' if closing_stack.is_empty() => {
                items.push(trim_json_whitespace(&content[start..index]));
                start = index + 1;
            }
            _ => {}
        }
    }

    if in_string || !closing_stack.is_empty() {
        return Err("unterminated JSON string or container".to_string());
    }
    items.push(trim_json_whitespace(&content[start..]));
    Ok(items)
}

fn object_member_parts(member: &[u8]) -> Result<(&[u8], &[u8]), String> {
    let mut in_string = false;
    let mut escaped = false;
    for (index, byte) in member.iter().copied().enumerate() {
        if in_string {
            if escaped {
                escaped = false;
            } else if byte == b'\\' {
                escaped = true;
            } else if byte == b'"' {
                in_string = false;
            }
        } else if byte == b'"' {
            in_string = true;
        } else if byte == b':' {
            return Ok((&member[..index], &member[index + 1..]));
        }
    }
    Err("JSON object member is missing `:`".to_string())
}

fn trim_json_whitespace(mut input: &[u8]) -> &[u8] {
    while matches!(input.first(), Some(b' ' | b'\n' | b'\r' | b'\t')) {
        input = &input[1..];
    }
    while matches!(input.last(), Some(b' ' | b'\n' | b'\r' | b'\t')) {
        input = &input[..input.len() - 1];
    }
    input
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
        MlrAggregateMetric::Sum => normalize_sum_value(index, field, value),
        MlrAggregateMetric::Avg => normalize_float_value(index, field, value),
    }
}

fn normalize_sum_value(index: usize, field: &str, value: Value) -> Result<Value, MlrError> {
    if let Some(number) = value.as_i64() {
        return Ok(Value::from(number));
    }
    if let Some(number) = value.as_u64() {
        return Ok(Value::from(number));
    }
    if let Some(text) = value.as_str() {
        if let Ok(parsed) = text.parse::<i64>() {
            return Ok(Value::from(parsed));
        }
        if let Ok(parsed) = text.parse::<u64>() {
            return Ok(Value::from(parsed));
        }
        if is_syntactically_integral(text) {
            return Err(MlrError::OutputIntegerOutOfRange {
                index,
                field: field.to_string(),
                value: text.to_string(),
            });
        }
    }

    normalize_float_value(index, field, value)
}

fn is_syntactically_integral(text: &str) -> bool {
    let digits = text
        .strip_prefix('-')
        .or_else(|| text.strip_prefix('+'))
        .unwrap_or(text)
        .as_bytes();
    !digits.is_empty() && digits.iter().all(u8::is_ascii_digit)
}

fn is_json_integral_token(text: &str) -> bool {
    let digits = text.strip_prefix('-').unwrap_or(text).as_bytes();
    !digits.is_empty()
        && digits.iter().all(u8::is_ascii_digit)
        && (digits.len() == 1 || digits.first() != Some(&b'0'))
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
        normalize_metric_value, run_sort_with_bin,
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
        match err {
            MlrError::Execution(_) => {}
            MlrError::Stdin(io_err) if io_err.kind() == std::io::ErrorKind::BrokenPipe => {}
            other => panic!("expected execution-like failure, got {other:?}"),
        }
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

    #[test]
    fn sum_preserves_integral_numbers_and_strings_beyond_f64_precision() {
        let exact_number = serde_json::json!(9_007_199_254_740_993_u64);
        assert_eq!(
            normalize_metric_value(0, "sum", MlrAggregateMetric::Sum, exact_number.clone())
                .expect("exact numeric sum"),
            exact_number
        );

        assert_eq!(
            normalize_metric_value(
                0,
                "sum",
                MlrAggregateMetric::Sum,
                serde_json::Value::String(u64::MAX.to_string()),
            )
            .expect("exact string sum"),
            serde_json::json!(u64::MAX)
        );
    }

    #[test]
    fn sum_rejects_integral_string_outside_i64_u64_range() {
        let err = normalize_metric_value(
            3,
            "sum",
            MlrAggregateMetric::Sum,
            serde_json::Value::String("18446744073709551617".to_string()),
        )
        .expect_err("out-of-range integral string must fail");

        assert!(matches!(
            err,
            MlrError::OutputIntegerOutOfRange {
                index: 3,
                ref field,
                ref value,
            } if field == "sum" && value == "18446744073709551617"
        ));
    }

    #[test]
    fn aggregate_rejects_raw_integral_token_outside_i64_u64_range() {
        let dir = tempfile::tempdir().expect("tempdir");
        let bin = write_test_script(
            dir.path().join("fake-mlr"),
            r#"cat >/dev/null
printf '[{"region":"first","price_sum":1},{"region":"overflow","price_sum":18446744073709551617}]'"#,
        );

        let err = aggregate_rows_with_bin(
            &[serde_json::json!({"region":"overflow","price":1})],
            "region",
            MlrAggregateMetric::Sum,
            "price",
            bin.to_str().expect("utf8 path"),
        )
        .expect_err("out-of-range raw integer must fail");

        assert!(matches!(
            err,
            MlrError::OutputIntegerOutOfRange {
                index: 1,
                ref field,
                ref value,
            } if field == "price_sum" && value == "18446744073709551617"
        ));
    }

    #[test]
    fn aggregate_keeps_supported_raw_integer_fractional_and_exponent_tokens() {
        let dir = tempfile::tempdir().expect("tempdir");
        let bin = write_test_script(
            dir.path().join("fake-mlr"),
            r#"cat >/dev/null
printf '[{"region":"maximum","price_sum":18446744073709551615},{"region":"fraction,quoted","metadata":{"items":[1,2]},"price_sum":18446744073709551617.0},{"region":"exponent","price_sum":1e3}]'"#,
        );

        let rows = aggregate_rows_with_bin(
            &[serde_json::json!({"region":"fraction,quoted","price":1.0})],
            "region",
            MlrAggregateMetric::Sum,
            "price",
            bin.to_str().expect("utf8 path"),
        )
        .expect("finite fractional and exponent sums");

        assert_eq!(rows[0]["sum"], serde_json::json!(u64::MAX));
        assert!(rows[1]["sum"].as_f64().is_some());
        assert_eq!(rows[2]["sum"], serde_json::json!(1000.0));
    }

    #[test]
    fn sum_and_avg_keep_fractional_results() {
        assert_eq!(
            normalize_metric_value(0, "sum", MlrAggregateMetric::Sum, serde_json::json!("12.5"),)
                .expect("fractional sum"),
            serde_json::json!(12.5)
        );
        assert_eq!(
            normalize_metric_value(0, "avg", MlrAggregateMetric::Avg, serde_json::json!("12.5"),)
                .expect("fractional average"),
            serde_json::json!(12.5)
        );
        assert_eq!(
            normalize_metric_value(0, "sum", MlrAggregateMetric::Sum, serde_json::json!("1e3"),)
                .expect("exponent sum"),
            serde_json::json!(1000.0)
        );
        assert!(
            normalize_metric_value(
                0,
                "sum",
                MlrAggregateMetric::Sum,
                serde_json::json!("18446744073709551617.0"),
            )
            .expect("finite fractional representation")
            .as_f64()
            .is_some()
        );
    }

    fn write_test_script(path: PathBuf, body: &str) -> PathBuf {
        let tmp_path = path.with_extension("tmp");
        let script = format!("#!/bin/sh\n{body}\n");
        fs::write(&tmp_path, script).expect("write script");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let permissions = fs::Permissions::from_mode(0o755);
            fs::set_permissions(&tmp_path, permissions).expect("chmod");
        }
        fs::rename(&tmp_path, &path).expect("rename script");
        path
    }
}
