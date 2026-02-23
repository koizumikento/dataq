use std::cmp::Ordering;
use std::io::Write;
use std::process::{Command, Stdio};

use serde_json::Value;
use thiserror::Error;

use crate::util::sort::sort_value_keys;

#[derive(Debug, Error)]
pub enum MlrError {
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
    #[error("failed to serialize mlr input: {0}")]
    Serialize(serde_json::Error),
}

pub fn sort_github_actions_jobs(values: &[Value]) -> Result<Vec<Value>, MlrError> {
    run_sort(values, "job_id")
}

pub fn sort_gitlab_ci_jobs(values: &[Value]) -> Result<Vec<Value>, MlrError> {
    run_sort(values, "job_name")
}

fn run_sort(values: &[Value], key_field: &str) -> Result<Vec<Value>, MlrError> {
    let mlr_bin = std::env::var("DATAQ_MLR_BIN").unwrap_or_else(|_| "mlr".to_string());
    run_sort_with_bin(values, key_field, &mlr_bin)
}

fn run_sort_with_bin(values: &[Value], key_field: &str, bin: &str) -> Result<Vec<Value>, MlrError> {
    let input = serde_json::to_vec(values).map_err(MlrError::Serialize)?;
    let mut child = match Command::new(bin)
        .arg("--ijson")
        .arg("--ojson")
        .arg("sort")
        .arg("-f")
        .arg(key_field)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
    {
        Ok(child) => child,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            return Err(MlrError::Unavailable);
        }
        Err(err) => return Err(MlrError::Spawn(err)),
    };

    if let Some(stdin) = child.stdin.as_mut() {
        stdin.write_all(&input).map_err(MlrError::Stdin)?;
    } else {
        return Err(MlrError::Execution(
            "mlr stdin was not piped as expected".to_string(),
        ));
    }

    let output = child.wait_with_output().map_err(MlrError::Spawn)?;
    if !output.status.success() {
        let stderr = String::from_utf8(output.stderr)
            .unwrap_or_else(|_| "failed to decode mlr stderr".to_string());
        return Err(MlrError::Execution(stderr.trim().to_string()));
    }

    let parsed: Value = serde_json::from_slice(&output.stdout).map_err(MlrError::Parse)?;
    let rows = match parsed {
        Value::Array(rows) => rows,
        _ => return Err(MlrError::OutputShape),
    };
    Ok(deterministic_sort_rows(rows, key_field))
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

    use super::{MlrError, run_sort_with_bin};

    #[test]
    fn maps_unavailable_binary_to_unavailable_error() {
        let err = run_sort_with_bin(&[], "job_id", "/definitely-missing/mlr")
            .expect_err("missing binary should fail");
        assert!(matches!(err, MlrError::Unavailable));
    }

    #[test]
    fn maps_invalid_json_output_to_parse_error() {
        let dir = tempfile::tempdir().expect("tempdir");
        let bin = write_test_script(dir.path().join("fake-mlr"), "printf 'not-json'");

        let err = run_sort_with_bin(&[], "job_id", bin.to_str().expect("utf8 path"))
            .expect_err("invalid JSON should fail");
        assert!(matches!(err, MlrError::Parse(_)));
    }

    #[test]
    fn maps_non_zero_exit_to_execution_error() {
        let dir = tempfile::tempdir().expect("tempdir");
        let bin = write_test_script(
            dir.path().join("fake-mlr"),
            "echo 'mlr failed in test' 1>&2\nexit 7",
        );

        let err = run_sort_with_bin(&[], "job_id", bin.to_str().expect("utf8 path"))
            .expect_err("non-zero should fail");
        assert!(matches!(err, MlrError::Execution(_)));
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
