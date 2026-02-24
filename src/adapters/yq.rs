use std::io::Write;
use std::process::{Command, Stdio};

use serde_json::Value;
use thiserror::Error;

const GITHUB_ACTIONS_JOBS_EXTRACT_FILTER: &str =
    r#"[.[] | .jobs | to_entries | sort_by(.key) | .[] | {"job_id": .key, "job": .value}]"#;

const GITLAB_CI_JOBS_EXTRACT_FILTER: &str =
    r#"[.[] | to_entries | sort_by(.key) | .[] | {"job_name": .key, "job": .value}]"#;
const GENERIC_MAP_JOBS_EXTRACT_FILTER: &str =
    r#"[.[] | to_entries | sort_by(.key) | .[] | {"job_name": .key, "job": .value}]"#;

#[derive(Debug, Error)]
pub enum YqError {
    #[error("`yq` is not available in PATH")]
    Unavailable,
    #[error("failed to spawn yq: {0}")]
    Spawn(std::io::Error),
    #[error("failed to write yq stdin: {0}")]
    Stdin(std::io::Error),
    #[error("yq execution failed: {0}")]
    Execution(String),
    #[error("yq output is not valid JSON: {0}")]
    Parse(serde_json::Error),
    #[error("yq output must be a JSON array")]
    OutputShape,
    #[error("failed to serialize yq input: {0}")]
    Serialize(serde_json::Error),
}

pub fn extract_github_actions_jobs(values: &[Value]) -> Result<Vec<Value>, YqError> {
    run_filter(values, GITHUB_ACTIONS_JOBS_EXTRACT_FILTER)
}

pub fn extract_gitlab_ci_jobs(values: &[Value]) -> Result<Vec<Value>, YqError> {
    run_filter(values, GITLAB_CI_JOBS_EXTRACT_FILTER)
}

pub fn extract_generic_map_jobs(values: &[Value]) -> Result<Vec<Value>, YqError> {
    run_filter(values, GENERIC_MAP_JOBS_EXTRACT_FILTER)
}

fn run_filter(values: &[Value], filter: &str) -> Result<Vec<Value>, YqError> {
    let yq_bin = std::env::var("DATAQ_YQ_BIN").unwrap_or_else(|_| "yq".to_string());
    run_filter_with_bin(values, filter, &yq_bin)
}

fn run_filter_with_bin(values: &[Value], filter: &str, bin: &str) -> Result<Vec<Value>, YqError> {
    let input = serde_json::to_vec(values).map_err(YqError::Serialize)?;
    let mut child = match Command::new(bin)
        .arg("eval")
        .arg("-o=json")
        .arg("-I=0")
        .arg(filter)
        .arg("-")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
    {
        Ok(child) => child,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Err(YqError::Unavailable),
        Err(err) => return Err(YqError::Spawn(err)),
    };

    if let Some(stdin) = child.stdin.as_mut() {
        stdin.write_all(&input).map_err(YqError::Stdin)?;
    } else {
        return Err(YqError::Execution(
            "yq stdin was not piped as expected".to_string(),
        ));
    }

    let output = child.wait_with_output().map_err(YqError::Spawn)?;
    if !output.status.success() {
        let stderr = String::from_utf8(output.stderr)
            .unwrap_or_else(|_| "failed to decode yq stderr".to_string());
        return Err(YqError::Execution(stderr.trim().to_string()));
    }

    let parsed: Value = serde_json::from_slice(&output.stdout).map_err(YqError::Parse)?;
    match parsed {
        Value::Array(items) => Ok(items),
        _ => Err(YqError::OutputShape),
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;

    use super::{YqError, run_filter_with_bin};

    #[test]
    fn maps_unavailable_binary_to_unavailable_error() {
        let err = run_filter_with_bin(&[], "[]", "/definitely-missing/yq")
            .expect_err("missing binary should fail");
        assert!(matches!(err, YqError::Unavailable));
    }

    #[test]
    fn maps_invalid_json_output_to_parse_error() {
        let dir = tempfile::tempdir().expect("tempdir");
        let bin = write_test_script(dir.path().join("fake-yq"), "printf 'not-json'");

        let err = run_filter_with_bin(&[], "[]", bin.to_str().expect("utf8 path"))
            .expect_err("invalid JSON should fail");
        assert!(matches!(err, YqError::Parse(_)));
    }

    #[test]
    fn maps_non_zero_exit_to_execution_error() {
        let dir = tempfile::tempdir().expect("tempdir");
        let bin = write_test_script(
            dir.path().join("fake-yq"),
            "echo 'yq failed in test' 1>&2\nexit 4",
        );

        let err = run_filter_with_bin(&[], "[]", bin.to_str().expect("utf8 path"))
            .expect_err("non-zero should fail");
        assert!(matches!(err, YqError::Execution(_)));
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
