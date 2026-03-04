use std::io::Write;
use std::process::{Command, Stdio};

use serde_json::Value;
use thiserror::Error;

/// Errors emitted by the DuckDB adapter.
#[derive(Debug, Error)]
pub enum DuckdbError {
    #[error("`duckdb` is not available in PATH")]
    Unavailable,
    #[error("failed to spawn duckdb: {0}")]
    Spawn(std::io::Error),
    #[error("failed to write duckdb stdin: {0}")]
    Stdin(std::io::Error),
    #[error("duckdb execution failed: {0}")]
    Output(String),
    #[error("duckdb output is not valid JSON: {0}")]
    Parse(serde_json::Error),
    #[error("failed to serialize duckdb input: {0}")]
    Serialize(serde_json::Error),
}

/// Executes a DuckDB SQL query and parses stdout as a JSON array.
///
/// `values` are written to DuckDB stdin as JSON bytes. The query itself is
/// passed as a distinct command argument (no shell interpolation).
pub fn run_query(values: &[Value], query: &str) -> Result<Vec<Value>, DuckdbError> {
    let duckdb_bin = std::env::var("DATAQ_DUCKDB_BIN").unwrap_or_else(|_| "duckdb".to_string());
    run_query_with_bin(values, query, &duckdb_bin)
}

fn run_query_with_bin(values: &[Value], query: &str, bin: &str) -> Result<Vec<Value>, DuckdbError> {
    let input = serde_json::to_vec(values).map_err(DuckdbError::Serialize)?;

    let mut child = match Command::new(bin)
        .arg("-json")
        .arg("-batch")
        .arg("-c")
        .arg(query)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
    {
        Ok(child) => child,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            return Err(DuckdbError::Unavailable);
        }
        Err(err) => return Err(DuckdbError::Spawn(err)),
    };

    if let Some(stdin) = child.stdin.as_mut() {
        if let Err(err) = stdin.write_all(&input) {
            if err.kind() != std::io::ErrorKind::BrokenPipe {
                return Err(DuckdbError::Stdin(err));
            }
        }
    } else {
        return Err(DuckdbError::Output(
            "duckdb stdin was not piped as expected".to_string(),
        ));
    }

    let output = child.wait_with_output().map_err(DuckdbError::Spawn)?;
    if !output.status.success() {
        let stderr = String::from_utf8(output.stderr)
            .unwrap_or_else(|_| "failed to decode duckdb stderr".to_string());
        return Err(DuckdbError::Output(stderr.trim().to_string()));
    }

    let parsed: Value = serde_json::from_slice(&output.stdout).map_err(DuckdbError::Parse)?;
    match parsed {
        Value::Array(rows) => Ok(rows),
        _ => Err(DuckdbError::Output(
            "duckdb output must be a JSON array".to_string(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;

    use super::{DuckdbError, run_query_with_bin};

    #[test]
    fn maps_unavailable_binary_to_unavailable_error() {
        let err = run_query_with_bin(&[], "select 1", "/definitely-missing/duckdb")
            .expect_err("missing binary should fail");
        assert!(matches!(err, DuckdbError::Unavailable));
    }

    #[test]
    fn maps_non_zero_exit_to_output_error() {
        let dir = tempfile::tempdir().expect("tempdir");
        let bin = write_test_script(
            dir.path().join("fake-duckdb"),
            "cat >/dev/null\necho 'duckdb failed in test' 1>&2\nexit 7",
        );

        let err = run_query_with_bin(&[], "select 1", bin.to_str().expect("utf8 path"))
            .expect_err("non-zero should fail");
        assert!(matches!(err, DuckdbError::Output(_)));
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
