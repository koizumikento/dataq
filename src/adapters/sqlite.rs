use std::path::Path;
use std::process::{Command, Stdio};

use serde_json::Value;
use thiserror::Error;

/// Errors produced while executing `sqlite3` in JSON mode.
#[derive(Debug, Error)]
pub enum SqliteError {
    #[error("`sqlite3` is not available in PATH")]
    Unavailable,
    #[error("failed to spawn sqlite3: {0}")]
    Spawn(std::io::Error),
    #[error("sqlite3 execution failed (code {code:?}): {stderr}")]
    Execution { code: Option<i32>, stderr: String },
    #[error("sqlite3 output is not valid JSON: {0}")]
    Parse(serde_json::Error),
    #[error("sqlite3 output must be a JSON array")]
    OutputShape,
}

/// Arguments for running one sqlite query in JSON mode.
#[derive(Debug, Clone)]
pub struct SqliteQueryArgs<'a> {
    pub database: &'a Path,
    pub sql: &'a str,
}

/// Execute `sqlite3` with JSON output and parse rows as a JSON array.
pub fn query_json(args: &SqliteQueryArgs<'_>) -> Result<Vec<Value>, SqliteError> {
    let sqlite_bin = resolve_sqlite_bin();
    query_json_with_bin(args, &sqlite_bin)
}

fn resolve_sqlite_bin() -> String {
    std::env::var("DATAQ_SQLITE_BIN").unwrap_or_else(|_| "sqlite3".to_string())
}

fn query_json_with_bin(args: &SqliteQueryArgs<'_>, bin: &str) -> Result<Vec<Value>, SqliteError> {
    let command_args = [
        "-batch".to_string(),
        "-json".to_string(),
        args.database.to_string_lossy().into_owned(),
        args.sql.to_string(),
    ];

    let output = match Command::new(bin)
        .args(&command_args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
    {
        Ok(output) => output,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Err(SqliteError::Unavailable);
        }
        Err(error) => return Err(SqliteError::Spawn(error)),
    };

    if !output.status.success() {
        let stderr = String::from_utf8(output.stderr)
            .unwrap_or_else(|_| "failed to decode sqlite3 stderr".to_string());
        let message = if stderr.trim().is_empty() {
            format!("exit status {}", output.status)
        } else {
            stderr.trim().to_string()
        };
        return Err(SqliteError::Execution {
            code: output.status.code(),
            stderr: message,
        });
    }

    if output.stdout.iter().all(|byte| byte.is_ascii_whitespace()) {
        return Ok(Vec::new());
    }

    let parsed: Value = serde_json::from_slice(&output.stdout).map_err(SqliteError::Parse)?;
    match parsed {
        Value::Array(rows) => Ok(rows),
        _ => Err(SqliteError::OutputShape),
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;

    use serde_json::json;

    use super::{SqliteError, SqliteQueryArgs, query_json_with_bin};

    #[test]
    fn maps_unavailable_binary_to_unavailable_error() {
        let dir = tempfile::tempdir().expect("tempdir");
        let db_path = dir.path().join("test.sqlite");
        let args = SqliteQueryArgs {
            database: &db_path,
            sql: "select 1 as id",
        };

        let err = query_json_with_bin(&args, "/definitely-missing/sqlite3")
            .expect_err("missing binary should fail");
        assert!(matches!(err, SqliteError::Unavailable));
    }

    #[test]
    fn maps_non_zero_exit_to_execution_error() {
        let dir = tempfile::tempdir().expect("tempdir");
        let bin = write_test_script(
            dir.path().join("fake-sqlite3"),
            "echo 'near \"from\": syntax error' 1>&2\nexit 2",
        );
        let db_path = dir.path().join("test.sqlite");
        let args = SqliteQueryArgs {
            database: &db_path,
            sql: "select from",
        };

        let err = query_json_with_bin(&args, bin.to_str().expect("utf8 path"))
            .expect_err("non-zero exit should fail");
        match err {
            SqliteError::Execution { code, stderr } => {
                assert_eq!(code, Some(2));
                assert!(stderr.contains("syntax error"));
            }
            other => panic!("expected execution error, got {other:?}"),
        }
    }

    #[test]
    fn parses_json_rows_output() {
        let dir = tempfile::tempdir().expect("tempdir");
        let bin = write_test_script(
            dir.path().join("fake-sqlite3"),
            "printf '[{\"id\":1,\"name\":\"alpha\"}]'",
        );
        let db_path = dir.path().join("test.sqlite");
        let args = SqliteQueryArgs {
            database: &db_path,
            sql: "select 1",
        };

        let rows =
            query_json_with_bin(&args, bin.to_str().expect("utf8 path")).expect("parse JSON rows");
        assert_eq!(rows, vec![json!({"id": 1, "name": "alpha"})]);
    }

    #[test]
    fn maps_empty_stdout_to_empty_rows() {
        let dir = tempfile::tempdir().expect("tempdir");
        let bin = write_test_script(dir.path().join("fake-sqlite3"), "cat >/dev/null");
        let db_path = dir.path().join("test.sqlite");
        let args = SqliteQueryArgs {
            database: &db_path,
            sql: "select 1",
        };

        let rows = query_json_with_bin(&args, bin.to_str().expect("utf8 path"))
            .expect("empty stdout should map to empty rows");
        assert!(rows.is_empty());
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
