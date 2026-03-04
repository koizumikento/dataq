use std::io::Write;
use std::path::Path;
use std::process::{Command, Stdio};

use serde_json::Value;
use tempfile::NamedTempFile;
use thiserror::Error;

/// Errors emitted by the DuckDB adapter.
#[derive(Debug, Error)]
pub enum DuckdbError {
    #[error("`duckdb` is not available in PATH")]
    Unavailable,
    #[error("failed to spawn duckdb: {0}")]
    Spawn(std::io::Error),
    #[error("failed to create or write temporary duckdb input: {0}")]
    TempFile(std::io::Error),
    #[error("duckdb execution failed: {0}")]
    Output(String),
    #[error("duckdb output is not valid JSON: {0}")]
    Parse(serde_json::Error),
    #[error("failed to serialize duckdb input: {0}")]
    Serialize(serde_json::Error),
}

/// Executes a DuckDB SQL query and parses stdout as a JSON array.
///
/// Input rows are materialized into a temporary `input` relation before
/// executing the user query, and the full script is passed as one explicit
/// command argument (no shell interpolation).
pub fn run_query(values: &[Value], query: &str) -> Result<Vec<Value>, DuckdbError> {
    let duckdb_bin = std::env::var("DATAQ_DUCKDB_BIN").unwrap_or_else(|_| "duckdb".to_string());
    run_query_with_bin(values, query, &duckdb_bin)
}

fn run_query_with_bin(values: &[Value], query: &str, bin: &str) -> Result<Vec<Value>, DuckdbError> {
    let mut input_file = NamedTempFile::new().map_err(DuckdbError::TempFile)?;
    serde_json::to_writer(input_file.as_file_mut(), values).map_err(DuckdbError::Serialize)?;
    input_file
        .as_file_mut()
        .flush()
        .map_err(DuckdbError::TempFile)?;
    let bootstrap_query = compose_bootstrap_query(input_file.path(), query);

    let output = match Command::new(bin)
        .arg("-json")
        .arg("-batch")
        .arg("-c")
        .arg(bootstrap_query)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
    {
        Ok(output) => output,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            return Err(DuckdbError::Unavailable);
        }
        Err(err) => return Err(DuckdbError::Spawn(err)),
    };
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

fn compose_bootstrap_query(input_path: &Path, query: &str) -> String {
    let escaped_path = input_path.display().to_string().replace('\'', "''");
    format!(
        "CREATE OR REPLACE TEMP TABLE input AS SELECT * FROM read_json_auto('{escaped_path}'); {query}"
    )
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;

    use serde_json::json;

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
            "echo 'duckdb failed in test' 1>&2\nexit 7",
        );

        let err = run_query_with_bin(&[], "select 1", bin.to_str().expect("utf8 path"))
            .expect_err("non-zero should fail");
        assert!(matches!(err, DuckdbError::Output(_)));
    }

    #[test]
    fn materializes_input_relation_before_running_user_query() {
        let dir = tempfile::tempdir().expect("tempdir");
        let bin = write_test_script(
            dir.path().join("fake-duckdb"),
            r#"if [ "$1" != "-json" ] || [ "$2" != "-batch" ] || [ "$3" != "-c" ]; then
  echo "unexpected args" 1>&2
  exit 9
fi
case "$4" in
  *"CREATE OR REPLACE TEMP TABLE input AS SELECT * FROM read_json_auto("*"; select * from input"*)
    printf '[{"ok":true}]'
    exit 0
    ;;
  *)
    echo "missing input bootstrap in query" 1>&2
    exit 9
    ;;
esac"#,
        );

        let rows = run_query_with_bin(
            &[json!({"id": 1})],
            "select * from input",
            bin.to_str().expect("utf8 path"),
        )
        .expect("query should succeed");
        assert_eq!(rows, vec![json!({"ok": true})]);
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
