use std::io::Write;
use std::path::Path;
use std::process::{Command, Stdio};

use serde_json::Value;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum CsvkitError {
    #[error("`csvkit` is not available in PATH")]
    Unavailable,
    #[error("failed to spawn csvkit `{command}`: {source}")]
    Spawn {
        command: &'static str,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to write csvkit `{command}` stdin: {source}")]
    Stdin {
        command: &'static str,
        #[source]
        source: std::io::Error,
    },
    #[error("csvkit `{command}` execution failed: {message}")]
    Execution {
        command: &'static str,
        message: String,
    },
    #[error("csvkit `csvjson` output is not valid JSON: {0}")]
    Parse(serde_json::Error),
    #[error("csvkit `csvjson` output must be a JSON array")]
    OutputShape,
    #[error("csvkit `csvjson` output row {index} must be an object")]
    OutputRowShape { index: usize },
}

pub fn in2csv_from_path(path: &Path) -> Result<Vec<u8>, CsvkitError> {
    let bin = resolve_in2csv_bin();
    run_in2csv_with_path(path, &bin)
}

pub fn in2csv_from_stdin(input: &[u8]) -> Result<Vec<u8>, CsvkitError> {
    let bin = resolve_in2csv_bin();
    run_in2csv_with_stdin(input, &bin)
}

pub fn csvjson_rows_from_csv_bytes(input: &[u8]) -> Result<Vec<Value>, CsvkitError> {
    let bin = resolve_csvjson_bin();
    run_csvjson_rows(input, &bin)
}

fn resolve_in2csv_bin() -> String {
    std::env::var("DATAQ_CSVKIT_IN2CSV_BIN").unwrap_or_else(|_| "in2csv".to_string())
}

fn resolve_csvjson_bin() -> String {
    std::env::var("DATAQ_CSVKIT_CSVJSON_BIN").unwrap_or_else(|_| "csvjson".to_string())
}

fn run_in2csv_with_path(path: &Path, bin: &str) -> Result<Vec<u8>, CsvkitError> {
    let output = match Command::new(bin)
        .arg(path)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
    {
        Ok(child) => child
            .wait_with_output()
            .map_err(|error| CsvkitError::Spawn {
                command: "in2csv",
                source: error,
            })?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Err(CsvkitError::Unavailable);
        }
        Err(error) => {
            return Err(CsvkitError::Spawn {
                command: "in2csv",
                source: error,
            });
        }
    };

    if !output.status.success() {
        return Err(CsvkitError::Execution {
            command: "in2csv",
            message: decode_stderr(output.stderr),
        });
    }

    Ok(output.stdout)
}

fn run_in2csv_with_stdin(input: &[u8], bin: &str) -> Result<Vec<u8>, CsvkitError> {
    let mut child = match Command::new(bin)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
    {
        Ok(child) => child,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Err(CsvkitError::Unavailable);
        }
        Err(error) => {
            return Err(CsvkitError::Spawn {
                command: "in2csv",
                source: error,
            });
        }
    };

    if let Some(stdin) = child.stdin.as_mut() {
        if let Err(error) = stdin.write_all(input)
            && error.kind() != std::io::ErrorKind::BrokenPipe
        {
            return Err(CsvkitError::Stdin {
                command: "in2csv",
                source: error,
            });
        }
    } else {
        return Err(CsvkitError::Execution {
            command: "in2csv",
            message: "in2csv stdin was not piped as expected".to_string(),
        });
    }

    let output = child
        .wait_with_output()
        .map_err(|error| CsvkitError::Spawn {
            command: "in2csv",
            source: error,
        })?;
    if !output.status.success() {
        return Err(CsvkitError::Execution {
            command: "in2csv",
            message: decode_stderr(output.stderr),
        });
    }

    Ok(output.stdout)
}

fn run_csvjson_rows(input: &[u8], bin: &str) -> Result<Vec<Value>, CsvkitError> {
    let mut child = match Command::new(bin)
        .arg("--no-inference")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
    {
        Ok(child) => child,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Err(CsvkitError::Unavailable);
        }
        Err(error) => {
            return Err(CsvkitError::Spawn {
                command: "csvjson",
                source: error,
            });
        }
    };

    if let Some(stdin) = child.stdin.as_mut() {
        if let Err(error) = stdin.write_all(input)
            && error.kind() != std::io::ErrorKind::BrokenPipe
        {
            return Err(CsvkitError::Stdin {
                command: "csvjson",
                source: error,
            });
        }
    } else {
        return Err(CsvkitError::Execution {
            command: "csvjson",
            message: "csvjson stdin was not piped as expected".to_string(),
        });
    }

    let output = child
        .wait_with_output()
        .map_err(|error| CsvkitError::Spawn {
            command: "csvjson",
            source: error,
        })?;
    if !output.status.success() {
        return Err(CsvkitError::Execution {
            command: "csvjson",
            message: decode_stderr(output.stderr),
        });
    }

    let parsed: Value = serde_json::from_slice(&output.stdout).map_err(CsvkitError::Parse)?;
    match parsed {
        Value::Array(rows) => {
            for (index, row) in rows.iter().enumerate() {
                if !row.is_object() {
                    return Err(CsvkitError::OutputRowShape { index });
                }
            }
            Ok(rows)
        }
        _ => Err(CsvkitError::OutputShape),
    }
}

fn decode_stderr(stderr: Vec<u8>) -> String {
    String::from_utf8(stderr)
        .unwrap_or_else(|_| "failed to decode csvkit stderr".to_string())
        .trim()
        .to_string()
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;

    use super::{CsvkitError, run_csvjson_rows, run_in2csv_with_path, run_in2csv_with_stdin};

    #[test]
    fn maps_missing_binary_to_unavailable_error() {
        let err = run_in2csv_with_path(
            PathBuf::from("missing.csv").as_path(),
            "/definitely-missing/in2csv",
        )
        .expect_err("missing binary should fail");
        assert!(matches!(err, CsvkitError::Unavailable));
    }

    #[test]
    fn parses_csvjson_array_output() {
        let dir = tempfile::tempdir().expect("tempdir");
        let script_path = write_exec_script(
            dir.path().join("fake-csvjson"),
            r#"cat >/dev/null
printf '[{"id":"1"},{"id":"2"}]'"#,
        );
        let rows = run_csvjson_rows(
            b"id,name\n1,alice\n2,bob\n",
            script_path.to_str().expect("utf8"),
        )
        .expect("csvjson rows");
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0]["id"], "1");
    }

    #[test]
    fn maps_non_array_csvjson_output_to_shape_error() {
        let dir = tempfile::tempdir().expect("tempdir");
        let script_path = write_exec_script(
            dir.path().join("fake-csvjson"),
            "cat >/dev/null\nprintf '{}' ",
        );
        let err = run_csvjson_rows(b"id\n1\n", script_path.to_str().expect("utf8"))
            .expect_err("non-array output should fail");
        assert!(matches!(err, CsvkitError::OutputShape));
    }

    #[test]
    fn runs_in2csv_with_stdin_and_path_modes() {
        let dir = tempfile::tempdir().expect("tempdir");
        let input_path = dir.path().join("input.csv");
        fs::write(&input_path, "id,name\n1,alice\n").expect("write input");
        let script_path = write_exec_script(
            dir.path().join("fake-in2csv"),
            r#"if [ $# -gt 0 ]; then
  cat "$1"
else
  cat
fi"#,
        );

        let from_path = run_in2csv_with_path(&input_path, script_path.to_str().expect("utf8"))
            .expect("path conversion");
        assert_eq!(from_path, b"id,name\n1,alice\n");

        let from_stdin =
            run_in2csv_with_stdin(b"id,name\n2,bob\n", script_path.to_str().expect("utf8"))
                .expect("stdin conversion");
        assert_eq!(from_stdin, b"id,name\n2,bob\n");
    }

    fn write_exec_script(path: PathBuf, body: &str) -> PathBuf {
        let script = format!("#!/bin/sh\n{body}\n");
        fs::write(&path, script).expect("write script");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&path, fs::Permissions::from_mode(0o755)).expect("chmod");
        }
        path
    }
}
