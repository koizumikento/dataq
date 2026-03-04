use std::io::Write;
use std::process::{Command, Stdio};

use thiserror::Error;

#[derive(Debug, Error)]
pub enum CsvkitError {
    #[error("`{tool}` is not available in PATH (resolved bin: `{bin}`)")]
    Unavailable { tool: &'static str, bin: String },
    #[error("failed to spawn `{tool}` (`{bin}`): {source}")]
    Spawn {
        tool: &'static str,
        bin: String,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to write `{tool}` stdin (`{bin}`): {source}")]
    Stdin {
        tool: &'static str,
        bin: String,
        #[source]
        source: std::io::Error,
    },
    #[error("`{tool}` stdin was not piped as expected (`{bin}`)")]
    StdinUnavailable { tool: &'static str, bin: String },
    #[error("failed to wait on `{tool}` (`{bin}`): {source}")]
    Wait {
        tool: &'static str,
        bin: String,
        #[source]
        source: std::io::Error,
    },
    #[error("`{tool}` execution failed (`{bin}`, exit {code:?}): {stderr}")]
    Execution {
        tool: &'static str,
        bin: String,
        code: Option<i32>,
        stderr: String,
    },
}

pub fn run_in2csv(args: &[String], input: Option<&[u8]>) -> Result<Vec<u8>, CsvkitError> {
    let bin = resolve_in2csv_bin();
    run_in2csv_with_bin(args, input, &bin)
}

pub fn run_csvsql(args: &[String], input: Option<&[u8]>) -> Result<Vec<u8>, CsvkitError> {
    let bin = resolve_csvsql_bin();
    run_csvsql_with_bin(args, input, &bin)
}

fn resolve_in2csv_bin() -> String {
    resolve_bin_with("DATAQ_IN2CSV_BIN", "in2csv", |name| {
        std::env::var(name).ok()
    })
}

fn resolve_csvsql_bin() -> String {
    resolve_bin_with("DATAQ_CSVSQL_BIN", "csvsql", |name| {
        std::env::var(name).ok()
    })
}

fn resolve_bin_with<F>(env_name: &str, fallback: &str, lookup: F) -> String
where
    F: Fn(&str) -> Option<String>,
{
    lookup(env_name)
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| fallback.to_string())
}

fn run_in2csv_with_bin(
    args: &[String],
    input: Option<&[u8]>,
    bin: &str,
) -> Result<Vec<u8>, CsvkitError> {
    run_csvkit_command("in2csv", bin, args, input)
}

fn run_csvsql_with_bin(
    args: &[String],
    input: Option<&[u8]>,
    bin: &str,
) -> Result<Vec<u8>, CsvkitError> {
    run_csvkit_command("csvsql", bin, args, input)
}

fn run_csvkit_command(
    tool: &'static str,
    bin: &str,
    args: &[String],
    input: Option<&[u8]>,
) -> Result<Vec<u8>, CsvkitError> {
    let stdin = if input.is_some() {
        Stdio::piped()
    } else {
        Stdio::null()
    };

    let mut child = match Command::new(bin)
        .args(args)
        .stdin(stdin)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
    {
        Ok(child) => child,
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => {
            return Err(CsvkitError::Unavailable {
                tool,
                bin: bin.to_string(),
            });
        }
        Err(source) => {
            return Err(CsvkitError::Spawn {
                tool,
                bin: bin.to_string(),
                source,
            });
        }
    };

    if let Some(payload) = input {
        if let Some(stdin) = child.stdin.as_mut() {
            if let Err(source) = stdin.write_all(payload) {
                if source.kind() != std::io::ErrorKind::BrokenPipe {
                    return Err(CsvkitError::Stdin {
                        tool,
                        bin: bin.to_string(),
                        source,
                    });
                }
            }
        } else {
            return Err(CsvkitError::StdinUnavailable {
                tool,
                bin: bin.to_string(),
            });
        }
    }

    let output = child
        .wait_with_output()
        .map_err(|source| CsvkitError::Wait {
            tool,
            bin: bin.to_string(),
            source,
        })?;

    if !output.status.success() {
        let stderr = String::from_utf8(output.stderr)
            .unwrap_or_else(|_| "failed to decode csvkit stderr".to_string());
        return Err(CsvkitError::Execution {
            tool,
            bin: bin.to_string(),
            code: output.status.code(),
            stderr: stderr.trim().to_string(),
        });
    }

    Ok(output.stdout)
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;

    use super::{CsvkitError, resolve_bin_with, run_csvsql_with_bin, run_in2csv_with_bin};

    #[test]
    fn maps_unavailable_binary_to_unavailable_error() {
        let err = run_in2csv_with_bin(&[], None, "/definitely-missing/in2csv")
            .expect_err("missing binary should fail");

        match err {
            CsvkitError::Unavailable { tool, .. } => assert_eq!(tool, "in2csv"),
            other => panic!("expected unavailable error, got {other:?}"),
        }
    }

    #[test]
    fn resolves_in2csv_bin_from_override() {
        let resolved = resolve_bin_with("DATAQ_IN2CSV_BIN", "in2csv", |name| {
            (name == "DATAQ_IN2CSV_BIN").then(|| "/custom/in2csv".to_string())
        });
        assert_eq!(resolved, "/custom/in2csv");
    }

    #[test]
    fn resolves_csvsql_bin_from_override() {
        let resolved = resolve_bin_with("DATAQ_CSVSQL_BIN", "csvsql", |name| {
            (name == "DATAQ_CSVSQL_BIN").then(|| "/custom/csvsql".to_string())
        });
        assert_eq!(resolved, "/custom/csvsql");
    }

    #[test]
    fn uses_explicit_argument_array_and_writes_stdin() {
        let dir = tempfile::tempdir().expect("tempdir");
        let bin = write_test_script(
            dir.path().join("fake-csvsql"),
            r#"
if [ "$#" -ne 2 ]; then
  echo "unexpected arg count: $#" 1>&2
  exit 10
fi
if [ "$1" != "--query" ]; then
  echo "missing --query arg" 1>&2
  exit 10
fi
if [ "$2" != "select * from t where name = 'A B'" ]; then
  echo "query arg was split unexpectedly" 1>&2
  exit 10
fi

stdin_file="$(mktemp)"
cat > "$stdin_file"
if ! grep -q '^name$' "$stdin_file"; then
  echo "stdin missing header row" 1>&2
  rm -f "$stdin_file"
  exit 10
fi
printf 'args-ok\n'
cat "$stdin_file"
rm -f "$stdin_file"
"#,
        );

        let args = vec![
            "--query".to_string(),
            "select * from t where name = 'A B'".to_string(),
        ];
        let output = run_csvsql_with_bin(&args, Some(b"name\nA B\n"), bin.to_str().expect("utf8"))
            .expect("csvsql should succeed");
        let output_text = String::from_utf8(output).expect("stdout utf8");

        assert!(output_text.starts_with("args-ok\n"));
        assert!(output_text.contains("name\nA B\n"));
    }

    #[test]
    fn maps_non_zero_exit_to_execution_error() {
        let dir = tempfile::tempdir().expect("tempdir");
        let bin = write_test_script(
            dir.path().join("fake-csvsql"),
            "echo 'csvsql failed in test' 1>&2\nexit 7",
        );

        let err = run_csvsql_with_bin(&[], None, bin.to_str().expect("utf8"))
            .expect_err("non-zero should fail");
        match err {
            CsvkitError::Execution {
                tool, code, stderr, ..
            } => {
                assert_eq!(tool, "csvsql");
                assert_eq!(code, Some(7));
                assert_eq!(stderr, "csvsql failed in test");
            }
            other => panic!("expected execution error, got {other:?}"),
        }
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
