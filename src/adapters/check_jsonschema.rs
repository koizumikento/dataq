use std::io::Write;
use std::path::Path;
use std::process::{Command, ExitStatus, Stdio};

use serde_json::Value;
use thiserror::Error;

/// Error shape produced by the `check-jsonschema` adapter.
#[derive(Debug, Error)]
pub enum CheckJsonSchemaError {
    #[error("`check-jsonschema` is not available in PATH")]
    Unavailable,
    #[error("failed to spawn check-jsonschema: {0}")]
    Spawn(std::io::Error),
    #[error("failed to write check-jsonschema stdin: {0}")]
    Stdin(std::io::Error),
    #[error("check-jsonschema exited with status {status}: {stderr}")]
    Exit { status: ExitStatus, stderr: String },
    #[error("check-jsonschema output is not valid JSON: {0}")]
    Parse(serde_json::Error),
}

/// Runs `check-jsonschema` with stdin JSON input and returns parsed JSON output.
///
/// The adapter honors `DATAQ_CHECK_JSONSCHEMA_BIN` and defaults to
/// `check-jsonschema` when the variable is unset.
pub fn validate_stdin_json(
    schema_path: &Path,
    input_json: &[u8],
) -> Result<Value, CheckJsonSchemaError> {
    let check_jsonschema_bin =
        std::env::var("DATAQ_CHECK_JSONSCHEMA_BIN").unwrap_or_else(|_| "check-jsonschema".into());
    validate_stdin_json_with_bin(schema_path, input_json, &check_jsonschema_bin)
}

fn validate_stdin_json_with_bin(
    schema_path: &Path,
    input_json: &[u8],
    bin: &str,
) -> Result<Value, CheckJsonSchemaError> {
    let args = build_check_jsonschema_args(schema_path);
    let mut child = match Command::new(bin)
        .args(&args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
    {
        Ok(child) => child,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Err(CheckJsonSchemaError::Unavailable);
        }
        Err(error) => return Err(CheckJsonSchemaError::Spawn(error)),
    };

    if let Some(stdin) = child.stdin.as_mut() {
        if let Err(error) = stdin.write_all(input_json) {
            if error.kind() != std::io::ErrorKind::BrokenPipe {
                return Err(CheckJsonSchemaError::Stdin(error));
            }
        }
    } else {
        return Err(CheckJsonSchemaError::Stdin(std::io::Error::new(
            std::io::ErrorKind::BrokenPipe,
            "check-jsonschema stdin was not piped as expected",
        )));
    }

    let output = child
        .wait_with_output()
        .map_err(CheckJsonSchemaError::Spawn)?;
    if !output.status.success() {
        let stderr = String::from_utf8(output.stderr)
            .unwrap_or_else(|_| "failed to decode check-jsonschema stderr".to_string());
        let stderr = if stderr.trim().is_empty() {
            "check-jsonschema exited with an empty stderr stream".to_string()
        } else {
            stderr.trim().to_string()
        };
        return Err(CheckJsonSchemaError::Exit {
            status: output.status,
            stderr,
        });
    }

    serde_json::from_slice(&output.stdout).map_err(CheckJsonSchemaError::Parse)
}

fn build_check_jsonschema_args(schema_path: &Path) -> Vec<String> {
    let schema_path = schema_path.to_string_lossy().into_owned();
    vec![
        "--output-format".to_string(),
        "json".to_string(),
        "--schemafile".to_string(),
        schema_path,
        "-".to_string(),
    ]
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::{Path, PathBuf};

    use super::{CheckJsonSchemaError, validate_stdin_json_with_bin};

    #[test]
    fn maps_unavailable_binary_to_unavailable_error() {
        let err = validate_stdin_json_with_bin(
            Path::new("/tmp/schema.json"),
            br#"{"id":1}"#,
            "/definitely-missing/check-jsonschema",
        )
        .expect_err("missing binary should fail");
        assert!(matches!(err, CheckJsonSchemaError::Unavailable));
    }

    #[test]
    fn maps_non_zero_exit_to_exit_error() {
        let dir = tempfile::tempdir().expect("tempdir");
        let bin = write_test_script(
            dir.path().join("fake-check-jsonschema"),
            "cat >/dev/null\necho 'schema mismatch in test' 1>&2\nexit 7",
        );

        let err = validate_stdin_json_with_bin(
            Path::new("/tmp/schema.json"),
            br#"{"id":1}"#,
            bin.to_str().expect("utf8 path"),
        )
        .expect_err("non-zero exit should fail");

        match err {
            CheckJsonSchemaError::Exit { status, stderr } => {
                assert_eq!(status.code(), Some(7));
                assert!(stderr.contains("schema mismatch in test"));
            }
            other => panic!("expected exit error, got {other:?}"),
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
