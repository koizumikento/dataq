use std::path::Path;
use std::process::{Command, ExitStatus, Stdio};

use thiserror::Error;

/// Errors produced while invoking `ajv-cli`.
#[derive(Debug, Error)]
pub enum AjvError {
    #[error("invalid ajv arguments: {0}")]
    InvalidArguments(String),
    #[error("`ajv` is not available in PATH")]
    Unavailable,
    #[error("failed to spawn ajv: {0}")]
    Spawn(std::io::Error),
    #[error("ajv validation failed: {0}")]
    Invalid(String),
    #[error("ajv usage error: {0}")]
    Usage(String),
    #[error("ajv internal execution error: {0}")]
    Internal(String),
}

/// Validate one data file against one schema file using:
/// `ajv validate -s <schema> -d <data>`.
pub fn validate(schema: &Path, data: &Path) -> Result<(), AjvError> {
    if schema.as_os_str().is_empty() {
        return Err(AjvError::InvalidArguments(
            "schema path cannot be empty".to_string(),
        ));
    }
    if data.as_os_str().is_empty() {
        return Err(AjvError::InvalidArguments(
            "data path cannot be empty".to_string(),
        ));
    }

    let args = vec![
        "validate".to_string(),
        "-s".to_string(),
        schema.to_string_lossy().into_owned(),
        "-d".to_string(),
        data.to_string_lossy().into_owned(),
    ];
    run_validate(&args)
}

/// Run `ajv` with explicit CLI argument arrays.
///
/// Exit mapping:
/// - `0` => success
/// - `1` => invalid (validation mismatch)
/// - `2` => usage (invalid command invocation)
/// - other => internal execution error
pub fn run_validate(args: &[String]) -> Result<(), AjvError> {
    if args.is_empty() {
        return Err(AjvError::InvalidArguments(
            "ajv arguments cannot be empty".to_string(),
        ));
    }
    if args.iter().any(|arg| arg.trim().is_empty()) {
        return Err(AjvError::InvalidArguments(
            "ajv arguments cannot include empty values".to_string(),
        ));
    }

    let ajv_bin = std::env::var("DATAQ_AJV_BIN").unwrap_or_else(|_| "ajv".to_string());
    run_validate_with_bin(args, &ajv_bin)
}

fn run_validate_with_bin(args: &[String], bin: &str) -> Result<(), AjvError> {
    let output = match Command::new(bin)
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
    {
        Ok(output) => output,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Err(AjvError::Unavailable);
        }
        Err(error) => return Err(AjvError::Spawn(error)),
    };

    if output.status.success() {
        return Ok(());
    }

    let message = format_failure_message(output.status, &output.stderr);
    match output.status.code() {
        Some(1) => Err(AjvError::Invalid(message)),
        Some(2) => Err(AjvError::Usage(message)),
        _ => Err(AjvError::Internal(message)),
    }
}

fn format_failure_message(status: ExitStatus, stderr: &[u8]) -> String {
    let stderr = String::from_utf8_lossy(stderr);
    if !stderr.trim().is_empty() {
        return stderr.trim().to_string();
    }
    match status.code() {
        Some(code) => format!("ajv exited with status {code}"),
        None => "ajv terminated by signal".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;

    use super::{AjvError, run_validate_with_bin};

    #[test]
    fn maps_unavailable_binary_to_unavailable_error() {
        let args = vec!["validate".to_string()];
        let err = run_validate_with_bin(&args, "/definitely-missing/ajv")
            .expect_err("missing binary should fail");
        assert!(matches!(err, AjvError::Unavailable));
    }

    #[test]
    fn maps_non_zero_exit_to_invalid_error() {
        let dir = tempfile::tempdir().expect("tempdir");
        let bin = write_test_script(
            dir.path().join("fake-ajv"),
            "echo 'validation mismatch' 1>&2\nexit 1",
        );
        let args = vec!["validate".to_string()];

        let err = run_validate_with_bin(&args, bin.to_str().expect("utf8 path"))
            .expect_err("exit 1 should fail");
        assert!(matches!(err, AjvError::Invalid(_)));
    }

    #[test]
    fn maps_usage_exit_to_usage_error() {
        let dir = tempfile::tempdir().expect("tempdir");
        let bin = write_test_script(
            dir.path().join("fake-ajv"),
            "echo 'invalid option' 1>&2\nexit 2",
        );
        let args = vec!["validate".to_string()];

        let err = run_validate_with_bin(&args, bin.to_str().expect("utf8 path"))
            .expect_err("exit 2 should fail");
        assert!(matches!(err, AjvError::Usage(_)));
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
