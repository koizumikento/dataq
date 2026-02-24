use std::path::Path;
use std::process::{Command, Stdio};

use thiserror::Error;

#[derive(Debug, Error)]
pub enum RgError {
    #[error("`rg` is not available in PATH")]
    Unavailable,
    #[error("failed to spawn rg: {0}")]
    Spawn(std::io::Error),
    #[error("rg execution failed: {0}")]
    Execution(String),
    #[error("rg stdout is not valid utf-8: {0}")]
    Utf8(std::string::FromUtf8Error),
}

#[derive(Debug, Clone)]
pub struct RgCommandArgs<'a> {
    pub pattern: &'a str,
    pub path: &'a Path,
    pub globs: &'a [String],
}

pub fn execute_json(args: &RgCommandArgs<'_>) -> Result<String, RgError> {
    let rg_bin = resolve_rg_bin();
    execute_json_with_bin(args, &rg_bin)
}

fn resolve_rg_bin() -> String {
    std::env::var("DATAQ_RG_BIN").unwrap_or_else(|_| "rg".to_string())
}

fn execute_json_with_bin(args: &RgCommandArgs<'_>, bin: &str) -> Result<String, RgError> {
    let mut command = Command::new(bin);
    command
        .arg("--json")
        .arg("--line-number")
        .arg("--column")
        .arg("--with-filename")
        .arg("--color")
        .arg("never")
        .arg("--no-heading")
        .arg("--multiline")
        .arg("--sort")
        .arg("path");

    for glob in args.globs {
        command.arg("--glob").arg(glob);
    }

    command
        .arg(args.pattern)
        .arg(args.path)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    let output = match command.output() {
        Ok(output) => output,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Err(RgError::Unavailable);
        }
        Err(error) => return Err(RgError::Spawn(error)),
    };

    if output.status.success() || output.status.code() == Some(1) {
        return String::from_utf8(output.stdout).map_err(RgError::Utf8);
    }

    let stderr = String::from_utf8_lossy(&output.stderr);
    let message = if stderr.trim().is_empty() {
        format!("exit status {}", output.status)
    } else {
        stderr.trim().to_string()
    };
    Err(RgError::Execution(message))
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;

    use super::{RgCommandArgs, RgError, execute_json_with_bin};

    #[test]
    fn maps_unavailable_binary_to_unavailable_error() {
        let path = tempfile::tempdir().expect("tempdir").path().to_path_buf();
        let args = RgCommandArgs {
            pattern: "x",
            path: &path,
            globs: &[],
        };
        let err = execute_json_with_bin(&args, "/definitely-missing/rg")
            .expect_err("missing binary should fail");
        assert!(matches!(err, RgError::Unavailable));
    }

    #[test]
    fn preserves_exit_one_as_success_for_no_match() {
        let dir = tempfile::tempdir().expect("tempdir");
        let bin = write_test_script(dir.path().join("fake-rg"), "exit 1");
        let path = dir.path().to_path_buf();
        let args = RgCommandArgs {
            pattern: "x",
            path: &path,
            globs: &[],
        };

        let output =
            execute_json_with_bin(&args, bin.to_str().expect("utf8 path")).expect("exit 1 success");
        assert_eq!(output, "");
    }

    #[test]
    fn maps_exit_non_zero_to_execution_error() {
        let dir = tempfile::tempdir().expect("tempdir");
        let bin = write_test_script(
            dir.path().join("fake-rg"),
            "echo 'regex parse error' 1>&2\nexit 2",
        );
        let path = dir.path().to_path_buf();
        let args = RgCommandArgs {
            pattern: "(",
            path: &path,
            globs: &[],
        };

        let err = execute_json_with_bin(&args, bin.to_str().expect("utf8 path"))
            .expect_err("exit 2 should fail");
        assert!(matches!(err, RgError::Execution(_)));
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
