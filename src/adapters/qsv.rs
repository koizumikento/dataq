use std::process::{Command, Stdio};

use thiserror::Error;

#[derive(Debug, Error)]
pub enum QsvError {
    #[error("`qsv` is not available in PATH")]
    Unavailable,
    #[error("failed to spawn qsv: {0}")]
    Spawn(std::io::Error),
    #[error("qsv execution failed: {0}")]
    Execution(String),
    #[error("qsv output is not valid UTF-8: {0}")]
    Utf8(std::string::FromUtf8Error),
}

pub fn stats(args: &[String]) -> Result<String, QsvError> {
    run_subcommand("stats", args)
}

pub fn sniff(args: &[String]) -> Result<String, QsvError> {
    run_subcommand("sniff", args)
}

pub fn schema(args: &[String]) -> Result<String, QsvError> {
    run_subcommand("schema", args)
}

fn run_subcommand(subcommand: &str, args: &[String]) -> Result<String, QsvError> {
    let qsv_bin = resolve_qsv_bin();
    run_subcommand_with_bin(subcommand, args, &qsv_bin)
}

fn resolve_qsv_bin() -> String {
    std::env::var("DATAQ_QSV_BIN").unwrap_or_else(|_| "qsv".to_string())
}

fn run_subcommand_with_bin(
    subcommand: &str,
    args: &[String],
    bin: &str,
) -> Result<String, QsvError> {
    let mut command_args = Vec::with_capacity(args.len() + 1);
    command_args.push(subcommand.to_string());
    command_args.extend(args.iter().cloned());

    let output = match Command::new(bin)
        .args(&command_args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
    {
        Ok(output) => output,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Err(QsvError::Unavailable);
        }
        Err(error) => return Err(QsvError::Spawn(error)),
    };

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let message = if stderr.trim().is_empty() {
            format!("exit status {}", output.status)
        } else {
            stderr.trim().to_string()
        };
        return Err(QsvError::Execution(message));
    }

    String::from_utf8(output.stdout).map_err(QsvError::Utf8)
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;

    use super::{QsvError, run_subcommand_with_bin};

    #[test]
    fn maps_unavailable_binary_to_unavailable_error() {
        let err = run_subcommand_with_bin("stats", &[], "/definitely-missing/qsv")
            .expect_err("missing binary should fail");
        assert!(matches!(err, QsvError::Unavailable));
    }

    #[test]
    fn maps_non_zero_exit_to_execution_error() {
        let dir = tempfile::tempdir().expect("tempdir");
        let bin = write_test_script(
            dir.path().join("fake-qsv"),
            "echo 'qsv failed in test' 1>&2\nexit 7",
        );

        for subcommand in ["stats", "sniff", "schema"] {
            let err = run_subcommand_with_bin(subcommand, &[], bin.to_str().expect("utf8 path"))
                .expect_err("non-zero should fail");
            assert!(
                matches!(err, QsvError::Execution(_)),
                "expected execution error for {subcommand}, got {err:?}"
            );
        }
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
