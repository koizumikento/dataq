use std::io::Write;
use std::process::{Command, Stdio};

use serde_json::Value;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum JcError {
    #[error("jc parser cannot be empty")]
    InvalidParser,
    #[error("jc argument at index {index} cannot be empty")]
    InvalidArgument { index: usize },
    #[error("`jc` is not available in PATH")]
    Unavailable,
    #[error("failed to spawn jc: {0}")]
    Spawn(std::io::Error),
    #[error("failed to write jc stdin: {0}")]
    Stdin(std::io::Error),
    #[error("jc execution failed (exit code: {code:?}): {stderr}")]
    Execution { code: Option<i32>, stderr: String },
    #[error("jc output is not valid JSON: {0}")]
    Parse(serde_json::Error),
}

/// Converts raw command output into JSON using a jc parser.
///
/// `parser` is passed via `jc --parser <parser>`, and `parser_args` are appended
/// as distinct argv entries in order.
pub fn parse_with_parser(
    input: &[u8],
    parser: &str,
    parser_args: &[String],
) -> Result<Value, JcError> {
    let jc_bin = std::env::var("DATAQ_JC_BIN").unwrap_or_else(|_| "jc".to_string());
    parse_with_parser_and_bin(input, parser, parser_args, &jc_bin)
}

fn parse_with_parser_and_bin(
    input: &[u8],
    parser: &str,
    parser_args: &[String],
    bin: &str,
) -> Result<Value, JcError> {
    if parser.trim().is_empty() {
        return Err(JcError::InvalidParser);
    }

    if let Some((index, _)) = parser_args
        .iter()
        .enumerate()
        .find(|(_, argument)| argument.trim().is_empty())
    {
        return Err(JcError::InvalidArgument { index });
    }

    let mut child = match Command::new(bin)
        .arg("--parser")
        .arg(parser)
        .args(parser_args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
    {
        Ok(child) => child,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Err(JcError::Unavailable),
        Err(err) => return Err(JcError::Spawn(err)),
    };

    if let Some(stdin) = child.stdin.as_mut() {
        if let Err(err) = stdin.write_all(input) {
            if err.kind() != std::io::ErrorKind::BrokenPipe {
                return Err(JcError::Stdin(err));
            }
        }
    } else {
        return Err(JcError::Execution {
            code: None,
            stderr: "jc stdin was not piped as expected".to_string(),
        });
    }

    let output = child.wait_with_output().map_err(JcError::Spawn)?;
    if !output.status.success() {
        let stderr = String::from_utf8(output.stderr)
            .unwrap_or_else(|_| "failed to decode jc stderr".to_string());
        return Err(JcError::Execution {
            code: output.status.code(),
            stderr: stderr.trim().to_string(),
        });
    }

    serde_json::from_slice(&output.stdout).map_err(JcError::Parse)
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;

    use serde_json::json;

    use super::{JcError, parse_with_parser_and_bin};

    #[test]
    fn maps_unavailable_binary_to_unavailable_error() {
        let err = parse_with_parser_and_bin(b"", "ifconfig", &[], "/definitely-missing/jc")
            .expect_err("missing binary should fail");
        assert!(matches!(err, JcError::Unavailable));
    }

    #[test]
    fn maps_non_zero_exit_to_execution_error() {
        let dir = tempfile::tempdir().expect("tempdir");
        let bin = write_test_script(
            dir.path().join("fake-jc"),
            "cat >/dev/null\necho 'jc failed in test' 1>&2\nexit 7",
        );

        let err = parse_with_parser_and_bin(b"raw", "ifconfig", &[], bin.to_str().expect("utf8"))
            .expect_err("non-zero exit should fail");

        match err {
            JcError::Execution { code, stderr } => {
                assert_eq!(code, Some(7));
                assert_eq!(stderr, "jc failed in test");
            }
            JcError::Stdin(io_err) if io_err.kind() == std::io::ErrorKind::BrokenPipe => {}
            other => panic!("expected execution-like failure, got {other:?}"),
        }
    }

    #[test]
    fn passes_parser_and_args_as_distinct_argv_entries() {
        let dir = tempfile::tempdir().expect("tempdir");
        let bin = write_test_script(
            dir.path().join("fake-jc"),
            r#"cat >/dev/null
if [ "$1" != "--parser" ] || [ "$2" != "ifconfig" ] || [ "$3" != "--raw" ] || [ "$4" != "--quiet" ]; then
  echo "unexpected args: $*" 1>&2
  exit 9
fi
printf '{"ok":true}'"#,
        );
        let parser_args = vec!["--raw".to_string(), "--quiet".to_string()];

        let value = parse_with_parser_and_bin(
            b"eth0 ...",
            "ifconfig",
            &parser_args,
            bin.to_str().expect("utf8"),
        )
        .expect("parser should succeed");

        assert_eq!(value, json!({ "ok": true }));
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
