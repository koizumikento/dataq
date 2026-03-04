use std::io::Write;
use std::process::{Command, Stdio};

use serde_json::Value;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum JcError {
    #[error("jc parser cannot be empty")]
    InvalidParser,
    #[error("jc parser `{0}` may contain only ASCII letters, digits, `_`, and `-`")]
    InvalidParserName(String),
    #[error("`jc` is not available in PATH")]
    Unavailable,
    #[error("failed to spawn jc: {0}")]
    Spawn(std::io::Error),
    #[error("failed to write jc stdin: {0}")]
    Stdin(std::io::Error),
    #[error("jc execution failed: {0}")]
    Execution(String),
    #[error("jc output is not valid JSON: {0}")]
    Parse(serde_json::Error),
}

pub fn parse_with_parser(parser: &str, input: &[u8]) -> Result<Value, JcError> {
    let jc_bin = std::env::var("DATAQ_JC_BIN").unwrap_or_else(|_| "jc".to_string());
    parse_with_parser_and_bin(parser, input, &jc_bin)
}

fn parse_with_parser_and_bin(parser: &str, input: &[u8], bin: &str) -> Result<Value, JcError> {
    let parser = normalize_parser(parser)?;
    let parser_arg = format!("--{parser}");

    let mut child = match Command::new(bin)
        .arg("--quiet")
        .arg(parser_arg)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
    {
        Ok(child) => child,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Err(JcError::Unavailable);
        }
        Err(error) => return Err(JcError::Spawn(error)),
    };

    if let Some(stdin) = child.stdin.as_mut() {
        if let Err(error) = stdin.write_all(input)
            && error.kind() != std::io::ErrorKind::BrokenPipe
        {
            return Err(JcError::Stdin(error));
        }
    } else {
        return Err(JcError::Execution(
            "jc stdin was not piped as expected".to_string(),
        ));
    }

    let output = child.wait_with_output().map_err(JcError::Spawn)?;
    if !output.status.success() {
        let stderr = String::from_utf8(output.stderr)
            .unwrap_or_else(|_| "failed to decode jc stderr".to_string());
        return Err(JcError::Execution(stderr.trim().to_string()));
    }

    serde_json::from_slice(&output.stdout).map_err(JcError::Parse)
}

fn normalize_parser(parser: &str) -> Result<String, JcError> {
    let trimmed = parser.trim();
    if trimmed.is_empty() {
        return Err(JcError::InvalidParser);
    }

    let stripped = trimmed.trim_start_matches('-');
    if stripped.is_empty() {
        return Err(JcError::InvalidParser);
    }

    if stripped
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-')
    {
        return Ok(stripped.to_ascii_lowercase());
    }

    Err(JcError::InvalidParserName(trimmed.to_string()))
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;

    use super::{JcError, normalize_parser, parse_with_parser_and_bin};

    #[test]
    fn parser_name_normalization_accepts_trimmed_and_prefixed_values() {
        assert_eq!(
            normalize_parser("ifconfig").expect("valid parser"),
            "ifconfig"
        );
        assert_eq!(
            normalize_parser(" --IFCONFIG ").expect("valid parser"),
            "ifconfig"
        );
    }

    #[test]
    fn parser_name_normalization_rejects_invalid_values() {
        assert!(matches!(normalize_parser(""), Err(JcError::InvalidParser)));
        assert!(matches!(
            normalize_parser("--"),
            Err(JcError::InvalidParser)
        ));
        assert!(matches!(
            normalize_parser("if config"),
            Err(JcError::InvalidParserName(_))
        ));
    }

    #[test]
    fn maps_unavailable_binary_to_unavailable_error() {
        let error = parse_with_parser_and_bin("ifconfig", b"{}", "/definitely-missing/jc")
            .expect_err("missing binary should fail");
        assert!(matches!(error, JcError::Unavailable));
    }

    #[test]
    fn parses_json_output_when_command_succeeds() {
        let temp = tempfile::tempdir().expect("tempdir");
        let bin = write_test_script(
            temp.path().join("fake-jc"),
            r#"
parser=""
while [ $# -gt 0 ]; do
  case "$1" in
    --quiet)
      shift
      ;;
    --*)
      parser="$1"
      shift
      ;;
    *)
      shift
      ;;
  esac
done
cat >/dev/null
if [ "$parser" = "--ifconfig" ]; then
  printf '{"b":2,"a":1}\n'
  exit 0
fi
echo "unexpected parser: $parser" 1>&2
exit 9
"#,
        );

        let payload =
            parse_with_parser_and_bin("ifconfig", b"raw-input", bin.to_str().expect("utf8 path"))
                .expect("parse payload");
        assert_eq!(payload["a"], 1);
        assert_eq!(payload["b"], 2);
    }

    #[test]
    fn maps_non_zero_exit_to_execution_error() {
        let temp = tempfile::tempdir().expect("tempdir");
        let bin = write_test_script(
            temp.path().join("fake-jc"),
            r#"
cat >/dev/null
echo "jc failed in test" 1>&2
exit 4
"#,
        );

        let error =
            parse_with_parser_and_bin("ifconfig", b"raw-input", bin.to_str().expect("utf8 path"))
                .expect_err("non-zero exit should fail");
        assert!(matches!(error, JcError::Execution(_)));
    }

    #[test]
    fn maps_invalid_json_output_to_parse_error() {
        let temp = tempfile::tempdir().expect("tempdir");
        let bin = write_test_script(
            temp.path().join("fake-jc"),
            r#"
cat >/dev/null
echo "not-json"
"#,
        );

        let error =
            parse_with_parser_and_bin("ifconfig", b"raw-input", bin.to_str().expect("utf8 path"))
                .expect_err("invalid json should fail");
        assert!(matches!(error, JcError::Parse(_)));
    }

    fn write_test_script(path: PathBuf, body: &str) -> PathBuf {
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
