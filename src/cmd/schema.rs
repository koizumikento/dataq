use std::fs::File;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use serde::Serialize;
use serde_json::{Value, json};

/// Input arguments for `schema infer` command execution API.
#[derive(Debug, Clone)]
pub struct SchemaInferCommandArgs {
    pub input: Option<PathBuf>,
}

/// Structured command response for `schema infer` execution.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct SchemaInferCommandResponse {
    pub exit_code: i32,
    pub payload: Value,
}

/// Returns true when a path value should be treated as stdin.
pub fn is_stdin_path(path: &Path) -> bool {
    path == Path::new("-") || path == Path::new("/dev/stdin")
}

pub fn run_infer_with_stdin<R: Read>(
    args: &SchemaInferCommandArgs,
    stdin: R,
) -> SchemaInferCommandResponse {
    match execute_infer(args, stdin) {
        Ok(payload) => SchemaInferCommandResponse {
            exit_code: 0,
            payload,
        },
        Err(SchemaInferCommandError::InputUsage(message)) => SchemaInferCommandResponse {
            exit_code: 3,
            payload: json!({
                "error": "input_usage_error",
                "message": message,
            }),
        },
        Err(SchemaInferCommandError::Internal(message)) => SchemaInferCommandResponse {
            exit_code: 1,
            payload: json!({
                "error": "internal_error",
                "message": message,
            }),
        },
    }
}

/// Ordered pipeline-step names used for `schema infer --emit-pipeline` diagnostics.
pub fn infer_pipeline_steps() -> Vec<String> {
    vec![
        "schema_infer_qsv".to_string(),
        "schema_infer_parse_json".to_string(),
    ]
}

/// Determinism guards applied by `schema infer`.
pub fn infer_deterministic_guards() -> Vec<String> {
    vec![
        "qsv_execution_with_explicit_arg_arrays".to_string(),
        "schema_infer_json_default_output".to_string(),
        "schema_infer_exit_mapping_0_3_1".to_string(),
    ]
}

enum SchemaInferCommandError {
    InputUsage(String),
    Internal(String),
}

fn execute_infer<R: Read>(
    args: &SchemaInferCommandArgs,
    mut stdin: R,
) -> Result<Value, SchemaInferCommandError> {
    let mut command = Command::new(resolve_qsv_bin());
    command.arg("schema");

    let write_stdin = match args.input.as_deref() {
        Some(path) if is_stdin_path(path) => {
            command.arg("-");
            true
        }
        Some(path) => {
            let _validated_file = File::open(path).map_err(|error| {
                SchemaInferCommandError::InputUsage(format!(
                    "failed to open input file `{}`: {error}",
                    path.display()
                ))
            })?;
            command.arg(path);
            false
        }
        None => {
            command.arg("-");
            true
        }
    };

    command.stdout(Stdio::piped()).stderr(Stdio::piped());
    if write_stdin {
        command.stdin(Stdio::piped());
    } else {
        command.stdin(Stdio::null());
    }

    let mut child = command.spawn().map_err(|error| {
        if error.kind() == std::io::ErrorKind::NotFound {
            SchemaInferCommandError::InputUsage("schema infer requires `qsv` in PATH".to_string())
        } else {
            SchemaInferCommandError::InputUsage(format!("failed to spawn qsv: {error}"))
        }
    })?;

    if write_stdin {
        let mut input_bytes = Vec::new();
        stdin.read_to_end(&mut input_bytes).map_err(|error| {
            SchemaInferCommandError::Internal(format!("failed to read stdin: {error}"))
        })?;

        let mut child_stdin = child.stdin.take().ok_or_else(|| {
            SchemaInferCommandError::Internal("qsv stdin was not piped as expected".to_string())
        })?;
        if let Err(error) = child_stdin.write_all(&input_bytes)
            && error.kind() != std::io::ErrorKind::BrokenPipe
        {
            return Err(SchemaInferCommandError::Internal(format!(
                "failed to write qsv stdin: {error}"
            )));
        }
    }

    let output = child.wait_with_output().map_err(|error| {
        SchemaInferCommandError::Internal(format!("failed to wait for qsv: {error}"))
    })?;

    if !output.status.success() {
        let message = String::from_utf8_lossy(&output.stderr)
            .lines()
            .map(str::trim)
            .find(|line| !line.is_empty())
            .map(ToOwned::to_owned)
            .unwrap_or_else(|| format!("qsv schema exited with status {}", output.status));
        return Err(SchemaInferCommandError::InputUsage(format!(
            "failed to infer schema with qsv: {message}"
        )));
    }

    serde_json::from_slice(&output.stdout).map_err(|error| {
        SchemaInferCommandError::Internal(format!("qsv schema output was not valid JSON: {error}"))
    })
}

fn resolve_qsv_bin() -> String {
    std::env::var("DATAQ_QSV_BIN").unwrap_or_else(|_| "qsv".to_string())
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;
    use std::path::{Path, PathBuf};

    use serde_json::json;

    use super::{SchemaInferCommandArgs, is_stdin_path, run_infer_with_stdin};

    #[test]
    fn schema_infer_reports_missing_input_path_as_usage_error() {
        let args = SchemaInferCommandArgs {
            input: Some(PathBuf::from("/definitely-missing/dataq-schema.csv")),
        };

        let response = run_infer_with_stdin(&args, Cursor::new(Vec::<u8>::new()));
        assert_eq!(response.exit_code, 3);
        assert_eq!(response.payload["error"], json!("input_usage_error"));
    }

    #[test]
    fn stdin_path_detection_accepts_known_sentinels() {
        assert!(is_stdin_path(Path::new("-")));
        assert!(is_stdin_path(Path::new("/dev/stdin")));
        assert!(!is_stdin_path(Path::new("input.csv")));
    }
}
