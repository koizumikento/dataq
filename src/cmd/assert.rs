use std::fs::File;
use std::io::Read;
use std::path::PathBuf;

use serde::Serialize;
use serde_json::{Value, json};

use crate::domain::rules::{AssertReport, AssertRules};
use crate::engine::r#assert::{self, AssertValidationError};
use crate::io::{self, Format, IoError};

/// Input arguments for assert command execution API.
#[derive(Debug, Clone)]
pub struct AssertCommandArgs {
    pub input: Option<PathBuf>,
    pub from: Option<Format>,
    pub rules: PathBuf,
}

/// Structured command response that carries exit-code mapping and JSON payload.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct AssertCommandResponse {
    pub exit_code: i32,
    pub payload: Value,
}

pub fn run_with_stdin<R: Read>(args: &AssertCommandArgs, stdin: R) -> AssertCommandResponse {
    match execute(args, stdin) {
        Ok(report) => report_response(report),
        Err(CommandError::InputUsage(message)) => AssertCommandResponse {
            exit_code: 3,
            payload: json!({
                "error": "input_usage_error",
                "message": message,
            }),
        },
        Err(CommandError::Internal(message)) => AssertCommandResponse {
            exit_code: 1,
            payload: json!({
                "error": "internal_error",
                "message": message,
            }),
        },
    }
}

fn report_response(report: AssertReport) -> AssertCommandResponse {
    let exit_code = if report.matched { 0 } else { 2 };
    match serde_json::to_value(&report) {
        Ok(payload) => AssertCommandResponse { exit_code, payload },
        Err(_) => AssertCommandResponse {
            exit_code: 1,
            payload: json!({
                "error": "internal_error",
                "message": "failed to serialize assert report"
            }),
        },
    }
}

fn execute<R: Read>(args: &AssertCommandArgs, stdin: R) -> Result<AssertReport, CommandError> {
    let rules = load_rules(args)?;
    let input_format = io::resolve_input_format(args.from, args.input.as_deref())
        .map_err(map_io_as_input_usage)?;
    let values = load_input_values(args, stdin, input_format)?;
    assert::execute_assert(&values, &rules).map_err(map_engine_error)
}

fn load_rules(args: &AssertCommandArgs) -> Result<AssertRules, CommandError> {
    let format = io::resolve_input_format(None, Some(args.rules.as_path())).map_err(|err| {
        CommandError::InputUsage(format!(
            "unable to resolve rules format from `{}`: {err}",
            args.rules.display()
        ))
    })?;
    let file = File::open(&args.rules).map_err(|err| {
        CommandError::InputUsage(format!(
            "failed to open rules file `{}`: {err}",
            args.rules.display()
        ))
    })?;
    let values = io::reader::read_values(file, format).map_err(map_io_as_input_usage)?;
    if values.len() != 1 {
        return Err(CommandError::InputUsage(
            "rules file must contain exactly one object".to_string(),
        ));
    }
    let rules_value = values.into_iter().next().unwrap_or(Value::Null);
    serde_json::from_value(rules_value)
        .map_err(|err| CommandError::InputUsage(format!("invalid rules schema: {err}")))
}

fn load_input_values<R: Read>(
    args: &AssertCommandArgs,
    stdin: R,
    format: Format,
) -> Result<Vec<Value>, CommandError> {
    if let Some(path) = &args.input {
        let file = File::open(path).map_err(|err| {
            CommandError::InputUsage(format!(
                "failed to open input file `{}`: {err}",
                path.display()
            ))
        })?;
        io::reader::read_values(file, format).map_err(map_io_as_input_usage)
    } else {
        io::reader::read_values(stdin, format).map_err(map_io_as_input_usage)
    }
}

fn map_io_as_input_usage(error: IoError) -> CommandError {
    CommandError::InputUsage(error.to_string())
}

fn map_engine_error(error: AssertValidationError) -> CommandError {
    match error {
        AssertValidationError::InputUsage(message) => CommandError::InputUsage(message),
        AssertValidationError::Internal(message) => CommandError::Internal(message),
    }
}

enum CommandError {
    InputUsage(String),
    Internal(String),
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use serde_json::json;
    use tempfile::tempdir;

    use crate::cmd::r#assert::{AssertCommandArgs, run_with_stdin};
    use crate::io::Format;

    #[test]
    fn maps_success_to_exit_zero() {
        let dir = tempdir().expect("tempdir");
        let rules_path = dir.path().join("rules.json");
        std::fs::write(
            &rules_path,
            r#"{
                "required_keys": ["id"],
                "types": {"id": "integer"},
                "count": {"min": 1, "max": 1},
                "ranges": {}
            }"#,
        )
        .expect("write rules");
        let args = AssertCommandArgs {
            input: None,
            from: Some(Format::Json),
            rules: rules_path,
        };

        let response = run_with_stdin(&args, Cursor::new(r#"[{"id":1}]"#));
        assert_eq!(response.exit_code, 0);
        assert_eq!(response.payload["matched"], json!(true));
    }

    #[test]
    fn maps_mismatch_to_exit_two() {
        let dir = tempdir().expect("tempdir");
        let rules_path = dir.path().join("rules.json");
        std::fs::write(
            &rules_path,
            r#"{
                "required_keys": ["id"],
                "types": {"id": "integer"},
                "count": {"min": 1, "max": 1},
                "ranges": {}
            }"#,
        )
        .expect("write rules");
        let args = AssertCommandArgs {
            input: None,
            from: Some(Format::Json),
            rules: rules_path,
        };

        let response = run_with_stdin(&args, Cursor::new(r#"[{"id":"oops"}]"#));
        assert_eq!(response.exit_code, 2);
        assert_eq!(response.payload["mismatch_count"], json!(1));
    }

    #[test]
    fn maps_input_usage_to_exit_three() {
        let dir = tempdir().expect("tempdir");
        let rules_path = dir.path().join("rules.invalid");
        std::fs::write(&rules_path, "{}").expect("write rules");
        let args = AssertCommandArgs {
            input: None,
            from: Some(Format::Json),
            rules: rules_path,
        };

        let response = run_with_stdin(&args, Cursor::new("[]"));
        assert_eq!(response.exit_code, 3);
        assert_eq!(response.payload["error"], json!("input_usage_error"));
    }
}
