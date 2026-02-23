use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};

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
    pub rules: Option<PathBuf>,
    pub schema: Option<PathBuf>,
}

/// Structured command response that carries exit-code mapping and JSON payload.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct AssertCommandResponse {
    pub exit_code: i32,
    pub payload: Value,
}

/// Ordered pipeline-step names used for `--emit-pipeline` diagnostics.
pub fn pipeline_steps() -> Vec<String> {
    vec![
        "load_rules".to_string(),
        "resolve_input_format".to_string(),
        "read_input_values".to_string(),
        "validate_assert_rules".to_string(),
    ]
}

/// Determinism guards applied by the `assert` command.
pub fn deterministic_guards() -> Vec<String> {
    vec![
        "rust_native_execution".to_string(),
        "no_shell_interpolation_for_user_input".to_string(),
        "rules_schema_deny_unknown_fields".to_string(),
    ]
}

/// Machine-readable help payload for `assert --rules` rule files.
pub fn rules_help_payload() -> Value {
    json!({
        "schema": "dataq.assert.rules.v1",
        "description": "Rule file schema for `dataq assert --rules`",
        "top_level_keys": {
            "required_keys": "array<string>",
            "forbid_keys": "array<string>",
            "fields": "object<string, field_rule>",
            "count": {
                "min": "usize (optional)",
                "max": "usize (optional)"
            }
        },
        "field_rule": {
            "type": "string|number|integer|boolean|object|array|null (optional)",
            "nullable": "bool (optional)",
            "enum": "array<any> (optional)",
            "pattern": "string regex (optional)",
            "range": {
                "min": "number (optional)",
                "max": "number (optional)"
            }
        },
        "path_notation": "dot-delimited object path (example: meta.blocked)",
        "constraints": [
            "fields.<path> must define at least one of type/nullable/enum/pattern/range",
            "count.min must be <= count.max",
            "fields.<path>.range.min must be <= fields.<path>.range.max",
            "unknown keys are rejected"
        ],
        "example": {
            "required_keys": ["id", "status"],
            "forbid_keys": ["debug", "meta.blocked"],
            "fields": {
                "id": {
                    "type": "integer"
                },
                "score": {
                    "type": "number",
                    "nullable": true,
                    "range": {
                        "min": 0,
                        "max": 100
                    }
                },
                "status": {
                    "enum": ["active", "archived"]
                },
                "name": {
                    "pattern": "^[a-z]+_[0-9]+$"
                }
            },
            "count": {
                "min": 1,
                "max": 1000
            }
        }
    })
}

/// Machine-readable help payload for `assert --schema` JSON Schema mode.
pub fn schema_help_payload() -> Value {
    json!({
        "schema": "dataq.assert.schema_help.v1",
        "description": "JSON Schema validation help for `dataq assert --schema`",
        "mode": {
            "validator": "jsonschema crate (Rust native)",
            "input_contract": "schema file must contain exactly one JSON/YAML value",
            "source_selection": "`--schema` and `--rules` are mutually exclusive"
        },
        "usage": [
            "dataq assert --schema schema.json < input.json",
            "dataq assert --input input.json --schema schema.json"
        ],
        "result_contract": {
            "exit_code_0": "all rows matched schema",
            "exit_code_2": "one or more mismatches",
            "exit_code_3": "input/usage error (for example invalid schema)"
        },
        "mismatch_shape": {
            "path": "$[row].<field> (canonicalized from JSON Pointer)",
            "rule_kind": "schema",
            "reason": "schema_mismatch",
            "actual": "actual value at instance path",
            "expected": {
                "schema_path": "JSON Pointer into schema",
                "message": "validator error message"
            }
        },
        "example_schema": {
            "type": "object",
            "required": ["id", "score"],
            "properties": {
                "id": { "type": "integer" },
                "score": { "type": "number", "maximum": 10 }
            }
        }
    })
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
    let source = resolve_validation_source(args)?;
    let input_format = io::resolve_input_format(args.from, args.input.as_deref())
        .map_err(map_io_as_input_usage)?;
    let values = load_input_values(args, stdin, input_format)?;
    match source {
        ValidationSource::Rules(rules) => assert::execute_assert(&values, &rules),
        ValidationSource::Schema(schema) => assert::execute_assert_with_schema(&values, &schema),
    }
    .map_err(map_engine_error)
}

enum ValidationSource {
    Rules(AssertRules),
    Schema(Value),
}

fn resolve_validation_source(args: &AssertCommandArgs) -> Result<ValidationSource, CommandError> {
    match (&args.rules, &args.schema) {
        (Some(_), Some(_)) => Err(CommandError::InputUsage(
            "`--rules` and `--schema` are mutually exclusive".to_string(),
        )),
        (None, None) => Err(CommandError::InputUsage(
            "either `--rules` or `--schema` must be provided".to_string(),
        )),
        (Some(rules_path), None) => load_rules(rules_path.as_path()).map(ValidationSource::Rules),
        (None, Some(schema_path)) => {
            load_schema(schema_path.as_path()).map(ValidationSource::Schema)
        }
    }
}

fn load_rules(path: &Path) -> Result<AssertRules, CommandError> {
    let format = io::resolve_input_format(None, Some(path)).map_err(|err| {
        CommandError::InputUsage(format!(
            "unable to resolve rules format from `{}`: {err}",
            path.display()
        ))
    })?;
    let file = File::open(path).map_err(|err| {
        CommandError::InputUsage(format!(
            "failed to open rules file `{}`: {err}",
            path.display()
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

fn load_schema(path: &Path) -> Result<Value, CommandError> {
    let format = io::resolve_input_format(None, Some(path)).map_err(|err| {
        CommandError::InputUsage(format!(
            "unable to resolve schema format from `{}`: {err}",
            path.display()
        ))
    })?;
    let file = File::open(path).map_err(|err| {
        CommandError::InputUsage(format!(
            "failed to open schema file `{}`: {err}",
            path.display()
        ))
    })?;
    let values = io::reader::read_values(file, format).map_err(map_io_as_input_usage)?;
    if values.len() != 1 {
        return Err(CommandError::InputUsage(
            "schema file must contain exactly one value".to_string(),
        ));
    }
    Ok(values.into_iter().next().unwrap_or(Value::Null))
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
                "fields": {"id": {"type": "integer"}},
                "count": {"min": 1, "max": 1},
                "forbid_keys": []
            }"#,
        )
        .expect("write rules");
        let args = AssertCommandArgs {
            input: None,
            from: Some(Format::Json),
            rules: Some(rules_path),
            schema: None,
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
                "fields": {"id": {"type": "integer"}},
                "count": {"min": 1, "max": 1},
                "forbid_keys": []
            }"#,
        )
        .expect("write rules");
        let args = AssertCommandArgs {
            input: None,
            from: Some(Format::Json),
            rules: Some(rules_path),
            schema: None,
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
            rules: Some(rules_path),
            schema: None,
        };

        let response = run_with_stdin(&args, Cursor::new("[]"));
        assert_eq!(response.exit_code, 3);
        assert_eq!(response.payload["error"], json!("input_usage_error"));
    }

    #[test]
    fn maps_schema_mismatch_to_exit_two() {
        let dir = tempdir().expect("tempdir");
        let schema_path = dir.path().join("schema.json");
        std::fs::write(
            &schema_path,
            r#"{
                "type": "object",
                "required": ["id"],
                "properties": {
                    "id": {"type": "integer"}
                }
            }"#,
        )
        .expect("write schema");
        let args = AssertCommandArgs {
            input: None,
            from: Some(Format::Json),
            rules: None,
            schema: Some(schema_path),
        };

        let response = run_with_stdin(&args, Cursor::new(r#"[{"id":"oops"}]"#));
        assert_eq!(response.exit_code, 2);
        assert_eq!(response.payload["mismatch_count"], json!(1));
        assert_eq!(
            response.payload["mismatches"][0]["reason"],
            json!("schema_mismatch")
        );
    }

    #[test]
    fn maps_rules_schema_conflict_to_exit_three() {
        let dir = tempdir().expect("tempdir");
        let rules_path = dir.path().join("rules.json");
        let schema_path = dir.path().join("schema.json");
        std::fs::write(&rules_path, "{}").expect("write rules");
        std::fs::write(&schema_path, "{}").expect("write schema");
        let args = AssertCommandArgs {
            input: None,
            from: Some(Format::Json),
            rules: Some(rules_path),
            schema: Some(schema_path),
        };

        let response = run_with_stdin(&args, Cursor::new("[]"));
        assert_eq!(response.exit_code, 3);
        assert_eq!(response.payload["error"], json!("input_usage_error"));
    }
}
