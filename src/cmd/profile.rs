use std::fs::File;
use std::io::Read;
use std::path::PathBuf;

use serde::Serialize;
use serde_json::{Value, json};

use crate::domain::error::ProfileError;
use crate::domain::report::ProfileReport;
use crate::engine::profile;
use crate::io::{self, Format};

/// Input arguments for profile command execution API.
#[derive(Debug, Clone)]
pub struct ProfileCommandArgs {
    pub input: Option<PathBuf>,
    pub from: Option<Format>,
}

/// Structured command response that carries exit-code mapping and JSON payload.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct ProfileCommandResponse {
    pub exit_code: i32,
    pub payload: Value,
}

pub fn run_with_stdin<R: Read>(args: &ProfileCommandArgs, stdin: R) -> ProfileCommandResponse {
    match execute(args, stdin).and_then(serialize_report) {
        Ok(payload) => ProfileCommandResponse {
            exit_code: 0,
            payload,
        },
        Err(ProfileError::SerializeReport { source: _ }) => ProfileCommandResponse {
            exit_code: 1,
            payload: json!({
                "error": "internal_error",
                "message": "failed to serialize profile report",
            }),
        },
        Err(error) => ProfileCommandResponse {
            exit_code: 3,
            payload: json!({
                "error": "input_usage_error",
                "message": error.to_string(),
            }),
        },
    }
}

fn execute<R: Read>(args: &ProfileCommandArgs, stdin: R) -> Result<ProfileReport, ProfileError> {
    let input_format = io::resolve_input_format(args.from, args.input.as_deref())
        .map_err(|source| ProfileError::ResolveInput { source })?;
    let values = load_input_values(args, stdin, input_format)?;
    Ok(profile::profile_values(&values))
}

fn serialize_report(report: ProfileReport) -> Result<Value, ProfileError> {
    serde_json::to_value(report).map_err(|source| ProfileError::SerializeReport { source })
}

fn load_input_values<R: Read>(
    args: &ProfileCommandArgs,
    stdin: R,
    format: Format,
) -> Result<Vec<Value>, ProfileError> {
    if let Some(path) = &args.input {
        let file = File::open(path).map_err(|source| ProfileError::OpenInput {
            path: path.display().to_string(),
            source,
        })?;
        io::reader::read_values(file, format)
            .map_err(|source| ProfileError::ReadInput { format, source })
    } else {
        io::reader::read_values(stdin, format)
            .map_err(|source| ProfileError::ReadInput { format, source })
    }
}

/// Ordered pipeline-step names used for `--emit-pipeline` diagnostics.
pub fn pipeline_steps() -> Vec<String> {
    vec![
        "resolve_profile_input".to_string(),
        "read_profile_values".to_string(),
        "compute_profile_summary".to_string(),
        "write_profile_report".to_string(),
    ]
}

/// Determinism guards planned for the `profile` command.
pub fn deterministic_guards() -> Vec<String> {
    vec![
        "rust_native_execution".to_string(),
        "no_shell_interpolation_for_user_input".to_string(),
        "deterministic_summary_key_ordering".to_string(),
    ]
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use serde_json::json;

    use super::{ProfileCommandArgs, run_with_stdin};
    use crate::io::Format;

    #[test]
    fn profile_api_success_with_json_stdin() {
        let args = ProfileCommandArgs {
            input: None,
            from: Some(Format::Json),
        };
        let response = run_with_stdin(
            &args,
            Cursor::new(r#"[{"id":1,"active":true},{"id":null}]"#),
        );

        assert_eq!(response.exit_code, 0);
        assert_eq!(response.payload["record_count"], json!(2));
        assert_eq!(response.payload["field_count"], json!(2));
        assert_eq!(
            response.payload["fields"]["$[\"id\"]"]["null_ratio"],
            json!(0.5)
        );
        assert_eq!(
            response.payload["fields"]["$[\"id\"]"]["numeric_stats"]["count"],
            json!(1)
        );
        assert_eq!(
            response.payload["fields"]["$[\"active\"]"]["numeric_stats"],
            json!(null)
        );
    }

    #[test]
    fn profile_api_reports_input_usage_errors() {
        let args = ProfileCommandArgs {
            input: None,
            from: Some(Format::Json),
        };
        let response = run_with_stdin(&args, Cursor::new("{"));

        assert_eq!(response.exit_code, 3);
        assert_eq!(response.payload["error"], json!("input_usage_error"));
    }
}
