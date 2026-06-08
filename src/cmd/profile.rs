use std::collections::BTreeSet;
use std::fs::File;
use std::io::Read;
use std::path::PathBuf;

use serde::Serialize;
use serde_json::{Value, json};

use crate::domain::error::ProfileError;
use crate::domain::report::{PipelineStageDiagnostic, ProfileReport};
use crate::domain::value_path::ValuePath;
use crate::engine::profile;
use crate::io::{self, Format};

/// Input arguments for profile command execution API.
#[derive(Debug, Clone)]
pub struct ProfileCommandArgs {
    pub input: Option<PathBuf>,
    pub from: Option<Format>,
    pub fields: Vec<String>,
    pub allow_missing_fields: bool,
}

/// Structured command response that carries exit-code mapping and JSON payload.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct ProfileCommandResponse {
    pub exit_code: i32,
    pub payload: Value,
}

/// Trace details used by `--emit-pipeline` for profile stages.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ProfilePipelineTrace {
    pub used_tools: Vec<String>,
    pub stage_diagnostics: Vec<PipelineStageDiagnostic>,
}

impl ProfilePipelineTrace {
    fn mark_tool_used(&mut self, tool: &'static str) {
        if self.used_tools.iter().any(|used| used == tool) {
            return;
        }
        self.used_tools.push(tool.to_string());
    }
}

pub fn run_with_stdin<R: Read>(args: &ProfileCommandArgs, stdin: R) -> ProfileCommandResponse {
    run_with_stdin_and_trace(args, stdin).0
}

pub fn run_with_stdin_and_trace<R: Read>(
    args: &ProfileCommandArgs,
    stdin: R,
) -> (ProfileCommandResponse, ProfilePipelineTrace) {
    let mut trace = ProfilePipelineTrace::default();
    let response = match execute(args, stdin, &mut trace).and_then(serialize_report) {
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
    };
    (response, trace)
}

fn execute<R: Read>(
    args: &ProfileCommandArgs,
    stdin: R,
    trace: &mut ProfilePipelineTrace,
) -> Result<ProfileReport, ProfileError> {
    let projection_fields = resolve_projection_fields(&args.fields)?;
    let input_format = io::resolve_input_format(args.from, args.input.as_deref())
        .map_err(|source| ProfileError::ResolveInput { source })?;
    let values = load_input_values(args, stdin, input_format)?;
    let report = profile_report_from_values(&values, input_format, trace)?;
    if projection_fields.is_empty() {
        return Ok(report);
    }
    profile::project_report(report, &projection_fields, args.allow_missing_fields)
        .map_err(|fields| ProfileError::MissingProjectedFields { fields })
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

fn profile_report_from_values(
    values: &[Value],
    input_format: Format,
    trace: &mut ProfilePipelineTrace,
) -> Result<ProfileReport, ProfileError> {
    if input_format == Format::Csv {
        match profile::normalize_qsv_profile_rows(values) {
            Ok(Some(report)) => {
                trace.mark_tool_used("qsv");
                trace
                    .stage_diagnostics
                    .push(PipelineStageDiagnostic::success(
                        1,
                        "profile_qsv_normalize",
                        "qsv",
                        values.len(),
                        report.field_count,
                    ));
                return Ok(report);
            }
            Ok(None) => {}
            Err(message) => {
                trace.mark_tool_used("qsv");
                trace
                    .stage_diagnostics
                    .push(PipelineStageDiagnostic::failure(
                        1,
                        "profile_qsv_normalize",
                        "qsv",
                        values.len(),
                    ));
                return Err(ProfileError::QsvNormalize { message });
            }
        }
    }

    Ok(profile::profile_values(values))
}

fn resolve_projection_fields(raw_fields: &[String]) -> Result<Vec<String>, ProfileError> {
    let mut fields = BTreeSet::new();
    for raw_field in raw_fields {
        if raw_field.is_empty() {
            return Err(ProfileError::InvalidProjectionField {
                field: raw_field.clone(),
                source: ValuePath::parse_canonical(raw_field)
                    .expect_err("empty field is not a valid canonical path"),
            });
        }
        let path = if raw_field.starts_with('$') {
            ValuePath::parse_canonical(raw_field).map_err(|source| {
                ProfileError::InvalidProjectionField {
                    field: raw_field.clone(),
                    source,
                }
            })?
        } else {
            ValuePath::object_key(raw_field.clone())
        };
        fields.insert(path.to_string());
    }
    Ok(fields.into_iter().collect())
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

    use super::{ProfileCommandArgs, run_with_stdin, run_with_stdin_and_trace};
    use crate::io::Format;

    #[test]
    fn profile_api_success_with_json_stdin() {
        let args = ProfileCommandArgs {
            input: None,
            from: Some(Format::Json),
            fields: Vec::new(),
            allow_missing_fields: false,
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
            fields: Vec::new(),
            allow_missing_fields: false,
        };
        let response = run_with_stdin(&args, Cursor::new("{"));

        assert_eq!(response.exit_code, 3);
        assert_eq!(response.payload["error"], json!("input_usage_error"));
    }

    #[test]
    fn profile_api_normalizes_qsv_csv_rows_with_trace() {
        let args = ProfileCommandArgs {
            input: None,
            from: Some(Format::Csv),
            fields: Vec::new(),
            allow_missing_fields: false,
        };
        let input = "field,type,nullcount,cardinality,record_count,min,max,mean,q2_median,p95\n\
id,Integer,1,3,4,1,4,2.333333,2,4\n\
flag,String,2,2,4,,,,,\n";
        let (response, trace) = run_with_stdin_and_trace(&args, Cursor::new(input));

        assert_eq!(response.exit_code, 0);
        assert_eq!(response.payload["record_count"], json!(4));
        assert_eq!(
            response.payload["fields"]["$[\"id\"]"]["type_distribution"]["number"],
            json!(3)
        );
        assert_eq!(trace.used_tools, vec!["qsv".to_string()]);
        assert_eq!(trace.stage_diagnostics.len(), 1);
        assert_eq!(trace.stage_diagnostics[0].step, "profile_qsv_normalize");
        assert_eq!(trace.stage_diagnostics[0].status, "ok");
    }
}
