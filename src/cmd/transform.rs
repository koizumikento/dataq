use std::fs::File;
use std::path::Path;
use std::path::PathBuf;

use serde::Serialize;
use serde_json::{Value, json};

use crate::cmd::stage_trace;
use crate::domain::report::PipelineStageDiagnostic;
use crate::engine::transform::{self, TransformRowsetError, TransformSqlError};
use crate::io;

/// SQL execution engine used for transform rowset stage 2.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransformRowsetSqlEngine {
    Sqlite,
}

impl TransformRowsetSqlEngine {
    const fn pipeline_stage_step(self) -> &'static str {
        match self {
            Self::Sqlite => "transform_rowset_mlr",
        }
    }

    const fn pipeline_tool_label(self) -> &'static str {
        match self {
            Self::Sqlite => "sqlite",
        }
    }

    const fn error_label(self) -> &'static str {
        match self {
            Self::Sqlite => "sqlite",
        }
    }
}

/// Input arguments for `transform rowset` execution.
#[derive(Debug, Clone)]
pub struct TransformRowsetCommandArgs {
    pub input: TransformRowsetCommandInput,
    pub jq_filter: String,
    pub mlr: Vec<String>,
}

/// Input arguments for `transform sql` execution.
#[derive(Debug, Clone)]
pub struct TransformSqlCommandArgs {
    pub input: TransformSqlCommandInput,
    pub sql: String,
}

/// Input source descriptor for `transform rowset`.
#[derive(Debug, Clone)]
pub enum TransformRowsetCommandInput {
    Path(PathBuf),
    Inline(Vec<Value>),
}

/// Input source descriptor for `transform sql`.
pub type TransformSqlCommandInput = TransformRowsetCommandInput;

/// Structured command response that carries exit-code mapping and JSON payload.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct TransformRowsetCommandResponse {
    pub exit_code: i32,
    pub payload: Value,
}

/// Structured command response that carries exit-code mapping and JSON payload.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct TransformSqlCommandResponse {
    pub exit_code: i32,
    pub payload: Value,
}

/// Trace details used by `--emit-pipeline` for transform stages.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TransformRowsetPipelineTrace {
    pub used_tools: Vec<String>,
    pub stage_diagnostics: Vec<PipelineStageDiagnostic>,
}

/// Trace details used by `--emit-pipeline` for transform SQL stages.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TransformSqlPipelineTrace {
    pub used_tools: Vec<String>,
    pub stage_diagnostics: Vec<PipelineStageDiagnostic>,
}

impl TransformRowsetPipelineTrace {
    fn mark_tool_used(&mut self, tool: &'static str) {
        if self.used_tools.iter().any(|used| used == tool) {
            return;
        }
        self.used_tools.push(tool.to_string());
    }
}

impl TransformSqlPipelineTrace {
    fn mark_tool_used(&mut self, tool: &'static str) {
        if self.used_tools.iter().any(|used| used == tool) {
            return;
        }
        self.used_tools.push(tool.to_string());
    }
}

pub fn run_rowset_with_trace(
    args: &TransformRowsetCommandArgs,
) -> (TransformRowsetCommandResponse, TransformRowsetPipelineTrace) {
    run_rowset_with_sql_engine_trace(args, TransformRowsetSqlEngine::Sqlite)
}

pub fn run_rowset_with_sql_engine_trace(
    args: &TransformRowsetCommandArgs,
    sql_engine: TransformRowsetSqlEngine,
) -> (TransformRowsetCommandResponse, TransformRowsetPipelineTrace) {
    let mut trace = TransformRowsetPipelineTrace::default();
    let values = match resolve_input_rows(&args.input) {
        Ok(values) => values,
        Err(message) => {
            return (
                TransformRowsetCommandResponse {
                    exit_code: 3,
                    payload: json!({
                        "error": "input_usage_error",
                        "message": message,
                    }),
                },
                trace,
            );
        }
    };

    match transform::execute_rowset(&values, &args.jq_filter, &args.mlr) {
        Ok(result) => {
            trace.mark_tool_used("jq");
            trace.mark_tool_used(sql_engine.pipeline_tool_label());
            trace
                .stage_diagnostics
                .push(PipelineStageDiagnostic::success(
                    1,
                    "transform_rowset_jq",
                    "jq",
                    result.jq_input_records,
                    result.jq_output_records,
                ));
            trace
                .stage_diagnostics
                .push(PipelineStageDiagnostic::success(
                    2,
                    sql_engine.pipeline_stage_step(),
                    sql_engine.pipeline_tool_label(),
                    result.jq_output_records,
                    result.mlr_output_records,
                ));
            (
                TransformRowsetCommandResponse {
                    exit_code: 0,
                    payload: Value::Array(result.rows),
                },
                trace,
            )
        }
        Err(TransformRowsetError::Jq {
            input_records,
            source,
        }) => {
            trace.mark_tool_used("jq");
            trace
                .stage_diagnostics
                .push(PipelineStageDiagnostic::failure(
                    1,
                    "transform_rowset_jq",
                    "jq",
                    input_records,
                ));
            (
                TransformRowsetCommandResponse {
                    exit_code: 3,
                    payload: json!({
                        "error": "input_usage_error",
                        "message": format!("failed to transform rowset with jq: {source}"),
                    }),
                },
                trace,
            )
        }
        Err(TransformRowsetError::Mlr {
            jq_input_records,
            jq_output_records,
            source,
        }) => {
            trace.mark_tool_used("jq");
            trace.mark_tool_used(sql_engine.pipeline_tool_label());
            trace
                .stage_diagnostics
                .push(PipelineStageDiagnostic::success(
                    1,
                    "transform_rowset_jq",
                    "jq",
                    jq_input_records,
                    jq_output_records,
                ));
            trace
                .stage_diagnostics
                .push(PipelineStageDiagnostic::failure(
                    2,
                    sql_engine.pipeline_stage_step(),
                    sql_engine.pipeline_tool_label(),
                    jq_output_records,
                ));
            (
                TransformRowsetCommandResponse {
                    exit_code: 3,
                    payload: json!({
                        "error": "input_usage_error",
                        "message": format!(
                            "failed to transform rowset with {}: {source}",
                            sql_engine.error_label()
                        ),
                    }),
                },
                trace,
            )
        }
        Err(error) => (
            TransformRowsetCommandResponse {
                exit_code: 3,
                payload: json!({
                    "error": "input_usage_error",
                    "message": error.to_string(),
                }),
            },
            trace,
        ),
    }
}

pub fn run_sql_with_trace<F, E>(
    args: &TransformSqlCommandArgs,
    execute_duckdb: F,
) -> (TransformSqlCommandResponse, TransformSqlPipelineTrace)
where
    F: FnOnce(&[Value], &str) -> Result<Vec<Value>, E>,
    E: std::fmt::Display,
{
    let mut trace = TransformSqlPipelineTrace::default();
    let values = match resolve_input_rows(&args.input) {
        Ok(values) => values,
        Err(message) => {
            return (
                TransformSqlCommandResponse {
                    exit_code: 3,
                    payload: json!({
                        "error": "input_usage_error",
                        "message": message,
                    }),
                },
                trace,
            );
        }
    };

    if args.sql.trim().is_empty() {
        return (
            TransformSqlCommandResponse {
                exit_code: 3,
                payload: json!({
                    "error": "input_usage_error",
                    "message": TransformSqlError::InvalidSql.to_string(),
                }),
            },
            trace,
        );
    }

    let (result, diagnostic) = stage_trace::run_value_stage(
        1,
        "transform_sql_duckdb",
        "duckdb",
        &[values.as_slice()],
        || transform::execute_sql_with_duckdb_hook(&values, &args.sql, execute_duckdb),
    );

    trace.mark_tool_used("duckdb");
    trace.stage_diagnostics.push(diagnostic);

    match result {
        Ok(rows) => (
            TransformSqlCommandResponse {
                exit_code: 0,
                payload: Value::Array(rows),
            },
            trace,
        ),
        Err(error) => (
            TransformSqlCommandResponse {
                exit_code: 3,
                payload: json!({
                    "error": "input_usage_error",
                    "message": error.to_string(),
                }),
            },
            trace,
        ),
    }
}

fn resolve_input_rows(source: &TransformRowsetCommandInput) -> Result<Vec<Value>, String> {
    match source {
        TransformRowsetCommandInput::Path(path) => load_input_rows(path.as_path()),
        TransformRowsetCommandInput::Inline(values) => Ok(values.clone()),
    }
}

fn load_input_rows(path: &Path) -> Result<Vec<Value>, String> {
    let format = io::resolve_input_format(None, Some(path)).map_err(|error| {
        format!(
            "unable to resolve input format from `{}`: {error}",
            path.display()
        )
    })?;
    let file = File::open(path)
        .map_err(|error| format!("failed to open input file `{}`: {error}", path.display()))?;
    io::reader::read_values(file, format).map_err(|error| format!("failed to read input: {error}"))
}

/// Ordered pipeline-step names used for `--emit-pipeline` diagnostics.
pub fn pipeline_steps() -> Vec<String> {
    vec![
        "transform_rowset_jq".to_string(),
        "transform_rowset_mlr".to_string(),
    ]
}

/// Determinism guards planned for the `transform rowset` command.
pub fn deterministic_guards() -> Vec<String> {
    vec![
        "jq_execution_with_explicit_arg_arrays".to_string(),
        "mlr_execution_with_explicit_arg_arrays".to_string(),
        "deterministic_row_sort_before_and_after_mlr".to_string(),
        "canonical_float_formatting_for_output".to_string(),
    ]
}

/// Ordered pipeline-step names used for `transform sql --emit-pipeline` diagnostics.
pub fn sql_pipeline_steps() -> Vec<String> {
    vec!["transform_sql_duckdb".to_string()]
}

/// Determinism guards planned for the `transform sql` command.
pub fn sql_deterministic_guards() -> Vec<String> {
    vec![
        "duckdb_execution_via_adapter_hooks".to_string(),
        "no_shell_interpolation_for_user_input".to_string(),
        "deterministic_row_sort_after_duckdb".to_string(),
        "canonical_float_formatting_for_output".to_string(),
    ]
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{TransformSqlCommandArgs, TransformSqlCommandInput, run_sql_with_trace};

    #[test]
    fn transform_sql_success_is_machine_readable_and_tracks_duckdb_stage() {
        let args = TransformSqlCommandArgs {
            input: TransformSqlCommandInput::Inline(vec![
                json!({"team": "a", "price": 10}),
                json!({"team": "b", "price": 7}),
            ]),
            sql: "select * from input order by team".to_string(),
        };

        let (response, trace) = run_sql_with_trace(
            &args,
            |_rows, _sql| -> Result<Vec<serde_json::Value>, String> {
                Ok(vec![
                    json!({"team": "z", "avg": 7.5}),
                    json!({"team": "a", "avg": 7.0}),
                ])
            },
        );

        assert_eq!(response.exit_code, 0);
        assert_eq!(
            response.payload,
            json!([
                {"avg": 7.0, "team": "a"},
                {"avg": 7.5, "team": "z"}
            ])
        );
        assert_eq!(trace.used_tools, vec!["duckdb".to_string()]);
        assert_eq!(trace.stage_diagnostics.len(), 1);
        assert_eq!(trace.stage_diagnostics[0].step, "transform_sql_duckdb");
        assert_eq!(trace.stage_diagnostics[0].tool, "duckdb");
        assert_eq!(trace.stage_diagnostics[0].input_records, 2);
        assert_eq!(trace.stage_diagnostics[0].output_records, 2);
        assert_eq!(trace.stage_diagnostics[0].status, "ok");
    }

    #[test]
    fn transform_sql_usage_errors_map_to_exit_three() {
        let args = TransformSqlCommandArgs {
            input: TransformSqlCommandInput::Inline(vec![json!({"team": "a"})]),
            sql: " ".to_string(),
        };

        let (response, trace) = run_sql_with_trace(
            &args,
            |_rows, _sql| -> Result<Vec<serde_json::Value>, String> {
                panic!("hook should not run when sql is invalid")
            },
        );

        assert_eq!(response.exit_code, 3);
        assert_eq!(response.payload["error"], json!("input_usage_error"));
        assert_eq!(
            response.payload["message"],
            json!("`--sql` cannot be empty")
        );
        assert!(trace.used_tools.is_empty());
        assert!(trace.stage_diagnostics.is_empty());
    }

    #[test]
    fn transform_sql_adapter_failures_map_to_exit_three_with_stage_diagnostics() {
        let args = TransformSqlCommandArgs {
            input: TransformSqlCommandInput::Inline(vec![json!({"team": "a"})]),
            sql: "select * from input".to_string(),
        };

        let (response, trace) = run_sql_with_trace(
            &args,
            |_rows, _sql| -> Result<Vec<serde_json::Value>, String> {
                Err("duckdb is not available in PATH".to_string())
            },
        );

        assert_eq!(response.exit_code, 3);
        assert_eq!(response.payload["error"], json!("input_usage_error"));
        assert_eq!(
            response.payload["message"],
            json!("failed to transform rowset with duckdb: duckdb is not available in PATH")
        );
        assert_eq!(trace.used_tools, vec!["duckdb".to_string()]);
        assert_eq!(trace.stage_diagnostics.len(), 1);
        assert_eq!(trace.stage_diagnostics[0].step, "transform_sql_duckdb");
        assert_eq!(trace.stage_diagnostics[0].tool, "duckdb");
        assert_eq!(trace.stage_diagnostics[0].input_records, 1);
        assert_eq!(trace.stage_diagnostics[0].output_records, 0);
        assert_eq!(trace.stage_diagnostics[0].status, "error");
    }
}
