use serde::Serialize;
use serde_json::{Value, json};

use crate::engine::emit_plan::{self, EmitPlanRequest};

/// Input arguments for `emit plan` execution API.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EmitPlanCommandArgs {
    pub command: String,
    pub args: Vec<String>,
}

/// Structured command response that carries exit-code mapping and JSON payload.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct EmitPlanCommandResponse {
    pub exit_code: i32,
    pub payload: Value,
}

/// Parse `--args` payload as a JSON array of strings.
pub fn parse_args_json(raw: Option<&str>) -> Result<Vec<String>, String> {
    let Some(raw) = raw else {
        return Ok(Vec::new());
    };

    let parsed: Value = serde_json::from_str(raw)
        .map_err(|error| format!("failed to parse `--args` as JSON: {error}"))?;
    let Value::Array(items) = parsed else {
        return Err("`--args` must be a JSON array of strings".to_string());
    };

    let mut values = Vec::with_capacity(items.len());
    for (index, item) in items.into_iter().enumerate() {
        let Value::String(text) = item else {
            return Err(format!(
                "`--args` must be a JSON array of strings (invalid item at index {index})"
            ));
        };
        values.push(text);
    }
    Ok(values)
}

pub fn run_plan(args: &EmitPlanCommandArgs) -> EmitPlanCommandResponse {
    match emit_plan::resolve(&EmitPlanRequest {
        command: args.command.clone(),
        args: args.args.clone(),
    }) {
        Ok(plan) => match serde_json::to_value(plan) {
            Ok(payload) => EmitPlanCommandResponse {
                exit_code: 0,
                payload,
            },
            Err(error) => EmitPlanCommandResponse {
                exit_code: 1,
                payload: json!({
                    "error": "internal_error",
                    "message": format!("failed to serialize emit plan payload: {error}"),
                }),
            },
        },
        Err(error) => EmitPlanCommandResponse {
            exit_code: 3,
            payload: json!({
                "error": "input_usage_error",
                "message": error.to_string(),
            }),
        },
    }
}

/// Ordered pipeline-step names used for `--emit-pipeline` diagnostics.
pub fn pipeline_steps() -> Vec<String> {
    vec![
        "emit_plan_parse".to_string(),
        "emit_plan_resolve".to_string(),
    ]
}

/// Determinism guards planned for the `emit plan` command.
pub fn deterministic_guards() -> Vec<String> {
    vec![
        "rust_native_execution".to_string(),
        "static_planner_without_external_execution".to_string(),
        "deterministic_stage_dependency_order".to_string(),
    ]
}
