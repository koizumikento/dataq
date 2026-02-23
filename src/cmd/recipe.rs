use std::path::PathBuf;

use serde::Serialize;
use serde_json::{Value, json};

use crate::engine::recipe::{self, RecipeExecutionErrorKind};

/// Input arguments for recipe run command execution API.
#[derive(Debug, Clone)]
pub struct RecipeCommandArgs {
    pub file: PathBuf,
}

/// Structured command response that carries exit-code mapping and JSON payload.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct RecipeCommandResponse {
    pub exit_code: i32,
    pub payload: Value,
}

/// Trace details used by `--emit-pipeline` for recipe execution stages.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RecipePipelineTrace {
    pub steps: Vec<String>,
}

pub fn run_with_trace(args: &RecipeCommandArgs) -> (RecipeCommandResponse, RecipePipelineTrace) {
    match recipe::run(args.file.as_path()) {
        Ok(execution) => {
            let exit_code = execution.report.exit_code;
            let payload = match serde_json::to_value(execution.report) {
                Ok(payload) => payload,
                Err(error) => {
                    return (
                        RecipeCommandResponse {
                            exit_code: 1,
                            payload: json!({
                                "error": "internal_error",
                                "message": format!("failed to serialize recipe report: {error}"),
                            }),
                        },
                        RecipePipelineTrace {
                            steps: execution.pipeline_steps,
                        },
                    );
                }
            };

            (
                RecipeCommandResponse { exit_code, payload },
                RecipePipelineTrace {
                    steps: execution.pipeline_steps,
                },
            )
        }
        Err(error) => {
            let response = match error.kind {
                RecipeExecutionErrorKind::InputUsage(message) => RecipeCommandResponse {
                    exit_code: 3,
                    payload: json!({
                        "error": "input_usage_error",
                        "message": message,
                    }),
                },
                RecipeExecutionErrorKind::Internal(message) => RecipeCommandResponse {
                    exit_code: 1,
                    payload: json!({
                        "error": "internal_error",
                        "message": message,
                    }),
                },
            };
            (
                response,
                RecipePipelineTrace {
                    steps: error.pipeline_steps,
                },
            )
        }
    }
}

/// Determinism guards planned for the `recipe run` command.
pub fn deterministic_guards() -> Vec<String> {
    vec![
        "rust_native_execution".to_string(),
        "no_shell_interpolation_for_user_input".to_string(),
        "recipe_step_order_from_definition".to_string(),
        "recipe_step_handoff_in_memory".to_string(),
    ]
}
