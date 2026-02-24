use std::collections::BTreeMap;
use std::path::PathBuf;

use serde::Serialize;
use serde_json::{Value, json};

use crate::engine::recipe::{self, RecipeExecutionErrorKind};

/// Input arguments for recipe run command execution API.
#[derive(Debug, Clone)]
pub struct RecipeCommandArgs {
    pub file_path: Option<PathBuf>,
    pub recipe: Option<Value>,
    pub base_dir: Option<PathBuf>,
}

/// Input arguments for `recipe replay` command execution API.
#[derive(Debug, Clone)]
pub struct RecipeReplayCommandArgs {
    pub file_path: PathBuf,
    pub lock_path: PathBuf,
    pub strict: bool,
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

/// Input arguments for recipe lock command execution API.
#[derive(Debug, Clone)]
pub struct RecipeLockCommandArgs {
    pub file_path: PathBuf,
}

/// Trace details used by `--emit-pipeline` for recipe lock stages.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RecipeLockPipelineTrace {
    pub steps: Vec<String>,
    pub tool_versions: BTreeMap<String, String>,
}

pub fn run_with_trace(args: &RecipeCommandArgs) -> (RecipeCommandResponse, RecipePipelineTrace) {
    let execution = match (&args.file_path, &args.recipe) {
        (Some(_), Some(_)) => {
            return (
                RecipeCommandResponse {
                    exit_code: 3,
                    payload: json!({
                        "error": "input_usage_error",
                        "message": "`file_path` and inline `recipe` are mutually exclusive",
                    }),
                },
                RecipePipelineTrace::default(),
            );
        }
        (None, None) => {
            return (
                RecipeCommandResponse {
                    exit_code: 3,
                    payload: json!({
                        "error": "input_usage_error",
                        "message": "either `file_path` or inline `recipe` must be provided",
                    }),
                },
                RecipePipelineTrace::default(),
            );
        }
        (Some(file_path), None) => recipe::run(file_path.as_path()),
        (None, Some(recipe_value)) => {
            recipe::run_from_value(recipe_value.clone(), args.base_dir.as_deref())
        }
    };

    match execution {
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
        Err(error) => (
            map_execution_error_response(error.kind),
            RecipePipelineTrace {
                steps: error.pipeline_steps,
            },
        ),
    }
}

pub fn lock_with_trace(
    args: &RecipeLockCommandArgs,
) -> (
    RecipeCommandResponse,
    RecipeLockPipelineTrace,
    Option<Vec<u8>>,
) {
    match recipe::lock(args.file_path.as_path()) {
        Ok(execution) => {
            let payload = match serde_json::from_slice::<Value>(&execution.serialized) {
                Ok(payload) => payload,
                Err(error) => {
                    return (
                        RecipeCommandResponse {
                            exit_code: 1,
                            payload: json!({
                                "error": "internal_error",
                                "message": format!("failed to decode recipe lock payload: {error}"),
                            }),
                        },
                        RecipeLockPipelineTrace {
                            steps: execution.pipeline_steps,
                            tool_versions: execution.tool_versions,
                        },
                        None,
                    );
                }
            };

            (
                RecipeCommandResponse {
                    exit_code: 0,
                    payload,
                },
                RecipeLockPipelineTrace {
                    steps: execution.pipeline_steps,
                    tool_versions: execution.tool_versions,
                },
                Some(execution.serialized),
            )
        }
        Err(error) => (
            map_execution_error_response(error.kind),
            RecipeLockPipelineTrace {
                steps: error.pipeline_steps,
                tool_versions: BTreeMap::new(),
            },
            None,
        ),
    }
}

pub fn replay_with_trace(
    args: &RecipeReplayCommandArgs,
) -> (RecipeCommandResponse, RecipePipelineTrace) {
    let execution = recipe::replay(
        args.file_path.as_path(),
        args.lock_path.as_path(),
        args.strict,
    );

    match execution {
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
                                "message": format!(
                                    "failed to serialize recipe replay report: {error}"
                                ),
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
        Err(error) => (
            map_execution_error_response(error.kind),
            RecipePipelineTrace {
                steps: error.pipeline_steps,
            },
        ),
    }
}

fn map_execution_error_response(error: RecipeExecutionErrorKind) -> RecipeCommandResponse {
    match error {
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
    }
}

/// Determinism guards planned for the `recipe run` command.
pub fn deterministic_guards_run() -> Vec<String> {
    vec![
        "rust_native_execution".to_string(),
        "no_shell_interpolation_for_user_input".to_string(),
        "recipe_step_order_from_definition".to_string(),
        "recipe_step_handoff_in_memory".to_string(),
    ]
}

/// Determinism guards planned for the `recipe lock` command.
pub fn deterministic_guards_lock() -> Vec<String> {
    vec![
        "rust_native_execution".to_string(),
        "fixed_recipe_lock_tool_probe_order".to_string(),
        "canonical_recipe_lock_serialization".to_string(),
    ]
}

/// Backward-compatible alias for recipe run guards.
pub fn deterministic_guards() -> Vec<String> {
    deterministic_guards_run()
}

/// Determinism guards planned for the `recipe replay` command.
pub fn deterministic_guards_replay() -> Vec<String> {
    vec![
        "rust_native_execution".to_string(),
        "no_shell_interpolation_for_user_input".to_string(),
        "recipe_replay_lock_constraints_checked_in_fixed_order".to_string(),
        "recipe_step_order_from_definition".to_string(),
        "recipe_step_handoff_in_memory".to_string(),
    ]
}
