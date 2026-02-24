use std::fs::File;
use std::path::{Path, PathBuf};

use serde::Serialize;
use serde_json::{Value, json};

use crate::domain::value_path::ValuePath;
use crate::engine::merge::{self, MergePolicy, PathMergePolicy};
use crate::io::{self, IoError};

/// Input arguments for merge command execution API.
#[derive(Debug, Clone)]
pub struct MergeCommandArgs {
    pub base: PathBuf,
    pub overlays: Vec<PathBuf>,
    pub policy: MergePolicy,
}

/// Structured command response that carries exit-code mapping and JSON payload.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct MergeCommandResponse {
    pub exit_code: i32,
    pub payload: Value,
}

/// Input source descriptor for merge command execution.
#[derive(Debug, Clone)]
pub enum MergeCommandInput {
    Path(PathBuf),
    Inline(Value),
}

/// Input arguments for inline-capable merge command execution.
#[derive(Debug, Clone)]
pub struct MergeCommandInputArgs {
    pub base: MergeCommandInput,
    pub overlays: Vec<MergeCommandInput>,
    pub policy: MergePolicy,
}

pub fn run(args: &MergeCommandArgs) -> MergeCommandResponse {
    run_with_policy_paths(args, &[])
}

/// Runs merge command with optional path-scoped policy overrides.
pub fn run_with_policy_paths(
    args: &MergeCommandArgs,
    policy_paths: &[String],
) -> MergeCommandResponse {
    let input_args = MergeCommandInputArgs {
        base: MergeCommandInput::Path(args.base.clone()),
        overlays: args
            .overlays
            .iter()
            .cloned()
            .map(MergeCommandInput::Path)
            .collect(),
        policy: args.policy,
    };
    run_with_policy_paths_from_inputs(&input_args, policy_paths)
}

/// Runs merge command with optional path-scoped policy overrides from mixed path/inline inputs.
pub fn run_with_policy_paths_from_inputs(
    args: &MergeCommandInputArgs,
    policy_paths: &[String],
) -> MergeCommandResponse {
    match execute_from_inputs(args, policy_paths) {
        Ok(merged) => MergeCommandResponse {
            exit_code: 0,
            payload: merged,
        },
        Err(CommandError::InputUsage(message)) => MergeCommandResponse {
            exit_code: 3,
            payload: json!({
                "error": "input_usage_error",
                "message": message,
            }),
        },
    }
}

fn execute_from_inputs(
    args: &MergeCommandInputArgs,
    policy_paths: &[String],
) -> Result<Value, CommandError> {
    if args.overlays.is_empty() {
        return Err(CommandError::InputUsage(
            "at least one --overlay is required".to_string(),
        ));
    }

    let base = load_document(&args.base, "base")?;
    let mut overlays = Vec::with_capacity(args.overlays.len());
    for overlay in &args.overlays {
        overlays.push(load_document(overlay, "overlay")?);
    }
    let path_policies = parse_policy_paths(policy_paths)?;

    Ok(merge::merge_with_path_policies(
        &base,
        &overlays,
        args.policy,
        &path_policies,
    ))
}

fn load_document(source: &MergeCommandInput, role: &'static str) -> Result<Value, CommandError> {
    match source {
        MergeCommandInput::Path(path) => load_document_from_path(path.as_path(), role),
        MergeCommandInput::Inline(value) => Ok(value.clone()),
    }
}

fn load_document_from_path(path: &Path, role: &'static str) -> Result<Value, CommandError> {
    let format = io::resolve_input_format(None, Some(path)).map_err(|error| {
        CommandError::InputUsage(format!(
            "unable to resolve {role} format from `{}`: {error}",
            path.display()
        ))
    })?;

    let file = File::open(path).map_err(|error| {
        CommandError::InputUsage(format!(
            "failed to open {role} file `{}`: {error}",
            path.display()
        ))
    })?;

    let values = io::reader::read_values(file, format).map_err(map_io_as_input_usage)?;

    Ok(match values.as_slice() {
        [single] => single.clone(),
        _ => Value::Array(values),
    })
}

fn map_io_as_input_usage(error: IoError) -> CommandError {
    CommandError::InputUsage(error.to_string())
}

enum CommandError {
    InputUsage(String),
}

fn parse_policy_paths(raw_definitions: &[String]) -> Result<Vec<PathMergePolicy>, CommandError> {
    let mut parsed = Vec::with_capacity(raw_definitions.len());
    for raw_definition in raw_definitions {
        let (path_literal, policy_literal) =
            raw_definition
                .rsplit_once('=')
                .ok_or_else(|| {
                    CommandError::InputUsage(format!(
                        "invalid `--policy-path` definition `{raw_definition}`: expected `<canonical-path>=<policy>`"
                    ))
                })?;

        let path_literal = path_literal.trim();
        if path_literal.is_empty() {
            return Err(CommandError::InputUsage(format!(
                "invalid `--policy-path` definition `{raw_definition}`: path cannot be empty"
            )));
        }
        let parsed_path = ValuePath::parse_canonical(path_literal).map_err(|source| {
            CommandError::InputUsage(format!(
                "invalid `--policy-path` path `{path_literal}`: {source}"
            ))
        })?;

        let policy_literal = policy_literal.trim();
        let parsed_policy = MergePolicy::parse_cli_name(policy_literal).ok_or_else(|| {
            CommandError::InputUsage(format!(
                "invalid `--policy-path` policy `{policy_literal}`: expected one of `last-wins`, `deep-merge`, `array-replace`"
            ))
        })?;

        parsed.push(PathMergePolicy {
            path: parsed_path,
            policy: parsed_policy,
        });
    }
    Ok(parsed)
}

/// Ordered pipeline-step names used for `--emit-pipeline` diagnostics.
pub fn pipeline_steps() -> Vec<String> {
    vec![
        "resolve_merge_inputs".to_string(),
        "read_merge_values".to_string(),
        "apply_merge_policy".to_string(),
        "write_merged_output".to_string(),
    ]
}

/// Determinism guards planned for the `merge` command.
pub fn deterministic_guards() -> Vec<String> {
    vec![
        "rust_native_execution".to_string(),
        "no_shell_interpolation_for_user_input".to_string(),
        "deterministic_merge_precedence".to_string(),
    ]
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use tempfile::tempdir;

    use crate::cmd::merge::{MergeCommandArgs, run, run_with_policy_paths};
    use crate::engine::merge::MergePolicy;

    #[test]
    fn merges_base_and_overlays_with_selected_policy() {
        let dir = tempdir().expect("tempdir");
        let base = dir.path().join("base.json");
        let overlay = dir.path().join("overlay.yaml");
        std::fs::write(&base, r#"{"cfg":{"keep":true,"tags":["a","b"]}}"#).expect("write base");
        std::fs::write(
            &overlay,
            r#"
cfg:
  tags: [z]
  added: 1
"#,
        )
        .expect("write overlay");

        let args = MergeCommandArgs {
            base,
            overlays: vec![overlay],
            policy: MergePolicy::ArrayReplace,
        };

        let response = run(&args);
        assert_eq!(response.exit_code, 0);
        assert_eq!(
            response.payload,
            json!({"cfg": {"added": 1, "keep": true, "tags": ["z"]}})
        );
    }

    #[test]
    fn resolves_multi_record_input_as_array_document() {
        let dir = tempdir().expect("tempdir");
        let base = dir.path().join("base.jsonl");
        let overlay = dir.path().join("overlay.json");
        std::fs::write(&base, "{\"id\":1}\n{\"id\":2}\n").expect("write base");
        std::fs::write(&overlay, r#"{"extra":true}"#).expect("write overlay");

        let args = MergeCommandArgs {
            base,
            overlays: vec![overlay],
            policy: MergePolicy::LastWins,
        };

        let response = run(&args);
        assert_eq!(response.exit_code, 0);
        assert_eq!(response.payload, json!({"extra": true}));
    }

    #[test]
    fn maps_unresolvable_extension_to_exit_three() {
        let dir = tempdir().expect("tempdir");
        let base = dir.path().join("base.unknown");
        let overlay = dir.path().join("overlay.json");
        std::fs::write(&base, "{}").expect("write base");
        std::fs::write(&overlay, "{}").expect("write overlay");

        let args = MergeCommandArgs {
            base,
            overlays: vec![overlay],
            policy: MergePolicy::DeepMerge,
        };

        let response = run(&args);
        assert_eq!(response.exit_code, 3);
        assert_eq!(response.payload["error"], json!("input_usage_error"));
    }

    #[test]
    fn requires_at_least_one_overlay() {
        let dir = tempdir().expect("tempdir");
        let base = dir.path().join("base.json");
        std::fs::write(&base, "{}").expect("write base");

        let args = MergeCommandArgs {
            base,
            overlays: Vec::new(),
            policy: MergePolicy::DeepMerge,
        };

        let response = run(&args);
        assert_eq!(response.exit_code, 3);
        assert_eq!(response.payload["error"], json!("input_usage_error"));
    }

    #[test]
    fn rejects_invalid_policy_path_definition_with_exit_three() {
        let dir = tempdir().expect("tempdir");
        let base = dir.path().join("base.json");
        let overlay = dir.path().join("overlay.json");
        std::fs::write(&base, "{}").expect("write base");
        std::fs::write(&overlay, "{}").expect("write overlay");

        let args = MergeCommandArgs {
            base: base.clone(),
            overlays: vec![overlay.clone()],
            policy: MergePolicy::DeepMerge,
        };
        let invalid_policy_response =
            run_with_policy_paths(&args, &["$[\"cfg\"]=bad-policy".to_string()]);
        assert_eq!(invalid_policy_response.exit_code, 3);
        assert_eq!(
            invalid_policy_response.payload["error"],
            json!("input_usage_error")
        );

        let invalid_path_response =
            run_with_policy_paths(&args, &["$[cfg]=deep-merge".to_string()]);
        assert_eq!(invalid_path_response.exit_code, 3);
        assert_eq!(
            invalid_path_response.payload["error"],
            json!("input_usage_error")
        );
    }
}
