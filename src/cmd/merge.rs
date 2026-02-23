use std::fs::File;
use std::path::{Path, PathBuf};

use serde::Serialize;
use serde_json::{Value, json};

use crate::engine::merge::{self, MergePolicy};
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

pub fn run(args: &MergeCommandArgs) -> MergeCommandResponse {
    match execute(args) {
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

fn execute(args: &MergeCommandArgs) -> Result<Value, CommandError> {
    if args.overlays.is_empty() {
        return Err(CommandError::InputUsage(
            "at least one --overlay is required".to_string(),
        ));
    }

    let base = load_document(&args.base, "base")?;
    let mut overlays = Vec::with_capacity(args.overlays.len());
    for overlay_path in &args.overlays {
        overlays.push(load_document(overlay_path, "overlay")?);
    }

    Ok(merge::merge_with_policy(&base, &overlays, args.policy))
}

fn load_document(path: &Path, role: &'static str) -> Result<Value, CommandError> {
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

#[cfg(test)]
mod tests {
    use serde_json::json;
    use tempfile::tempdir;

    use crate::cmd::merge::{MergeCommandArgs, run};
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
}
