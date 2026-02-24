use std::io::Read;
use std::path::{Path, PathBuf};

use serde_json::json;

use crate::cmd::r#assert::{
    self, AssertCommandArgs, AssertCommandResponse, AssertInputNormalizeMode, AssertPipelineTrace,
};
use crate::io::Format;

const GATE_SCHEMA_PRESETS: [&str; 2] = ["github-actions-jobs", "gitlab-ci-jobs"];

/// Input arguments for `gate schema` command execution API.
#[derive(Debug, Clone)]
pub struct GateSchemaCommandArgs {
    pub schema: PathBuf,
    pub input: Option<PathBuf>,
    pub from: Option<String>,
}

/// Preset IDs accepted by `gate schema --from`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GateSchemaPreset {
    GithubActionsJobs,
    GitlabCiJobs,
}

impl GateSchemaPreset {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::GithubActionsJobs => "github-actions-jobs",
            Self::GitlabCiJobs => "gitlab-ci-jobs",
        }
    }

    fn normalize_mode(&self) -> AssertInputNormalizeMode {
        match self {
            Self::GithubActionsJobs => AssertInputNormalizeMode::GithubActionsJobs,
            Self::GitlabCiJobs => AssertInputNormalizeMode::GitlabCiJobs,
        }
    }

    fn stdin_format(&self) -> Format {
        Format::Yaml
    }
}

/// Resolve optional `gate schema --from` value into a known preset.
pub fn resolve_preset(raw: Option<&str>) -> Result<Option<GateSchemaPreset>, String> {
    let Some(raw) = raw else {
        return Ok(None);
    };

    if raw.trim().is_empty() {
        return Err("`--from` cannot be empty".to_string());
    }

    let preset = match raw {
        "github-actions-jobs" => GateSchemaPreset::GithubActionsJobs,
        "gitlab-ci-jobs" => GateSchemaPreset::GitlabCiJobs,
        other => {
            return Err(format!(
                "unsupported `--from` preset `{other}`; supported presets: {}",
                GATE_SCHEMA_PRESETS.join(", ")
            ));
        }
    };

    Ok(Some(preset))
}

/// Ordered pipeline-step names used for `--emit-pipeline` diagnostics.
pub fn pipeline_steps() -> Vec<String> {
    vec![
        "gate_schema_ingest".to_string(),
        "gate_schema_validate".to_string(),
    ]
}

/// Determinism guards applied by `gate schema`.
pub fn deterministic_guards() -> Vec<String> {
    vec![
        "rust_native_schema_validation".to_string(),
        "gate_schema_exit_mapping_0_2_3_1".to_string(),
        "gate_schema_mismatch_order_stable".to_string(),
        "gate_schema_error_path_format_stable".to_string(),
        "gate_schema_preset_resolution_explicit".to_string(),
    ]
}

pub fn run_schema_with_stdin_and_trace<R: Read>(
    args: &GateSchemaCommandArgs,
    stdin: R,
) -> (AssertCommandResponse, AssertPipelineTrace) {
    let preset = match resolve_preset(args.from.as_deref()) {
        Ok(value) => value,
        Err(message) => {
            return (
                AssertCommandResponse {
                    exit_code: 3,
                    payload: json!({
                        "error": "input_usage_error",
                        "message": message,
                    }),
                },
                AssertPipelineTrace::default(),
            );
        }
    };

    let input = normalize_input_path(args.input.as_deref());
    let stdin_format = preset
        .as_ref()
        .map(GateSchemaPreset::stdin_format)
        .unwrap_or(Format::Json);
    let normalize_mode = preset.as_ref().map(GateSchemaPreset::normalize_mode);

    let assert_args = AssertCommandArgs {
        input: input.clone(),
        from: if input.is_some() && preset.is_none() {
            None
        } else {
            Some(stdin_format)
        },
        rules: None,
        schema: Some(args.schema.clone()),
    };

    r#assert::run_with_stdin_and_normalize_with_trace(&assert_args, stdin, normalize_mode)
}

/// Treat `-` as stdin for command-level input resolution.
pub fn is_stdin_path(path: &Path) -> bool {
    path == Path::new("-") || path == Path::new("/dev/stdin")
}

fn normalize_input_path(path: Option<&Path>) -> Option<PathBuf> {
    path.and_then(|value| {
        if is_stdin_path(value) {
            None
        } else {
            Some(value.to_path_buf())
        }
    })
}

#[cfg(test)]
mod tests {
    use super::{GateSchemaPreset, is_stdin_path, resolve_preset};

    #[test]
    fn resolves_supported_presets() {
        assert_eq!(
            resolve_preset(Some("github-actions-jobs")).expect("preset"),
            Some(GateSchemaPreset::GithubActionsJobs)
        );
        assert_eq!(
            resolve_preset(Some("gitlab-ci-jobs")).expect("preset"),
            Some(GateSchemaPreset::GitlabCiJobs)
        );
    }

    #[test]
    fn rejects_unknown_preset_with_actionable_error() {
        let error = resolve_preset(Some("unknown")).expect_err("unknown preset should fail");
        assert!(error.contains("unsupported `--from` preset `unknown`"));
        assert!(error.contains("github-actions-jobs"));
        assert!(error.contains("gitlab-ci-jobs"));
    }

    #[test]
    fn treats_dash_as_stdin_path() {
        assert!(is_stdin_path(std::path::Path::new("-")));
        assert!(is_stdin_path(std::path::Path::new("/dev/stdin")));
        assert!(!is_stdin_path(std::path::Path::new("./input.json")));
    }
}
