use std::env;
use std::fs;
use std::path::{Path, PathBuf};

use serde::Serialize;
use serde_json::{Value, json};

const INSTALL_SKILL_OUTPUT_SCHEMA: &str = "dataq.codex.install_skill.output.v1";
const DATAQ_SKILL_NAME: &str = "dataq";
const DATAQ_SKILL_MARKDOWN: &str = include_str!("../../.agents/skills/dataq/SKILL.md");
const DATAQ_SKILL_OPENAI_YAML: &str = include_str!("../../.agents/skills/dataq/agents/openai.yaml");

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct EmbeddedSkillAsset {
    relative_path: &'static str,
    content: &'static str,
}

const EMBEDDED_DATAQ_SKILL_ASSETS: [EmbeddedSkillAsset; 2] = [
    EmbeddedSkillAsset {
        relative_path: "SKILL.md",
        content: DATAQ_SKILL_MARKDOWN,
    },
    EmbeddedSkillAsset {
        relative_path: "agents/openai.yaml",
        content: DATAQ_SKILL_OPENAI_YAML,
    },
];

/// Input arguments for `codex install-skill` command execution API.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodexInstallSkillCommandArgs {
    pub dest_root: Option<PathBuf>,
    pub force: bool,
}

/// Structured command response that carries exit-code mapping and JSON payload.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct CodexInstallSkillCommandResponse {
    pub exit_code: i32,
    pub payload: Value,
}

/// Trace details used by `--emit-pipeline` for skill installation.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CodexInstallSkillPipelineTrace {
    pub resolved_root: Option<PathBuf>,
    pub destination_path: Option<PathBuf>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
struct CodexInstallSkillSuccessPayload {
    schema: &'static str,
    skill_name: &'static str,
    destination: String,
    copied_files: Vec<String>,
    overwrite: bool,
}

pub fn install_skill_with_trace(
    args: &CodexInstallSkillCommandArgs,
) -> (
    CodexInstallSkillCommandResponse,
    CodexInstallSkillPipelineTrace,
) {
    let mut trace = CodexInstallSkillPipelineTrace::default();
    let root = match resolve_skill_root(args.dest_root.as_deref()) {
        Ok(path) => path,
        Err(message) => {
            return (input_usage_error(message), trace);
        }
    };
    trace.resolved_root = Some(root.clone());

    let destination_path = root.join(DATAQ_SKILL_NAME);
    trace.destination_path = Some(destination_path.clone());

    if let Err(message) = install_embedded_skill_assets(destination_path.as_path(), args.force) {
        return (input_usage_error(message), trace);
    }

    let payload = CodexInstallSkillSuccessPayload {
        schema: INSTALL_SKILL_OUTPUT_SCHEMA,
        skill_name: DATAQ_SKILL_NAME,
        destination: destination_path.display().to_string(),
        copied_files: EMBEDDED_DATAQ_SKILL_ASSETS
            .iter()
            .map(|asset| asset.relative_path.to_string())
            .collect(),
        overwrite: args.force,
    };

    (
        serialize_success_payload(payload),
        CodexInstallSkillPipelineTrace {
            resolved_root: trace.resolved_root,
            destination_path: trace.destination_path,
        },
    )
}

/// Ordered pipeline-step names used for `--emit-pipeline` diagnostics.
pub fn install_skill_pipeline_steps() -> Vec<String> {
    vec![
        "resolve_codex_skill_root".to_string(),
        "prepare_codex_skill_destination".to_string(),
        "write_embedded_codex_skill_files".to_string(),
        "emit_codex_install_skill_output".to_string(),
    ]
}

/// Determinism guards planned for the `codex install-skill` command.
pub fn install_skill_deterministic_guards() -> Vec<String> {
    vec![
        "rust_native_fs_execution".to_string(),
        "compile_time_embedded_skill_assets".to_string(),
        "fixed_embedded_asset_write_order".to_string(),
    ]
}

fn serialize_success_payload(
    payload: CodexInstallSkillSuccessPayload,
) -> CodexInstallSkillCommandResponse {
    match serde_json::to_value(payload) {
        Ok(payload) => CodexInstallSkillCommandResponse {
            exit_code: 0,
            payload,
        },
        Err(error) => CodexInstallSkillCommandResponse {
            exit_code: 1,
            payload: json!({
                "error": "internal_error",
                "message": format!("failed to serialize codex install-skill payload: {error}"),
            }),
        },
    }
}

fn resolve_skill_root(dest_root: Option<&Path>) -> Result<PathBuf, String> {
    if let Some(dest_root) = dest_root {
        if dest_root.as_os_str().is_empty() {
            return Err("`--dest` must not be empty".to_string());
        }
        return Ok(dest_root.to_path_buf());
    }

    if let Some(codex_home) = env::var_os("CODEX_HOME").filter(|value| !value.is_empty()) {
        return Ok(PathBuf::from(codex_home).join("skills"));
    }

    if let Some(home) = env::var_os("HOME").filter(|value| !value.is_empty()) {
        return Ok(PathBuf::from(home).join(".codex").join("skills"));
    }

    Err(
        "failed to resolve Codex skill root: set `CODEX_HOME` or `HOME`, or pass `--dest`"
            .to_string(),
    )
}

fn install_embedded_skill_assets(destination_path: &Path, force: bool) -> Result<(), String> {
    if destination_path.exists() {
        if !force {
            return Err(format!(
                "skill destination `{}` already exists; pass `--force` to overwrite",
                destination_path.display()
            ));
        }
        remove_existing_path(destination_path)?;
    }

    fs::create_dir_all(destination_path).map_err(|error| {
        format!(
            "failed to create skill destination `{}`: {error}",
            destination_path.display()
        )
    })?;

    for asset in EMBEDDED_DATAQ_SKILL_ASSETS {
        let output_path = destination_path.join(asset.relative_path);
        if let Some(parent) = output_path.parent() {
            fs::create_dir_all(parent).map_err(|error| {
                format!(
                    "failed to create parent directory `{}`: {error}",
                    parent.display()
                )
            })?;
        }
        fs::write(&output_path, asset.content.as_bytes()).map_err(|error| {
            format!(
                "failed to write embedded skill file `{}`: {error}",
                output_path.display()
            )
        })?;
    }

    Ok(())
}

fn remove_existing_path(path: &Path) -> Result<(), String> {
    let metadata = fs::symlink_metadata(path).map_err(|error| {
        format!(
            "failed to inspect existing destination `{}`: {error}",
            path.display()
        )
    })?;

    if metadata.is_dir() {
        fs::remove_dir_all(path).map_err(|error| {
            format!(
                "failed to remove existing destination directory `{}`: {error}",
                path.display()
            )
        })
    } else {
        fs::remove_file(path).map_err(|error| {
            format!(
                "failed to remove existing destination path `{}`: {error}",
                path.display()
            )
        })
    }
}

fn input_usage_error(message: String) -> CodexInstallSkillCommandResponse {
    CodexInstallSkillCommandResponse {
        exit_code: 3,
        payload: json!({
            "error": "input_usage_error",
            "message": message,
        }),
    }
}
