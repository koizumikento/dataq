use std::fs;
use std::io::Cursor;
use std::path::PathBuf;

use serde::Serialize;
use serde_json::Value;
use thiserror::Error;

use crate::cmd::{
    r#assert::{self, AssertInputNormalizeMode},
    sdiff,
};
use crate::engine::sdiff::SdiffReport;
use crate::io::{self, Format};

/// Ordered pipeline-step names used for `--emit-pipeline` diagnostics.
pub fn pipeline_steps() -> Vec<String> {
    vec![
        "diff_source_resolve_left".to_string(),
        "diff_source_resolve_right".to_string(),
        "diff_source_compare".to_string(),
    ]
}

/// Determinism guards applied by the `diff source` command.
pub fn deterministic_guards() -> Vec<String> {
    vec![
        "rust_native_execution".to_string(),
        "no_shell_interpolation_for_user_input".to_string(),
        "source_resolution_is_explicit".to_string(),
        "deterministic_diff_item_ordering".to_string(),
    ]
}

#[derive(Debug, Clone)]
pub struct DiffSourceExecution {
    pub report: SdiffReport,
    pub sources: DiffSourceMetadata,
    pub left: ResolvedDiffSource,
    pub right: ResolvedDiffSource,
    pub used_tools: Vec<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct DiffSourceReport {
    #[serde(flatten)]
    pub report: SdiffReport,
    pub sources: DiffSourceMetadata,
}

impl DiffSourceReport {
    pub fn new(report: SdiffReport, sources: DiffSourceMetadata) -> Self {
        Self { report, sources }
    }
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct DiffSourceMetadata {
    pub left: DiffSourceSideMetadata,
    pub right: DiffSourceSideMetadata,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct DiffSourceSideMetadata {
    pub kind: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub preset: Option<String>,
    pub path: String,
    pub format: String,
}

#[derive(Debug, Clone)]
pub struct ResolvedDiffSource {
    pub values: Vec<Value>,
    pub format: Format,
    pub bytes: Vec<u8>,
    pub metadata: DiffSourceSideMetadata,
    pub hash_source: String,
    pub used_tools: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiffSourcePreset {
    GithubActionsJobs,
    GitlabCiJobs,
}

impl DiffSourcePreset {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::GithubActionsJobs => "github-actions-jobs",
            Self::GitlabCiJobs => "gitlab-ci-jobs",
        }
    }

    fn from_str(value: &str) -> Option<Self> {
        match value {
            "github-actions-jobs" => Some(Self::GithubActionsJobs),
            "gitlab-ci-jobs" => Some(Self::GitlabCiJobs),
            _ => None,
        }
    }

    fn as_normalize_mode(&self) -> AssertInputNormalizeMode {
        match self {
            Self::GithubActionsJobs => AssertInputNormalizeMode::GithubActionsJobs,
            Self::GitlabCiJobs => AssertInputNormalizeMode::GitlabCiJobs,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DiffSourceLocator {
    Path(PathBuf),
    Preset {
        preset: DiffSourcePreset,
        path: PathBuf,
    },
}

pub fn execute(left: &str, right: &str) -> Result<DiffSourceExecution, DiffSourceCommandError> {
    let left = resolve_source(left, "left")?;
    let right = resolve_source(right, "right")?;
    let report = sdiff::execute(&left.values, &right.values);
    let used_tools = merge_used_tools(&left.used_tools, &right.used_tools);
    let sources = DiffSourceMetadata {
        left: left.metadata.clone(),
        right: right.metadata.clone(),
    };

    Ok(DiffSourceExecution {
        report,
        sources,
        left,
        right,
        used_tools,
    })
}

pub fn parse_source_locator(
    raw: &str,
    side: &str,
) -> Result<DiffSourceLocator, DiffSourceCommandError> {
    if raw.trim().is_empty() {
        return Err(DiffSourceCommandError::InputUsage(format!(
            "`--{side}` cannot be empty"
        )));
    }

    let Some(rest) = raw.strip_prefix("preset:") else {
        return Ok(DiffSourceLocator::Path(PathBuf::from(raw)));
    };

    let mut parts = rest.splitn(2, ':');
    let preset_name = parts.next().unwrap_or_default();
    let path = parts.next().unwrap_or_default();
    if preset_name.is_empty() || path.is_empty() {
        return Err(DiffSourceCommandError::InputUsage(format!(
            "invalid `--{side}` source `{raw}`: preset sources must be `preset:<github-actions-jobs|gitlab-ci-jobs>:<path>`"
        )));
    }

    let Some(preset) = DiffSourcePreset::from_str(preset_name) else {
        return Err(DiffSourceCommandError::InputUsage(format!(
            "invalid `--{side}` preset `{preset_name}`: expected `github-actions-jobs` or `gitlab-ci-jobs`"
        )));
    };

    Ok(DiffSourceLocator::Preset {
        preset,
        path: PathBuf::from(path),
    })
}

fn resolve_source(raw: &str, side: &str) -> Result<ResolvedDiffSource, DiffSourceCommandError> {
    match parse_source_locator(raw, side)? {
        DiffSourceLocator::Path(path) => {
            let (values, format, bytes) = read_values_from_path(&path, side)?;
            Ok(ResolvedDiffSource {
                values,
                format,
                bytes,
                metadata: DiffSourceSideMetadata {
                    kind: "file".to_string(),
                    preset: None,
                    path: path.display().to_string(),
                    format: format.as_str().to_string(),
                },
                hash_source: "path".to_string(),
                used_tools: Vec::new(),
            })
        }
        DiffSourceLocator::Preset { preset, path } => {
            let (values, format, bytes) = read_values_from_path(&path, side)?;
            let (normalized, trace) =
                r#assert::normalize_values_for_mode(values, preset.as_normalize_mode()).map_err(
                    |message| {
                        DiffSourceCommandError::InputUsage(format!(
                            "failed to normalize `--{side}` preset `{}` from `{}`: {message}",
                            preset.as_str(),
                            path.display()
                        ))
                    },
                )?;

            Ok(ResolvedDiffSource {
                values: normalized,
                format,
                bytes,
                metadata: DiffSourceSideMetadata {
                    kind: "preset".to_string(),
                    preset: Some(preset.as_str().to_string()),
                    path: path.display().to_string(),
                    format: format.as_str().to_string(),
                },
                hash_source: "preset".to_string(),
                used_tools: trace.used_tools,
            })
        }
    }
}

fn read_values_from_path(
    path: &PathBuf,
    side: &str,
) -> Result<(Vec<Value>, Format, Vec<u8>), DiffSourceCommandError> {
    let format = io::resolve_input_format(None, Some(path.as_path())).map_err(|error| {
        DiffSourceCommandError::InputUsage(format!(
            "failed to resolve `--{side}` source format from `{}`: {error}",
            path.display()
        ))
    })?;
    let bytes = fs::read(path).map_err(|error| {
        DiffSourceCommandError::InputUsage(format!(
            "failed to read `--{side}` source `{}`: {error}",
            path.display()
        ))
    })?;
    let values =
        io::reader::read_values(Cursor::new(bytes.as_slice()), format).map_err(|error| {
            DiffSourceCommandError::InputUsage(format!(
                "failed to parse `--{side}` source `{}`: {error}",
                path.display()
            ))
        })?;
    Ok((values, format, bytes))
}

fn merge_used_tools(left: &[String], right: &[String]) -> Vec<String> {
    let mut merged = Vec::new();
    for tool in left.iter().chain(right.iter()) {
        if !merged.iter().any(|entry| entry == tool) {
            merged.push(tool.clone());
        }
    }
    merged
}

#[derive(Debug, Error)]
pub enum DiffSourceCommandError {
    #[error("{0}")]
    InputUsage(String),
}
