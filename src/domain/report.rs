use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// Deterministic profile report for `profile` command output.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ProfileReport {
    pub record_count: usize,
    pub field_count: usize,
    pub fields: BTreeMap<String, ProfileFieldReport>,
}

/// Deterministic per-field statistics.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ProfileFieldReport {
    pub null_ratio: f64,
    pub unique_count: usize,
    pub type_distribution: ProfileTypeDistribution,
}

/// Deterministic type distribution for one field path.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProfileTypeDistribution {
    pub null: usize,
    pub boolean: usize,
    pub number: usize,
    pub string: usize,
    pub array: usize,
    pub object: usize,
}

/// Diagnostics report emitted when `--emit-pipeline` is enabled.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct PipelineReport {
    pub command: String,
    pub input: PipelineInput,
    pub steps: Vec<String>,
    pub external_tools: Vec<ExternalToolUsage>,
    pub deterministic_guards: Vec<String>,
}

impl PipelineReport {
    pub fn new(
        command: impl Into<String>,
        input: PipelineInput,
        steps: Vec<String>,
        deterministic_guards: Vec<String>,
    ) -> Self {
        Self {
            command: command.into(),
            input,
            steps,
            external_tools: ExternalToolUsage::default_set(),
            deterministic_guards,
        }
    }

    pub fn mark_external_tool_used(mut self, tool_name: &str) -> Self {
        if let Some(tool) = self
            .external_tools
            .iter_mut()
            .find(|tool| tool.name == tool_name)
        {
            tool.used = true;
        }
        self
    }
}

/// Input-source descriptors used in pipeline diagnostics.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct PipelineInput {
    pub sources: Vec<PipelineInputSource>,
}

impl PipelineInput {
    pub fn new(sources: Vec<PipelineInputSource>) -> Self {
        Self { sources }
    }
}

/// Single input source descriptor.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct PipelineInputSource {
    pub label: String,
    pub source: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub format: Option<String>,
}

impl PipelineInputSource {
    pub fn stdin(label: impl Into<String>, format: Option<&str>) -> Self {
        Self {
            label: label.into(),
            source: "stdin".to_string(),
            path: None,
            format: format.map(ToOwned::to_owned),
        }
    }

    pub fn path(label: impl Into<String>, path: impl Into<String>, format: Option<&str>) -> Self {
        Self {
            label: label.into(),
            source: "path".to_string(),
            path: Some(path.into()),
            format: format.map(ToOwned::to_owned),
        }
    }
}

/// External-tool usage summary in a deterministic order.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ExternalToolUsage {
    pub name: String,
    pub used: bool,
}

impl ExternalToolUsage {
    pub fn default_set() -> Vec<Self> {
        vec![
            Self {
                name: "jq".to_string(),
                used: false,
            },
            Self {
                name: "yq".to_string(),
                used: false,
            },
            Self {
                name: "mlr".to_string(),
                used: false,
            },
        ]
    }
}
