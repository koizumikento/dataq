use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub numeric_stats: Option<ProfileNumericStats>,
}

/// Deterministic numeric statistics for one field path.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ProfileNumericStats {
    pub count: usize,
    pub min: f64,
    pub max: f64,
    pub mean: f64,
    pub p50: f64,
    pub p95: f64,
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

/// Deterministic extraction report for `ingest doc` command output.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct IngestDocReport {
    pub meta: Value,
    pub headings: Vec<IngestDocHeading>,
    pub links: Vec<IngestDocLink>,
    pub tables: Vec<IngestDocTable>,
    pub code_blocks: Vec<IngestDocCodeBlock>,
}

/// Heading entry extracted from a source document.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct IngestDocHeading {
    pub level: i64,
    pub text: String,
}

/// Link entry extracted from a source document.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct IngestDocLink {
    pub url: String,
    pub title: String,
    pub text: String,
}

/// Table entry extracted from a source document.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct IngestDocTable {
    pub caption: String,
}

/// Code-block entry extracted from a source document.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct IngestDocCodeBlock {
    pub language: String,
    pub code: String,
}

/// Deterministic report for `recipe run` command output.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RecipeRunReport {
    pub matched: bool,
    pub exit_code: i32,
    pub steps: Vec<RecipeStepReport>,
}

/// Deterministic report for `recipe replay` command output.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RecipeReplayReport {
    pub matched: bool,
    pub exit_code: i32,
    pub lock_check: RecipeReplayLockCheckReport,
    pub steps: Vec<RecipeStepReport>,
}

/// Deterministic lock verification summary for `recipe replay`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RecipeReplayLockCheckReport {
    pub strict: bool,
    pub matched: bool,
    pub mismatch_count: usize,
    pub mismatches: Vec<RecipeReplayLockMismatchReport>,
}

/// One lock verification mismatch emitted in deterministic check order.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RecipeReplayLockMismatchReport {
    pub constraint: String,
    pub expected: String,
    pub actual: String,
}

/// Per-step result summary in recipe execution order.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RecipeStepReport {
    pub index: usize,
    pub kind: String,
    pub matched: bool,
    pub exit_code: i32,
    pub summary: Value,
}

/// Deterministic lock metadata for reproducible `recipe` execution.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RecipeLockReport {
    pub version: String,
    pub command_graph_hash: String,
    pub args_hash: String,
    pub tool_versions: BTreeMap<String, String>,
    pub dataq_version: String,
}

/// Diagnostics report emitted when `--emit-pipeline` is enabled.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct PipelineReport {
    pub command: String,
    pub input: PipelineInput,
    pub steps: Vec<String>,
    pub external_tools: Vec<ExternalToolUsage>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stage_diagnostics: Option<Vec<PipelineStageDiagnostic>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fingerprint: Option<PipelineFingerprint>,
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
            stage_diagnostics: None,
            fingerprint: None,
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
        } else {
            self.external_tools.push(ExternalToolUsage {
                name: tool_name.to_string(),
                used: true,
            });
        }
        self
    }

    pub fn with_stage_diagnostics(
        mut self,
        stage_diagnostics: Vec<PipelineStageDiagnostic>,
    ) -> Self {
        if !stage_diagnostics.is_empty() {
            self.stage_diagnostics = Some(stage_diagnostics);
        }
        self
    }

    pub fn with_fingerprint(mut self, fingerprint: PipelineFingerprint) -> Self {
        self.fingerprint = Some(fingerprint);
        self
    }
}

/// Deterministic execution fingerprint emitted in pipeline diagnostics.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct PipelineFingerprint {
    pub command: String,
    pub args_hash: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub input_hash: Option<String>,
    pub tool_versions: BTreeMap<String, String>,
    pub dataq_version: String,
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

/// Per-stage diagnostic information for external-tool pipelines.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct PipelineStageDiagnostic {
    pub order: usize,
    pub step: String,
    pub tool: String,
    pub input_records: usize,
    pub output_records: usize,
    pub input_bytes: usize,
    pub output_bytes: usize,
    pub duration_ms: u64,
    pub status: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PipelineStageMetrics {
    pub input_bytes: usize,
    pub output_bytes: usize,
    pub duration_ms: u64,
}

impl PipelineStageMetrics {
    pub const fn zero() -> Self {
        Self {
            input_bytes: 0,
            output_bytes: 0,
            duration_ms: 0,
        }
    }
}

impl PipelineStageDiagnostic {
    pub fn success(
        order: usize,
        stage: impl Into<String>,
        tool: impl Into<String>,
        input_records: usize,
        output_records: usize,
    ) -> Self {
        Self::success_with_metrics(
            order,
            stage,
            tool,
            input_records,
            output_records,
            PipelineStageMetrics::zero(),
        )
    }

    pub fn success_with_metrics(
        order: usize,
        stage: impl Into<String>,
        tool: impl Into<String>,
        input_records: usize,
        output_records: usize,
        metrics: PipelineStageMetrics,
    ) -> Self {
        Self {
            order,
            step: stage.into(),
            tool: tool.into(),
            input_records,
            output_records,
            input_bytes: metrics.input_bytes,
            output_bytes: metrics.output_bytes,
            duration_ms: metrics.duration_ms,
            status: "ok".to_string(),
        }
    }

    pub fn failure(
        order: usize,
        stage: impl Into<String>,
        tool: impl Into<String>,
        input_records: usize,
    ) -> Self {
        Self::failure_with_metrics(
            order,
            stage,
            tool,
            input_records,
            PipelineStageMetrics::zero(),
        )
    }

    pub fn failure_with_metrics(
        order: usize,
        stage: impl Into<String>,
        tool: impl Into<String>,
        input_records: usize,
        metrics: PipelineStageMetrics,
    ) -> Self {
        Self {
            order,
            step: stage.into(),
            tool: tool.into(),
            input_records,
            output_records: 0,
            input_bytes: metrics.input_bytes,
            output_bytes: 0,
            duration_ms: metrics.duration_ms,
            status: "error".to_string(),
        }
    }
}
