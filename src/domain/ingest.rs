use serde::{Deserialize, Serialize};

/// Normalize modes supported by `dataq ingest yaml-jobs`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IngestYamlJobsMode {
    GithubActions,
    GitlabCi,
    GenericMap,
}

impl IngestYamlJobsMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::GithubActions => "github-actions",
            Self::GitlabCi => "gitlab-ci",
            Self::GenericMap => "generic-map",
        }
    }
}

/// Deterministic normalized row schema for `github-actions` mode.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct GithubActionsJobRecord {
    pub job_id: String,
    pub runs_on: String,
    pub steps_count: usize,
    pub uses_unpinned_action: bool,
}

/// Deterministic normalized row schema for `gitlab-ci` mode.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct GitlabCiJobRecord {
    pub job_name: String,
    pub stage: String,
    pub script_count: usize,
    pub uses_only_except: bool,
}

/// Deterministic normalized row schema for `generic-map` mode.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct GenericMapJobRecord {
    pub job_name: String,
    pub field_count: usize,
    pub has_stage: bool,
    pub has_script: bool,
}
