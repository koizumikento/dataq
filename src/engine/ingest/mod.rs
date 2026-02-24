use serde::de::DeserializeOwned;
use serde_json::Value;
use thiserror::Error;

use crate::adapters::jq;
use crate::adapters::pandoc::{self, PandocInputFormat};
use crate::domain::ingest::{
    GenericMapJobRecord, GithubActionsJobRecord, GitlabCiJobRecord, IngestYamlJobsMode,
};
use crate::domain::report::IngestDocReport;

/// Supported `ingest doc --from` formats.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IngestDocInputFormat {
    Md,
    Html,
    Docx,
    Rst,
    Latex,
}

impl IngestDocInputFormat {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Md => "md",
            Self::Html => "html",
            Self::Docx => "docx",
            Self::Rst => "rst",
            Self::Latex => "latex",
        }
    }

    fn as_pandoc(self) -> PandocInputFormat {
        match self {
            Self::Md => PandocInputFormat::Markdown,
            Self::Html => PandocInputFormat::Html,
            Self::Docx => PandocInputFormat::Docx,
            Self::Rst => PandocInputFormat::Rst,
            Self::Latex => PandocInputFormat::Latex,
        }
    }
}

/// Domain errors for deterministic ingest row shaping.
#[derive(Debug, Error)]
pub enum IngestYamlJobsError {
    #[error("normalized `{mode}` row {index} does not match expected schema: {source}")]
    RowShape {
        mode: &'static str,
        index: usize,
        #[source]
        source: serde_json::Error,
    },
    #[error("failed to serialize normalized `{mode}` row {index}: {source}")]
    Serialize {
        mode: &'static str,
        index: usize,
        #[source]
        source: serde_json::Error,
    },
}

/// Domain errors for ingest document extraction.
#[derive(Debug, Error)]
pub enum IngestDocError {
    #[error("{0}")]
    Input(String),
    #[error("input is not valid UTF-8 for `--from {from}`")]
    InvalidUtf8 { from: &'static str },
    #[error("ingest doc requires `pandoc` in PATH")]
    MissingPandoc,
    #[error("failed to parse document with pandoc: {0}")]
    PandocExecution(String),
    #[error("pandoc produced invalid JSON AST: {0}")]
    PandocParse(String),
    #[error("ingest doc requires `jq` in PATH")]
    MissingJq,
    #[error("failed to project pandoc AST with jq: {0}")]
    JqExecution(String),
    #[error("jq projection for ingest doc was not valid schema: {0}")]
    ProjectionSchema(String),
}

/// Validates and re-shapes normalized rows into deterministic mode-specific schemas.
pub fn shape_rows(
    mode: IngestYamlJobsMode,
    rows: Vec<Value>,
) -> Result<Vec<Value>, IngestYamlJobsError> {
    match mode {
        IngestYamlJobsMode::GithubActions => shape_rows_typed::<GithubActionsJobRecord>(
            rows,
            IngestYamlJobsMode::GithubActions.as_str(),
        ),
        IngestYamlJobsMode::GitlabCi => {
            shape_rows_typed::<GitlabCiJobRecord>(rows, IngestYamlJobsMode::GitlabCi.as_str())
        }
        IngestYamlJobsMode::GenericMap => {
            shape_rows_typed::<GenericMapJobRecord>(rows, IngestYamlJobsMode::GenericMap.as_str())
        }
    }
}

/// Run stage1 pandoc AST conversion and stage2 jq projection for document ingest.
pub fn ingest_document(
    input: &[u8],
    from: IngestDocInputFormat,
) -> Result<IngestDocReport, IngestDocError> {
    let pandoc_format = from.as_pandoc();
    if pandoc_format.requires_utf8() && std::str::from_utf8(input).is_err() {
        return Err(IngestDocError::InvalidUtf8 {
            from: from.as_str(),
        });
    }

    let ast = pandoc::to_json_ast(input, pandoc_format).map_err(|error| match error {
        pandoc::PandocError::Unavailable => IngestDocError::MissingPandoc,
        pandoc::PandocError::Execution(message) => IngestDocError::PandocExecution(message),
        pandoc::PandocError::Parse(source) => IngestDocError::PandocParse(source.to_string()),
        pandoc::PandocError::Spawn(source) => IngestDocError::PandocExecution(source.to_string()),
        pandoc::PandocError::Stdin(source) => IngestDocError::PandocExecution(source.to_string()),
    })?;

    let projected = jq::project_document_ast(&ast).map_err(|error| match error {
        jq::JqError::Unavailable => IngestDocError::MissingJq,
        jq::JqError::Execution(message) => IngestDocError::JqExecution(message),
        jq::JqError::Parse(source) => IngestDocError::JqExecution(source.to_string()),
        jq::JqError::Spawn(source) => IngestDocError::JqExecution(source.to_string()),
        jq::JqError::Stdin(source) => IngestDocError::JqExecution(source.to_string()),
        jq::JqError::Serialize(source) => IngestDocError::JqExecution(source.to_string()),
        jq::JqError::OutputShape | jq::JqError::OutputObjectShape => {
            IngestDocError::ProjectionSchema("jq output must be a JSON object".to_string())
        }
    })?;

    serde_json::from_value(projected)
        .map_err(|error| IngestDocError::ProjectionSchema(error.to_string()))
}

/// Ordered pipeline-step names used for `--emit-pipeline` diagnostics.
pub fn pipeline_steps() -> Vec<String> {
    vec![
        "ingest_doc_pandoc_ast".to_string(),
        "ingest_doc_jq_project".to_string(),
    ]
}

/// Determinism guards planned for `ingest doc` command.
pub fn deterministic_guards() -> Vec<String> {
    vec![
        "pandoc_execution_with_explicit_arg_arrays".to_string(),
        "jq_execution_with_explicit_arg_arrays".to_string(),
        "no_shell_interpolation_for_user_input".to_string(),
        "deterministic_schema_key_order".to_string(),
        "source_order_preserved_for_arrays".to_string(),
    ]
}

fn shape_rows_typed<T>(
    rows: Vec<Value>,
    mode: &'static str,
) -> Result<Vec<Value>, IngestYamlJobsError>
where
    T: DeserializeOwned + serde::Serialize,
{
    rows.into_iter()
        .enumerate()
        .map(|(index, row)| {
            let typed: T =
                serde_json::from_value(row).map_err(|source| IngestYamlJobsError::RowShape {
                    mode,
                    index,
                    source,
                })?;
            serde_json::to_value(typed).map_err(|source| IngestYamlJobsError::Serialize {
                mode,
                index,
                source,
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{IngestYamlJobsError, shape_rows};
    use crate::domain::ingest::IngestYamlJobsMode;

    #[test]
    fn github_actions_shape_rejects_unknown_field() {
        let err = shape_rows(
            IngestYamlJobsMode::GithubActions,
            vec![json!({
                "job_id": "build",
                "runs_on": "ubuntu-latest",
                "steps_count": 1,
                "uses_unpinned_action": false,
                "extra": true
            })],
        )
        .expect_err("unknown field must fail");

        assert!(matches!(
            err,
            IngestYamlJobsError::RowShape { index: 0, .. }
        ));
    }

    #[test]
    fn generic_map_shape_keeps_mode_specific_fields() {
        let rows = shape_rows(
            IngestYamlJobsMode::GenericMap,
            vec![json!({
                "job_name": "build",
                "field_count": 2,
                "has_stage": true,
                "has_script": true
            })],
        )
        .expect("shape rows");

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["job_name"], json!("build"));
        assert_eq!(rows[0]["field_count"], json!(2));
    }
}
