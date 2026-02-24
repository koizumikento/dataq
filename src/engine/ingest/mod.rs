use serde::de::DeserializeOwned;
use serde_json::Value;
use thiserror::Error;

use crate::domain::ingest::{
    GenericMapJobRecord, GithubActionsJobRecord, GitlabCiJobRecord, IngestYamlJobsMode,
};

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
