use std::io::Write;
use std::process::{Command, Stdio};

use serde_json::Value;
use thiserror::Error;

const GITHUB_ACTIONS_JOBS_FILTER: &str = r#"
map(
  if type != "object" then
    error("normalize mode `github-actions-jobs` expects object input rows")
  else . end
  | .jobs as $jobs
  | if ($jobs | type) != "object" then
      error("normalize mode `github-actions-jobs` expects `jobs` object")
    else $jobs end
  | to_entries
  | sort_by(.key)
  | map({
      job_id: .key,
      runs_on: ((.value["runs-on"] // "") | tostring),
      steps_count: (((.value.steps // []) | if type == "array" then . else [] end) | length),
      uses_unpinned_action: (((.value.steps // []) | if type == "array" then . else [] end)
        | map(.uses? // empty)
        | map(select(type == "string"))
        | map(contains("@") | not)
        | any)
    })
)
| add // []
"#;

const GITLAB_CI_JOBS_FILTER: &str = r#"
def reserved:
  ["stages","variables","workflow","default","include","image","services","before_script","after_script","cache","pages"];

map(
  if type != "object" then
    error("normalize mode `gitlab-ci-jobs` expects object input rows")
  else . end
  | to_entries
  | sort_by(.key)
  | map(
      . as $entry
      | select(($entry.key | startswith(".")) | not)
      | select((reserved | index($entry.key)) | not)
      | select(($entry.value | type) == "object")
      | {
          job_name: $entry.key,
          stage: (($entry.value.stage // "") | tostring),
          script_count: (
            $entry.value.script as $script
            | if ($script | type) == "array" then ($script | length)
              elif ($script | type) == "string" then ([$script | split("\n")[] | select(length > 0)] | length)
              else 0 end
          ),
          uses_only_except: (($entry.value | has("only")) or ($entry.value | has("except")))
        }
    )
)
| add // []
"#;

#[derive(Debug, Error)]
pub enum JqError {
    #[error("`jq` is not available in PATH")]
    Unavailable,
    #[error("failed to spawn jq: {0}")]
    Spawn(std::io::Error),
    #[error("failed to write jq stdin: {0}")]
    Stdin(std::io::Error),
    #[error("jq execution failed: {0}")]
    Execution(String),
    #[error("jq output is not valid JSON: {0}")]
    Parse(serde_json::Error),
    #[error("jq output must be a JSON array")]
    OutputShape,
    #[error("failed to serialize jq input: {0}")]
    Serialize(serde_json::Error),
}

pub fn normalize_github_actions_jobs(values: &[Value]) -> Result<Vec<Value>, JqError> {
    run_filter(values, GITHUB_ACTIONS_JOBS_FILTER)
}

pub fn normalize_gitlab_ci_jobs(values: &[Value]) -> Result<Vec<Value>, JqError> {
    run_filter(values, GITLAB_CI_JOBS_FILTER)
}

fn run_filter(values: &[Value], filter: &str) -> Result<Vec<Value>, JqError> {
    let input = serde_json::to_vec(values).map_err(JqError::Serialize)?;
    let mut child = match Command::new("jq")
        .arg("-c")
        .arg(filter)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
    {
        Ok(child) => child,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Err(JqError::Unavailable),
        Err(err) => return Err(JqError::Spawn(err)),
    };

    if let Some(stdin) = child.stdin.as_mut() {
        stdin.write_all(&input).map_err(JqError::Stdin)?;
    } else {
        return Err(JqError::Execution(
            "jq stdin was not piped as expected".to_string(),
        ));
    }

    let output = child.wait_with_output().map_err(JqError::Spawn)?;
    if !output.status.success() {
        let stderr = String::from_utf8(output.stderr)
            .unwrap_or_else(|_| "failed to decode jq stderr".to_string());
        return Err(JqError::Execution(stderr.trim().to_string()));
    }

    let parsed: Value = serde_json::from_slice(&output.stdout).map_err(JqError::Parse)?;
    match parsed {
        Value::Array(items) => Ok(items),
        _ => Err(JqError::OutputShape),
    }
}
