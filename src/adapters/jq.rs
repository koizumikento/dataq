use std::io::Write;
use std::process::{Command, Stdio};

use serde_json::Value;
use thiserror::Error;

const GITHUB_ACTIONS_JOBS_FILTER: &str = r#"
map(
  if type != "object" then
    error("normalize mode `github-actions-jobs` expects object rows from yq stage")
  else . end
  | .job_id as $job_id
  | if ($job_id | type) != "string" then
      error("normalize mode `github-actions-jobs` expects `job_id` string from yq stage")
    else . end
  | .job as $job
  | if ($job | type) != "object" then
      error("normalize mode `github-actions-jobs` expects `job` object from yq stage")
    else . end
  | {
      job_id: $job_id,
      runs_on: (($job["runs-on"] // "") | tostring),
      steps_count: ((($job.steps // []) | if type == "array" then . else [] end) | length),
      uses_unpinned_action: (((($job.steps // []) | if type == "array" then . else [] end)
        | map(.uses? // empty)
        | map(select(type == "string"))
        | map(contains("@") | not)
        | any))
    }
)
"#;

const GITLAB_CI_JOBS_FILTER: &str = r#"
def reserved:
  ["stages","variables","workflow","default","include","image","services","before_script","after_script","cache","pages"];

map(
  if type != "object" then
    error("normalize mode `gitlab-ci-jobs` expects object rows from yq stage")
  else . end
  | .job_name as $job_name
  | if ($job_name | type) != "string" then
      error("normalize mode `gitlab-ci-jobs` expects `job_name` string from yq stage")
    else . end
  | select(($job_name | startswith(".")) | not)
  | select((reserved | index($job_name)) | not)
  | .job as $job
  | if ($job | type) != "object" then empty else . end
  | {
      job_name: $job_name,
      stage: (($job.stage // "") | tostring),
      script_count: (
        $job.script as $script
        | if ($script | type) == "array" then ($script | length)
          elif ($script | type) == "string" then ([$script | split("\n")[] | select(length > 0)] | length)
          else 0 end
      ),
      uses_only_except: (($job | has("only")) or ($job | has("except")))
    }
)
"#;

const INGEST_API_NORMALIZE_FILTER: &str = r#"
def allowlist: ["cache-control","content-type","date","etag","last-modified"];
{
  source: .source,
  status: (.status | tonumber),
  headers: (
    (.headers // {})
    | to_entries
    | map({key: (.key | ascii_downcase), value: (.value | tostring)})
    | map(select(.key as $k | allowlist | index($k)))
    | sort_by(.key)
    | from_entries
  ),
  body: (
    .body as $body
    | if ($body | type) == "string" then
        (try ($body | fromjson) catch $body)
      else
        $body
      end
  ),
  fetched_at: .fetched_at
}
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
    #[error("jq output must be a JSON object")]
    OutputObjectShape,
    #[error("failed to serialize jq input: {0}")]
    Serialize(serde_json::Error),
}

pub fn normalize_github_actions_jobs(values: &[Value]) -> Result<Vec<Value>, JqError> {
    run_filter(values, GITHUB_ACTIONS_JOBS_FILTER)
}

pub fn normalize_gitlab_ci_jobs(values: &[Value]) -> Result<Vec<Value>, JqError> {
    run_filter(values, GITLAB_CI_JOBS_FILTER)
}

pub fn normalize_ingest_api_response(value: &Value) -> Result<Value, JqError> {
    let parsed = run_filter_value(value, INGEST_API_NORMALIZE_FILTER)?;
    match parsed {
        Value::Object(_) => Ok(parsed),
        _ => Err(JqError::OutputObjectShape),
    }
}

fn run_filter(values: &[Value], filter: &str) -> Result<Vec<Value>, JqError> {
    let input = serde_json::to_vec(values).map_err(JqError::Serialize)?;
    let parsed = run_filter_bytes(&input, filter)?;
    match parsed {
        Value::Array(items) => Ok(items),
        _ => Err(JqError::OutputShape),
    }
}

fn run_filter_value(value: &Value, filter: &str) -> Result<Value, JqError> {
    let input = serde_json::to_vec(value).map_err(JqError::Serialize)?;
    run_filter_bytes(&input, filter)
}

fn run_filter_bytes(input: &[u8], filter: &str) -> Result<Value, JqError> {
    let jq_bin = std::env::var("DATAQ_JQ_BIN").unwrap_or_else(|_| "jq".to_string());
    let mut child = match Command::new(&jq_bin)
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

    serde_json::from_slice(&output.stdout).map_err(JqError::Parse)
}
