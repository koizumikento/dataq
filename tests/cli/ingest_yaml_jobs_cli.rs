use std::fs;
use std::path::PathBuf;
use std::process::Command;

use predicates::prelude::predicate;
use serde_json::{Value, json};
use tempfile::tempdir;

fn parse_stderr_json_lines(stderr: &[u8]) -> Vec<Value> {
    let text = String::from_utf8(stderr.to_vec()).expect("stderr utf8");
    text.lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| serde_json::from_str(line).expect("stderr json line"))
        .collect()
}

fn create_ingest_tool_shims() -> Option<(tempfile::TempDir, String, String)> {
    if Command::new("jq").arg("--version").output().is_err() {
        return None;
    }

    let dir = tempdir().expect("tempdir");
    let yq_path = dir.path().join("fake-yq");
    let mlr_path = dir.path().join("fake-mlr");

    write_exec_script(
        &yq_path,
        r#"#!/bin/sh
if [ "$1" = "eval" ]; then shift; fi
if [ "$1" = "-o=json" ]; then shift; fi
if [ "$1" = "-I=0" ]; then shift; fi
filter="$1"
exec jq -c "$filter"
"#,
    );
    write_exec_script(
        &mlr_path,
        r#"#!/bin/sh
key="job_id"
while [ $# -gt 0 ]; do
  if [ "$1" = "-f" ]; then
    key="$2"
    break
  fi
  shift
done
exec jq -c --arg key "$key" 'sort_by(.[$key] // "")'
"#,
    );

    Some((
        dir,
        yq_path.display().to_string(),
        mlr_path.display().to_string(),
    ))
}

fn write_exec_script(path: &PathBuf, body: &str) {
    fs::write(path, body).expect("write script");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o755)).expect("chmod");
    }
}

#[test]
fn ingest_yaml_jobs_github_actions_mode_emits_deterministic_records() {
    let Some((tool_dir, yq_bin, mlr_bin)) = create_ingest_tool_shims() else {
        return;
    };
    let dir = tempdir().expect("tempdir");
    let workflow_path = dir.path().join("workflow.yml");
    fs::write(
        &workflow_path,
        r#"
name: CI
on:
  push: {}
jobs:
  lint:
    runs-on: ubuntu-latest
    steps:
      - run: npm run lint
  build:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
"#,
    )
    .expect("write workflow");

    let output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .env("DATAQ_YQ_BIN", &yq_bin)
        .env("DATAQ_MLR_BIN", &mlr_bin)
        .args([
            "ingest",
            "yaml-jobs",
            "--input",
            workflow_path.to_str().expect("utf8 path"),
            "--mode",
            "github-actions",
        ])
        .output()
        .expect("run command");

    assert_eq!(output.status.code(), Some(0));
    let payload: Value = serde_json::from_slice(&output.stdout).expect("stdout json");
    let rows = payload.as_array().expect("array output");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["job_id"], json!("build"));
    assert_eq!(rows[1]["job_id"], json!("lint"));
    assert_eq!(rows[0]["steps_count"], json!(1));
    assert_eq!(rows[0]["uses_unpinned_action"], json!(false));
    drop(tool_dir);
}

#[test]
fn ingest_yaml_jobs_gitlab_ci_mode_emits_expected_shape() {
    let Some((tool_dir, yq_bin, mlr_bin)) = create_ingest_tool_shims() else {
        return;
    };
    let dir = tempdir().expect("tempdir");
    let workflow_path = dir.path().join(".gitlab-ci.yml");
    fs::write(
        &workflow_path,
        r#"
stages: [build, deploy]
.base:
  image: alpine
build:
  stage: build
  script:
    - echo build
deploy:
  stage: deploy
  script: echo deploy
  only:
    - main
"#,
    )
    .expect("write workflow");

    let output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .env("DATAQ_YQ_BIN", &yq_bin)
        .env("DATAQ_MLR_BIN", &mlr_bin)
        .args([
            "ingest",
            "yaml-jobs",
            "--input",
            workflow_path.to_str().expect("utf8 path"),
            "--mode",
            "gitlab-ci",
        ])
        .output()
        .expect("run command");

    assert_eq!(output.status.code(), Some(0));
    let payload: Value = serde_json::from_slice(&output.stdout).expect("stdout json");
    let rows = payload.as_array().expect("array output");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["job_name"], json!("build"));
    assert_eq!(rows[1]["job_name"], json!("deploy"));
    assert_eq!(rows[1]["uses_only_except"], json!(true));
    drop(tool_dir);
}

#[test]
fn ingest_yaml_jobs_generic_map_mode_emits_expected_shape() {
    let Some((tool_dir, yq_bin, mlr_bin)) = create_ingest_tool_shims() else {
        return;
    };
    let dir = tempdir().expect("tempdir");
    let workflow_path = dir.path().join("jobs.yaml");
    fs::write(
        &workflow_path,
        r#"
zeta:
  stage: test
  script:
    - echo z
alpha:
  script:
    - echo a
meta: plain-string
"#,
    )
    .expect("write workflow");

    let output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .env("DATAQ_YQ_BIN", &yq_bin)
        .env("DATAQ_MLR_BIN", &mlr_bin)
        .args([
            "ingest",
            "yaml-jobs",
            "--input",
            workflow_path.to_str().expect("utf8 path"),
            "--mode",
            "generic-map",
        ])
        .output()
        .expect("run command");

    assert_eq!(output.status.code(), Some(0));
    let payload: Value = serde_json::from_slice(&output.stdout).expect("stdout json");
    let rows = payload.as_array().expect("array output");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["job_name"], json!("alpha"));
    assert_eq!(rows[0]["has_script"], json!(true));
    assert_eq!(rows[1]["job_name"], json!("zeta"));
    assert_eq!(rows[1]["has_stage"], json!(true));
    drop(tool_dir);
}

#[test]
fn ingest_yaml_jobs_emit_pipeline_reports_three_stages() {
    let Some((tool_dir, yq_bin, mlr_bin)) = create_ingest_tool_shims() else {
        return;
    };
    let dir = tempdir().expect("tempdir");
    let workflow_path = dir.path().join("jobs.yaml");
    fs::write(
        &workflow_path,
        r#"
a:
  stage: build
  script:
    - echo a
"#,
    )
    .expect("write workflow");

    let output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .env("DATAQ_YQ_BIN", &yq_bin)
        .env("DATAQ_MLR_BIN", &mlr_bin)
        .args([
            "ingest",
            "yaml-jobs",
            "--emit-pipeline",
            "--input",
            workflow_path.to_str().expect("utf8 path"),
            "--mode",
            "generic-map",
        ])
        .output()
        .expect("run command");

    assert_eq!(output.status.code(), Some(0));
    let stderr_json_lines = parse_stderr_json_lines(&output.stderr);
    let pipeline_json = stderr_json_lines.last().expect("pipeline json");
    assert_eq!(
        pipeline_json["steps"],
        json!([
            "ingest_yaml_jobs_yq_extract",
            "ingest_yaml_jobs_jq_normalize",
            "ingest_yaml_jobs_mlr_shape"
        ])
    );
    assert_eq!(
        pipeline_json["stage_diagnostics"][0]["step"],
        json!("ingest_yaml_jobs_yq_extract")
    );
    assert_eq!(
        pipeline_json["stage_diagnostics"][1]["step"],
        json!("ingest_yaml_jobs_jq_normalize")
    );
    assert_eq!(
        pipeline_json["stage_diagnostics"][2]["step"],
        json!("ingest_yaml_jobs_mlr_shape")
    );
    drop(tool_dir);
}

#[test]
fn ingest_yaml_jobs_malformed_yaml_returns_exit_three() {
    let dir = tempdir().expect("tempdir");
    let workflow_path = dir.path().join("bad.yaml");
    fs::write(&workflow_path, "jobs: [broken").expect("write malformed yaml");

    assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .args([
            "ingest",
            "yaml-jobs",
            "--input",
            workflow_path.to_str().expect("utf8 path"),
            "--mode",
            "github-actions",
        ])
        .assert()
        .code(3)
        .stderr(predicate::str::contains("\"error\":\"input_usage_error\""))
        .stderr(predicate::str::contains("yaml parse error"));
}

#[test]
fn ingest_yaml_jobs_missing_tools_return_exit_three() {
    let Some((tool_dir, yq_bin, _mlr_bin)) = create_ingest_tool_shims() else {
        return;
    };
    let dir = tempdir().expect("tempdir");
    let workflow_path = dir.path().join("workflow.yml");
    fs::write(
        &workflow_path,
        r#"
name: CI
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
"#,
    )
    .expect("write workflow");

    assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .env("DATAQ_YQ_BIN", "/definitely-missing/yq")
        .args([
            "ingest",
            "yaml-jobs",
            "--input",
            workflow_path.to_str().expect("utf8 path"),
            "--mode",
            "github-actions",
        ])
        .assert()
        .code(3)
        .stderr(predicate::str::contains("requires `yq` in PATH"));

    assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .env("DATAQ_YQ_BIN", &yq_bin)
        .env("DATAQ_JQ_BIN", "/definitely-missing/jq")
        .args([
            "ingest",
            "yaml-jobs",
            "--input",
            workflow_path.to_str().expect("utf8 path"),
            "--mode",
            "github-actions",
        ])
        .assert()
        .code(3)
        .stderr(predicate::str::contains("requires `jq` in PATH"));

    assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .env("DATAQ_YQ_BIN", &yq_bin)
        .env("DATAQ_MLR_BIN", "/definitely-missing/mlr")
        .args([
            "ingest",
            "yaml-jobs",
            "--input",
            workflow_path.to_str().expect("utf8 path"),
            "--mode",
            "github-actions",
        ])
        .assert()
        .code(3)
        .stderr(predicate::str::contains("requires `mlr` in PATH"));

    drop(tool_dir);
}

#[test]
fn ingest_yaml_jobs_unknown_mode_returns_exit_three() {
    assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .args(["ingest", "yaml-jobs", "--input", "-", "--mode", "unknown"])
        .assert()
        .code(3)
        .stderr(predicate::str::contains("\"error\":\"input_usage_error\""));
}
