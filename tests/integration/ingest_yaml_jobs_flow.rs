use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use serde_json::Value;
use tempfile::tempdir;

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
fn ingest_yaml_jobs_github_actions_output_is_assert_compatible() {
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
  build:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
"#,
    )
    .expect("write workflow");
    let normalized_path = dir.path().join("normalized.json");
    let rules_path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("examples/assert-rules/github-actions/jobs.rules.yaml");

    let ingest_output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
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
        .expect("run ingest");

    assert_eq!(ingest_output.status.code(), Some(0));
    let parsed: Value = serde_json::from_slice(&ingest_output.stdout).expect("ingest stdout json");
    assert!(parsed.is_array());
    fs::write(&normalized_path, ingest_output.stdout).expect("write normalized output");

    let assert_output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .args([
            "assert",
            "--input",
            normalized_path.to_str().expect("utf8 path"),
            "--rules",
            rules_path.to_str().expect("utf8 path"),
        ])
        .output()
        .expect("run assert");

    assert_eq!(assert_output.status.code(), Some(0));
    drop(tool_dir);
}

#[test]
fn ingest_yaml_jobs_supports_stdin_input_marker() {
    let Some((tool_dir, yq_bin, mlr_bin)) = create_ingest_tool_shims() else {
        return;
    };

    let output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .env("DATAQ_YQ_BIN", &yq_bin)
        .env("DATAQ_MLR_BIN", &mlr_bin)
        .args([
            "ingest",
            "yaml-jobs",
            "--input",
            "-",
            "--mode",
            "generic-map",
        ])
        .write_stdin(
            r#"
a:
  stage: build
  script:
    - echo ok
"#,
        )
        .output()
        .expect("run ingest with stdin");

    assert_eq!(output.status.code(), Some(0));
    let payload: Value = serde_json::from_slice(&output.stdout).expect("stdout json");
    let rows = payload.as_array().expect("array output");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["job_name"], Value::from("a"));
    drop(tool_dir);
}
