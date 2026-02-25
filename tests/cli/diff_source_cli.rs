use std::fs;
use std::path::PathBuf;
use std::process::Command;

use serde_json::{Value, json};
use tempfile::tempdir;

#[test]
fn diff_source_file_vs_file_matches_sdiff_sections() {
    let dir = tempdir().expect("temp dir");
    let left_path = dir.path().join("left.json");
    let right_path = dir.path().join("right.json");
    fs::write(&left_path, r#"[{"id":1,"v":"left"}]"#).expect("write left");
    fs::write(&right_path, r#"[{"id":1,"v":"right"}]"#).expect("write right");

    let sdiff_output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .args([
            "sdiff",
            "--left",
            left_path.to_str().expect("utf8 left"),
            "--right",
            right_path.to_str().expect("utf8 right"),
        ])
        .output()
        .expect("run sdiff");
    let diff_output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .args([
            "diff",
            "source",
            "--left",
            left_path.to_str().expect("utf8 left"),
            "--right",
            right_path.to_str().expect("utf8 right"),
        ])
        .output()
        .expect("run diff source");

    assert_eq!(sdiff_output.status.code(), Some(0));
    assert_eq!(diff_output.status.code(), Some(0));

    let sdiff_report = parse_stdout_json(&sdiff_output.stdout);
    let diff_report = parse_stdout_json(&diff_output.stdout);

    assert_eq!(diff_report["counts"], sdiff_report["counts"]);
    assert_eq!(diff_report["keys"], sdiff_report["keys"]);
    assert_eq!(diff_report["ignored_paths"], sdiff_report["ignored_paths"]);
    assert_eq!(diff_report["values"], sdiff_report["values"]);
    assert_eq!(diff_report["sources"]["left"]["kind"], json!("file"));
    assert_eq!(diff_report["sources"]["right"]["kind"], json!("file"));
}

#[test]
fn diff_source_fail_on_diff_returns_exit_two() {
    let dir = tempdir().expect("temp dir");
    let left_path = dir.path().join("left.json");
    let right_path = dir.path().join("right.json");
    fs::write(&left_path, r#"[{"id":1,"v":"left"}]"#).expect("write left");
    fs::write(&right_path, r#"[{"id":1,"v":"right"}]"#).expect("write right");

    let output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .args([
            "diff",
            "source",
            "--left",
            left_path.to_str().expect("utf8 left"),
            "--right",
            right_path.to_str().expect("utf8 right"),
            "--fail-on-diff",
        ])
        .output()
        .expect("run diff source");

    assert_eq!(output.status.code(), Some(2));
    let report = parse_stdout_json(&output.stdout);
    assert_eq!(report["values"]["total"], json!(1));
}

#[test]
fn diff_source_supports_mixed_preset_and_file_inputs() {
    let Some((tool_dir, yq_bin, mlr_bin)) = create_normalize_tool_shims() else {
        return;
    };

    let dir = tempdir().expect("temp dir");
    let workflow_path = dir.path().join("workflow.yml");
    let expected_path = dir.path().join("expected.json");
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
    fs::write(
        &expected_path,
        r#"[{"job_id":"build","runs_on":"ubuntu-latest","steps_count":1,"uses_unpinned_action":false}]"#,
    )
    .expect("write expected");
    let left_locator = format!(
        "preset:github-actions-jobs:{}",
        workflow_path.to_str().expect("utf8 path")
    );

    let output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .env("DATAQ_YQ_BIN", &yq_bin)
        .env("DATAQ_MLR_BIN", &mlr_bin)
        .args([
            "diff",
            "source",
            "--left",
            left_locator.as_str(),
            "--right",
            expected_path.to_str().expect("utf8 path"),
        ])
        .output()
        .expect("run diff source");

    assert_eq!(output.status.code(), Some(0));
    let report = parse_stdout_json(&output.stdout);
    assert_eq!(report["values"]["total"], json!(0));
    assert_eq!(report["sources"]["left"]["kind"], json!("preset"));
    assert_eq!(
        report["sources"]["left"]["preset"],
        json!("github-actions-jobs")
    );
    assert_eq!(report["sources"]["right"]["kind"], json!("file"));
    drop(tool_dir);
}

fn parse_stdout_json(stdout: &[u8]) -> Value {
    let text = String::from_utf8(stdout.to_vec()).expect("stdout utf8");
    let line = text
        .lines()
        .find(|candidate| !candidate.trim().is_empty())
        .expect("non-empty stdout line");
    serde_json::from_str(line).expect("stdout json")
}

fn create_normalize_tool_shims() -> Option<(tempfile::TempDir, String, String)> {
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
