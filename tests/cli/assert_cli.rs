use std::fs;
use std::io::Cursor;
use std::path::{Path, PathBuf};
use std::process::Command;

use dataq::cmd::r#assert::{
    AssertCommandArgs, AssertCommandResponse, rules_help_payload, run_with_stdin,
};
use dataq::io::Format;
use serde_json::Value;
use tempfile::tempdir;

fn sample_rules_path(relative: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join(relative)
}

fn run_with_sample_rules(
    rules_relative_path: &str,
    format: Format,
    input: &str,
) -> AssertCommandResponse {
    let rules_path = sample_rules_path(rules_relative_path);
    assert!(
        rules_path.exists(),
        "sample rules must exist: {rules_relative_path}"
    );
    let args = AssertCommandArgs {
        input: None,
        from: Some(format),
        rules: Some(rules_path),
        schema: None,
    };
    run_with_stdin(&args, Cursor::new(input.as_bytes()))
}

fn has_mismatch(payload: &Value, path: &str, rule_kind: &str, reason: &str) -> bool {
    payload["mismatches"]
        .as_array()
        .expect("mismatches array")
        .iter()
        .any(|entry| {
            entry["path"].as_str() == Some(path)
                && entry["rule_kind"].as_str() == Some(rule_kind)
                && entry["reason"].as_str() == Some(reason)
        })
}

fn parse_stderr_json_lines(stderr: &[u8]) -> Vec<Value> {
    let text = String::from_utf8(stderr.to_vec()).expect("stderr utf8");
    text.lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| serde_json::from_str(line).expect("stderr json line"))
        .collect()
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

#[test]
fn assert_normalize_emit_pipeline_reports_three_stage_diagnostics() {
    let Some((tool_dir, yq_bin, mlr_bin)) = create_normalize_tool_shims() else {
        return;
    };
    let dir = tempdir().expect("tempdir");
    let workflow_path = dir.path().join("workflow.yml");
    std::fs::write(
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
    let rules_path = sample_rules_path("examples/assert-rules/github-actions/jobs.rules.yaml");

    let output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .env("DATAQ_YQ_BIN", &yq_bin)
        .env("DATAQ_MLR_BIN", &mlr_bin)
        .args([
            "assert",
            "--emit-pipeline",
            "--input",
            workflow_path.to_str().expect("utf8 path"),
            "--normalize",
            "github-actions-jobs",
            "--rules",
            rules_path.to_str().expect("utf8 path"),
        ])
        .output()
        .expect("run command");

    assert_eq!(output.status.code(), Some(0));
    let stderr_json_lines = parse_stderr_json_lines(&output.stderr);
    let pipeline_json = stderr_json_lines.last().expect("pipeline json line");
    assert_eq!(pipeline_json["command"], Value::from("assert"));
    assert_eq!(
        pipeline_json["stage_diagnostics"][0]["step"],
        Value::from("normalize_yq_extract")
    );
    assert_eq!(
        pipeline_json["stage_diagnostics"][1]["step"],
        Value::from("normalize_jq_project")
    );
    assert_eq!(
        pipeline_json["stage_diagnostics"][2]["step"],
        Value::from("normalize_mlr_sort")
    );
    assert_eq!(
        pipeline_json["stage_diagnostics"][0]["tool"],
        Value::from("yq")
    );
    assert_eq!(
        pipeline_json["stage_diagnostics"][1]["tool"],
        Value::from("jq")
    );
    assert_eq!(
        pipeline_json["stage_diagnostics"][2]["tool"],
        Value::from("mlr")
    );
    assert_eq!(
        pipeline_json["stage_diagnostics"][0]["order"],
        Value::from(1)
    );
    assert_eq!(
        pipeline_json["stage_diagnostics"][1]["order"],
        Value::from(2)
    );
    assert_eq!(
        pipeline_json["stage_diagnostics"][2]["order"],
        Value::from(3)
    );
    assert_eq!(
        pipeline_json["stage_diagnostics"][0]["status"],
        Value::from("ok")
    );
    assert_eq!(
        pipeline_json["stage_diagnostics"][1]["status"],
        Value::from("ok")
    );
    assert_eq!(
        pipeline_json["stage_diagnostics"][2]["status"],
        Value::from("ok")
    );

    let tools = pipeline_json["external_tools"]
        .as_array()
        .expect("external_tools array");
    assert_eq!(tools.len(), 3);
    assert_eq!(tools[0]["name"], Value::from("jq"));
    assert_eq!(tools[0]["used"], Value::Bool(true));
    assert_eq!(tools[1]["name"], Value::from("yq"));
    assert_eq!(tools[1]["used"], Value::Bool(true));
    assert_eq!(tools[2]["name"], Value::from("mlr"));
    assert_eq!(tools[2]["used"], Value::Bool(true));
    drop(tool_dir);
}

#[test]
fn assert_normalize_missing_yq_maps_to_input_usage_error() {
    let dir = tempdir().expect("tempdir");
    let workflow_path = dir.path().join("workflow.yml");
    std::fs::write(
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
    let rules_path = sample_rules_path("examples/assert-rules/github-actions/jobs.rules.yaml");

    let output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .env("DATAQ_YQ_BIN", "/definitely-missing/yq")
        .args([
            "assert",
            "--emit-pipeline",
            "--input",
            workflow_path.to_str().expect("utf8 path"),
            "--normalize",
            "github-actions-jobs",
            "--rules",
            rules_path.to_str().expect("utf8 path"),
        ])
        .output()
        .expect("run command");

    assert_eq!(output.status.code(), Some(3));
    let stderr_json_lines = parse_stderr_json_lines(&output.stderr);
    assert_eq!(
        stderr_json_lines
            .first()
            .expect("error json")
            .get("message")
            .and_then(Value::as_str)
            .expect("message"),
        "normalize mode `github-actions-jobs` requires `yq` in PATH"
    );
    let pipeline_json = stderr_json_lines.last().expect("pipeline json");
    assert_eq!(
        pipeline_json["stage_diagnostics"][0]["step"],
        Value::from("normalize_yq_extract")
    );
    assert_eq!(
        pipeline_json["stage_diagnostics"][0]["tool"],
        Value::from("yq")
    );
    assert_eq!(
        pipeline_json["stage_diagnostics"][0]["status"],
        Value::from("error")
    );
    let tools = pipeline_json["external_tools"]
        .as_array()
        .expect("external_tools array");
    assert_eq!(tools[0]["name"], Value::from("jq"));
    assert_eq!(tools[0]["used"], Value::Bool(false));
    assert_eq!(tools[1]["name"], Value::from("yq"));
    assert_eq!(tools[1]["used"], Value::Bool(true));
    assert_eq!(tools[2]["name"], Value::from("mlr"));
    assert_eq!(tools[2]["used"], Value::Bool(false));
}

#[test]
fn assert_normalize_missing_mlr_maps_to_input_usage_error() {
    let Some((tool_dir, yq_bin, _mlr_bin)) = create_normalize_tool_shims() else {
        return;
    };
    let dir = tempdir().expect("tempdir");
    let workflow_path = dir.path().join("workflow.yml");
    std::fs::write(
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
    let rules_path = sample_rules_path("examples/assert-rules/github-actions/jobs.rules.yaml");

    let output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .env("DATAQ_YQ_BIN", &yq_bin)
        .env("DATAQ_MLR_BIN", "/definitely-missing/mlr")
        .args([
            "assert",
            "--emit-pipeline",
            "--input",
            workflow_path.to_str().expect("utf8 path"),
            "--normalize",
            "github-actions-jobs",
            "--rules",
            rules_path.to_str().expect("utf8 path"),
        ])
        .output()
        .expect("run command");

    assert_eq!(output.status.code(), Some(3));
    let stderr_json_lines = parse_stderr_json_lines(&output.stderr);
    assert_eq!(
        stderr_json_lines
            .first()
            .expect("error json")
            .get("message")
            .and_then(Value::as_str)
            .expect("message"),
        "normalize mode `github-actions-jobs` requires `mlr` in PATH"
    );
    let pipeline_json = stderr_json_lines.last().expect("pipeline json");
    assert_eq!(
        pipeline_json["stage_diagnostics"][0]["step"],
        Value::from("normalize_yq_extract")
    );
    assert_eq!(
        pipeline_json["stage_diagnostics"][1]["step"],
        Value::from("normalize_jq_project")
    );
    assert_eq!(
        pipeline_json["stage_diagnostics"][2]["step"],
        Value::from("normalize_mlr_sort")
    );
    assert_eq!(
        pipeline_json["stage_diagnostics"][2]["status"],
        Value::from("error")
    );
    let tools = pipeline_json["external_tools"]
        .as_array()
        .expect("external_tools array");
    assert_eq!(tools[0]["name"], Value::from("jq"));
    assert_eq!(tools[0]["used"], Value::Bool(true));
    assert_eq!(tools[1]["name"], Value::from("yq"));
    assert_eq!(tools[1]["used"], Value::Bool(true));
    assert_eq!(tools[2]["name"], Value::from("mlr"));
    assert_eq!(tools[2]["used"], Value::Bool(true));
    drop(tool_dir);
}

#[test]
fn assert_api_success_with_stdin_input() {
    let dir = tempdir().expect("tempdir");
    let rules_path = dir.path().join("rules.yaml");
    std::fs::write(
        &rules_path,
        r#"
required_keys: [id, score]
fields:
  id:
    type: integer
  score:
    type: number
    range:
      min: 0
      max: 100
count:
  min: 1
  max: 2
"#,
    )
    .expect("write rules");

    let args = AssertCommandArgs {
        input: None,
        from: Some(Format::Json),
        rules: Some(rules_path),
        schema: None,
    };

    let response = run_with_stdin(&args, Cursor::new(r#"[{"id":1,"score":10.5}]"#));
    assert_eq!(response.exit_code, 0);
    assert_eq!(response.payload["matched"], Value::Bool(true));
    assert_eq!(response.payload["mismatch_count"], Value::from(0));
}

#[test]
fn assert_api_supports_field_centric_rule_schema() {
    let dir = tempdir().expect("tempdir");
    let rules_path = dir.path().join("rules.yaml");
    std::fs::write(
        &rules_path,
        r#"
required_keys: [id, score]
fields:
  id:
    type: integer
  score:
    type: number
    nullable: true
    range:
      min: 0
      max: 100
count:
  min: 1
  max: 2
"#,
    )
    .expect("write rules");

    let args = AssertCommandArgs {
        input: None,
        from: Some(Format::Json),
        rules: Some(rules_path),
        schema: None,
    };

    let response = run_with_stdin(&args, Cursor::new(r#"[{"id":1,"score":null}]"#));
    assert_eq!(response.exit_code, 0);
    assert_eq!(response.payload["matched"], Value::Bool(true));
    assert_eq!(response.payload["mismatch_count"], Value::from(0));
}

#[test]
fn assert_api_supports_single_extends_with_parent_relative_resolution() {
    let dir = tempdir().expect("tempdir");
    let shared_dir = dir.path().join("shared");
    let leaf_dir = dir.path().join("leaf");
    std::fs::create_dir_all(&shared_dir).expect("create shared dir");
    std::fs::create_dir_all(&leaf_dir).expect("create leaf dir");

    let base_rules_path = shared_dir.join("base.rules.yaml");
    std::fs::write(
        &base_rules_path,
        r#"
required_keys: [id]
fields:
  id:
    type: integer
count:
  min: 1
  max: 1
"#,
    )
    .expect("write base rules");

    let leaf_rules_path = leaf_dir.join("leaf.rules.yaml");
    std::fs::write(
        &leaf_rules_path,
        r#"
extends: ../shared/base.rules.yaml
required_keys: [status]
fields:
  status:
    enum: [ok, done]
"#,
    )
    .expect("write leaf rules");

    let args = AssertCommandArgs {
        input: None,
        from: Some(Format::Json),
        rules: Some(leaf_rules_path),
        schema: None,
    };

    let response = run_with_stdin(&args, Cursor::new(r#"[{"id":1,"status":"ok"}]"#));
    assert_eq!(response.exit_code, 0);
    assert_eq!(response.payload["matched"], Value::Bool(true));
    assert_eq!(response.payload["mismatch_count"], Value::from(0));
}

#[test]
fn assert_api_merges_multi_extends_in_order_with_last_wins() {
    let dir = tempdir().expect("tempdir");
    let base_a = dir.path().join("base-a.yaml");
    let base_b = dir.path().join("base-b.yaml");
    let leaf = dir.path().join("leaf.yaml");

    std::fs::write(
        &base_a,
        r#"
required_keys: [id]
forbid_keys: [debug]
fields:
  score:
    range:
      max: 100
count:
  max: 10
"#,
    )
    .expect("write base a");

    std::fs::write(
        &base_b,
        r#"
required_keys: [status]
forbid_keys: [trace]
fields:
  score:
    range:
      max: 5
count:
  max: 3
"#,
    )
    .expect("write base b");

    std::fs::write(
        &leaf,
        r#"
extends: [./base-a.yaml, ./base-b.yaml]
fields:
  score:
    range:
      max: 2
"#,
    )
    .expect("write leaf");

    let args = AssertCommandArgs {
        input: None,
        from: Some(Format::Json),
        rules: Some(leaf),
        schema: None,
    };

    let response = run_with_stdin(&args, Cursor::new(r#"[{"id":1,"status":"ok","score":3}]"#));
    assert_eq!(response.exit_code, 2);
    assert_eq!(response.payload["mismatch_count"], Value::from(1));
    assert_eq!(
        response.payload["mismatches"][0]["path"],
        Value::from("$[0].score")
    );
    assert_eq!(
        response.payload["mismatches"][0]["reason"],
        Value::from("above_max")
    );
    assert_eq!(
        response.payload["mismatches"][0]["expected"],
        Value::from(2),
    );
}

#[test]
fn assert_api_rejects_extends_cycles() {
    let dir = tempdir().expect("tempdir");
    let a = dir.path().join("a.yaml");
    let b = dir.path().join("b.yaml");

    std::fs::write(&a, "extends: ./b.yaml\nrequired_keys: [id]\ncount: {}\n").expect("write a");
    std::fs::write(&b, "extends: ./a.yaml\nrequired_keys: [id]\ncount: {}\n").expect("write b");

    let args = AssertCommandArgs {
        input: None,
        from: Some(Format::Json),
        rules: Some(a),
        schema: None,
    };

    let response = run_with_stdin(&args, Cursor::new("[]"));
    assert_eq!(response.exit_code, 3);
    assert_eq!(
        response.payload["error"],
        Value::String("input_usage_error".to_string())
    );
    assert!(
        response.payload["message"]
            .as_str()
            .expect("message")
            .contains("cycle")
    );
}

#[test]
fn assert_api_rejects_missing_extends_reference() {
    let dir = tempdir().expect("tempdir");
    let leaf = dir.path().join("leaf.yaml");
    std::fs::write(
        &leaf,
        r#"
extends: ./missing.rules.yaml
required_keys: [id]
count: {}
"#,
    )
    .expect("write rules");

    let args = AssertCommandArgs {
        input: None,
        from: Some(Format::Json),
        rules: Some(leaf),
        schema: None,
    };

    let response = run_with_stdin(&args, Cursor::new("[]"));
    assert_eq!(response.exit_code, 3);
    assert_eq!(
        response.payload["error"],
        Value::String("input_usage_error".to_string())
    );
    assert!(
        response.payload["message"]
            .as_str()
            .expect("message")
            .contains("failed to open rules file")
    );
}

#[test]
fn assert_rules_help_payload_includes_extends_key() {
    let payload = rules_help_payload();
    assert_eq!(
        payload["top_level_keys"]["extends"],
        Value::from("string | array<string> (optional, parent-relative path)")
    );
}

#[test]
fn assert_api_extends_merge_is_deterministic() {
    let dir = tempdir().expect("tempdir");
    let base = dir.path().join("base.yaml");
    let leaf = dir.path().join("leaf.yaml");

    std::fs::write(
        &base,
        r#"
required_keys: [z, a, z]
count: {}
"#,
    )
    .expect("write base");
    std::fs::write(
        &leaf,
        r#"
extends: ./base.yaml
required_keys: [b, a]
count: {}
"#,
    )
    .expect("write leaf");

    let args = AssertCommandArgs {
        input: None,
        from: Some(Format::Json),
        rules: Some(leaf),
        schema: None,
    };

    let first = run_with_stdin(&args, Cursor::new("[{}]"));
    let second = run_with_stdin(&args, Cursor::new("[{}]"));

    assert_eq!(first.exit_code, 2);
    assert_eq!(second.exit_code, 2);
    assert_eq!(first.payload, second.payload);
    assert_eq!(
        first.payload["mismatches"][0]["path"],
        Value::from("$[0].a")
    );
    assert_eq!(
        first.payload["mismatches"][1]["path"],
        Value::from("$[0].b")
    );
    assert_eq!(
        first.payload["mismatches"][2]["path"],
        Value::from("$[0].z")
    );
}

#[test]
fn assert_api_reports_mismatch_shape() {
    let dir = tempdir().expect("tempdir");
    let rules_path = dir.path().join("rules.json");
    std::fs::write(
        &rules_path,
        r#"{
            "required_keys": ["id", "score"],
            "fields": {
                "id": {"type": "integer"},
                "score": {"type": "number", "range": {"min": 0.0, "max": 1.0}}
            },
            "count": {"min": 1, "max": 1},
            "forbid_keys": []
        }"#,
    )
    .expect("write rules");

    let args = AssertCommandArgs {
        input: None,
        from: Some(Format::Json),
        rules: Some(rules_path),
        schema: None,
    };

    let response = run_with_stdin(&args, Cursor::new(r#"[{"id":"x","score":4}]"#));
    assert_eq!(response.exit_code, 2);
    assert_eq!(response.payload["matched"], Value::Bool(false));
    assert_eq!(response.payload["mismatch_count"], Value::from(2));

    let mismatches = response.payload["mismatches"]
        .as_array()
        .expect("mismatches array");
    assert!(!mismatches.is_empty());
    for mismatch in mismatches {
        let obj = mismatch.as_object().expect("mismatch object");
        assert!(obj.contains_key("path"));
        assert!(obj.contains_key("rule_kind"));
        assert!(obj.contains_key("reason"));
        assert!(obj.contains_key("actual"));
        assert!(obj.contains_key("expected"));
    }
}

#[test]
fn assert_api_rejects_legacy_top_level_rule_keys() {
    let dir = tempdir().expect("tempdir");
    let rules_path = dir.path().join("rules.json");
    std::fs::write(
        &rules_path,
        r#"{
            "required_keys": [],
            "types": {"id": "integer"},
            "count": {}
        }"#,
    )
    .expect("write rules");

    let args = AssertCommandArgs {
        input: None,
        from: Some(Format::Json),
        rules: Some(rules_path),
        schema: None,
    };

    let response = run_with_stdin(&args, Cursor::new("[]"));
    assert_eq!(response.exit_code, 3);
    assert_eq!(
        response.payload["error"],
        Value::String("input_usage_error".to_string())
    );
    let message = response.payload["message"]
        .as_str()
        .expect("input usage message");
    assert!(message.contains("unknown field"));
}

#[test]
fn assert_api_rejects_empty_field_rule_entry() {
    let dir = tempdir().expect("tempdir");
    let rules_path = dir.path().join("rules.json");
    std::fs::write(
        &rules_path,
        r#"{
            "required_keys": [],
            "fields": {"id": {}},
            "count": {}
        }"#,
    )
    .expect("write rules");

    let args = AssertCommandArgs {
        input: None,
        from: Some(Format::Json),
        rules: Some(rules_path),
        schema: None,
    };

    let response = run_with_stdin(&args, Cursor::new("[]"));
    assert_eq!(response.exit_code, 3);
    assert_eq!(
        response.payload["error"],
        Value::String("input_usage_error".to_string())
    );
    let message = response.payload["message"]
        .as_str()
        .expect("input usage message");
    assert!(message.contains("must define at least one"));
}

#[test]
fn assert_api_reports_input_usage_errors() {
    let dir = tempdir().expect("tempdir");
    let rules_path = dir.path().join("rules.json");
    std::fs::write(
        &rules_path,
        r#"{
            "required_keys": [],
            "count": {}
        }"#,
    )
    .expect("write rules");

    let args = AssertCommandArgs {
        input: None,
        from: None,
        rules: Some(rules_path),
        schema: None,
    };

    let response = run_with_stdin(&args, Cursor::new("[]"));
    assert_eq!(response.exit_code, 3);
    assert_eq!(
        response.payload["error"],
        Value::String("input_usage_error".to_string())
    );
}

#[test]
fn assert_api_rejects_unknown_rule_keys() {
    let dir = tempdir().expect("tempdir");
    let rules_path = dir.path().join("rules.json");
    std::fs::write(
        &rules_path,
        r#"{
            "required_keys": [],
            "count": {"min": 0, "max": 1, "oops": 2},
            "fields": {},
            "unexpected": true
        }"#,
    )
    .expect("write rules");

    let args = AssertCommandArgs {
        input: None,
        from: Some(Format::Json),
        rules: Some(rules_path),
        schema: None,
    };

    let response = run_with_stdin(&args, Cursor::new("[]"));
    assert_eq!(response.exit_code, 3);
    assert_eq!(
        response.payload["error"],
        Value::String("input_usage_error".to_string())
    );
    let message = response.payload["message"]
        .as_str()
        .expect("input usage message");
    assert!(message.contains("unknown field"));
}

#[test]
fn assert_api_compares_large_integer_ranges_exactly() {
    let dir = tempdir().expect("tempdir");
    let rules_path = dir.path().join("rules.json");
    std::fs::write(
        &rules_path,
        r#"{
            "required_keys": [],
            "count": {},
            "fields": {
                "value": {"range": {"max": 9007199254740992}}
            }
        }"#,
    )
    .expect("write rules");

    let args = AssertCommandArgs {
        input: None,
        from: Some(Format::Json),
        rules: Some(rules_path),
        schema: None,
    };

    let response = run_with_stdin(&args, Cursor::new(r#"[{"value":9007199254740993}]"#));
    assert_eq!(response.exit_code, 2);
    assert_eq!(response.payload["mismatch_count"], Value::from(1));
    assert_eq!(
        response.payload["mismatches"][0]["path"],
        Value::from("$[0].value")
    );
    assert_eq!(
        response.payload["mismatches"][0]["reason"],
        Value::from("above_max")
    );
    assert_eq!(
        response.payload["mismatches"][0]["rule_kind"],
        Value::from("ranges")
    );
}

#[test]
fn assert_api_supports_enum_pattern_forbid_keys_and_nullable() {
    let dir = tempdir().expect("tempdir");
    let rules_path = dir.path().join("rules.json");
    std::fs::write(
        &rules_path,
        r#"{
            "required_keys": [],
            "forbid_keys": ["meta.blocked"],
            "fields": {
                "optional": {"nullable": true},
                "status": {"enum": ["ok", "done"]},
                "name": {"pattern": "^[a-z]+_[0-9]+$"}
            },
            "count": {}
        }"#,
    )
    .expect("write rules");

    let args = AssertCommandArgs {
        input: None,
        from: Some(Format::Json),
        rules: Some(rules_path),
        schema: None,
    };

    let response = run_with_stdin(
        &args,
        Cursor::new(
            r#"[{"status":"pending","name":"User-1","meta":{"blocked":true},"optional":null}]"#,
        ),
    );
    assert_eq!(response.exit_code, 2);
    assert_eq!(response.payload["matched"], Value::Bool(false));
    assert_eq!(response.payload["mismatch_count"], Value::from(3));
    assert_eq!(
        response.payload["mismatches"][0]["rule_kind"],
        Value::from("forbid_keys")
    );
    assert_eq!(
        response.payload["mismatches"][1]["rule_kind"],
        Value::from("enum")
    );
    assert_eq!(
        response.payload["mismatches"][2]["rule_kind"],
        Value::from("pattern")
    );
}

#[test]
fn assert_api_rejects_invalid_pattern_rules() {
    let dir = tempdir().expect("tempdir");
    let rules_path = dir.path().join("rules.json");
    std::fs::write(
        &rules_path,
        r#"{
            "required_keys": [],
            "forbid_keys": [],
            "fields": {
                "name": {"pattern": "[a-z"}
            },
            "count": {}
        }"#,
    )
    .expect("write rules");

    let args = AssertCommandArgs {
        input: None,
        from: Some(Format::Json),
        rules: Some(rules_path),
        schema: None,
    };

    let response = run_with_stdin(&args, Cursor::new("[]"));
    assert_eq!(response.exit_code, 3);
    assert_eq!(
        response.payload["error"],
        Value::String("input_usage_error".to_string())
    );
    let message = response.payload["message"]
        .as_str()
        .expect("input usage message");
    assert!(message.contains("invalid pattern"));
    assert!(message.contains("fields.name.pattern"));
}

#[test]
fn assert_api_supports_jsonschema_mode() {
    let dir = tempdir().expect("tempdir");
    let schema_path = dir.path().join("schema.json");
    std::fs::write(
        &schema_path,
        r#"{
            "type": "object",
            "required": ["id", "score"],
            "properties": {
                "id": {"type": "integer"},
                "score": {"type": "number", "maximum": 10}
            }
        }"#,
    )
    .expect("write schema");

    let args = AssertCommandArgs {
        input: None,
        from: Some(Format::Json),
        rules: None,
        schema: Some(schema_path),
    };

    let response = run_with_stdin(&args, Cursor::new(r#"[{"id":"x","score":12}]"#));
    assert_eq!(response.exit_code, 2);
    assert_eq!(response.payload["matched"], Value::Bool(false));
    assert_eq!(response.payload["mismatch_count"], Value::from(2));

    let mismatches = response.payload["mismatches"]
        .as_array()
        .expect("mismatches array");
    assert!(!mismatches.is_empty());
    for mismatch in mismatches {
        let obj = mismatch.as_object().expect("mismatch object");
        assert!(obj.contains_key("path"));
        assert!(obj.contains_key("reason"));
        assert!(obj.contains_key("actual"));
        assert!(obj.contains_key("expected"));
        assert_eq!(
            obj.get("reason"),
            Some(&Value::String("schema_mismatch".to_string()))
        );
    }
}

#[test]
fn assert_api_rejects_rules_and_schema_together() {
    let dir = tempdir().expect("tempdir");
    let rules_path = dir.path().join("rules.json");
    let schema_path = dir.path().join("schema.json");
    std::fs::write(&rules_path, r#"{"required_keys":[],"count":{}}"#).expect("write rules");
    std::fs::write(&schema_path, r#"{"type":"object"}"#).expect("write schema");

    let args = AssertCommandArgs {
        input: None,
        from: Some(Format::Json),
        rules: Some(rules_path),
        schema: Some(schema_path),
    };

    let response = run_with_stdin(&args, Cursor::new("[]"));
    assert_eq!(response.exit_code, 3);
    assert_eq!(
        response.payload["error"],
        Value::String("input_usage_error".to_string())
    );
}

#[test]
fn assert_api_maps_schema_parse_errors_to_exit_three() {
    let dir = tempdir().expect("tempdir");
    let schema_path = dir.path().join("schema.json");
    std::fs::write(&schema_path, r#"{"type":"object","properties":{"id":}"#)
        .expect("write invalid schema");

    let args = AssertCommandArgs {
        input: None,
        from: Some(Format::Json),
        rules: None,
        schema: Some(schema_path),
    };

    let response = run_with_stdin(&args, Cursor::new(r#"[{"id":1}]"#));
    assert_eq!(response.exit_code, 3);
    assert_eq!(
        response.payload["error"],
        Value::String("input_usage_error".to_string())
    );
}

#[test]
fn assert_api_schema_mode_keeps_numeric_object_key_paths_unambiguous() {
    let dir = tempdir().expect("tempdir");
    let schema_path = dir.path().join("schema.json");
    std::fs::write(
        &schema_path,
        r#"{
            "type": "object",
            "required": ["0"],
            "properties": {
                "0": {"type": "integer"}
            }
        }"#,
    )
    .expect("write schema");

    let args = AssertCommandArgs {
        input: None,
        from: Some(Format::Json),
        rules: None,
        schema: Some(schema_path),
    };

    let response = run_with_stdin(&args, Cursor::new(r#"[{"0":"x"}]"#));
    assert_eq!(response.exit_code, 2);
    assert_eq!(
        response.payload["mismatches"][0]["path"],
        Value::from("$[0][\"0\"]")
    );
}

#[test]
fn cloud_run_raw_sample_rules_accept_valid_input() {
    let input = r#"
apiVersion: serving.knative.dev/v1
kind: Service
metadata:
  name: demo-service
spec:
  template:
    spec:
      containers:
        - image: us-docker.pkg.dev/p/r/app:1
"#;
    let response = run_with_sample_rules(
        "examples/assert-rules/cloud-run/raw.rules.yaml",
        Format::Yaml,
        input,
    );
    assert_eq!(response.exit_code, 0);
    assert_eq!(response.payload["matched"], Value::Bool(true));
}

#[test]
fn cloud_run_raw_sample_rules_reject_invalid_input() {
    let input = r#"
apiVersion: serving.knative.dev/v1
kind: Revision
metadata:
  name: demo-service
spec:
  template:
    spec:
      containers:
        - image: us-docker.pkg.dev/p/r/app:1
status: {}
"#;
    let response = run_with_sample_rules(
        "examples/assert-rules/cloud-run/raw.rules.yaml",
        Format::Yaml,
        input,
    );
    assert_eq!(response.exit_code, 2);
    assert_eq!(response.payload["matched"], Value::Bool(false));
    assert!(has_mismatch(
        &response.payload,
        "$[0].status",
        "forbid_keys",
        "forbidden_key"
    ));
    assert!(has_mismatch(
        &response.payload,
        "$[0].kind",
        "enum",
        "enum_mismatch"
    ));
}

#[test]
fn github_actions_raw_sample_rules_accept_valid_input() {
    let input = r#"
name: CI
'on':
  push:
    branches: [main]
jobs: {}
permissions:
  contents: read
"#;
    let response = run_with_sample_rules(
        "examples/assert-rules/github-actions/raw.rules.yaml",
        Format::Yaml,
        input,
    );
    assert_eq!(response.exit_code, 0);
    assert_eq!(response.payload["matched"], Value::Bool(true));
}

#[test]
fn github_actions_raw_sample_rules_reject_invalid_input() {
    let input = r#"
name: CI
'on': push
jobs: {}
"#;
    let response = run_with_sample_rules(
        "examples/assert-rules/github-actions/raw.rules.yaml",
        Format::Yaml,
        input,
    );
    assert_eq!(response.exit_code, 2);
    assert_eq!(response.payload["matched"], Value::Bool(false));
    assert!(has_mismatch(
        &response.payload,
        "$[0].permissions",
        "required_keys",
        "missing_key"
    ));
    assert!(has_mismatch(
        &response.payload,
        "$[0].on",
        "types",
        "type_mismatch"
    ));
}

#[test]
fn gitlab_ci_raw_sample_rules_accept_valid_input() {
    let input = r#"
stages: [build, test]
build:
  stage: build
  script: ["echo ok"]
"#;
    let response = run_with_sample_rules(
        "examples/assert-rules/gitlab-ci/raw.rules.yaml",
        Format::Yaml,
        input,
    );
    assert_eq!(response.exit_code, 0);
    assert_eq!(response.payload["matched"], Value::Bool(true));
}

#[test]
fn gitlab_ci_raw_sample_rules_reject_invalid_input() {
    let input = r#"
stages: build
"#;
    let response = run_with_sample_rules(
        "examples/assert-rules/gitlab-ci/raw.rules.yaml",
        Format::Yaml,
        input,
    );
    assert_eq!(response.exit_code, 2);
    assert_eq!(response.payload["matched"], Value::Bool(false));
    assert!(has_mismatch(
        &response.payload,
        "$[0].stages",
        "types",
        "type_mismatch"
    ));
}

#[test]
fn github_actions_jobs_sample_rules_accept_valid_input() {
    let input = r#"[{"job_id":"build","runs_on":"ubuntu-latest","steps_count":2,"uses_unpinned_action":false}]"#;
    let response = run_with_sample_rules(
        "examples/assert-rules/github-actions/jobs.rules.yaml",
        Format::Json,
        input,
    );
    assert_eq!(response.exit_code, 0);
    assert_eq!(response.payload["matched"], Value::Bool(true));
}

#[test]
fn github_actions_jobs_sample_rules_reject_invalid_input() {
    let input = r#"[{"job_id":"build","runs_on":"ubuntu-latest","steps_count":0,"uses_unpinned_action":true}]"#;
    let response = run_with_sample_rules(
        "examples/assert-rules/github-actions/jobs.rules.yaml",
        Format::Json,
        input,
    );
    assert_eq!(response.exit_code, 2);
    assert_eq!(response.payload["matched"], Value::Bool(false));
    assert!(has_mismatch(
        &response.payload,
        "$[0].steps_count",
        "ranges",
        "below_min"
    ));
    assert!(has_mismatch(
        &response.payload,
        "$[0].uses_unpinned_action",
        "enum",
        "enum_mismatch"
    ));
}

#[test]
fn gitlab_ci_jobs_sample_rules_accept_valid_input() {
    let input =
        r#"[{"job_name":"build:test","stage":"build","script_count":1,"uses_only_except":false}]"#;
    let response = run_with_sample_rules(
        "examples/assert-rules/gitlab-ci/jobs.rules.yaml",
        Format::Json,
        input,
    );
    assert_eq!(response.exit_code, 0);
    assert_eq!(response.payload["matched"], Value::Bool(true));
}

#[test]
fn gitlab_ci_jobs_sample_rules_reject_invalid_input() {
    let input =
        r#"[{"job_name":"build test","stage":"build","script_count":0,"uses_only_except":true}]"#;
    let response = run_with_sample_rules(
        "examples/assert-rules/gitlab-ci/jobs.rules.yaml",
        Format::Json,
        input,
    );
    assert_eq!(response.exit_code, 2);
    assert_eq!(response.payload["matched"], Value::Bool(false));
    assert!(has_mismatch(
        &response.payload,
        "$[0].job_name",
        "pattern",
        "pattern_mismatch"
    ));
    assert!(has_mismatch(
        &response.payload,
        "$[0].script_count",
        "ranges",
        "below_min"
    ));
    assert!(has_mismatch(
        &response.payload,
        "$[0].uses_only_except",
        "enum",
        "enum_mismatch"
    ));
}

#[test]
fn all_sample_rules_files_are_loadable() {
    let cases = [
        (
            "examples/assert-rules/cloud-run/raw.rules.yaml",
            Format::Yaml,
            r#"
apiVersion: serving.knative.dev/v1
kind: Service
metadata:
  name: demo-service
spec:
  template:
    spec:
      containers:
        - image: us-docker.pkg.dev/p/r/app:1
"#,
        ),
        (
            "examples/assert-rules/github-actions/raw.rules.yaml",
            Format::Yaml,
            r#"
name: CI
'on':
  push: {}
jobs: {}
permissions:
  contents: read
"#,
        ),
        (
            "examples/assert-rules/github-actions/jobs.rules.yaml",
            Format::Json,
            r#"[{"job_id":"build","runs_on":"ubuntu-latest","steps_count":1,"uses_unpinned_action":false}]"#,
        ),
        (
            "examples/assert-rules/gitlab-ci/raw.rules.yaml",
            Format::Yaml,
            "stages: [build]\n",
        ),
        (
            "examples/assert-rules/gitlab-ci/jobs.rules.yaml",
            Format::Json,
            r#"[{"job_name":"build","stage":"build","script_count":1,"uses_only_except":false}]"#,
        ),
    ];

    for (rules_path, format, input) in cases {
        let response = run_with_sample_rules(rules_path, format, input);
        assert_eq!(
            response.exit_code, 0,
            "sample rules should load: {rules_path}"
        );
    }
}

#[test]
fn sample_rules_files_do_not_contain_unknown_fields() {
    let rules_paths = [
        "examples/assert-rules/cloud-run/raw.rules.yaml",
        "examples/assert-rules/github-actions/raw.rules.yaml",
        "examples/assert-rules/github-actions/jobs.rules.yaml",
        "examples/assert-rules/gitlab-ci/raw.rules.yaml",
        "examples/assert-rules/gitlab-ci/jobs.rules.yaml",
    ];

    for rules_path in rules_paths {
        let response = run_with_sample_rules(rules_path, Format::Json, "[]");
        assert_eq!(
            response.exit_code, 2,
            "invalid sample rules schema should fail with exit=3: {rules_path}"
        );
        assert_ne!(
            response.payload["error"],
            Value::String("input_usage_error".to_string())
        );
    }
}
