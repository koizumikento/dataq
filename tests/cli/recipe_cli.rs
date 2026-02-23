use std::fs;

use serde_json::Value;
use tempfile::tempdir;

#[test]
fn recipe_run_success_outputs_deterministic_summary() {
    let dir = tempdir().expect("temp dir");
    let input_path = dir.path().join("input.json");
    let recipe_path = dir.path().join("recipe.yaml");

    fs::write(&input_path, r#"[{"id":"1"},{"id":"2"}]"#).expect("write input");
    fs::write(
        &recipe_path,
        format!(
            r#"
version: dataq.recipe.v1
steps:
  - kind: canon
    args:
      input: "{}"
      from: json
  - kind: assert
    args:
      rules:
        required_keys: [id]
        fields:
          id:
            type: integer
"#,
            input_path.display()
        ),
    )
    .expect("write recipe");

    let output_first = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .args([
            "recipe",
            "run",
            "--file",
            recipe_path.to_str().expect("utf8 path"),
        ])
        .output()
        .expect("run first");
    let output_second = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .args([
            "recipe",
            "run",
            "--file",
            recipe_path.to_str().expect("utf8 path"),
        ])
        .output()
        .expect("run second");

    assert_eq!(output_first.status.code(), Some(0));
    assert_eq!(output_second.status.code(), Some(0));
    assert_eq!(output_first.stdout, output_second.stdout);
    assert!(output_first.stderr.is_empty());

    let summary: Value = serde_json::from_slice(&output_first.stdout).expect("summary json");
    assert_eq!(summary["matched"], Value::Bool(true));
    assert_eq!(summary["exit_code"], Value::from(0));
    assert_eq!(summary["steps"][0]["kind"], Value::from("canon"));
    assert_eq!(summary["steps"][1]["kind"], Value::from("assert"));
    assert_eq!(summary["steps"][1]["matched"], Value::Bool(true));
}

#[test]
fn recipe_run_resolves_relative_paths_from_recipe_directory() {
    let dir = tempdir().expect("temp dir");
    let recipe_dir = dir.path().join("recipes");
    let run_dir = dir.path().join("run");
    fs::create_dir_all(&recipe_dir).expect("create recipe dir");
    fs::create_dir_all(&run_dir).expect("create run dir");

    let input_path = recipe_dir.join("input.json");
    let recipe_path = recipe_dir.join("recipe.yaml");
    fs::write(&input_path, r#"[{"id":"1"}]"#).expect("write input");
    fs::write(
        &recipe_path,
        r#"
version: dataq.recipe.v1
steps:
  - kind: canon
    args:
      input: ./input.json
      from: json
"#,
    )
    .expect("write recipe");

    let output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .current_dir(&run_dir)
        .args([
            "recipe",
            "run",
            "--file",
            recipe_path.to_str().expect("utf8 path"),
        ])
        .output()
        .expect("run command");

    assert_eq!(output.status.code(), Some(0));
    assert!(output.stderr.is_empty());
    let summary: Value = serde_json::from_slice(&output.stdout).expect("summary json");
    assert_eq!(summary["matched"], Value::Bool(true));
    assert_eq!(summary["steps"][0]["kind"], Value::from("canon"));
}

#[test]
fn recipe_run_invalid_schema_returns_exit_three() {
    let dir = tempdir().expect("temp dir");
    let recipe_path = dir.path().join("recipe.json");

    fs::write(&recipe_path, r#"{"version":"wrong.version","steps":[]}"#).expect("write recipe");

    let output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .args([
            "recipe",
            "run",
            "--file",
            recipe_path.to_str().expect("utf8 path"),
        ])
        .output()
        .expect("run command");

    assert_eq!(output.status.code(), Some(3));
    let stderr_json = parse_last_stderr_json(&output.stderr);
    assert_eq!(stderr_json["error"], Value::from("input_usage_error"));
}

#[test]
fn recipe_run_sdiff_mismatch_returns_exit_two() {
    let dir = tempdir().expect("temp dir");
    let input_path = dir.path().join("input.json");
    let right_path = dir.path().join("right.json");
    let recipe_path = dir.path().join("recipe.json");

    fs::write(&input_path, r#"[{"id":"1","v":"a"}]"#).expect("write input");
    fs::write(&right_path, r#"[{"id":"1","v":"b"}]"#).expect("write right");
    fs::write(
        &recipe_path,
        format!(
            r#"{{
  "version": "dataq.recipe.v1",
  "steps": [
    {{
      "kind": "canon",
      "args": {{
        "input": "{}",
        "from": "json"
      }}
    }},
    {{
      "kind": "sdiff",
      "args": {{
        "right": "{}",
        "right_from": "json"
      }}
    }}
  ]
}}"#,
            input_path.display(),
            right_path.display()
        ),
    )
    .expect("write recipe");

    let output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .args([
            "recipe",
            "run",
            "--file",
            recipe_path.to_str().expect("utf8 path"),
        ])
        .output()
        .expect("run command");

    assert_eq!(output.status.code(), Some(2));
    assert!(output.stderr.is_empty());

    let summary: Value = serde_json::from_slice(&output.stdout).expect("summary json");
    assert_eq!(summary["matched"], Value::Bool(false));
    assert_eq!(summary["exit_code"], Value::from(2));
    assert_eq!(summary["steps"][1]["kind"], Value::from("sdiff"));
    assert_eq!(summary["steps"][1]["matched"], Value::Bool(false));
}

#[test]
fn recipe_run_assert_mismatch_returns_exit_two() {
    let dir = tempdir().expect("temp dir");
    let input_path = dir.path().join("input.json");
    let recipe_path = dir.path().join("recipe.json");

    fs::write(&input_path, r#"[{"id":"oops"}]"#).expect("write input");
    fs::write(
        &recipe_path,
        format!(
            r#"{{
  "version": "dataq.recipe.v1",
  "steps": [
    {{
      "kind": "canon",
      "args": {{
        "input": "{}",
        "from": "json"
      }}
    }},
    {{
      "kind": "assert",
      "args": {{
        "rules": {{
          "required_keys": ["id"],
          "fields": {{
            "id": {{"type": "integer"}}
          }}
        }}
      }}
    }}
  ]
}}"#,
            input_path.display()
        ),
    )
    .expect("write recipe");

    let output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .args([
            "recipe",
            "run",
            "--file",
            recipe_path.to_str().expect("utf8 path"),
        ])
        .output()
        .expect("run command");

    assert_eq!(output.status.code(), Some(2));
    assert!(output.stderr.is_empty());

    let summary: Value = serde_json::from_slice(&output.stdout).expect("summary json");
    assert_eq!(summary["matched"], Value::Bool(false));
    assert_eq!(summary["exit_code"], Value::from(2));
    assert_eq!(summary["steps"][1]["kind"], Value::from("assert"));
    assert_eq!(summary["steps"][1]["matched"], Value::Bool(false));
}

#[test]
fn recipe_run_emit_pipeline_keeps_stdout_clean() {
    let dir = tempdir().expect("temp dir");
    let input_path = dir.path().join("input.json");
    let recipe_path = dir.path().join("recipe.json");

    fs::write(&input_path, r#"[{"id":"1"}]"#).expect("write input");
    fs::write(
        &recipe_path,
        format!(
            r#"{{
  "version": "dataq.recipe.v1",
  "steps": [
    {{
      "kind": "canon",
      "args": {{
        "input": "{}",
        "from": "json"
      }}
    }}
  ]
}}"#,
            input_path.display()
        ),
    )
    .expect("write recipe");

    let output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .args([
            "recipe",
            "run",
            "--file",
            recipe_path.to_str().expect("utf8 path"),
            "--emit-pipeline",
        ])
        .output()
        .expect("run command");

    assert_eq!(output.status.code(), Some(0));
    let summary: Value = serde_json::from_slice(&output.stdout).expect("stdout json");
    assert_eq!(summary["matched"], Value::Bool(true));

    let pipeline_json = parse_last_stderr_json(&output.stderr);
    assert_eq!(pipeline_json["command"], Value::from("recipe"));
    assert!(
        pipeline_json["steps"]
            .as_array()
            .expect("steps array")
            .iter()
            .any(|entry| entry == "execute_step_0_canon")
    );
}

fn parse_last_stderr_json(stderr: &[u8]) -> Value {
    let text = String::from_utf8(stderr.to_vec()).expect("stderr utf8");
    let line = text
        .lines()
        .rev()
        .find(|candidate| !candidate.trim().is_empty())
        .expect("non-empty stderr line");
    serde_json::from_str(line).expect("stderr json")
}
