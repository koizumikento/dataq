use std::fs;
use std::path::Path;

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

#[test]
fn recipe_lock_regenerates_byte_identically() {
    let dir = tempdir().expect("temp dir");
    let bin_dir = dir.path().join("bin");
    fs::create_dir_all(&bin_dir).expect("create bin dir");
    write_exec_script(&bin_dir.join("jq"), "#!/bin/sh\necho 'jq-1.7'\n");
    write_exec_script(&bin_dir.join("yq"), "#!/bin/sh\necho 'yq-4.44.6'\n");
    write_exec_script(&bin_dir.join("mlr"), "#!/bin/sh\necho 'mlr-6.13.0'\n");

    let recipe_path = dir.path().join("recipe-lock.json");
    fs::write(&recipe_path, r#"{"version":"dataq.recipe.v1","steps":[]}"#).expect("write recipe");

    let first_stdout = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .env("PATH", bin_dir.as_path())
        .args([
            "recipe",
            "lock",
            "--file",
            recipe_path.to_str().expect("utf8 path"),
        ])
        .output()
        .expect("first lock run");
    let second_stdout = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .env("PATH", bin_dir.as_path())
        .args([
            "recipe",
            "lock",
            "--file",
            recipe_path.to_str().expect("utf8 path"),
        ])
        .output()
        .expect("second lock run");

    assert_eq!(first_stdout.status.code(), Some(0));
    assert_eq!(second_stdout.status.code(), Some(0));
    assert_eq!(first_stdout.stdout, second_stdout.stdout);
    assert!(first_stdout.stderr.is_empty());
    assert!(second_stdout.stderr.is_empty());

    let lock_json: Value = serde_json::from_slice(&first_stdout.stdout).expect("stdout lock json");
    assert_eq!(lock_json["version"], Value::from("dataq.recipe.lock.v1"));
    assert!(lock_json["command_graph_hash"].is_string());
    assert!(lock_json["args_hash"].is_string());
    assert!(lock_json["tool_versions"].is_object());
    assert!(lock_json["dataq_version"].is_string());
    let stdout_text = String::from_utf8(first_stdout.stdout.clone()).expect("stdout utf8");
    let jq_pos = stdout_text.find("\"jq\":").expect("jq key");
    let mlr_pos = stdout_text.find("\"mlr\":").expect("mlr key");
    let yq_pos = stdout_text.find("\"yq\":").expect("yq key");
    assert!(jq_pos < mlr_pos);
    assert!(mlr_pos < yq_pos);

    let lock_path = dir.path().join("recipe.lock.json");
    let first_file = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .env("PATH", bin_dir.as_path())
        .args([
            "recipe",
            "lock",
            "--file",
            recipe_path.to_str().expect("utf8 path"),
            "--out",
            lock_path.to_str().expect("utf8 path"),
        ])
        .output()
        .expect("first file lock run");
    let bytes_first = fs::read(&lock_path).expect("read first lock bytes");
    let second_file = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .env("PATH", bin_dir.as_path())
        .args([
            "recipe",
            "lock",
            "--file",
            recipe_path.to_str().expect("utf8 path"),
            "--out",
            lock_path.to_str().expect("utf8 path"),
        ])
        .output()
        .expect("second file lock run");
    let bytes_second = fs::read(&lock_path).expect("read second lock bytes");

    assert_eq!(first_file.status.code(), Some(0));
    assert_eq!(second_file.status.code(), Some(0));
    assert!(first_file.stdout.is_empty());
    assert!(second_file.stdout.is_empty());
    assert!(first_file.stderr.is_empty());
    assert!(second_file.stderr.is_empty());
    assert_eq!(bytes_first, bytes_second);
}

#[test]
fn recipe_lock_invalid_recipe_returns_exit_three() {
    let dir = tempdir().expect("temp dir");
    let recipe_path = dir.path().join("recipe.json");
    fs::write(&recipe_path, r#"{"version":"wrong.version","steps":[]}"#).expect("write recipe");

    let output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .args([
            "recipe",
            "lock",
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
fn recipe_lock_invalid_step_args_returns_exit_three() {
    let dir = tempdir().expect("temp dir");
    let input_path = dir.path().join("input.json");
    let recipe_path = dir.path().join("recipe.json");
    fs::write(&input_path, r#"[{"id":"1"}]"#).expect("write input");
    fs::write(
        &recipe_path,
        serde_json::json!({
            "version":"dataq.recipe.v1",
            "steps":[
                {
                    "kind":"canon",
                    "args":{
                        "input": input_path,
                        "from":"json"
                    }
                },
                {
                    "kind":"assert",
                    "args":{
                        "rules":{"required_keys":[],"forbid_keys":[],"fields":{}},
                        "schema":{"type":"object"}
                    }
                }
            ]
        })
        .to_string(),
    )
    .expect("write recipe");

    let output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .args([
            "recipe",
            "lock",
            "--file",
            recipe_path.to_str().expect("utf8 path"),
        ])
        .output()
        .expect("run command");

    assert_eq!(output.status.code(), Some(3));
    let stderr_json = parse_last_stderr_json(&output.stderr);
    assert_eq!(stderr_json["error"], Value::from("input_usage_error"));
    assert_eq!(
        stderr_json["message"],
        Value::from("assert step cannot combine rules and schema sources")
    );
}

#[test]
fn recipe_lock_validates_missing_canon_input_without_prior_values_like_recipe_run() {
    let dir = tempdir().expect("temp dir");
    let recipe_path = dir.path().join("recipe.json");
    fs::write(
        &recipe_path,
        r#"{
            "version":"dataq.recipe.v1",
            "steps":[
                {
                    "kind":"canon",
                    "args":{}
                }
            ]
        }"#,
    )
    .expect("write recipe");

    let lock_output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .args([
            "recipe",
            "lock",
            "--file",
            recipe_path.to_str().expect("utf8 path"),
        ])
        .output()
        .expect("run recipe lock");
    let run_output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .args([
            "recipe",
            "run",
            "--file",
            recipe_path.to_str().expect("utf8 path"),
        ])
        .output()
        .expect("run recipe run");

    assert_eq!(lock_output.status.code(), Some(3));
    assert_eq!(run_output.status.code(), Some(3));
    let lock_stderr = parse_last_stderr_json(&lock_output.stderr);
    let run_stderr = parse_last_stderr_json(&run_output.stderr);
    assert_eq!(lock_stderr["error"], Value::from("input_usage_error"));
    assert_eq!(lock_stderr["message"], run_stderr["message"]);
    assert_eq!(
        lock_stderr["message"],
        Value::from("canon step requires `args.input` or prior in-memory values")
    );
}

#[test]
fn recipe_lock_validates_implicit_canon_input_format_like_recipe_run() {
    let dir = tempdir().expect("temp dir");
    let invalid_input = dir.path().join("input.unsupported");
    let recipe_path = dir.path().join("recipe.json");
    fs::write(
        &recipe_path,
        format!(
            r#"{{
  "version": "dataq.recipe.v1",
  "steps": [
    {{
      "kind": "canon",
      "args": {{
        "input": "{}"
      }}
    }}
  ]
}}"#,
            invalid_input.display()
        ),
    )
    .expect("write recipe");

    let lock_output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .args([
            "recipe",
            "lock",
            "--file",
            recipe_path.to_str().expect("utf8 path"),
        ])
        .output()
        .expect("run recipe lock");
    let run_output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .args([
            "recipe",
            "run",
            "--file",
            recipe_path.to_str().expect("utf8 path"),
        ])
        .output()
        .expect("run recipe run");

    assert_eq!(lock_output.status.code(), Some(3));
    assert_eq!(run_output.status.code(), Some(3));
    let lock_stderr = parse_last_stderr_json(&lock_output.stderr);
    let run_stderr = parse_last_stderr_json(&run_output.stderr);
    assert_eq!(lock_stderr["error"], Value::from("input_usage_error"));
    assert_eq!(lock_stderr["message"], run_stderr["message"]);
    assert!(
        lock_stderr["message"]
            .as_str()
            .expect("message")
            .contains("canon.args.input")
    );
}

#[test]
fn recipe_lock_validates_implicit_sdiff_right_format_like_recipe_run() {
    let dir = tempdir().expect("temp dir");
    let input_path = dir.path().join("input.json");
    let invalid_right = dir.path().join("right.unsupported");
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
    }},
    {{
      "kind": "sdiff",
      "args": {{
        "right": "{}"
      }}
    }}
  ]
}}"#,
            input_path.display(),
            invalid_right.display()
        ),
    )
    .expect("write recipe");

    let lock_output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .args([
            "recipe",
            "lock",
            "--file",
            recipe_path.to_str().expect("utf8 path"),
        ])
        .output()
        .expect("run recipe lock");
    let run_output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .args([
            "recipe",
            "run",
            "--file",
            recipe_path.to_str().expect("utf8 path"),
        ])
        .output()
        .expect("run recipe run");

    assert_eq!(lock_output.status.code(), Some(3));
    assert_eq!(run_output.status.code(), Some(3));
    let lock_stderr = parse_last_stderr_json(&lock_output.stderr);
    let run_stderr = parse_last_stderr_json(&run_output.stderr);
    assert_eq!(lock_stderr["error"], Value::from("input_usage_error"));
    assert_eq!(lock_stderr["message"], run_stderr["message"]);
    assert!(
        lock_stderr["message"]
            .as_str()
            .expect("message")
            .contains("sdiff.args.right")
    );
}

#[test]
fn recipe_lock_validates_implicit_assert_rules_file_format_like_recipe_run() {
    let dir = tempdir().expect("temp dir");
    let input_path = dir.path().join("input.json");
    let invalid_rules = dir.path().join("rules.unsupported");
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
    }},
    {{
      "kind": "assert",
      "args": {{
        "rules_file": "{}"
      }}
    }}
  ]
}}"#,
            input_path.display(),
            invalid_rules.display()
        ),
    )
    .expect("write recipe");

    let lock_output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .args([
            "recipe",
            "lock",
            "--file",
            recipe_path.to_str().expect("utf8 path"),
        ])
        .output()
        .expect("run recipe lock");
    let run_output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .args([
            "recipe",
            "run",
            "--file",
            recipe_path.to_str().expect("utf8 path"),
        ])
        .output()
        .expect("run recipe run");

    assert_eq!(lock_output.status.code(), Some(3));
    assert_eq!(run_output.status.code(), Some(3));
    let lock_stderr = parse_last_stderr_json(&lock_output.stderr);
    let run_stderr = parse_last_stderr_json(&run_output.stderr);
    assert_eq!(lock_stderr["error"], Value::from("input_usage_error"));
    assert_eq!(lock_stderr["message"], run_stderr["message"]);
    assert!(
        lock_stderr["message"]
            .as_str()
            .expect("message")
            .contains("assert.rules_file")
    );
}

#[test]
fn recipe_lock_validates_implicit_assert_schema_file_format_like_recipe_run() {
    let dir = tempdir().expect("temp dir");
    let input_path = dir.path().join("input.json");
    let invalid_schema = dir.path().join("schema.unsupported");
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
    }},
    {{
      "kind": "assert",
      "args": {{
        "schema_file": "{}"
      }}
    }}
  ]
}}"#,
            input_path.display(),
            invalid_schema.display()
        ),
    )
    .expect("write recipe");

    let lock_output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .args([
            "recipe",
            "lock",
            "--file",
            recipe_path.to_str().expect("utf8 path"),
        ])
        .output()
        .expect("run recipe lock");
    let run_output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .args([
            "recipe",
            "run",
            "--file",
            recipe_path.to_str().expect("utf8 path"),
        ])
        .output()
        .expect("run recipe run");

    assert_eq!(lock_output.status.code(), Some(3));
    assert_eq!(run_output.status.code(), Some(3));
    let lock_stderr = parse_last_stderr_json(&lock_output.stderr);
    let run_stderr = parse_last_stderr_json(&run_output.stderr);
    assert_eq!(lock_stderr["error"], Value::from("input_usage_error"));
    assert_eq!(lock_stderr["message"], run_stderr["message"]);
    assert!(
        lock_stderr["message"]
            .as_str()
            .expect("message")
            .contains("assert.schema_file")
    );
}

#[test]
fn recipe_lock_unresolved_tool_returns_exit_three() {
    let dir = tempdir().expect("temp dir");
    let recipe_path = dir.path().join("recipe.json");
    fs::write(&recipe_path, r#"{"version":"dataq.recipe.v1","steps":[]}"#).expect("write recipe");

    let output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .env("DATAQ_JQ_BIN", "/definitely-missing/jq")
        .args([
            "recipe",
            "lock",
            "--file",
            recipe_path.to_str().expect("utf8 path"),
        ])
        .output()
        .expect("run command");

    assert_eq!(output.status.code(), Some(3));
    let stderr_json = parse_last_stderr_json(&output.stderr);
    assert_eq!(stderr_json["error"], Value::from("input_usage_error"));
    assert!(
        stderr_json["message"]
            .as_str()
            .expect("message")
            .contains("jq")
    );
}

#[test]
fn recipe_lock_emit_pipeline_reports_lock_steps() {
    let dir = tempdir().expect("temp dir");
    let bin_dir = dir.path().join("bin");
    fs::create_dir_all(&bin_dir).expect("create bin dir");
    write_exec_script(&bin_dir.join("jq"), "#!/bin/sh\necho 'jq-1.7'\n");
    write_exec_script(&bin_dir.join("yq"), "#!/bin/sh\necho 'yq-4.44.6'\n");
    write_exec_script(&bin_dir.join("mlr"), "#!/bin/sh\necho 'mlr-6.13.0'\n");

    let recipe_path = dir.path().join("recipe.json");
    fs::write(&recipe_path, r#"{"version":"dataq.recipe.v1","steps":[]}"#).expect("write recipe");

    let output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .env("PATH", bin_dir.as_path())
        .args([
            "recipe",
            "lock",
            "--file",
            recipe_path.to_str().expect("utf8 path"),
            "--emit-pipeline",
        ])
        .output()
        .expect("run command");

    assert_eq!(output.status.code(), Some(0));
    let pipeline_json = parse_last_stderr_json(&output.stderr);
    assert_eq!(pipeline_json["command"], Value::from("recipe"));
    assert_eq!(
        pipeline_json["steps"],
        Value::from(vec![
            "recipe_lock_parse",
            "recipe_lock_probe_tools",
            "recipe_lock_fingerprint",
        ])
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

fn write_exec_script(path: &Path, body: &str) {
    fs::write(path, body).expect("write script");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o755)).expect("chmod");
    }
}
