use std::fs;
use std::path::{Path, PathBuf};

use serde_json::{Value, json};
use tempfile::tempdir;

#[test]
fn doctor_returns_exit_zero_with_fixed_order_and_schema_when_all_tools_are_available() {
    let dir = tempdir().expect("tempdir");
    write_exec_script(&dir.path().join("jq"), "#!/bin/sh\necho 'jq-1.7'\n");
    write_exec_script(
        &dir.path().join("yq"),
        "#!/bin/sh\necho 'yq (https://github.com/mikefarah/yq/) version 4.44.6'\n",
    );
    write_exec_script(&dir.path().join("mlr"), "#!/bin/sh\necho 'mlr 6.13.0'\n");

    let output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .env("PATH", dir.path())
        .arg("doctor")
        .output()
        .expect("run doctor");

    assert_eq!(output.status.code(), Some(0));
    assert!(output.stderr.is_empty());

    let stdout_json: Value = serde_json::from_slice(&output.stdout).expect("stdout json");
    assert!(stdout_json.get("capabilities").is_none());
    let tools = stdout_json["tools"].as_array().expect("tools array");
    assert_eq!(tools.len(), 3);

    assert_eq!(tools[0]["name"], json!("jq"));
    assert_eq!(tools[0]["found"], json!(true));
    assert_eq!(tools[0]["executable"], json!(true));
    assert_eq!(tools[0]["version"], json!("jq-1.7"));
    assert_eq!(tools[0]["message"], json!("ok"));

    assert_eq!(tools[1]["name"], json!("yq"));
    assert_eq!(tools[1]["found"], json!(true));
    assert_eq!(tools[1]["executable"], json!(true));
    assert_eq!(tools[1]["message"], json!("ok"));

    assert_eq!(tools[2]["name"], json!("mlr"));
    assert_eq!(tools[2]["found"], json!(true));
    assert_eq!(tools[2]["executable"], json!(true));
    assert_eq!(tools[2]["message"], json!("ok"));
}

#[test]
fn doctor_capabilities_reports_capability_array_in_fixed_order() {
    let dir = tempdir().expect("tempdir");
    write_exec_script(&dir.path().join("jq"), "#!/bin/sh\necho 'jq-1.7'\n");
    write_exec_script(
        &dir.path().join("yq"),
        "#!/bin/sh\necho 'yq (https://github.com/mikefarah/yq/) version 4.44.6'\n",
    );
    write_exec_script(&dir.path().join("mlr"), "#!/bin/sh\necho 'mlr 6.13.0'\n");

    let output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .env("PATH", dir.path())
        .args(["doctor", "--capabilities"])
        .output()
        .expect("run doctor");

    assert_eq!(output.status.code(), Some(0));
    assert!(output.stderr.is_empty());

    let stdout_json: Value = serde_json::from_slice(&output.stdout).expect("stdout json");
    let tools = stdout_json["tools"].as_array().expect("tools array");
    assert_eq!(tools.len(), 3);

    let capabilities = stdout_json["capabilities"]
        .as_array()
        .expect("capabilities array");
    assert_eq!(capabilities.len(), 3);
    assert_eq!(capabilities[0]["name"], json!("jq.null_input_eval"));
    assert_eq!(capabilities[0]["tool"], json!("jq"));
    assert_eq!(capabilities[0]["available"], json!(true));
    assert_eq!(capabilities[0]["message"], json!("ok"));
    assert_eq!(capabilities[1]["name"], json!("yq.null_input_eval"));
    assert_eq!(capabilities[1]["tool"], json!("yq"));
    assert_eq!(capabilities[1]["available"], json!(true));
    assert_eq!(capabilities[2]["name"], json!("mlr.help_command"));
    assert_eq!(capabilities[2]["tool"], json!("mlr"));
    assert_eq!(capabilities[2]["available"], json!(true));
}

#[test]
fn doctor_returns_exit_three_when_any_tool_is_missing() {
    let dir = tempdir().expect("tempdir");
    write_exec_script(&dir.path().join("jq"), "#!/bin/sh\necho 'jq-1.7'\n");
    write_exec_script(&dir.path().join("yq"), "#!/bin/sh\necho 'yq-4.44.6'\n");

    let output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .env("PATH", dir.path())
        .arg("doctor")
        .output()
        .expect("run doctor");

    assert_eq!(output.status.code(), Some(3));
    let stdout_json: Value = serde_json::from_slice(&output.stdout).expect("stdout json");
    let tools = stdout_json["tools"].as_array().expect("tools array");

    assert_eq!(tools[2]["name"], json!("mlr"));
    assert_eq!(tools[2]["found"], json!(false));
    assert_eq!(tools[2]["version"], Value::Null);
    assert_eq!(tools[2]["executable"], json!(false));
    assert!(
        tools[2]["message"]
            .as_str()
            .expect("message")
            .contains("Install `mlr`")
    );
}

#[test]
fn doctor_capabilities_can_report_partial_state_without_failing_default_exit_logic() {
    let dir = tempdir().expect("tempdir");
    write_exec_script(
        &dir.path().join("jq"),
        "#!/bin/sh\nif [ \"$1\" = \"--version\" ]; then\n  echo 'jq-1.7'\nelse\n  echo 'null'\nfi\n",
    );
    write_exec_script(
        &dir.path().join("yq"),
        "#!/bin/sh\nif [ \"$1\" = \"--version\" ]; then\n  echo 'yq-4.44.6'\n  exit 0\nfi\necho 'capability probe failed' 1>&2\nexit 9\n",
    );
    write_exec_script(
        &dir.path().join("mlr"),
        "#!/bin/sh\nif [ \"$1\" = \"--version\" ]; then\n  echo 'mlr-6.13.0'\nelse\n  echo 'help text'\nfi\n",
    );

    let output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .env("PATH", dir.path())
        .args(["doctor", "--capabilities"])
        .output()
        .expect("run doctor");

    assert_eq!(output.status.code(), Some(0));
    assert!(output.stderr.is_empty());

    let stdout_json: Value = serde_json::from_slice(&output.stdout).expect("stdout json");
    let capabilities = stdout_json["capabilities"]
        .as_array()
        .expect("capabilities array");

    assert_eq!(capabilities[0]["name"], json!("jq.null_input_eval"));
    assert_eq!(capabilities[0]["available"], json!(true));
    assert_eq!(capabilities[1]["name"], json!("yq.null_input_eval"));
    assert_eq!(capabilities[1]["available"], json!(false));
    assert_eq!(capabilities[2]["name"], json!("mlr.help_command"));
    assert_eq!(capabilities[2]["available"], json!(true));
}

#[test]
fn doctor_returns_exit_three_when_any_tool_is_not_executable() {
    let dir = tempdir().expect("tempdir");
    write_exec_script(&dir.path().join("jq"), "#!/bin/sh\necho 'jq-1.7'\n");
    write_exec_script(&dir.path().join("yq"), "#!/bin/sh\necho 'yq-4.44.6'\n");
    write_non_exec_script(&dir.path().join("mlr"), "#!/bin/sh\necho 'mlr-6.13.0'\n");

    let output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .env("PATH", dir.path())
        .arg("doctor")
        .output()
        .expect("run doctor");

    assert_eq!(output.status.code(), Some(3));
    let stdout_json: Value = serde_json::from_slice(&output.stdout).expect("stdout json");
    let tools = stdout_json["tools"].as_array().expect("tools array");

    assert_eq!(tools[2]["name"], json!("mlr"));
    assert_eq!(tools[2]["found"], json!(true));
    assert_eq!(tools[2]["executable"], json!(false));
    assert!(
        tools[2]["message"]
            .as_str()
            .expect("message")
            .contains("not executable")
    );
}

#[test]
fn doctor_emit_pipeline_writes_doctor_steps_to_stderr() {
    let dir = tempdir().expect("tempdir");
    write_exec_script(&dir.path().join("jq"), "#!/bin/sh\necho 'jq-1.7'\n");
    write_exec_script(&dir.path().join("yq"), "#!/bin/sh\necho 'yq-4.44.6'\n");
    write_exec_script(&dir.path().join("mlr"), "#!/bin/sh\necho 'mlr-6.13.0'\n");

    let output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .env("PATH", dir.path())
        .args(["--emit-pipeline", "doctor"])
        .output()
        .expect("run doctor");

    assert_eq!(output.status.code(), Some(0));
    let stderr_json = parse_last_stderr_json(&output.stderr);

    assert_eq!(stderr_json["command"], json!("doctor"));
    assert_eq!(
        stderr_json["steps"],
        json!(["doctor_probe_tools", "doctor_probe_capabilities"])
    );

    let tools = stderr_json["external_tools"]
        .as_array()
        .expect("external_tools array");
    assert_eq!(tools[0]["name"], json!("jq"));
    assert_eq!(tools[0]["used"], json!(true));
    assert_eq!(tools[1]["name"], json!("yq"));
    assert_eq!(tools[1]["used"], json!(true));
    assert_eq!(tools[2]["name"], json!("mlr"));
    assert_eq!(tools[2]["used"], json!(true));
}

#[test]
fn doctor_emit_pipeline_fingerprint_reuses_probe_versions() {
    let dir = tempdir().expect("tempdir");
    let jq_counter = dir.path().join("jq.counter");
    let jq_script = format!(
        "#!/bin/sh\ncount_file='{}'\ncount=0\nif [ -f \"$count_file\" ]; then count=$(cat \"$count_file\"); fi\ncount=$((count + 1))\necho \"$count\" > \"$count_file\"\necho \"jq-v$count\"\n",
        jq_counter.display()
    );
    write_exec_script(&dir.path().join("jq"), &jq_script);
    write_exec_script(&dir.path().join("yq"), "#!/bin/sh\necho 'yq-v1'\n");
    write_exec_script(&dir.path().join("mlr"), "#!/bin/sh\necho 'mlr-v1'\n");

    let output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .env("PATH", dir.path())
        .args(["--emit-pipeline", "doctor"])
        .output()
        .expect("run doctor");

    assert_eq!(output.status.code(), Some(0));
    let stdout_json: Value = serde_json::from_slice(&output.stdout).expect("stdout json");
    let tools = stdout_json["tools"].as_array().expect("tools array");
    assert_eq!(tools[0]["name"], json!("jq"));
    assert_eq!(tools[0]["version"], json!("jq-v1"));

    let stderr_json = parse_last_stderr_json(&output.stderr);
    assert_eq!(
        stderr_json["fingerprint"]["tool_versions"]["jq"],
        json!("jq-v1")
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

fn write_non_exec_script(path: &PathBuf, body: &str) {
    fs::write(path, body).expect("write script");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o644)).expect("chmod");
    }
}
