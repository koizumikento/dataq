use std::fs;
use std::path::Path;

use predicates::prelude::predicate;
use serde_json::{Value, json};
use tempfile::tempdir;

#[test]
fn contract_command_returns_expected_machine_readable_shape() {
    let output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .args(["contract", "--command", "assert"])
        .output()
        .expect("run contract");

    assert_eq!(output.status.code(), Some(0));
    assert!(output.stderr.is_empty());

    let payload: Value = serde_json::from_slice(&output.stdout).expect("stdout json");
    assert_eq!(payload["command"], json!("assert"));
    assert_eq!(payload["schema"], json!("dataq.assert.output.v1"));
    assert_eq!(
        payload["output_fields"],
        json!(["matched", "mismatch_count", "mismatches"])
    );
    assert!(payload["exit_codes"]["0"].is_string());
    assert!(payload["exit_codes"]["2"].is_string());
    assert!(payload["exit_codes"]["3"].is_string());
    assert!(payload["exit_codes"]["1"].is_string());
    assert!(payload["notes"].is_array());
}

#[test]
fn contract_all_returns_deterministic_order() {
    let first = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .args(["contract", "--all"])
        .output()
        .expect("run first");
    let second = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .args(["contract", "--all"])
        .output()
        .expect("run second");

    assert_eq!(first.status.code(), Some(0));
    assert_eq!(second.status.code(), Some(0));
    assert_eq!(first.stdout, second.stdout);

    let payload: Value = serde_json::from_slice(&first.stdout).expect("stdout json");
    let contracts = payload.as_array().expect("contract array");
    let commands: Vec<&str> = contracts
        .iter()
        .map(|entry| entry["command"].as_str().expect("command string"))
        .collect();

    assert_eq!(
        commands,
        vec![
            "canon", "assert", "sdiff", "profile", "merge", "doctor", "recipe"
        ]
    );
    for entry in contracts {
        assert!(entry["command"].is_string());
        assert!(entry["schema"].is_string());
        assert!(entry["output_fields"].is_array());
        assert!(entry["exit_codes"].is_object());
        assert!(entry["notes"].is_array());
    }
}

#[test]
fn contract_command_unknown_value_returns_exit_three() {
    assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .args(["contract", "--command", "unknown"])
        .assert()
        .code(3)
        .stderr(predicate::str::contains("\"error\":\"input_usage_error\""));
}

#[test]
fn contract_doctor_command_exit_three_describes_dependency_failure() {
    let output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .args(["contract", "--command", "doctor"])
        .output()
        .expect("run contract doctor");

    assert_eq!(output.status.code(), Some(0));
    assert!(output.stderr.is_empty());

    let payload: Value = serde_json::from_slice(&output.stdout).expect("stdout json");
    assert_eq!(payload["command"], json!("doctor"));
    assert_eq!(
        payload["exit_codes"]["3"],
        json!(
            "tool/dependency availability failure (missing or non-executable `jq`, `yq`, or `mlr`) or missing required capabilities for a requested profile"
        )
    );
}

#[test]
fn contract_doctor_output_fields_match_guaranteed_default_doctor_root_fields() {
    let dir = tempdir().expect("tempdir");
    write_exec_script(&dir.path().join("jq"), "#!/bin/sh\necho 'jq-1.7'\n");
    write_exec_script(
        &dir.path().join("yq"),
        "#!/bin/sh\necho 'yq (https://github.com/mikefarah/yq/) version 4.44.6'\n",
    );
    write_exec_script(&dir.path().join("mlr"), "#!/bin/sh\necho 'mlr 6.13.0'\n");

    let doctor_output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .env("PATH", dir.path())
        .arg("doctor")
        .output()
        .expect("run doctor");
    assert_eq!(doctor_output.status.code(), Some(0));

    let doctor_payload: Value = serde_json::from_slice(&doctor_output.stdout).expect("doctor json");
    let guaranteed_root_fields: Vec<String> = doctor_payload
        .as_object()
        .expect("doctor object")
        .keys()
        .cloned()
        .collect();

    let contract_output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .args(["contract", "--command", "doctor"])
        .output()
        .expect("run contract doctor");
    assert_eq!(contract_output.status.code(), Some(0));

    let contract_payload: Value =
        serde_json::from_slice(&contract_output.stdout).expect("contract doctor json");
    let contract_fields: Vec<String> = contract_payload["output_fields"]
        .as_array()
        .expect("output_fields")
        .iter()
        .map(|field| field.as_str().expect("field string").to_owned())
        .collect();

    assert_eq!(contract_fields, guaranteed_root_fields);
}

fn write_exec_script(path: &Path, body: &str) {
    fs::write(path, body).expect("write script");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o755)).expect("chmod");
    }
}
