use predicates::prelude::predicate;
use serde_json::{Value, json};

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
            "canon",
            "assert",
            "gate-schema",
            "gate",
            "sdiff",
            "profile",
            "merge",
            "doctor",
            "recipe"
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
fn contract_gate_command_reports_policy_contract_fields() {
    let output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .args(["contract", "--command", "gate"])
        .output()
        .expect("run contract gate");

    assert_eq!(output.status.code(), Some(0));
    assert!(output.stderr.is_empty());

    let payload: Value = serde_json::from_slice(&output.stdout).expect("stdout json");
    assert_eq!(payload["command"], json!("gate"));
    assert_eq!(payload["schema"], json!("dataq.gate.policy.output.v1"));
    assert_eq!(
        payload["output_fields"],
        json!(["matched", "violations", "details"])
    );
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
fn contract_doctor_command_exit_three_describes_profile_aware_semantics() {
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
            "without `--profile`: missing/non-executable `jq|yq|mlr`; with `--profile`: selected profile requirements are unsatisfied"
        )
    );
}
