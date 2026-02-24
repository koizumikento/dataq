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
            "ingest-api",
            "ingest yaml-jobs",
            "assert",
            "gate-schema",
            "gate",
            "sdiff",
            "diff-source",
            "profile",
            "merge",
            "doctor",
            "recipe-run",
            "recipe-lock",
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
fn contract_ingest_command_exit_three_describes_yaml_mode_tool_failures() {
    let output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .args(["contract", "--command", "ingest"])
        .output()
        .expect("run contract ingest");

    assert_eq!(output.status.code(), Some(0));
    assert!(output.stderr.is_empty());

    let payload: Value = serde_json::from_slice(&output.stdout).expect("stdout json");
    assert_eq!(payload["command"], json!("ingest yaml-jobs"));
    assert_eq!(payload["schema"], json!("dataq.ingest.yaml_jobs.output.v1"));
    assert_eq!(
        payload["exit_codes"]["3"],
        json!("input/usage error (malformed YAML, unknown mode, or missing `jq`/`yq`/`mlr`)")
    );
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
fn contract_diff_source_command_includes_sources_field() {
    let output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .args(["contract", "--command", "diff-source"])
        .output()
        .expect("run contract diff-source");

    assert_eq!(output.status.code(), Some(0));
    assert!(output.stderr.is_empty());

    let payload: Value = serde_json::from_slice(&output.stdout).expect("stdout json");
    assert_eq!(payload["command"], json!("diff-source"));
    assert_eq!(payload["schema"], json!("dataq.diff.source.output.v1"));
    assert_eq!(
        payload["output_fields"],
        json!(["counts", "keys", "ignored_paths", "values", "sources"])
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

#[test]
fn contract_recipe_run_command_matches_recipe_run_shape() {
    let output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .args(["contract", "--command", "recipe-run"])
        .output()
        .expect("run contract recipe-run");

    assert_eq!(output.status.code(), Some(0));
    assert!(output.stderr.is_empty());

    let payload: Value = serde_json::from_slice(&output.stdout).expect("stdout json");
    assert_eq!(payload["command"], json!("recipe-run"));
    assert_eq!(payload["schema"], json!("dataq.recipe.run.output.v1"));
    assert_eq!(
        payload["output_fields"],
        json!(["matched", "exit_code", "steps"])
    );
    assert!(
        !payload["output_fields"]
            .as_array()
            .expect("output_fields array")
            .iter()
            .any(|field| field == "lock_check")
    );
    assert!(
        !payload["notes"]
            .as_array()
            .expect("notes array")
            .iter()
            .filter_map(|entry| entry.as_str())
            .any(|note| note.contains("recipe replay") || note.contains("lock_check"))
    );
}

#[test]
fn contract_recipe_lock_command_reports_lock_output_shape() {
    let output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .args(["contract", "--command", "recipe-lock"])
        .output()
        .expect("run contract recipe-lock");

    assert_eq!(output.status.code(), Some(0));
    assert!(output.stderr.is_empty());

    let payload: Value = serde_json::from_slice(&output.stdout).expect("stdout json");
    assert_eq!(payload["command"], json!("recipe-lock"));
    assert_eq!(payload["schema"], json!("dataq.recipe.lock.output.v1"));
    assert_eq!(
        payload["output_fields"],
        json!([
            "version",
            "command_graph_hash",
            "args_hash",
            "tool_versions",
            "dataq_version"
        ])
    );
    assert!(payload["exit_codes"]["0"].is_string());
    assert!(payload["exit_codes"]["2"].is_string());
    assert!(payload["exit_codes"]["3"].is_string());
    assert!(payload["exit_codes"]["1"].is_string());
    assert_eq!(
        payload["notes"],
        json!([
            "`tool_versions` keys are deterministically sorted by tool name (`jq`, `mlr`, `yq`).",
            "Lock output is canonicalized before write/emit."
        ])
    );
}
