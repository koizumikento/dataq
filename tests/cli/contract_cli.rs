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
            "ingest-jc",
            "ingest-tabular",
            "assert",
            "gate-schema",
            "gate",
            "schema-infer",
            "sdiff",
            "diff-source",
            "profile",
            "ingest.doc",
            "ingest.notes",
            "ingest-book",
            "join",
            "aggregate",
            "scan",
            "transform-rowset",
            "transform-sql",
            "merge",
            "doctor",
            "recipe-run",
            "recipe-lock",
            "recipe-replay",
            "emit-plan",
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
fn contract_profile_command_mentions_projection_fields() {
    let output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .args(["contract", "--command", "profile"])
        .output()
        .expect("run contract profile");

    assert_eq!(output.status.code(), Some(0));
    assert!(output.stderr.is_empty());

    let payload: Value = serde_json::from_slice(&output.stdout).expect("stdout json");
    assert_eq!(payload["command"], json!("profile"));
    assert_eq!(
        payload["output_fields"],
        json!([
            "record_count",
            "field_count",
            "truncated",
            "returned_field_count",
            "fields",
            "missing_fields"
        ])
    );
    assert!(
        payload["notes"]
            .as_array()
            .expect("notes")
            .iter()
            .any(|note| note.as_str().unwrap_or_default().contains("--field"))
    );
    assert!(
        payload["notes"]
            .as_array()
            .expect("notes")
            .iter()
            .any(|note| note.as_str().unwrap_or_default().contains("--brief"))
    );
    assert!(
        payload["notes"]
            .as_array()
            .expect("notes")
            .iter()
            .any(|note| note
                .as_str()
                .unwrap_or_default()
                .contains("large JSON numbers"))
    );
}

#[test]
fn contract_aggregate_command_mentions_exact_integer_sum_and_sort() {
    let output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .args(["contract", "--command", "aggregate"])
        .output()
        .expect("run contract aggregate");

    assert_eq!(output.status.code(), Some(0));
    assert!(output.stderr.is_empty());

    let payload: Value = serde_json::from_slice(&output.stdout).expect("stdout json");
    let notes = payload["notes"].as_array().expect("notes array");
    assert!(notes.iter().any(|note| {
        let note = note.as_str().unwrap_or_default();
        note.contains("i64/u64") && note.contains("without f64 precision loss")
    }));
    assert!(notes.iter().any(|note| {
        let note = note.as_str().unwrap_or_default();
        note.contains("input/representation errors") && note.contains("rounded to f64")
    }));
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
fn contract_transform_rowset_command_is_available() {
    let output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .args(["contract", "--command", "transform-rowset"])
        .output()
        .expect("run contract transform-rowset");

    assert_eq!(output.status.code(), Some(0));
    assert!(output.stderr.is_empty());

    let payload: Value = serde_json::from_slice(&output.stdout).expect("stdout json");
    assert_eq!(payload["command"], json!("transform-rowset"));
    assert_eq!(payload["schema"], json!("dataq.transform.rowset.output.v1"));
}

#[test]
fn contract_transform_sql_command_is_available() {
    let output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .args(["contract", "--command", "transform-sql"])
        .output()
        .expect("run contract transform-sql");

    assert_eq!(output.status.code(), Some(0));
    assert!(output.stderr.is_empty());

    let payload: Value = serde_json::from_slice(&output.stdout).expect("stdout json");
    assert_eq!(payload["command"], json!("transform-sql"));
    assert_eq!(payload["schema"], json!("dataq.transform.sql.output.v1"));
}

#[test]
fn contract_required_public_commands_are_available() {
    let cases = [
        ("join", "dataq.join.output.v1", json!([])),
        ("aggregate", "dataq.aggregate.output.v1", json!([])),
        (
            "recipe-replay",
            "dataq.recipe.replay.output.v1",
            json!(["matched", "exit_code", "lock_check", "steps"]),
        ),
        ("schema-infer", "dataq.schema.infer.output.v1", json!([])),
        (
            "ingest-tabular",
            "dataq.ingest.tabular.output.v1",
            json!([]),
        ),
        (
            "ingest-jc",
            "dataq.ingest.jc.output.v1",
            json!(["source", "parser", "result_type", "record_count", "records"]),
        ),
        (
            "emit-plan",
            "dataq.emit.plan.output.v1",
            json!(["command", "args", "stages", "tools"]),
        ),
    ];

    for (command, schema, output_fields) in cases {
        let output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
            .args(["contract", "--command", command])
            .output()
            .unwrap_or_else(|error| panic!("run contract {command}: {error}"));

        assert_eq!(output.status.code(), Some(0), "{command}");
        assert!(output.stderr.is_empty(), "{command}");

        let payload: Value = serde_json::from_slice(&output.stdout).expect("stdout json");
        assert_eq!(payload["command"], json!(command));
        assert_eq!(payload["schema"], json!(schema));
        assert_eq!(payload["output_fields"], output_fields);
        assert!(payload["exit_codes"]["0"].is_string());
        assert!(payload["exit_codes"]["2"].is_string());
        assert!(payload["exit_codes"]["3"].is_string());
        assert!(payload["exit_codes"]["1"].is_string());
        assert!(payload["notes"].is_array());
    }
}

#[test]
fn contract_all_contains_transform_sql_output_contract() {
    let output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .args(["contract", "--all"])
        .output()
        .expect("run contract all");

    assert_eq!(output.status.code(), Some(0));
    assert!(output.stderr.is_empty());

    let payload: Value = serde_json::from_slice(&output.stdout).expect("stdout json");
    let contracts = payload.as_array().expect("contract array");
    let transform_sql = contracts
        .iter()
        .find(|entry| entry["command"] == json!("transform-sql"))
        .expect("transform-sql contract");

    assert_eq!(
        transform_sql["schema"],
        json!("dataq.transform.sql.output.v1")
    );
    assert_eq!(transform_sql["output_fields"], json!([]));
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
