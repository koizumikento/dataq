use std::fs;

use dataq::cmd::merge::{MergeCommandArgs, run};
use dataq::engine::merge::MergePolicy;
use predicates::prelude::predicate;
use serde_json::json;
use tempfile::tempdir;

#[test]
fn merge_command_policies_produce_expected_differences() {
    let dir = tempdir().expect("tempdir");
    let base = dir.path().join("base.json");
    let overlay = dir.path().join("overlay.json");

    fs::write(
        &base,
        r#"{"cfg":{"obj":{"a":1},"arr":[{"x":1},2]},"keep":true}"#,
    )
    .expect("write base");
    fs::write(
        &overlay,
        r#"{"cfg":{"obj":{"b":2},"arr":[{"y":9}]},"add":"ok"}"#,
    )
    .expect("write overlay");

    let last = run(&MergeCommandArgs {
        base: base.clone(),
        overlays: vec![overlay.clone()],
        policy: MergePolicy::LastWins,
        policy_paths: Vec::new(),
    });
    let deep = run(&MergeCommandArgs {
        base: base.clone(),
        overlays: vec![overlay.clone()],
        policy: MergePolicy::DeepMerge,
        policy_paths: Vec::new(),
    });
    let replace = run(&MergeCommandArgs {
        base,
        overlays: vec![overlay],
        policy: MergePolicy::ArrayReplace,
        policy_paths: Vec::new(),
    });

    assert_eq!(last.exit_code, 0);
    assert_eq!(deep.exit_code, 0);
    assert_eq!(replace.exit_code, 0);

    assert_eq!(
        last.payload,
        json!({
            "add": "ok",
            "cfg": {
                "arr": [{"y": 9}],
                "obj": {"b": 2}
            },
            "keep": true
        })
    );
    assert_eq!(
        deep.payload,
        json!({
            "add": "ok",
            "cfg": {
                "arr": [{"x": 1, "y": 9}, 2],
                "obj": {"a": 1, "b": 2}
            },
            "keep": true
        })
    );
    assert_eq!(
        replace.payload,
        json!({
            "add": "ok",
            "cfg": {
                "arr": [{"y": 9}],
                "obj": {"a": 1, "b": 2}
            },
            "keep": true
        })
    );
}

#[test]
fn unsupported_policy_returns_exit_code_three() {
    let dir = tempdir().expect("tempdir");
    let base = dir.path().join("base.json");
    let overlay = dir.path().join("overlay.json");
    fs::write(&base, "{}").expect("write base");
    fs::write(&overlay, "{}").expect("write overlay");

    assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .args([
            "merge",
            "--base",
            base.to_str().expect("utf8 base path"),
            "--overlay",
            overlay.to_str().expect("utf8 overlay path"),
            "--policy",
            "invalid-policy",
        ])
        .assert()
        .code(3)
        .stderr(predicate::str::contains("\"error\":\"input_usage_error\""));
}

#[test]
fn policy_path_applies_to_subtree_only() {
    let dir = tempdir().expect("tempdir");
    let base = dir.path().join("base.json");
    let overlay = dir.path().join("overlay.json");
    fs::write(
        &base,
        r#"{"cfg":{"items":[{"left":1},2],"obj":{"left":1}}}"#,
    )
    .expect("write base");
    fs::write(
        &overlay,
        r#"{"cfg":{"items":[{"right":2}],"obj":{"right":2}}}"#,
    )
    .expect("write overlay");

    let response = run(&MergeCommandArgs {
        base,
        overlays: vec![overlay],
        policy: MergePolicy::DeepMerge,
        policy_paths: vec![r#"$["cfg"]["items"]=array-replace"#.to_string()],
    });

    assert_eq!(response.exit_code, 0);
    assert_eq!(
        response.payload,
        json!({
            "cfg": {
                "items": [{"right": 2}],
                "obj": {"left": 1, "right": 2}
            }
        })
    );
}

#[test]
fn invalid_policy_path_definition_returns_exit_code_three() {
    let dir = tempdir().expect("tempdir");
    let base = dir.path().join("base.json");
    let overlay = dir.path().join("overlay.json");
    fs::write(&base, "{}").expect("write base");
    fs::write(&overlay, "{}").expect("write overlay");

    let invalid_path = run(&MergeCommandArgs {
        base: base.clone(),
        overlays: vec![overlay.clone()],
        policy: MergePolicy::DeepMerge,
        policy_paths: vec![r#"$[cfg]=deep-merge"#.to_string()],
    });
    assert_eq!(invalid_path.exit_code, 3);
    assert_eq!(invalid_path.payload["error"], json!("input_usage_error"));

    let invalid_policy = run(&MergeCommandArgs {
        base,
        overlays: vec![overlay],
        policy: MergePolicy::DeepMerge,
        policy_paths: vec![r#"$["cfg"]=not-a-policy"#.to_string()],
    });
    assert_eq!(invalid_policy.exit_code, 3);
    assert_eq!(invalid_policy.payload["error"], json!("input_usage_error"));
}
