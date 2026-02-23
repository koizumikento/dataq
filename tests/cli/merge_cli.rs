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
    });
    let deep = run(&MergeCommandArgs {
        base: base.clone(),
        overlays: vec![overlay.clone()],
        policy: MergePolicy::DeepMerge,
    });
    let replace = run(&MergeCommandArgs {
        base,
        overlays: vec![overlay],
        policy: MergePolicy::ArrayReplace,
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
