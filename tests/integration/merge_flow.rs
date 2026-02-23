use std::fs;

use serde_json::json;
use tempfile::tempdir;

#[test]
fn overlay_order_is_applied_left_to_right() {
    let dir = tempdir().expect("tempdir");
    let base = dir.path().join("base.yaml");
    let overlay1 = dir.path().join("overlay1.json");
    let overlay2 = dir.path().join("overlay2.yaml");

    fs::write(
        &base,
        r#"
meta:
  source: base
cfg:
  threshold: 1
  tags: [base]
"#,
    )
    .expect("write base");
    fs::write(
        &overlay1,
        r#"{"cfg":{"threshold":2,"tags":["first"]},"left":1}"#,
    )
    .expect("write overlay1");
    fs::write(
        &overlay2,
        r#"
cfg:
  threshold: 3
  tags: [second]
right: 1
"#,
    )
    .expect("write overlay2");

    let first_then_second = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .args([
            "merge",
            "--base",
            base.to_str().expect("utf8 base path"),
            "--overlay",
            overlay1.to_str().expect("utf8 overlay1 path"),
            "--overlay",
            overlay2.to_str().expect("utf8 overlay2 path"),
            "--policy",
            "last-wins",
        ])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();

    let second_then_first = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .args([
            "merge",
            "--base",
            base.to_str().expect("utf8 base path"),
            "--overlay",
            overlay2.to_str().expect("utf8 overlay2 path"),
            "--overlay",
            overlay1.to_str().expect("utf8 overlay1 path"),
            "--policy",
            "last-wins",
        ])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();

    let lhs: serde_json::Value = serde_json::from_slice(&first_then_second).expect("lhs json");
    let rhs: serde_json::Value = serde_json::from_slice(&second_then_first).expect("rhs json");

    assert_eq!(
        lhs,
        json!({
            "cfg": {"tags": ["second"], "threshold": 3},
            "left": 1,
            "meta": {"source": "base"},
            "right": 1
        })
    );
    assert_eq!(
        rhs,
        json!({
            "cfg": {"tags": ["first"], "threshold": 2},
            "left": 1,
            "meta": {"source": "base"},
            "right": 1
        })
    );
    assert_ne!(lhs, rhs);
}

#[test]
fn re_run_with_same_input_is_deterministic() {
    let dir = tempdir().expect("tempdir");
    let base = dir.path().join("base.json");
    let overlay1 = dir.path().join("overlay1.json");
    let overlay2 = dir.path().join("overlay2.json");

    fs::write(
        &base,
        r#"{"cfg":{"arr":[{"a":1},2],"obj":{"left":1}},"stable":true}"#,
    )
    .expect("write base");
    fs::write(&overlay1, r#"{"cfg":{"arr":[{"b":2}]},"x":1}"#).expect("write overlay1");
    fs::write(&overlay2, r#"{"cfg":{"obj":{"right":2}},"y":2}"#).expect("write overlay2");

    let first = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .args([
            "merge",
            "--base",
            base.to_str().expect("utf8 base path"),
            "--overlay",
            overlay1.to_str().expect("utf8 overlay1 path"),
            "--overlay",
            overlay2.to_str().expect("utf8 overlay2 path"),
            "--policy",
            "deep-merge",
        ])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();

    let second = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .args([
            "merge",
            "--base",
            base.to_str().expect("utf8 base path"),
            "--overlay",
            overlay1.to_str().expect("utf8 overlay1 path"),
            "--overlay",
            overlay2.to_str().expect("utf8 overlay2 path"),
            "--policy",
            "deep-merge",
        ])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();

    assert_eq!(first, second);
}

#[test]
fn merge_without_policy_path_preserves_existing_behavior() {
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

    let stdout = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .args([
            "merge",
            "--base",
            base.to_str().expect("utf8 base path"),
            "--overlay",
            overlay.to_str().expect("utf8 overlay path"),
            "--policy",
            "deep-merge",
        ])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();

    let actual: serde_json::Value = serde_json::from_slice(&stdout).expect("merge output json");
    assert_eq!(
        actual,
        json!({
            "cfg": {
                "items": [{"left": 1, "right": 2}, 2],
                "obj": {"left": 1, "right": 2}
            }
        })
    );
}

#[test]
fn longest_matching_policy_path_wins() {
    let dir = tempdir().expect("tempdir");
    let base = dir.path().join("base.json");
    let overlay = dir.path().join("overlay.json");

    fs::write(&base, r#"{"cfg":{"items":[{"left":1},2]}}"#).expect("write base");
    fs::write(&overlay, r#"{"cfg":{"items":[{"right":2}]}}"#).expect("write overlay");

    let stdout = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .args([
            "merge",
            "--base",
            base.to_str().expect("utf8 base path"),
            "--overlay",
            overlay.to_str().expect("utf8 overlay path"),
            "--policy",
            "deep-merge",
            "--policy-path",
            r#"$["cfg"]=array-replace"#,
            "--policy-path",
            r#"$["cfg"]["items"]=deep-merge"#,
        ])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();

    let actual: serde_json::Value = serde_json::from_slice(&stdout).expect("merge output json");
    assert_eq!(
        actual,
        json!({
            "cfg": {
                "items": [{"left": 1, "right": 2}, 2]
            }
        })
    );
}
