use std::fs;

use serde_json::Value;
use tempfile::tempdir;

#[test]
fn gate_policy_source_preset_matches_standalone_result() {
    let dir = tempdir().expect("tempdir");
    let rules_path = dir.path().join("rules.json");
    let input_path = dir.path().join("input.json");

    fs::write(
        &rules_path,
        r#"{
            "required_keys": ["id", "score"],
            "forbid_keys": [],
            "fields": {
                "id": {"type": "integer"},
                "score": {"type": "number", "range": {"min": 0, "max": 10}}
            },
            "count": {"min": 1, "max": 2}
        }"#,
    )
    .expect("write rules");

    fs::write(&input_path, r#"[{"id":"oops","score":12}]"#).expect("write input");

    let standalone = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .args([
            "gate",
            "policy",
            "--rules",
            rules_path.to_str().expect("utf8 rules path"),
            "--input",
            input_path.to_str().expect("utf8 input path"),
        ])
        .output()
        .expect("run standalone gate policy");

    let with_source = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .args([
            "gate",
            "policy",
            "--rules",
            rules_path.to_str().expect("utf8 rules path"),
            "--input",
            input_path.to_str().expect("utf8 input path"),
            "--source",
            "scan-text",
        ])
        .output()
        .expect("run source preset gate policy");

    assert_eq!(standalone.status.code(), Some(2));
    assert_eq!(with_source.status.code(), Some(2));

    let standalone_json: Value = serde_json::from_slice(&standalone.stdout).expect("stdout json");
    let with_source_json: Value = serde_json::from_slice(&with_source.stdout).expect("stdout json");
    assert_eq!(standalone_json, with_source_json);
}
