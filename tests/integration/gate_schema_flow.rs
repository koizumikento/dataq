use std::fs;

use serde_json::Value;
use tempfile::tempdir;

#[test]
fn canon_then_gate_schema_flow_succeeds() {
    let dir = tempdir().expect("temp dir");
    let input_path = dir.path().join("raw.json");
    let schema_path = dir.path().join("schema.json");

    fs::write(
        &input_path,
        r#"[
            {"id":"2","active":"false"},
            {"active":"true","id":"1"}
        ]"#,
    )
    .expect("write input");
    fs::write(
        &schema_path,
        r#"{
            "type": "object",
            "required": ["id", "active"],
            "properties": {
                "id": {"type": "integer"},
                "active": {"type": "boolean"}
            }
        }"#,
    )
    .expect("write schema");

    let canon_output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .args([
            "canon",
            "--input",
            input_path.to_str().expect("utf8 input path"),
            "--from",
            "json",
        ])
        .output()
        .expect("run canon");

    assert_eq!(canon_output.status.code(), Some(0));

    let gate_output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .args([
            "gate",
            "schema",
            "--schema",
            schema_path.to_str().expect("utf8 schema path"),
        ])
        .write_stdin(canon_output.stdout)
        .output()
        .expect("run gate schema");

    assert_eq!(gate_output.status.code(), Some(0));
    let payload: Value = serde_json::from_slice(&gate_output.stdout).expect("stdout json");
    assert_eq!(payload["matched"], Value::Bool(true));
    assert_eq!(payload["mismatch_count"], Value::from(0));
}
