use std::fs;
use std::path::Path;

use serde_json::{Value, json};
use tempfile::tempdir;

#[test]
fn mcp_single_request_flow_for_canon_with_pipeline() {
    let initialize_output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .arg("mcp")
        .write_stdin(
            json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": {},
            })
            .to_string(),
        )
        .output()
        .expect("initialize");
    assert_eq!(initialize_output.status.code(), Some(0));

    let list_output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .arg("mcp")
        .write_stdin(
            json!({
                "jsonrpc": "2.0",
                "id": 2,
                "method": "tools/list",
                "params": {},
            })
            .to_string(),
        )
        .output()
        .expect("tools/list");
    assert_eq!(list_output.status.code(), Some(0));

    let call_output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .arg("mcp")
        .write_stdin(
            json!({
                "jsonrpc": "2.0",
                "id": 3,
                "method": "tools/call",
                "params": {
                    "name": "dataq.canon",
                    "arguments": {
                        "input": [{"z":"2","a":"true"}],
                        "emit_pipeline": true
                    }
                }
            })
            .to_string(),
        )
        .output()
        .expect("tools/call canon");

    assert_eq!(call_output.status.code(), Some(0));
    assert!(call_output.stderr.is_empty());

    let response: Value = serde_json::from_slice(&call_output.stdout).expect("stdout json");
    let structured = response["result"]["structuredContent"].clone();
    assert_eq!(structured["exit_code"], Value::from(0));
    assert_eq!(structured["payload"], json!({"a": true, "z": 2}));
    assert!(structured["pipeline"].is_object());

    let text = response["result"]["content"][0]["text"]
        .as_str()
        .expect("text content");
    let reparsed: Value = serde_json::from_str(text).expect("content text json");
    assert_eq!(reparsed, structured);
}

#[test]
fn mcp_doctor_profile_flow_reports_pass_and_fail() {
    let dir = tempdir().expect("tempdir");
    write_exec_script(&dir.path().join("jq"), "#!/bin/sh\necho 'jq-1.7'\n");
    write_exec_script(&dir.path().join("rg"), "#!/bin/sh\necho 'ripgrep 14.1.0'\n");

    let pass_output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .env("PATH", dir.path())
        .arg("mcp")
        .write_stdin(
            json!({
                "jsonrpc": "2.0",
                "id": 11,
                "method": "tools/call",
                "params": {
                    "name": "dataq.doctor",
                    "arguments": {
                        "profile": "scan"
                    }
                }
            })
            .to_string(),
        )
        .output()
        .expect("tools/call doctor profile scan");
    assert_eq!(pass_output.status.code(), Some(0));

    let pass_response: Value = serde_json::from_slice(&pass_output.stdout).expect("pass json");
    assert_eq!(pass_response["result"]["isError"], Value::Bool(false));
    assert_eq!(
        pass_response["result"]["structuredContent"]["exit_code"],
        Value::from(0)
    );
    assert_eq!(
        pass_response["result"]["structuredContent"]["payload"]["profile"]["satisfied"],
        Value::Bool(true)
    );

    let fail_output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .env("PATH", dir.path())
        .arg("mcp")
        .write_stdin(
            json!({
                "jsonrpc": "2.0",
                "id": 12,
                "method": "tools/call",
                "params": {
                    "name": "dataq.doctor",
                    "arguments": {
                        "profile": "doc"
                    }
                }
            })
            .to_string(),
        )
        .output()
        .expect("tools/call doctor profile doc");
    assert_eq!(fail_output.status.code(), Some(0));

    let fail_response: Value = serde_json::from_slice(&fail_output.stdout).expect("fail json");
    assert_eq!(fail_response["result"]["isError"], Value::Bool(true));
    assert_eq!(
        fail_response["result"]["structuredContent"]["exit_code"],
        Value::from(3)
    );
    assert_eq!(
        fail_response["result"]["structuredContent"]["payload"]["profile"]["satisfied"],
        Value::Bool(false)
    );
}

fn write_exec_script(path: &Path, body: &str) {
    fs::write(path, body).expect("write script");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o755)).expect("chmod");
    }
}
