use serde_json::{Value, json};

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
