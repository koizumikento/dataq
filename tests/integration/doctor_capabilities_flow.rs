use std::fs;
use std::path::Path;

use serde_json::{Value, json};
use tempfile::tempdir;

#[test]
fn doctor_capabilities_partial_state_and_profile_gate_flow() {
    let dir = tempdir().expect("tempdir");
    write_exec_script(
        &dir.path().join("jq"),
        "#!/bin/sh\nif [ \"$1\" = \"--version\" ]; then\n  echo 'jq-1.7'\nelse\n  echo 'null'\nfi\n",
    );
    write_exec_script(
        &dir.path().join("yq"),
        "#!/bin/sh\nif [ \"$1\" = \"--version\" ]; then\n  echo 'yq-4.44.6'\n  exit 0\nfi\necho 'capability probe failed' 1>&2\nexit 9\n",
    );
    write_exec_script(
        &dir.path().join("mlr"),
        "#!/bin/sh\nif [ \"$1\" = \"--version\" ]; then\n  echo 'mlr-6.13.0'\nelse\n  echo 'help text'\nfi\n",
    );

    let cli_output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .env("PATH", dir.path())
        .args(["doctor", "--capabilities"])
        .output()
        .expect("run doctor");
    assert_eq!(cli_output.status.code(), Some(0));
    assert!(cli_output.stderr.is_empty());
    let cli_payload: Value = serde_json::from_slice(&cli_output.stdout).expect("stdout json");
    assert_eq!(
        cli_payload["capabilities"][1]["name"],
        json!("yq.null_input_eval")
    );
    assert_eq!(cli_payload["capabilities"][1]["available"], json!(false));

    let request = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "tools/call",
        "params": {
            "name": "dataq.doctor",
            "arguments": {
                "capabilities": true,
                "profile": "core"
            }
        }
    });
    let mcp_output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .env("PATH", dir.path())
        .arg("mcp")
        .write_stdin(request.to_string())
        .output()
        .expect("run mcp");
    assert_eq!(mcp_output.status.code(), Some(0));
    assert!(mcp_output.stderr.is_empty());
    let mcp_payload: Value = serde_json::from_slice(&mcp_output.stdout).expect("stdout json");
    assert_eq!(
        mcp_payload["result"]["structuredContent"]["exit_code"],
        json!(3)
    );
    assert_eq!(
        mcp_payload["result"]["structuredContent"]["payload"]["capabilities"][1]["available"],
        json!(false)
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
