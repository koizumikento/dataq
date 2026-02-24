use std::env;
use std::fs;
use std::path::PathBuf;

use serde_json::{Value, json};
use tempfile::{TempDir, tempdir};

const TOOL_ORDER: [&str; 18] = [
    "dataq.canon",
    "dataq.ingest.api",
    "dataq.ingest.yaml_jobs",
    "dataq.assert",
    "dataq.gate.schema",
    "dataq.gate.policy",
    "dataq.sdiff",
    "dataq.diff.source",
    "dataq.profile",
    "dataq.join",
    "dataq.aggregate",
    "dataq.merge",
    "dataq.doctor",
    "dataq.contract",
    "dataq.emit.plan",
    "dataq.recipe.run",
    "dataq.recipe.lock",
    "dataq.recipe.replay",
];

#[test]
fn help_mentions_mcp_subcommand() {
    let output = assert_cmd::cargo::cargo_bin_cmd!("dataq")
        .arg("--help")
        .output()
        .expect("run --help");

    assert_eq!(output.status.code(), Some(0));
    let stdout = String::from_utf8(output.stdout).expect("stdout utf8");
    assert!(stdout.contains("mcp"));
}

#[test]
fn initialize_returns_expected_shape() {
    let output = run_mcp(
        &json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {},
        }),
        None,
    );

    assert_eq!(output.status.code(), Some(0));
    let response = parse_stdout_json(&output.stdout);

    assert_eq!(response["jsonrpc"], Value::from("2.0"));
    assert_eq!(response["id"], Value::from(1));
    assert_eq!(
        response["result"]["protocolVersion"],
        Value::from("2024-11-05")
    );
    assert_eq!(
        response["result"]["serverInfo"]["name"],
        Value::from("dataq")
    );
    assert!(response["result"]["capabilities"]["tools"].is_object());
}

#[test]
fn tools_list_is_deterministic_and_in_fixed_order() {
    let request = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "tools/list",
        "params": {},
    });

    let first = run_mcp(&request, None);
    let second = run_mcp(&request, None);

    assert_eq!(first.status.code(), Some(0));
    assert_eq!(second.status.code(), Some(0));

    let first_json = parse_stdout_json(&first.stdout);
    let second_json = parse_stdout_json(&second.stdout);

    assert_eq!(first_json, second_json);

    let listed: Vec<String> = first_json["result"]["tools"]
        .as_array()
        .expect("tools array")
        .iter()
        .map(|tool| tool["name"].as_str().expect("tool name").to_string())
        .collect();
    assert_eq!(listed, TOOL_ORDER);
}

#[test]
fn tools_call_minimal_success_for_all_tools() {
    let toolchain = FakeToolchain::new();
    let dir = tempdir().expect("tempdir");
    let schema_path = dir.path().join("gate-schema.json");
    let gate_rules_path = dir.path().join("gate-rules.json");
    let diff_input_path = dir.path().join("diff.json");
    fs::write(
        &schema_path,
        r#"{
            "type": "object",
            "required": ["id"],
            "properties": {
                "id": {"type": "integer"}
            }
        }"#,
    )
    .expect("write schema");
    fs::write(
        &gate_rules_path,
        r#"{
            "required_keys": ["id"],
            "forbid_keys": [],
            "fields": {
                "id": {"type": "integer"}
            },
            "count": {"min": 1, "max": 1}
        }"#,
    )
    .expect("write gate rules");
    fs::write(&diff_input_path, r#"[{"id":1,"v":"same"}]"#).expect("write diff fixture");
    let diff_input = diff_input_path.display().to_string();
    let recipe_path = dir.path().join("recipe-lock.json");
    fs::write(&recipe_path, r#"{"version":"dataq.recipe.v1","steps":[]}"#).expect("write recipe");
    let replay_recipe_path = dir.path().join("replay.recipe.json");
    let replay_lock_path = dir.path().join("replay.lock.json");
    fs::write(
        &replay_recipe_path,
        r#"{"version":"dataq.recipe.v1","steps":[]}"#,
    )
    .expect("write replay recipe");
    fs::write(
        &replay_lock_path,
        format!(
            r#"{{
  "version": "dataq.recipe.lock.v1",
  "command_graph_hash": "placeholder",
  "args_hash": "placeholder",
  "tool_versions": {{
    "jq": "jq-1.7",
    "mlr": "mlr 6.13.0",
    "yq": "yq 4.35.2"
  }},
  "dataq_version": "{}"
}}"#,
            env!("CARGO_PKG_VERSION")
        ),
    )
    .expect("write replay lock");
    let requests = vec![
        (
            "dataq.canon",
            json!({
                "input": [{"z":"2","a":"1"}],
            }),
        ),
        (
            "dataq.ingest.api",
            json!({
                "url": "https://example.test/items",
                "method": "GET",
                "header": ["accept:application/json"]
            }),
        ),
        (
            "dataq.ingest.yaml_jobs",
            json!({
                "mode": "generic-map",
                "input": [{
                    "job_name": "build",
                    "field_count": 2,
                    "has_stage": true,
                    "has_script": true
                }]
            }),
        ),
        (
            "dataq.assert",
            json!({
                "input": [{"id": 1}],
                "rules": {
                    "required_keys": ["id"],
                    "forbid_keys": [],
                    "fields": {
                        "id": {"type": "integer"}
                    },
                    "count": {"min": 1, "max": 1}
                }
            }),
        ),
        (
            "dataq.gate.schema",
            json!({
                "input": [{"id": 1}],
                "schema_path": schema_path,
            }),
        ),
        (
            "dataq.gate.policy",
            json!({
                "input": [{"id": 1}],
                "rules_path": gate_rules_path
            }),
        ),
        (
            "dataq.sdiff",
            json!({
                "left": [{"id": 1}],
                "right": [{"id": 1}]
            }),
        ),
        (
            "dataq.diff.source",
            json!({
                "left": diff_input.clone(),
                "right": diff_input.clone()
            }),
        ),
        (
            "dataq.profile",
            json!({
                "input": [{"id": 1}, {"id": 2}]
            }),
        ),
        (
            "dataq.join",
            json!({
                "left": [{"id":1,"l":"L1"},{"id":2,"l":"L2"}],
                "right": [{"id":1,"r":"R1"}],
                "on": "id",
                "how": "inner"
            }),
        ),
        (
            "dataq.aggregate",
            json!({
                "input": [
                    {"team":"a","price":10.0},
                    {"team":"a","price":5.0},
                    {"team":"b","price":7.0}
                ],
                "group_by": "team",
                "metric": "count",
                "target": "price"
            }),
        ),
        (
            "dataq.merge",
            json!({
                "base": {"cfg": {"a": 1}},
                "overlays": [{"cfg": {"b": 2}}],
                "policy": "deep-merge"
            }),
        ),
        ("dataq.doctor", json!({})),
        ("dataq.contract", json!({"all": true})),
        (
            "dataq.emit.plan",
            json!({
                "command": "canon"
            }),
        ),
        (
            "dataq.recipe.run",
            json!({
                "recipe": {
                    "version": "dataq.recipe.v1",
                    "steps": []
                }
            }),
        ),
        (
            "dataq.recipe.lock",
            json!({
                "file_path": recipe_path
            }),
        ),
        (
            "dataq.recipe.replay",
            json!({
                "file_path": replay_recipe_path,
                "lock_path": replay_lock_path,
                "strict": false
            }),
        ),
    ];

    for (index, (tool_name, arguments)) in requests.into_iter().enumerate() {
        let request = tool_call_request(index as i64, tool_name, arguments);
        let output = run_mcp(&request, Some(&toolchain));

        assert_eq!(output.status.code(), Some(0), "tool: {tool_name}");
        let response = parse_stdout_json(&output.stdout);
        assert_eq!(response["error"], Value::Null, "tool: {tool_name}");
        assert_eq!(
            response["result"]["isError"],
            Value::Bool(false),
            "tool: {tool_name}"
        );
        assert_eq!(
            response["result"]["structuredContent"]["exit_code"],
            Value::from(0),
            "tool: {tool_name}",
        );
    }
}

#[test]
fn tools_call_ingest_api_accepts_mixed_case_method() {
    let toolchain = FakeToolchain::new();
    let request = tool_call_request(
        101,
        "dataq.ingest.api",
        json!({
            "url": "https://example.test/items",
            "method": "Get",
            "header": ["accept:application/json"]
        }),
    );

    let output = run_mcp(&request, Some(&toolchain));
    assert_eq!(output.status.code(), Some(0));

    let response = parse_stdout_json(&output.stdout);
    assert_eq!(response["result"]["isError"], Value::Bool(false));
    assert_eq!(
        response["result"]["structuredContent"]["exit_code"],
        Value::from(0)
    );
    assert_eq!(
        response["result"]["structuredContent"]["payload"]["status"],
        Value::from(200)
    );
}

#[test]
fn ingest_yaml_jobs_tool_supports_mode_and_pipeline() {
    let toolchain = FakeToolchain::new();
    let request = tool_call_request(
        102,
        "dataq.ingest.yaml_jobs",
        json!({
            "emit_pipeline": true,
            "mode": "generic-map",
            "input": [{
                "job_name": "build",
                "field_count": 2,
                "has_stage": true,
                "has_script": true
            }]
        }),
    );

    let output = run_mcp(&request, Some(&toolchain));
    assert_eq!(output.status.code(), Some(0));

    let response = parse_stdout_json(&output.stdout);
    assert_eq!(response["result"]["isError"], Value::Bool(false));
    assert_eq!(
        response["result"]["structuredContent"]["exit_code"],
        Value::from(0)
    );
    assert_eq!(
        response["result"]["structuredContent"]["pipeline"]["steps"],
        json!([
            "ingest_yaml_jobs_yq_extract",
            "ingest_yaml_jobs_jq_normalize",
            "ingest_yaml_jobs_mlr_shape"
        ])
    );
}

#[test]
fn emit_pipeline_true_includes_pipeline() {
    let request = tool_call_request(
        1,
        "dataq.profile",
        json!({
            "emit_pipeline": true,
            "input": [{"id": 1}, {"id": 2}]
        }),
    );

    let output = run_mcp(&request, None);
    assert_eq!(output.status.code(), Some(0));
    assert!(output.stderr.is_empty());

    let response = parse_stdout_json(&output.stdout);
    assert_eq!(
        response["result"]["structuredContent"]["exit_code"],
        Value::from(0)
    );
    assert!(response["result"]["structuredContent"]["pipeline"].is_object());
    assert_eq!(
        response["result"]["structuredContent"]["pipeline"]["command"],
        Value::from("profile")
    );
}

#[test]
fn inline_path_conflict_returns_exit_three() {
    let dir = tempdir().expect("tempdir");
    let left_path = dir.path().join("left.json");
    fs::write(&left_path, r#"[{"id":1}]"#).expect("write left");

    let request = tool_call_request(
        7,
        "dataq.join",
        json!({
            "left_path": left_path,
            "left": [{"id": 1}],
            "right": [{"id": 1}],
            "on": "id",
            "how": "inner"
        }),
    );

    let output = run_mcp(&request, None);
    assert_eq!(output.status.code(), Some(0));

    let response = parse_stdout_json(&output.stdout);
    assert_eq!(response["result"]["isError"], Value::Bool(true));
    assert_eq!(
        response["result"]["structuredContent"]["exit_code"],
        Value::from(3)
    );
    assert_eq!(
        response["result"]["structuredContent"]["payload"]["error"],
        Value::from("input_usage_error")
    );
}

#[test]
fn gate_schema_rejects_input_path_stdin_sentinels() {
    let dir = tempdir().expect("tempdir");
    let schema_path = dir.path().join("schema.json");
    fs::write(&schema_path, r#"{"type":"object"}"#).expect("write schema");

    for sentinel in ["-", "/dev/stdin"] {
        let request = tool_call_request(
            22,
            "dataq.gate.schema",
            json!({
                "input_path": sentinel,
                "schema_path": schema_path,
            }),
        );

        let output = run_mcp(&request, None);
        assert_eq!(output.status.code(), Some(0));

        let response = parse_stdout_json(&output.stdout);
        assert_eq!(response["result"]["isError"], Value::Bool(true));
        assert_eq!(
            response["result"]["structuredContent"]["exit_code"],
            Value::from(3)
        );
        assert_eq!(
            response["result"]["structuredContent"]["payload"]["error"],
            Value::from("input_usage_error")
        );
        let message = response["result"]["structuredContent"]["payload"]["message"]
            .as_str()
            .expect("error message");
        assert!(message.contains("stdin sentinel paths"));
        assert!(message.contains("inline `input`"));
    }
}

#[test]
fn gate_policy_unknown_source_returns_exit_three() {
    let dir = tempdir().expect("tempdir");
    let rules_path = dir.path().join("rules.json");
    fs::write(
        &rules_path,
        r#"{
            "required_keys": ["id"],
            "forbid_keys": [],
            "fields": {
                "id": {"type": "integer"}
            },
            "count": {"min": 1, "max": 1}
        }"#,
    )
    .expect("write rules");

    let request = tool_call_request(
        12,
        "dataq.gate.policy",
        json!({
            "input": [{"id": 1}],
            "rules_path": rules_path,
            "source": "unknown-source"
        }),
    );

    let output = run_mcp(&request, None);
    assert_eq!(output.status.code(), Some(0));

    let response = parse_stdout_json(&output.stdout);
    assert_eq!(response["result"]["isError"], Value::Bool(true));
    assert_eq!(
        response["result"]["structuredContent"]["exit_code"],
        Value::from(3)
    );
    assert_eq!(
        response["result"]["structuredContent"]["payload"]["message"],
        Value::from(
            "unknown source `unknown-source`: expected one of `scan-text`, `ingest-doc`, `ingest-api`, `ingest-notes`, `ingest-book`"
        )
    );
}

#[test]
fn gate_policy_rejects_stdin_sentinel_input_path() {
    let dir = tempdir().expect("tempdir");
    let rules_path = dir.path().join("rules.json");
    fs::write(
        &rules_path,
        r#"{
            "required_keys": ["id"],
            "forbid_keys": [],
            "fields": {
                "id": {"type": "integer"}
            },
            "count": {"min": 1, "max": 1}
        }"#,
    )
    .expect("write rules");

    let expected_message = "`dataq.gate.policy` does not accept stdin sentinel paths for `input_path` (`-`, `/dev/stdin`); provide a file path or inline `input`";
    for (index, input_path) in ["-", "/dev/stdin"].into_iter().enumerate() {
        let request = tool_call_request(
            20 + index as i64,
            "dataq.gate.policy",
            json!({
                "input_path": input_path,
                "rules_path": rules_path
            }),
        );

        let output = run_mcp(&request, None);
        assert_eq!(output.status.code(), Some(0), "input_path: {input_path}");

        let response = parse_stdout_json(&output.stdout);
        assert_eq!(response["result"]["isError"], Value::Bool(true));
        assert_eq!(
            response["result"]["structuredContent"]["exit_code"],
            Value::from(3)
        );
        assert_eq!(
            response["result"]["structuredContent"]["payload"]["error"],
            Value::from("input_usage_error")
        );
        assert_eq!(
            response["result"]["structuredContent"]["payload"]["message"],
            Value::from(expected_message)
        );
    }
}

#[test]
fn unknown_tool_name_returns_exit_three() {
    let request = tool_call_request(2, "dataq.unknown", json!({}));

    let output = run_mcp(&request, None);
    assert_eq!(output.status.code(), Some(0));

    let response = parse_stdout_json(&output.stdout);
    assert_eq!(response["result"]["isError"], Value::Bool(true));
    assert_eq!(
        response["result"]["structuredContent"]["exit_code"],
        Value::from(3)
    );
}

#[test]
fn invalid_jsonrpc_returns_error_object_when_possible() {
    let parse_error_output = run_mcp_raw("{", None);
    assert_eq!(parse_error_output.status.code(), Some(0));
    let parse_error_json = parse_stdout_json(&parse_error_output.stdout);
    assert_eq!(parse_error_json["error"]["code"], Value::from(-32700));
    assert_eq!(parse_error_json["id"], Value::Null);

    let invalid_request_output = run_mcp(
        &json!({
            "jsonrpc": "2.0",
            "id": 5,
            "method": 10,
        }),
        None,
    );
    assert_eq!(invalid_request_output.status.code(), Some(0));
    let invalid_request_json = parse_stdout_json(&invalid_request_output.stdout);
    assert_eq!(invalid_request_json["error"]["code"], Value::from(-32600));
    assert_eq!(invalid_request_json["id"], Value::from(5));
}

#[test]
fn non_zero_command_exit_code_is_preserved_in_structured_content() {
    let request = tool_call_request(
        10,
        "dataq.assert",
        json!({
            "input": [{"id": "oops"}],
            "rules": {
                "required_keys": ["id"],
                "forbid_keys": [],
                "fields": {
                    "id": {"type": "integer"}
                },
                "count": {"min": 1, "max": 1}
            }
        }),
    );

    let output = run_mcp(&request, None);
    assert_eq!(output.status.code(), Some(0));

    let response = parse_stdout_json(&output.stdout);
    assert_eq!(response["result"]["isError"], Value::Bool(true));
    assert_eq!(
        response["result"]["structuredContent"]["exit_code"],
        Value::from(2)
    );
}

#[test]
fn recipe_supports_file_path_and_inline_recipe() {
    let dir = tempdir().expect("tempdir");
    let recipe_path = dir.path().join("recipe.json");
    fs::write(&recipe_path, r#"{"version":"dataq.recipe.v1","steps":[]}"#).expect("write recipe");

    let file_request = tool_call_request(
        1,
        "dataq.recipe.run",
        json!({
            "file_path": recipe_path
        }),
    );
    let file_output = run_mcp(&file_request, None);
    assert_eq!(file_output.status.code(), Some(0));
    let file_response = parse_stdout_json(&file_output.stdout);
    assert_eq!(
        file_response["result"]["structuredContent"]["exit_code"],
        Value::from(0)
    );

    let inline_request = tool_call_request(
        2,
        "dataq.recipe.run",
        json!({
            "recipe": {
                "version": "dataq.recipe.v1",
                "steps": []
            }
        }),
    );
    let inline_output = run_mcp(&inline_request, None);
    assert_eq!(inline_output.status.code(), Some(0));
    let inline_response = parse_stdout_json(&inline_output.stdout);
    assert_eq!(
        inline_response["result"]["structuredContent"]["exit_code"],
        Value::from(0)
    );
}

#[test]
fn recipe_lock_supports_file_path() {
    let toolchain = FakeToolchain::new();
    let dir = tempdir().expect("tempdir");
    let recipe_path = dir.path().join("recipe.json");
    fs::write(&recipe_path, r#"{"version":"dataq.recipe.v1","steps":[]}"#).expect("write recipe");

    let request = tool_call_request(
        3,
        "dataq.recipe.lock",
        json!({
            "file_path": recipe_path
        }),
    );
    let output = run_mcp(&request, Some(&toolchain));
    assert_eq!(output.status.code(), Some(0));

    let response = parse_stdout_json(&output.stdout);
    assert_eq!(
        response["result"]["structuredContent"]["exit_code"],
        Value::from(0)
    );
    assert_eq!(
        response["result"]["structuredContent"]["payload"]["version"],
        Value::from("dataq.recipe.lock.v1")
    );
    assert!(response["result"]["structuredContent"]["payload"]["tool_versions"].is_object());
}

#[test]
fn contract_supports_recipe_lock_command() {
    let request = tool_call_request(
        11,
        "dataq.contract",
        json!({
            "command": "recipe-lock"
        }),
    );

    let output = run_mcp(&request, None);
    assert_eq!(output.status.code(), Some(0));

    let response = parse_stdout_json(&output.stdout);
    assert_eq!(
        response["result"]["structuredContent"]["exit_code"],
        Value::from(0)
    );
    assert_eq!(
        response["result"]["structuredContent"]["payload"]["command"],
        Value::from("recipe-lock")
    );
    assert_eq!(
        response["result"]["structuredContent"]["payload"]["schema"],
        Value::from("dataq.recipe.lock.output.v1")
    );
}

#[test]
fn recipe_lock_invalid_step_args_return_exit_three() {
    let toolchain = FakeToolchain::new();
    let dir = tempdir().expect("tempdir");
    let input_path = dir.path().join("input.json");
    let recipe_path = dir.path().join("recipe.json");
    fs::write(&input_path, r#"[{"id":"1"}]"#).expect("write input");
    fs::write(
        &recipe_path,
        json!({
            "version":"dataq.recipe.v1",
            "steps":[
                {
                    "kind":"canon",
                    "args":{
                        "input": input_path,
                        "from":"json"
                    }
                },
                {
                    "kind":"assert",
                    "args":{
                        "rules":{"required_keys":[],"forbid_keys":[],"fields":{}},
                        "schema":{"type":"object"}
                    }
                }
            ]
        })
        .to_string(),
    )
    .expect("write recipe");

    let request = tool_call_request(
        12,
        "dataq.recipe.lock",
        json!({
            "file_path": recipe_path
        }),
    );

    let output = run_mcp(&request, Some(&toolchain));
    assert_eq!(output.status.code(), Some(0));

    let response = parse_stdout_json(&output.stdout);
    assert_eq!(response["result"]["isError"], Value::Bool(true));
    assert_eq!(
        response["result"]["structuredContent"]["exit_code"],
        Value::from(3)
    );
    assert_eq!(
        response["result"]["structuredContent"]["payload"]["error"],
        Value::from("input_usage_error")
    );
    assert_eq!(
        response["result"]["structuredContent"]["payload"]["message"],
        Value::from("assert step cannot combine rules and schema sources")
    );
}

#[test]
fn recipe_lock_invalid_step_order_returns_exit_three() {
    let toolchain = FakeToolchain::new();
    let dir = tempdir().expect("tempdir");
    let recipe_path = dir.path().join("recipe.json");
    fs::write(
        &recipe_path,
        r#"{
            "version":"dataq.recipe.v1",
            "steps":[
                {
                    "kind":"assert",
                    "args":{
                        "rules":{"required_keys":[],"forbid_keys":[],"fields":{}}
                    }
                }
            ]
        }"#,
    )
    .expect("write recipe");

    let request = tool_call_request(
        13,
        "dataq.recipe.lock",
        json!({
            "file_path": recipe_path
        }),
    );

    let output = run_mcp(&request, Some(&toolchain));
    assert_eq!(output.status.code(), Some(0));

    let response = parse_stdout_json(&output.stdout);
    assert_eq!(response["result"]["isError"], Value::Bool(true));
    assert_eq!(
        response["result"]["structuredContent"]["exit_code"],
        Value::from(3)
    );
    assert_eq!(
        response["result"]["structuredContent"]["payload"]["error"],
        Value::from("input_usage_error")
    );
    assert_eq!(
        response["result"]["structuredContent"]["payload"]["message"],
        Value::from(
            "assert step requires prior in-memory values (for example a preceding canon step)"
        )
    );
}

#[test]
fn recipe_lock_emit_pipeline_survives_out_path_write_failure() {
    let toolchain = FakeToolchain::new();
    let dir = tempdir().expect("tempdir");
    let recipe_path = dir.path().join("recipe.json");
    fs::write(&recipe_path, r#"{"version":"dataq.recipe.v1","steps":[]}"#).expect("write recipe");

    let request = tool_call_request(
        14,
        "dataq.recipe.lock",
        json!({
            "file_path": recipe_path,
            "out_path": dir.path(),
            "emit_pipeline": true
        }),
    );

    let output = run_mcp(&request, Some(&toolchain));
    assert_eq!(output.status.code(), Some(0));

    let response = parse_stdout_json(&output.stdout);
    assert_eq!(response["result"]["isError"], Value::Bool(true));
    assert_eq!(
        response["result"]["structuredContent"]["exit_code"],
        Value::from(3)
    );
    assert_eq!(
        response["result"]["structuredContent"]["payload"]["error"],
        Value::from("input_usage_error")
    );
    assert!(response["result"]["structuredContent"]["pipeline"].is_object());
    assert_eq!(
        response["result"]["structuredContent"]["pipeline"]["steps"],
        Value::from(vec![
            "recipe_lock_parse",
            "recipe_lock_probe_tools",
            "recipe_lock_fingerprint",
        ])
    );
}

fn tool_call_request(id: i64, tool_name: &str, arguments: Value) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "method": "tools/call",
        "params": {
            "name": tool_name,
            "arguments": arguments
        }
    })
}

fn run_mcp(request: &Value, toolchain: Option<&FakeToolchain>) -> std::process::Output {
    run_mcp_raw(request.to_string().as_str(), toolchain)
}

fn run_mcp_raw(raw: &str, toolchain: Option<&FakeToolchain>) -> std::process::Output {
    let mut command = assert_cmd::cargo::cargo_bin_cmd!("dataq");
    command.arg("mcp").write_stdin(raw);

    if let Some(toolchain) = toolchain {
        command.env("DATAQ_MLR_BIN", &toolchain.mlr_bin);
        command.env("PATH", toolchain.path_with_current());
    }

    command.output().expect("run mcp")
}

fn parse_stdout_json(stdout: &[u8]) -> Value {
    serde_json::from_slice(stdout).expect("stdout json")
}

struct FakeToolchain {
    _dir: TempDir,
    bin_dir: PathBuf,
    mlr_bin: PathBuf,
}

impl FakeToolchain {
    fn new() -> Self {
        let dir = tempdir().expect("tempdir");
        let bin_dir = dir.path().to_path_buf();

        let mlr_bin = write_fake_mlr_script(bin_dir.join("mlr"));
        write_fake_ingest_jq_script(bin_dir.join("jq"));
        write_fake_yq_script(bin_dir.join("yq"));
        write_fake_xh_script(bin_dir.join("xh"));

        Self {
            _dir: dir,
            bin_dir,
            mlr_bin,
        }
    }

    fn path_with_current(&self) -> String {
        let mut entries = vec![self.bin_dir.display().to_string()];
        if let Some(existing) = env::var_os("PATH") {
            entries.push(existing.to_string_lossy().to_string());
        }
        entries.join(":")
    }
}

fn write_fake_ingest_jq_script(path: PathBuf) {
    let script = r#"#!/bin/sh
if [ "$1" = "--version" ]; then
  printf 'jq-1.7\n'
  exit 0
fi
cat
"#;
    fs::write(&path, script).expect("write fake jq script");
    set_executable(&path);
}

fn write_fake_yq_script(path: PathBuf) {
    let script = r#"#!/bin/sh
if [ "$1" = "--version" ]; then
  printf 'yq 4.35.2\n'
  exit 0
fi
cat
"#;
    fs::write(&path, script).expect("write fake yq script");
    set_executable(&path);
}

fn write_fake_xh_script(path: PathBuf) {
    let script = r#"#!/bin/sh
for arg in "$@"; do
  if [ "$arg" = "--version" ]; then
    printf 'xh 0.23.0\n'
    exit 0
  fi
done

cat <<'EOF'
HTTP/1.1 200 OK
Date: Mon, 24 Feb 2025 10:00:00 GMT
Content-Type: application/json
ETag: W/"abc"
X-Trace-Id: trace-123

{"ok":true,"n":1}
EOF
"#;
    fs::write(&path, script).expect("write fake xh script");
    set_executable(&path);
}

fn write_fake_mlr_script(path: PathBuf) -> PathBuf {
    let script = r#"#!/bin/sh
for arg in "$@"; do
  if [ "$arg" = "--version" ]; then
    printf 'mlr 6.13.0\n'
    exit 0
  fi
done

mode=""
action=""
left_file=""
capture_next_f=0
for arg in "$@"; do
  if [ "$capture_next_f" = "1" ]; then
    left_file="$arg"
    capture_next_f=0
    continue
  fi
  if [ "$arg" = "join" ]; then mode="join"; fi
  if [ "$arg" = "stats1" ]; then mode="stats1"; fi
  if [ "$arg" = "sort" ]; then mode="sort"; fi
  if [ "$arg" = "count" ] || [ "$arg" = "sum" ] || [ "$arg" = "mean" ]; then action="$arg"; fi
  if [ "$arg" = "-f" ]; then capture_next_f=1; fi
  if [ "$arg" = "--ul" ]; then left_join="1"; fi
done

if [ "$mode" = "join" ]; then
  if [ -n "$left_join" ]; then
    printf '[{"id":1,"l":"L1","r":"R1"},{"id":2,"l":"L2","r":null}]'
  else
    printf '[{"id":1,"l":"L1","r":"R1"}]'
  fi
  exit 0
fi

if [ "$mode" = "stats1" ]; then
  if [ "$action" = "count" ]; then
    printf '[{"team":"a","price_count":"2"},{"team":"b","price_count":"1"}]'
    exit 0
  fi
  if [ "$action" = "sum" ]; then
    printf '[{"team":"a","price_sum":"15.0"},{"team":"b","price_sum":"7.0"}]'
    exit 0
  fi
  if [ "$action" = "mean" ]; then
    printf '[{"team":"a","price_mean":"7.5"},{"team":"b","price_mean":"7.0"}]'
    exit 0
  fi
fi

if [ "$mode" = "sort" ]; then
  cat
  exit 0
fi

echo 'unexpected mlr args' 1>&2
exit 9
"#;

    fs::write(&path, script).expect("write fake mlr script");
    set_executable(&path);
    path
}

fn set_executable(path: &PathBuf) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o755)).expect("chmod");
    }
}
