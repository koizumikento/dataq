use std::env;
use std::fs;
use std::path::PathBuf;

use serde_json::{Value, json};
use tempfile::{TempDir, tempdir};

const TOOL_ORDER: [&str; 23] = [
    "dataq.canon",
    "dataq.ingest.api",
    "dataq.ingest.yaml_jobs",
    "dataq.assert",
    "dataq.gate.schema",
    "dataq.gate.policy",
    "dataq.sdiff",
    "dataq.diff.source",
    "dataq.profile",
    "dataq.ingest.doc",
    "dataq.ingest.notes",
    "dataq.ingest.book",
    "dataq.join",
    "dataq.aggregate",
    "dataq.scan.text",
    "dataq.transform.rowset",
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

    let canon_tool = first_json["result"]["tools"]
        .as_array()
        .expect("tools array")
        .iter()
        .find(|tool| tool["name"] == json!("dataq.canon"))
        .expect("canon tool");
    assert_eq!(
        canon_tool["inputSchema"]["additionalProperties"],
        Value::Bool(false)
    );
    assert!(canon_tool["inputSchema"]["oneOf"].is_array());
    assert!(canon_tool["examples"].is_array());
    assert!(canon_tool["meta"]["exit_code_contract"].is_object());

    let ingest_api_tool = first_json["result"]["tools"]
        .as_array()
        .expect("tools array")
        .iter()
        .find(|tool| tool["name"] == json!("dataq.ingest.api"))
        .expect("ingest api tool");
    assert_eq!(ingest_api_tool["inputSchema"]["required"], json!(["url"]));
    assert_eq!(
        ingest_api_tool["inputSchema"]["properties"]["method"]["pattern"],
        Value::from(
            "^(?:[Gg][Ee][Tt]|[Pp][Oo][Ss][Tt]|[Pp][Uu][Tt]|[Pp][Aa][Tt][Cc][Hh]|[Dd][Ee][Ll][Ee][Tt][Ee])$",
        )
    );

    let replay_tool = first_json["result"]["tools"]
        .as_array()
        .expect("tools array")
        .iter()
        .find(|tool| tool["name"] == json!("dataq.recipe.replay"))
        .expect("recipe replay tool");
    assert_eq!(
        replay_tool["meta"]["exit_code_contract"]["2"],
        Value::from("strict lock mismatch or step-level validation mismatch")
    );

    let contract_tool = first_json["result"]["tools"]
        .as_array()
        .expect("tools array")
        .iter()
        .find(|tool| tool["name"] == json!("dataq.contract"))
        .expect("contract tool");
    let contract_commands = contract_tool["inputSchema"]["properties"]["command"]["enum"]
        .as_array()
        .expect("contract command enum");
    for command in [
        "join",
        "aggregate",
        "recipe-replay",
        "schema-infer",
        "ingest-tabular",
        "ingest-jc",
        "emit-plan",
        "transform-sql",
    ] {
        assert!(
            contract_commands
                .iter()
                .any(|entry| entry.as_str() == Some(command)),
            "missing contract command enum value {command}"
        );
    }

    let aggregate_tool = first_json["result"]["tools"]
        .as_array()
        .expect("tools array")
        .iter()
        .find(|tool| tool["name"] == json!("dataq.aggregate"))
        .expect("aggregate tool");
    assert_eq!(
        aggregate_tool["inputSchema"]["properties"]["sort_by"]["enum"],
        json!(["group", "metric"])
    );
    assert_eq!(
        aggregate_tool["inputSchema"]["properties"]["order"]["enum"],
        json!(["asc", "desc"])
    );
    assert_eq!(
        aggregate_tool["inputSchema"]["properties"]["limit"]["minimum"],
        Value::from(0)
    );
    assert!(
        aggregate_tool["examples"]
            .as_array()
            .expect("aggregate examples")
            .iter()
            .any(|example| example["name"] == json!("aggregate-top-k"))
    );
}

#[test]
fn contract_tool_accepts_added_command_values() {
    for (command, schema) in [
        ("join", "dataq.join.output.v1"),
        ("aggregate", "dataq.aggregate.output.v1"),
        ("recipe-replay", "dataq.recipe.replay.output.v1"),
        ("schema-infer", "dataq.schema.infer.output.v1"),
        ("ingest-tabular", "dataq.ingest.tabular.output.v1"),
        ("ingest-jc", "dataq.ingest.jc.output.v1"),
        ("emit-plan", "dataq.emit.plan.output.v1"),
        ("transform-sql", "dataq.transform.sql.output.v1"),
    ] {
        let output = run_mcp(
            &json!({
                "jsonrpc": "2.0",
                "id": command,
                "method": "tools/call",
                "params": {
                    "name": "dataq.contract",
                    "arguments": {
                        "command": command
                    }
                }
            }),
            None,
        );

        assert_eq!(output.status.code(), Some(0), "{command}");
        let response = parse_stdout_json(&output.stdout);
        let payload = &response["result"]["structuredContent"]["payload"];
        assert_eq!(payload["schema"], Value::from(schema));
    }
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
            "dataq.ingest.doc",
            json!({
                "input": "# Overview\n\nSee [site](https://example.com/docs)\n",
                "from": "md"
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
            "dataq.transform.rowset",
            json!({
                "input": [
                    {"team":"a","price":10.0},
                    {"team":"a","price":5.0},
                    {"team":"b","price":7.0}
                ],
                "jq_filter": ".",
                "mlr": ["stats1", "-a", "count", "-f", "price", "-g", "team"]
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
fn aggregate_tool_sorts_by_metric_desc_and_limits_top_k() {
    let toolchain = FakeToolchain::new();
    let request = tool_call_request(
        24,
        "dataq.aggregate",
        json!({
            "input": [
                {"team":"a","price":10.0},
                {"team":"a","price":5.0},
                {"team":"b","price":7.0},
                {"team":"c","price":8.0},
                {"team":"c","price":7.0}
            ],
            "group_by": "team",
            "metric": "sum",
            "target": "price",
            "sort_by": "metric",
            "order": "desc",
            "limit": 2
        }),
    );

    let output = run_mcp(&request, Some(&toolchain));
    assert_eq!(output.status.code(), Some(0));

    let response = parse_stdout_json(&output.stdout);
    assert_eq!(response["result"]["isError"], Value::Bool(false));
    assert_eq!(
        response["result"]["structuredContent"]["payload"],
        json!([
            {"sum": 15.0, "team": "a"},
            {"sum": 15.0, "team": "c"}
        ])
    );
}

#[test]
fn aggregate_tool_preserves_large_integer_sum_and_top_k_order() {
    let toolchain = FakeToolchain::new();
    let request = tool_call_request(
        241,
        "dataq.aggregate",
        json!({
            "input": [
                {"team":"a","price":9_007_199_254_740_991_u64},
                {"team":"a","price":1},
                {"team":"z","price":9_007_199_254_740_991_u64},
                {"team":"z","price":2}
            ],
            "group_by": "team",
            "metric": "sum",
            "target": "price",
            "sort_by": "metric",
            "order": "desc",
            "limit": 1
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
        response["result"]["structuredContent"]["payload"],
        json!([{"sum": 9_007_199_254_740_993_u64, "team": "z"}])
    );
}

#[test]
fn aggregate_tool_rejects_out_of_range_integer_string_without_partial_output() {
    let toolchain = FakeToolchain::new();
    let request = tool_call_request(
        245,
        "dataq.aggregate",
        json!({
            "input": [
                {"team":"first","price":1},
                {"team":"overflow-string","price":1}
            ],
            "group_by": "team",
            "metric": "sum",
            "target": "price"
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
    let payload = &response["result"]["structuredContent"]["payload"];
    assert_eq!(payload["error"], Value::from("input_usage_error"));
    assert!(payload.as_array().is_none(), "must not return partial rows");
    let message = payload["message"].as_str().expect("error message");
    assert!(message.contains("18446744073709551617"));
    assert!(message.contains("outside the supported JSON integer range"));
}

#[test]
fn aggregate_tool_rejects_invalid_sort_order_and_limit_values() {
    let invalid_cases = [
        (
            json!({
                "input": [{"team": "a", "value": 1}],
                "group_by": "team",
                "target": "value",
                "sort_by": "score"
            }),
            "sort_by",
        ),
        (
            json!({
                "input": [{"team": "a", "value": 1}],
                "group_by": "team",
                "target": "value",
                "order": "sideways"
            }),
            "order",
        ),
        (
            json!({
                "input": [{"team": "a", "value": 1}],
                "group_by": "team",
                "target": "value",
                "limit": -1
            }),
            "limit",
        ),
    ];

    for (index, (arguments, expected_message)) in invalid_cases.into_iter().enumerate() {
        let request = tool_call_request(240 + index as i64, "dataq.aggregate", arguments);
        let output = run_mcp(&request, None);
        assert_eq!(output.status.code(), Some(0));

        let response = parse_stdout_json(&output.stdout);
        assert_eq!(response["result"]["isError"], Value::Bool(true));
        assert_eq!(
            response["result"]["structuredContent"]["exit_code"],
            Value::from(3)
        );
        assert!(
            response["result"]["structuredContent"]["payload"]["message"]
                .as_str()
                .unwrap_or_default()
                .contains(expected_message)
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
fn tools_call_alias_arguments_emit_deprecation_warnings() {
    let request = tool_call_request(
        103,
        "dataq.canon",
        json!({
            "input_inline": [{"z":"2","a":"1"}]
        }),
    );

    let output = run_mcp(&request, None);
    assert_eq!(output.status.code(), Some(0));

    let response = parse_stdout_json(&output.stdout);
    assert_eq!(response["result"]["isError"], Value::Bool(false));
    let warnings = response["result"]["structuredContent"]["meta"]["warnings"]
        .as_array()
        .expect("warnings array");
    assert_eq!(warnings.len(), 1);
    assert_eq!(warnings[0]["code"], Value::from("deprecated_arg_alias"));
    assert_eq!(warnings[0]["alias"], Value::from("input_inline"));
    assert_eq!(warnings[0]["canonical"], Value::from("input"));
}

#[test]
fn tools_call_rejects_unknown_arguments() {
    let request = tool_call_request(
        104,
        "dataq.canon",
        json!({
            "input": [{"id": 1}],
            "typo_option": true
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
        response["result"]["structuredContent"]["payload"]["invalid_params"][0]["name"],
        Value::from("typo_option")
    );
}

#[test]
fn assert_missing_rules_or_schema_uses_canonical_invalid_param_names() {
    let request = tool_call_request(
        105,
        "dataq.assert",
        json!({
            "input": [{"id": 1}]
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

    let names: Vec<String> = response["result"]["structuredContent"]["payload"]["invalid_params"]
        .as_array()
        .expect("invalid params")
        .iter()
        .map(|item| item["name"].as_str().unwrap_or_default().to_string())
        .collect();
    assert!(names.contains(&"rules".to_string()));
    assert!(names.contains(&"schema".to_string()));
    assert!(!names.iter().any(|name| name.contains("(_path)")));
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
fn profile_tool_projects_field_string_or_array_and_reports_missing_when_allowed() {
    let request = tool_call_request(
        30,
        "dataq.profile",
        json!({
            "input": [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}],
            "field": ["missing", "id", "$[\"missing\"]"],
            "allow_missing_fields": true
        }),
    );

    let output = run_mcp(&request, None);
    assert_eq!(output.status.code(), Some(0));

    let response = parse_stdout_json(&output.stdout);
    assert_eq!(
        response["result"]["structuredContent"]["exit_code"],
        Value::from(0)
    );
    let payload = &response["result"]["structuredContent"]["payload"];
    assert_eq!(payload["field_count"], json!(2));
    assert_eq!(payload["returned_field_count"], json!(1));
    assert_eq!(payload["missing_fields"], json!(["$[\"missing\"]"]));
    assert_eq!(payload["fields"]["$[\"id\"]"]["unique_count"], json!(2));

    let string_request = tool_call_request(
        301,
        "dataq.profile",
        json!({
            "input": [{"id": 1, "name": "a"}],
            "field": "name"
        }),
    );
    let string_output = run_mcp(&string_request, None);
    let string_response = parse_stdout_json(&string_output.stdout);
    assert_eq!(
        string_response["result"]["structuredContent"]["payload"]["returned_field_count"],
        Value::from(1)
    );
}

#[test]
fn profile_tool_schema_and_examples_include_brief_arguments() {
    let output = run_mcp(
        &json!({
            "jsonrpc": "2.0",
            "id": 303,
            "method": "tools/list",
            "params": {},
        }),
        None,
    );
    assert_eq!(output.status.code(), Some(0));

    let response = parse_stdout_json(&output.stdout);
    let profile_tool = response["result"]["tools"]
        .as_array()
        .expect("tools array")
        .iter()
        .find(|tool| tool["name"] == json!("dataq.profile"))
        .expect("profile tool");

    let properties = &profile_tool["inputSchema"]["properties"];
    assert_eq!(properties["brief"]["type"], Value::from("boolean"));
    assert_eq!(properties["max_fields"]["minimum"], Value::from(0));
    assert_eq!(
        properties["sort_fields"]["enum"],
        json!(["path", "unique_count", "null_ratio"])
    );
    assert!(
        profile_tool["examples"]
            .as_array()
            .expect("examples")
            .iter()
            .any(|example| example["arguments"]["brief"] == json!(true))
    );
}

#[test]
fn profile_tool_brief_accepts_sort_and_max_fields() {
    let request = tool_call_request(
        304,
        "dataq.profile",
        json!({
            "input": [{"a": 1, "b": 1}, {"a": 2, "b": 1}, {"a": 3, "b": null}],
            "brief": true,
            "sort_fields": "unique_count",
            "max_fields": 1
        }),
    );

    let output = run_mcp(&request, None);
    assert_eq!(output.status.code(), Some(0));

    let response = parse_stdout_json(&output.stdout);
    let payload = &response["result"]["structuredContent"]["payload"];
    assert_eq!(payload["truncated"], json!(true));
    assert_eq!(payload["fields"][0]["path"], json!("$[\"a\"]"));
    assert!(payload["fields"][0].get("type_distribution").is_none());
    assert!(payload["fields"][0].get("numeric_stats").is_none());
    assert_eq!(payload["fields"][0]["numeric"]["count"], json!(3));
}

#[test]
fn profile_tool_missing_projected_field_is_input_usage_error_by_default() {
    let request = tool_call_request(
        302,
        "dataq.profile",
        json!({
            "input": [{"id": 1}],
            "field": "missing"
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
fn profile_emit_pipeline_marks_qsv_used_for_qsv_csv_input() {
    let input_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/input/profile_qsv_20_1_everything_mixed.csv");

    let request = tool_call_request(
        31,
        "dataq.profile",
        json!({
            "emit_pipeline": true,
            "from": "csv",
            "input_path": input_path,
            "field": "name",
            "brief": true
        }),
    );

    let output = run_mcp(&request, None);
    assert_eq!(output.status.code(), Some(0));

    let response = parse_stdout_json(&output.stdout);
    assert_eq!(
        response["result"]["structuredContent"]["exit_code"],
        Value::from(0)
    );
    let payload = &response["result"]["structuredContent"]["payload"];
    assert_eq!(payload["record_count"], Value::from(4));
    assert_eq!(payload["field_count"], Value::from(4));
    assert_eq!(payload["fields"].as_array().expect("fields").len(), 1);
    assert_eq!(payload["fields"][0]["path"], Value::from("$[\"name\"]"));
    assert_eq!(payload["fields"][0]["null_ratio"], Value::from(0.25));
    assert_eq!(payload["fields"][0]["dominant_type"], Value::from("string"));
    assert_eq!(payload["fields"][0]["numeric"], Value::Null);

    let tools = response["result"]["structuredContent"]["pipeline"]["external_tools"]
        .as_array()
        .expect("external_tools");
    let qsv_entry = tools
        .iter()
        .find(|entry| entry["name"] == json!("qsv"))
        .expect("qsv entry");
    assert_eq!(qsv_entry["used"], Value::Bool(true));

    let stage = response["result"]["structuredContent"]["pipeline"]["stage_diagnostics"]
        .as_array()
        .expect("stage diagnostics")
        .first()
        .expect("qsv stage");
    assert_eq!(stage["step"], Value::from("profile_qsv_normalize"));
    assert_eq!(stage["tool"], Value::from("qsv"));
}

#[test]
fn profile_tool_reports_ambiguous_real_qsv_rows_as_structured_exit_three() {
    let input_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/input/profile_qsv_20_1_everything_all_string.csv");
    let request = tool_call_request(
        305,
        "dataq.profile",
        json!({
            "emit_pipeline": true,
            "from": "csv",
            "input_path": input_path
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
    assert!(
        response["result"]["structuredContent"]["payload"]["message"]
            .as_str()
            .expect("message")
            .contains("`record_count`, `records`, `rows`, `row_count`, or `total_rows`")
    );
    let stage = response["result"]["structuredContent"]["pipeline"]["stage_diagnostics"]
        .as_array()
        .expect("stage diagnostics")
        .first()
        .expect("qsv stage");
    assert_eq!(stage["step"], Value::from("profile_qsv_normalize"));
    assert_eq!(stage["status"], Value::from("error"));
}

#[test]
fn ingest_doc_emit_pipeline_marks_pandoc_and_jq_used() {
    let toolchain = FakeToolchain::new();
    let request = tool_call_request(
        11,
        "dataq.ingest.doc",
        json!({
            "emit_pipeline": true,
            "input": "# heading",
            "from": "md"
        }),
    );

    let output = run_mcp(&request, Some(&toolchain));
    assert_eq!(output.status.code(), Some(0));

    let response = parse_stdout_json(&output.stdout);
    assert_eq!(
        response["result"]["structuredContent"]["exit_code"],
        Value::from(0)
    );

    let tools = response["result"]["structuredContent"]["pipeline"]["external_tools"]
        .as_array()
        .expect("external_tools array");
    let jq_entry = tools
        .iter()
        .find(|entry| entry["name"].as_str() == Some("jq"))
        .expect("jq entry");
    assert_eq!(jq_entry["used"], Value::Bool(true));

    let pandoc_entry = tools
        .iter()
        .find(|entry| entry["name"].as_str() == Some("pandoc"))
        .expect("pandoc entry");
    assert_eq!(pandoc_entry["used"], Value::Bool(true));
}

#[test]
fn ingest_doc_input_path_dash_returns_input_usage_error() {
    let toolchain = FakeToolchain::new();
    let request = tool_call_request(
        12,
        "dataq.ingest.doc",
        json!({
            "emit_pipeline": true,
            "input_path": "-",
            "from": "md"
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
            "`input` path `-` is not supported for `dataq.ingest.doc`; pass file path or inline `input`",
        )
    );
}

#[test]
fn ingest_doc_accepts_empty_inline_input() {
    let toolchain = FakeToolchain::new();
    let request = tool_call_request(
        13,
        "dataq.ingest.doc",
        json!({
            "input": "",
            "from": "md"
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
    let invalid_params = response["result"]["structuredContent"]["payload"]["invalid_params"]
        .as_array()
        .expect("invalid params");
    assert!(!invalid_params.is_empty());
    assert_eq!(invalid_params[0]["name"], Value::from("left"));
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
fn gate_schema_missing_schema_path_uses_canonical_invalid_param_name() {
    let request = tool_call_request(
        23,
        "dataq.gate.schema",
        json!({
            "input": [{"id": 1}]
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
        response["result"]["structuredContent"]["payload"]["invalid_params"][0]["name"],
        Value::from("schema_path")
    );
}

#[test]
fn gate_schema_conflicting_schema_forms_use_canonical_invalid_param_name() {
    let request = tool_call_request(
        24,
        "dataq.gate.schema",
        json!({
            "input": [{"id": 1}],
            "schema_path": "schema.json",
            "schema": {"type": "object"}
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
        response["result"]["structuredContent"]["payload"]["invalid_params"][0]["name"],
        Value::from("schema_path")
    );
}

#[test]
fn gate_schema_invalid_schema_path_type_uses_canonical_invalid_param_name() {
    let request = tool_call_request(
        25,
        "dataq.gate.schema",
        json!({
            "input": [{"id": 1}],
            "schema_path": 1
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
        response["result"]["structuredContent"]["payload"]["invalid_params"][0]["name"],
        Value::from("schema_path")
    );
}

#[test]
fn ingest_book_invalid_include_files_type_uses_canonical_invalid_param_name() {
    let request = tool_call_request(
        26,
        "dataq.ingest.book",
        json!({
            "root": ".",
            "include_files": "yes"
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
        response["result"]["structuredContent"]["payload"]["invalid_params"][0]["name"],
        Value::from("include_files")
    );
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

#[test]
fn ingest_yaml_jobs_tool_rejects_stdin_input_path_sentinels() {
    for (index, sentinel) in ["-", "/dev/stdin"].into_iter().enumerate() {
        let request = tool_call_request(
            40 + index as i64,
            "dataq.ingest.yaml_jobs",
            json!({
                "mode": "github-actions",
                "input_path": sentinel,
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
            .expect("usage message");
        assert!(message.contains("stdin sentinels"));
    }
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
        write_fake_pandoc_script(bin_dir.join("pandoc"));
        write_fake_rg_script(bin_dir.join("rg"));

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
payload="$(cat)"
if printf '%s' "$payload" | grep -q '"pandoc-api-version"'; then
  printf '{"meta":{"title":"Sample"},"headings":[],"links":[],"tables":[],"code_blocks":[]}'
else
  printf '%s' "$payload"
fi
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

fn write_fake_pandoc_script(path: PathBuf) {
    let script = r#"#!/bin/sh
for arg in "$@"; do
  if [ "$arg" = "--version" ]; then
    printf 'pandoc 3.5.0\n'
    exit 0
  fi
done

printf '{"pandoc-api-version":[1,23,1],"meta":{"title":{"t":"MetaString","c":"Sample"}},"blocks":[]}'
"#;

    fs::write(&path, script).expect("write fake pandoc script");
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
  stdin_payload="$(cat)"
  if [ "$action" = "count" ]; then
    if printf '%s' "$stdin_payload" | grep -q '"team":"c"'; then
      printf '[{"team":"c","price_count":"2"},{"team":"a","price_count":"2"},{"team":"b","price_count":"1"}]'
      exit 0
    fi
    printf '[{"team":"a","price_count":"2"},{"team":"b","price_count":"1"}]'
    exit 0
  fi
  if [ "$action" = "sum" ]; then
    if printf '%s' "$stdin_payload" | grep -q '"team":"overflow-string"'; then
      printf '[{"team":"first","price_sum":"1"},{"team":"overflow-string","price_sum":"18446744073709551617"}]'
      exit 0
    fi
    if printf '%s' "$stdin_payload" | grep -q '"price":9007199254740991' &&
       printf '%s' "$stdin_payload" | grep -q '"team":"z","price":2'; then
      printf '[{"team":"a","price_sum":"9007199254740992"},{"team":"z","price_sum":"9007199254740993"}]'
      exit 0
    fi
    if printf '%s' "$stdin_payload" | grep -q '"team":"c"'; then
      printf '[{"team":"c","price_sum":"15.0"},{"team":"a","price_sum":"15.0"},{"team":"b","price_sum":"7.0"}]'
      exit 0
    fi
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

fn write_fake_rg_script(path: PathBuf) -> PathBuf {
    let script = r#"#!/bin/sh
for arg in "$@"; do
  if [ "$arg" = "--version" ]; then
    printf 'ripgrep 14.1.1\n'
    exit 0
  fi
done

pattern=""
root=""
capture_pattern=0
capture_path=0
for arg in "$@"; do
  if [ "$capture_pattern" = "1" ]; then
    pattern="$arg"
    capture_pattern=0
    continue
  fi
  if [ "$capture_path" = "1" ]; then
    root="$arg"
    capture_path=0
    continue
  fi
  if [ "$arg" = "-e" ]; then
    capture_pattern=1
    continue
  fi
  if [ "$arg" = "--" ]; then
    capture_path=1
    continue
  fi
done

if [ -z "$pattern" ] || [ -z "$root" ]; then
  prev=""
  last=""
  for arg in "$@"; do
    prev="$last"
    last="$arg"
  done
  if [ -z "$pattern" ]; then
    pattern="$prev"
  fi
  if [ -z "$root" ]; then
    root="$last"
  fi
fi

if [ "$pattern" = "token" ]; then
  printf '{"type":"match","data":{"path":{"text":"%s/file.txt"},"lines":{"text":"token\\n"},"line_number":1,"submatches":[{"match":{"text":"token"},"start":0,"end":5}]}}\n' "$root"
  exit 0
fi

exit 1
"#;

    fs::write(&path, script).expect("write fake rg script");
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
