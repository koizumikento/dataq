use std::fs::File;
use std::io::{Cursor, Read, Write};
use std::path::{Path, PathBuf};
use std::str::FromStr;

use serde_json::{Map, Value, json};

use crate::cmd::{
    aggregate,
    r#assert::{self as assert_cmd, AssertInputNormalizeMode},
    canon, contract, diff, doctor, emit, gate, ingest, ingest_api, ingest_yaml_jobs, join, merge,
    profile, recipe, scan, sdiff, transform,
};
use crate::domain::ingest::IngestYamlJobsMode;
use crate::domain::report::{
    ExternalToolUsage, PipelineInput, PipelineInputSource, PipelineReport,
};
use crate::domain::rules::AssertRules;
use crate::engine::aggregate::AggregateMetric;
use crate::engine::r#assert as assert_engine;
use crate::engine::canon::{CanonOptions, canonicalize_values};
use crate::engine::ingest as ingest_engine;
use crate::engine::ingest::IngestDocInputFormat;
use crate::engine::join::JoinHow;
use crate::engine::merge::MergePolicy;
use crate::io::{self, Format};

const JSONRPC_VERSION: &str = "2.0";
const MCP_PROTOCOL_VERSION: &str = "2024-11-05";

const JSONRPC_PARSE_ERROR: i64 = -32700;
const JSONRPC_INVALID_REQUEST: i64 = -32600;
const JSONRPC_METHOD_NOT_FOUND: i64 = -32601;
const JSONRPC_INVALID_PARAMS: i64 = -32602;
const JSONRPC_INTERNAL_ERROR: i64 = -32603;
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

#[derive(Debug, Clone)]
struct JsonRpcRequest {
    id: Value,
    method: String,
    params: Map<String, Value>,
}

#[derive(Debug, Clone)]
struct ToolExecution {
    exit_code: i32,
    payload: Value,
    pipeline: Option<Value>,
}

#[derive(Debug, Clone)]
enum ValueInputSource {
    Path(PathBuf),
    Inline(Vec<Value>),
}

#[derive(Debug, Clone)]
enum DocumentInputSource {
    Path(PathBuf),
    Inline(Value),
}

#[derive(Debug, Clone)]
struct LoadedValues {
    values: Vec<Value>,
    format: Option<Format>,
}

pub fn run_single_request<R: Read, W: Write>(mut input: R, mut output: W) -> i32 {
    let mut raw = Vec::new();
    if input.read_to_end(&mut raw).is_err() {
        return 3;
    }

    let response = match parse_request_bytes(&raw) {
        Ok(request) => handle_request(request),
        Err(error_response) => error_response,
    };

    if serde_json::to_writer(&mut output, &response).is_err() {
        return 3;
    }
    if output.write_all(b"\n").is_err() {
        return 3;
    }
    0
}

fn parse_request_bytes(raw: &[u8]) -> Result<JsonRpcRequest, Value> {
    let parsed: Value = match serde_json::from_slice(raw) {
        Ok(value) => value,
        Err(_) => {
            return Err(error_response(
                Value::Null,
                JSONRPC_PARSE_ERROR,
                "parse error",
            ));
        }
    };

    parse_request_value(parsed)
        .map_err(|(id, code, message)| error_response(id, code, message.as_str()))
}

fn parse_request_value(value: Value) -> Result<JsonRpcRequest, (Value, i64, String)> {
    let object = value.as_object().ok_or_else(|| {
        (
            Value::Null,
            JSONRPC_INVALID_REQUEST,
            "request must be a JSON object".to_string(),
        )
    })?;

    for key in object.keys() {
        if !matches!(key.as_str(), "jsonrpc" | "id" | "method" | "params") {
            return Err((
                extract_error_id(object),
                JSONRPC_INVALID_REQUEST,
                format!("unexpected request field `{key}`"),
            ));
        }
    }

    let jsonrpc = object
        .get("jsonrpc")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            (
                extract_error_id(object),
                JSONRPC_INVALID_REQUEST,
                "`jsonrpc` must be the string `2.0`".to_string(),
            )
        })?;
    if jsonrpc != JSONRPC_VERSION {
        return Err((
            extract_error_id(object),
            JSONRPC_INVALID_REQUEST,
            "`jsonrpc` must be the string `2.0`".to_string(),
        ));
    }

    let id = object.get("id").cloned().ok_or_else(|| {
        (
            Value::Null,
            JSONRPC_INVALID_REQUEST,
            "`id` is required".to_string(),
        )
    })?;
    if !matches!(id, Value::Null | Value::String(_) | Value::Number(_)) {
        return Err((
            Value::Null,
            JSONRPC_INVALID_REQUEST,
            "`id` must be null, string, or number".to_string(),
        ));
    }

    let method = object
        .get("method")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            (
                id.clone(),
                JSONRPC_INVALID_REQUEST,
                "`method` must be a string".to_string(),
            )
        })?
        .to_string();

    let params = match object.get("params") {
        None => Map::new(),
        Some(Value::Object(map)) => map.clone(),
        Some(_) => {
            return Err((
                id,
                JSONRPC_INVALID_REQUEST,
                "`params` must be an object".to_string(),
            ));
        }
    };

    Ok(JsonRpcRequest { id, method, params })
}

fn handle_request(request: JsonRpcRequest) -> Value {
    match request.method.as_str() {
        "initialize" => success_response(request.id, initialize_result()),
        "tools/list" => success_response(request.id, tools_list_result()),
        "tools/call" => handle_tools_call(request.id, request.params),
        _ => error_response(request.id, JSONRPC_METHOD_NOT_FOUND, "method not found"),
    }
}

fn handle_tools_call(id: Value, params: Map<String, Value>) -> Value {
    let Some(name) = params.get("name").and_then(Value::as_str) else {
        return error_response(
            id,
            JSONRPC_INVALID_PARAMS,
            "`tools/call` requires `params.name` as string",
        );
    };
    let arguments = match params.get("arguments") {
        None => Map::new(),
        Some(Value::Object(map)) => map.clone(),
        Some(_) => {
            return error_response(
                id,
                JSONRPC_INVALID_PARAMS,
                "`tools/call` requires `params.arguments` as object",
            );
        }
    };

    let execution = match std::panic::catch_unwind(|| dispatch_tool_call(name, &arguments)) {
        Ok(execution) => execution,
        Err(_) => {
            return error_response(id, JSONRPC_INTERNAL_ERROR, "internal error");
        }
    };
    success_response(id, tool_call_result(execution))
}

fn dispatch_tool_call(tool_name: &str, args: &Map<String, Value>) -> ToolExecution {
    match tool_name {
        "dataq.canon" => execute_canon(args),
        "dataq.ingest.api" => execute_ingest_api(args),
        "dataq.ingest.yaml_jobs" => execute_ingest_yaml_jobs(args),
        "dataq.assert" => execute_assert(args),
        "dataq.gate.schema" => execute_gate_schema(args),
        "dataq.gate.policy" => execute_gate_policy(args),
        "dataq.sdiff" => execute_sdiff(args),
        "dataq.diff.source" => execute_diff_source(args),
        "dataq.profile" => execute_profile(args),
        "dataq.ingest.doc" => execute_ingest_doc(args),
        "dataq.ingest.notes" => execute_ingest_notes(args),
        "dataq.ingest.book" => execute_ingest_book(args),
        "dataq.join" => execute_join(args),
        "dataq.aggregate" => execute_aggregate(args),
        "dataq.scan.text" => execute_scan_text(args),
        "dataq.transform.rowset" => execute_transform_rowset(args),
        "dataq.merge" => execute_merge(args),
        "dataq.doctor" => execute_doctor(args),
        "dataq.contract" => execute_contract(args),
        "dataq.emit.plan" => execute_emit_plan(args),
        "dataq.recipe.run" => execute_recipe_run(args),
        "dataq.recipe.lock" => execute_recipe_lock(args),
        "dataq.recipe.replay" => execute_recipe_replay(args),
        unknown => input_usage_error(format!("unknown tool `{unknown}`")),
    }
}

fn execute_canon(args: &Map<String, Value>) -> ToolExecution {
    let emit_pipeline = match parse_emit_pipeline(args) {
        Ok(value) => value,
        Err(message) => return input_usage_error(message),
    };
    let sort_keys = match parse_bool(args, &["sort_keys"], true, "sort_keys") {
        Ok(value) => value,
        Err(message) => return input_usage_error(message),
    };
    let normalize_time = match parse_bool(args, &["normalize_time"], false, "normalize_time") {
        Ok(value) => value,
        Err(message) => return input_usage_error(message),
    };
    let from = match parse_optional_format(args, &["from"], "from") {
        Ok(value) => value,
        Err(message) => return input_usage_error(message),
    };
    if let Err(message) = parse_optional_format(args, &["to"], "to") {
        return input_usage_error(message);
    }

    let input = match parse_value_input(
        args,
        &["input_path", "input_file"],
        &["input", "input_inline", "input_value"],
        "input",
        true,
    ) {
        Ok(Some(source)) => source,
        Ok(None) => return input_usage_error("missing required `input`"),
        Err(message) => return input_usage_error(message),
    };

    let loaded = match read_values_from_source(&input, "input", from) {
        Ok(loaded) => loaded,
        Err(message) => return input_usage_error(message),
    };

    let canonical = canonicalize_values(
        loaded.values,
        CanonOptions {
            sort_keys,
            normalize_time,
        },
    );

    let mut execution = ToolExecution {
        exit_code: 0,
        payload: values_to_payload(canonical),
        pipeline: None,
    };

    if emit_pipeline {
        let pipeline = PipelineReport::new(
            "canon",
            PipelineInput::new(vec![pipeline_source("input", &input, loaded.format)]),
            canon::pipeline_steps(),
            canon::deterministic_guards(canon::CanonCommandOptions {
                sort_keys,
                normalize_time,
            }),
        );
        execution.pipeline = pipeline_as_value(pipeline).ok();
    }

    execution
}

fn execute_ingest_api(args: &Map<String, Value>) -> ToolExecution {
    let emit_pipeline = match parse_emit_pipeline(args) {
        Ok(value) => value,
        Err(message) => return input_usage_error(message),
    };
    let url = match parse_required_string(args, &["url"], "url") {
        Ok(value) => value,
        Err(message) => return input_usage_error(message),
    };
    let method = match parse_optional_string(args, &["method"], "method") {
        Ok(Some(value)) => match value.to_ascii_uppercase().as_str() {
            "GET" => ingest_api::IngestApiMethod::Get,
            "POST" => ingest_api::IngestApiMethod::Post,
            "PUT" => ingest_api::IngestApiMethod::Put,
            "PATCH" => ingest_api::IngestApiMethod::Patch,
            "DELETE" => ingest_api::IngestApiMethod::Delete,
            _ => return input_usage_error("`method` must be GET|POST|PUT|PATCH|DELETE"),
        },
        Ok(None) => ingest_api::IngestApiMethod::Get,
        Err(message) => return input_usage_error(message),
    };
    let headers = match parse_string_list(args, &["header", "headers"], "header") {
        Ok(value) => value,
        Err(message) => return input_usage_error(message),
    };
    let body = match parse_optional_json_body(args, &["body"], "body") {
        Ok(value) => value,
        Err(message) => return input_usage_error(message),
    };
    let expect_status =
        match parse_optional_u16(args, &["expect_status", "expect-status"], "expect_status") {
            Ok(value) => value,
            Err(message) => return input_usage_error(message),
        };

    let (response, trace) = ingest_api::run_with_trace(&ingest_api::IngestApiCommandArgs {
        url,
        method,
        headers,
        body,
        expect_status,
    });

    let mut execution = ToolExecution {
        exit_code: response.exit_code,
        payload: response.payload,
        pipeline: None,
    };

    if emit_pipeline {
        let mut report = PipelineReport::new(
            "ingest_api",
            PipelineInput::new(vec![PipelineInputSource {
                label: "url".to_string(),
                source: "url".to_string(),
                path: Some(
                    args.get("url")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_string(),
                ),
                format: Some("http".to_string()),
            }]),
            ingest_api::pipeline_steps(),
            ingest_api::deterministic_guards(),
        );
        for tool in &trace.used_tools {
            report = report.mark_external_tool_used(tool);
        }
        report = report.with_stage_diagnostics(trace.stage_diagnostics);
        execution.pipeline = pipeline_as_value(report).ok();
    }

    execution
}

fn execute_ingest_yaml_jobs(args: &Map<String, Value>) -> ToolExecution {
    let emit_pipeline = match parse_emit_pipeline(args) {
        Ok(value) => value,
        Err(message) => return input_usage_error(message),
    };
    let mode = match parse_required_ingest_yaml_jobs_mode(args) {
        Ok(mode) => mode,
        Err(message) => return input_usage_error(message),
    };
    let input = match parse_value_input(
        args,
        &["input_path", "input_file"],
        &["input", "input_inline", "input_value"],
        "input",
        true,
    ) {
        Ok(Some(source)) => source,
        Ok(None) => return input_usage_error("missing required `input`"),
        Err(message) => return input_usage_error(message),
    };
    if let ValueInputSource::Path(path) = &input {
        if is_stdin_input_path_sentinel(path.as_path()) {
            return input_usage_error(
                "`input_path` does not accept stdin sentinels (`-`, `/dev/stdin`) for `dataq.ingest.yaml_jobs`; use inline `input`",
            );
        }
    }

    let input_format = match &input {
        ValueInputSource::Path(_) => Some(Format::Yaml),
        ValueInputSource::Inline(_) => Some(Format::Json),
    };
    let command_args = ingest_yaml_jobs::IngestYamlJobsCommandArgs {
        input: to_ingest_yaml_jobs_input(input.clone()),
        mode,
    };
    let (response, trace) =
        ingest_yaml_jobs::run_with_stdin_and_trace(&command_args, Cursor::new(Vec::new()));

    let mut execution = ToolExecution {
        exit_code: response.exit_code,
        payload: response.payload,
        pipeline: None,
    };

    if emit_pipeline {
        let mut report = PipelineReport::new(
            "ingest_yaml_jobs",
            PipelineInput::new(vec![pipeline_source("input", &input, input_format)]),
            ingest_yaml_jobs::pipeline_steps(),
            ingest_yaml_jobs::deterministic_guards(mode),
        );
        for used_tool in &trace.used_tools {
            report = report.mark_external_tool_used(used_tool);
        }
        report = report.with_stage_diagnostics(trace.stage_diagnostics);
        execution.pipeline = pipeline_as_value(report).ok();
    }

    execution
}

fn execute_assert(args: &Map<String, Value>) -> ToolExecution {
    let emit_pipeline = match parse_emit_pipeline(args) {
        Ok(value) => value,
        Err(message) => return input_usage_error(message),
    };
    let normalize_mode = match parse_optional_normalize_mode(args) {
        Ok(mode) => mode,
        Err(message) => return input_usage_error(message),
    };
    let from = match parse_optional_format(args, &["from"], "from") {
        Ok(value) => value,
        Err(message) => return input_usage_error(message),
    };

    let input = match parse_value_input(
        args,
        &["input_path", "input_file"],
        &["input", "input_inline", "input_value"],
        "input",
        true,
    ) {
        Ok(Some(source)) => source,
        Ok(None) => return input_usage_error("missing required `input`"),
        Err(message) => return input_usage_error(message),
    };

    let rules_source = match parse_document_input(
        args,
        &["rules_path", "rules_file"],
        &["rules", "rules_inline"],
        "rules",
    ) {
        Ok(source) => source,
        Err(message) => return input_usage_error(message),
    };
    let schema_source = match parse_document_input(
        args,
        &["schema_path", "schema_file"],
        &["schema", "schema_inline"],
        "schema",
    ) {
        Ok(source) => source,
        Err(message) => return input_usage_error(message),
    };

    match (&rules_source, &schema_source) {
        (None, None) => {
            return input_usage_error(
                "assert requires exactly one of `rules(_path)` or `schema(_path)`",
            );
        }
        (Some(_), Some(_)) => {
            return input_usage_error("`rules` and `schema` are mutually exclusive");
        }
        _ => {}
    }

    if !matches!(rules_source, Some(DocumentInputSource::Inline(_)))
        && !matches!(schema_source, Some(DocumentInputSource::Inline(_)))
    {
        return execute_assert_with_command_api(
            emit_pipeline,
            normalize_mode,
            from,
            input,
            rules_source,
            schema_source,
        );
    }

    if normalize_mode.is_some() {
        return input_usage_error(
            "`normalize` is only supported when using path-based rules/schema",
        );
    }

    let input_loaded = match read_values_from_source(&input, "input", from) {
        Ok(loaded) => loaded,
        Err(message) => return input_usage_error(message),
    };

    let result = if let Some(source) = rules_source.as_ref() {
        let rules_value = match read_document_from_source(source, "rules") {
            Ok(value) => value,
            Err(message) => return input_usage_error(message),
        };
        let rules: AssertRules = match serde_json::from_value(rules_value) {
            Ok(value) => value,
            Err(error) => {
                return input_usage_error(format!("invalid assert rules: {error}"));
            }
        };
        match assert_engine::execute_assert(&input_loaded.values, &rules) {
            Ok(report) => {
                let matched = report.matched;
                match serde_json::to_value(report) {
                    Ok(payload) => ToolExecution {
                        exit_code: if matched { 0 } else { 2 },
                        payload,
                        pipeline: None,
                    },
                    Err(error) => {
                        return internal_error(format!(
                            "failed to serialize assert report: {error}"
                        ));
                    }
                }
            }
            Err(assert_engine::AssertValidationError::InputUsage(message)) => {
                input_usage_error(message)
            }
            Err(assert_engine::AssertValidationError::Internal(message)) => internal_error(message),
        }
    } else if let Some(source) = schema_source.as_ref() {
        let schema_value = match read_document_from_source(source, "schema") {
            Ok(value) => value,
            Err(message) => return input_usage_error(message),
        };
        match assert_engine::execute_assert_with_schema(&input_loaded.values, &schema_value) {
            Ok(report) => {
                let matched = report.matched;
                match serde_json::to_value(report) {
                    Ok(payload) => ToolExecution {
                        exit_code: if matched { 0 } else { 2 },
                        payload,
                        pipeline: None,
                    },
                    Err(error) => {
                        return internal_error(format!(
                            "failed to serialize assert report: {error}"
                        ));
                    }
                }
            }
            Err(assert_engine::AssertValidationError::InputUsage(message)) => {
                input_usage_error(message)
            }
            Err(assert_engine::AssertValidationError::Internal(message)) => internal_error(message),
        }
    } else {
        return input_usage_error("assert requires a rules or schema source");
    };

    if !emit_pipeline {
        return result;
    }

    let report = PipelineReport::new(
        "assert",
        PipelineInput::new(assert_pipeline_sources(
            &input,
            input_loaded.format,
            rules_source.as_ref(),
            schema_source.as_ref(),
        )),
        assert_cmd::pipeline_steps(None),
        assert_cmd::deterministic_guards(None),
    );
    let pipeline = pipeline_as_value(report).ok();
    ToolExecution { pipeline, ..result }
}

fn execute_assert_with_command_api(
    emit_pipeline: bool,
    normalize_mode: Option<AssertInputNormalizeMode>,
    from: Option<Format>,
    input: ValueInputSource,
    rules_source: Option<DocumentInputSource>,
    schema_source: Option<DocumentInputSource>,
) -> ToolExecution {
    let rules_path = match rules_source {
        Some(DocumentInputSource::Path(path)) => Some(path),
        Some(DocumentInputSource::Inline(_)) => {
            return input_usage_error("inline `rules` are not supported in this assert mode");
        }
        None => None,
    };
    let schema_path = match schema_source {
        Some(DocumentInputSource::Path(path)) => Some(path),
        Some(DocumentInputSource::Inline(_)) => {
            return input_usage_error("inline `schema` are not supported in this assert mode");
        }
        None => None,
    };

    let (input_path, stdin_payload, input_format) = match input {
        ValueInputSource::Path(path) => {
            let resolved_format = from.or_else(|| io::resolve_input_format(None, Some(&path)).ok());
            (Some(path), Vec::new(), resolved_format)
        }
        ValueInputSource::Inline(values) => (
            None,
            serialize_values_as_json_input(&values),
            Some(Format::Json),
        ),
    };

    let command_args = assert_cmd::AssertCommandArgs {
        input: input_path.clone(),
        from: if input_path.is_some() {
            from
        } else {
            Some(from.unwrap_or(Format::Json))
        },
        rules: rules_path.clone(),
        schema: schema_path.clone(),
    };

    let (response, trace) = assert_cmd::run_with_stdin_and_normalize_with_trace(
        &command_args,
        Cursor::new(stdin_payload),
        normalize_mode,
    );

    let mut execution = ToolExecution {
        exit_code: response.exit_code,
        payload: response.payload,
        pipeline: None,
    };

    if emit_pipeline {
        let mut report = PipelineReport::new(
            "assert",
            PipelineInput::new(assert_pipeline_sources_for_paths(
                input_path.as_deref(),
                input_format,
                rules_path.as_deref(),
                schema_path.as_deref(),
            )),
            assert_cmd::pipeline_steps(normalize_mode),
            assert_cmd::deterministic_guards(normalize_mode),
        );
        for used_tool in &trace.used_tools {
            report = report.mark_external_tool_used(used_tool);
        }
        report = report.with_stage_diagnostics(trace.stage_diagnostics);
        execution.pipeline = pipeline_as_value(report).ok();
    }

    execution
}

fn execute_gate_schema(args: &Map<String, Value>) -> ToolExecution {
    let emit_pipeline = match parse_emit_pipeline(args) {
        Ok(value) => value,
        Err(message) => return input_usage_error(message),
    };
    let from = match parse_optional_string(args, &["from"], "from") {
        Ok(value) => value,
        Err(message) => return input_usage_error(message),
    };
    let preset = match gate::resolve_preset(from.as_deref()) {
        Ok(value) => value,
        Err(message) => return input_usage_error(message),
    };

    let input = match parse_value_input(
        args,
        &["input_path", "input_file"],
        &["input", "input_inline", "input_value"],
        "input",
        true,
    ) {
        Ok(Some(source)) => source,
        Ok(None) => return input_usage_error("missing required `input`"),
        Err(message) => return input_usage_error(message),
    };
    if from.is_some() && matches!(input, ValueInputSource::Inline(_)) {
        return input_usage_error("`from` presets require path/stdin input");
    }
    if matches!(
        &input,
        ValueInputSource::Path(path) if gate::is_stdin_path(path.as_path())
    ) {
        return input_usage_error(
            "`dataq.gate.schema` does not accept stdin sentinel paths (`-` or `/dev/stdin`) for `input_path`; pass inline `input` instead",
        );
    }

    let schema_source = match parse_document_input(
        args,
        &["schema_path", "schema_file"],
        &["schema", "schema_inline"],
        "schema",
    ) {
        Ok(Some(source)) => source,
        Ok(None) => return input_usage_error("missing required `schema`"),
        Err(message) => return input_usage_error(message),
    };
    let schema_path = match schema_source {
        DocumentInputSource::Path(path) => path,
        DocumentInputSource::Inline(_) => {
            return input_usage_error(
                "inline `schema` is not supported for `dataq.gate.schema`; use `schema_path`",
            );
        }
    };

    let stdin_format = if preset.is_some() {
        Some(Format::Yaml)
    } else {
        Some(Format::Json)
    };
    let (input_path, stdin_payload, input_format) = match &input {
        ValueInputSource::Path(path) => {
            let format = if preset.is_some() {
                Some(Format::Yaml)
            } else if gate::is_stdin_path(path.as_path()) {
                stdin_format
            } else {
                io::resolve_input_format(None, Some(path.as_path())).ok()
            };
            (Some(path.clone()), Vec::new(), format)
        }
        ValueInputSource::Inline(values) => (
            None,
            serialize_values_as_json_input(values),
            Some(Format::Json),
        ),
    };

    let command_args = gate::GateSchemaCommandArgs {
        schema: schema_path.clone(),
        input: input_path.clone(),
        from: from.clone(),
    };
    let (response, trace) =
        gate::run_schema_with_stdin_and_trace(&command_args, Cursor::new(stdin_payload));

    let mut execution = ToolExecution {
        exit_code: response.exit_code,
        payload: response.payload,
        pipeline: None,
    };
    if !emit_pipeline {
        return execution;
    }

    let schema_format = io::resolve_input_format(None, Some(schema_path.as_path()))
        .ok()
        .map(Format::as_str);
    let input_source = match &input {
        ValueInputSource::Path(path) if gate::is_stdin_path(path.as_path()) => {
            PipelineInputSource::stdin("input", input_format.map(Format::as_str))
        }
        ValueInputSource::Path(path) => PipelineInputSource::path(
            "input",
            path.display().to_string(),
            input_format.map(Format::as_str),
        ),
        ValueInputSource::Inline(_) => inline_source("input", input_format),
    };

    let mut report = PipelineReport::new(
        "gate.schema",
        PipelineInput::new(vec![
            PipelineInputSource::path("schema", schema_path.display().to_string(), schema_format),
            input_source,
        ]),
        gate::schema_pipeline_steps(),
        gate::schema_deterministic_guards(),
    );
    for used_tool in &trace.used_tools {
        report = report.mark_external_tool_used(used_tool);
    }
    report = report.with_stage_diagnostics(trace.stage_diagnostics);
    execution.pipeline = pipeline_as_value(report).ok();
    execution
}

fn execute_gate_policy(args: &Map<String, Value>) -> ToolExecution {
    let emit_pipeline = match parse_emit_pipeline(args) {
        Ok(value) => value,
        Err(message) => return input_usage_error(message),
    };

    let source = match parse_optional_string(args, &["source"], "source") {
        Ok(Some(raw)) => match gate::GatePolicySourcePreset::parse_cli_name(raw.as_str()) {
            Ok(source) => Some(source),
            Err(message) => return input_usage_error(message),
        },
        Ok(None) => None,
        Err(message) => return input_usage_error(message),
    };

    let rules_path =
        match parse_optional_path(args, &["rules_path", "rules_file", "rules"], "rules") {
            Ok(Some(path)) => path,
            Ok(None) => return input_usage_error("missing required `rules`"),
            Err(message) => return input_usage_error(message),
        };

    let input = match parse_value_input(
        args,
        &["input_path", "input_file"],
        &["input", "input_inline", "input_value"],
        "input",
        true,
    ) {
        Ok(Some(source)) => source,
        Ok(None) => return input_usage_error("missing required `input`"),
        Err(message) => return input_usage_error(message),
    };
    if let ValueInputSource::Path(path) = &input
        && gate::is_stdin_path(path.as_path())
    {
        return input_usage_error(
            "`dataq.gate.policy` does not accept stdin sentinel paths for `input_path` (`-`, `/dev/stdin`); provide a file path or inline `input`",
        );
    }

    let input_format = match &input {
        ValueInputSource::Path(path) => io::resolve_input_format(None, Some(path.as_path())).ok(),
        ValueInputSource::Inline(_) => Some(Format::Json),
    };

    let (input_path, stdin_payload) = match &input {
        ValueInputSource::Path(path) => (Some(path.clone()), Vec::new()),
        ValueInputSource::Inline(values) => (None, serialize_values_as_json_input(values)),
    };

    let command_args = gate::GatePolicyCommandArgs {
        rules: rules_path.clone(),
        input: input_path.clone(),
        source,
    };
    let response = gate::run_policy_with_stdin(&command_args, Cursor::new(stdin_payload));

    let mut execution = ToolExecution {
        exit_code: response.exit_code,
        payload: response.payload,
        pipeline: None,
    };

    if emit_pipeline {
        let mut sources = Vec::with_capacity(2);
        sources.push(PipelineInputSource::path(
            "rules",
            rules_path.display().to_string(),
            io::resolve_input_format(None, Some(rules_path.as_path()))
                .ok()
                .map(Format::as_str),
        ));
        match &input {
            ValueInputSource::Path(path) => {
                sources.push(PipelineInputSource::path(
                    "input",
                    path.display().to_string(),
                    input_format.map(Format::as_str),
                ));
            }
            ValueInputSource::Inline(_) => {
                sources.push(inline_source("input", input_format));
            }
        }

        let report = PipelineReport::new(
            "gate.policy",
            PipelineInput::new(sources),
            gate::policy_pipeline_steps(),
            gate::policy_deterministic_guards(source),
        );
        execution.pipeline = pipeline_as_value(report).ok();
    }

    execution
}

fn execute_sdiff(args: &Map<String, Value>) -> ToolExecution {
    let emit_pipeline = match parse_emit_pipeline(args) {
        Ok(value) => value,
        Err(message) => return input_usage_error(message),
    };
    let left = match parse_value_input(
        args,
        &["left_path", "left_file"],
        &["left", "left_inline", "left_value"],
        "left",
        true,
    ) {
        Ok(Some(source)) => source,
        Ok(None) => return input_usage_error("missing required `left`"),
        Err(message) => return input_usage_error(message),
    };
    let right = match parse_value_input(
        args,
        &["right_path", "right_file"],
        &["right", "right_inline", "right_value"],
        "right",
        true,
    ) {
        Ok(Some(source)) => source,
        Ok(None) => return input_usage_error("missing required `right`"),
        Err(message) => return input_usage_error(message),
    };

    let left_from = match parse_optional_format(args, &["left_from"], "left_from") {
        Ok(value) => value,
        Err(message) => return input_usage_error(message),
    };
    let right_from = match parse_optional_format(args, &["right_from"], "right_from") {
        Ok(value) => value,
        Err(message) => return input_usage_error(message),
    };
    let key = match parse_optional_string(args, &["key"], "key") {
        Ok(value) => value,
        Err(message) => return input_usage_error(message),
    };
    let ignore_path = match parse_string_list(args, &["ignore_path", "ignore_paths"], "ignore_path")
    {
        Ok(value) => value,
        Err(message) => return input_usage_error(message),
    };
    let fail_on_diff = match parse_bool(args, &["fail_on_diff"], false, "fail_on_diff") {
        Ok(value) => value,
        Err(message) => return input_usage_error(message),
    };
    let value_diff_cap = match parse_usize(
        args,
        &["value_diff_cap"],
        sdiff::DEFAULT_VALUE_DIFF_CAP,
        "value_diff_cap",
    ) {
        Ok(value) => value,
        Err(message) => return input_usage_error(message),
    };

    let left_loaded = match read_values_from_source(&left, "left", left_from) {
        Ok(loaded) => loaded,
        Err(message) => return input_usage_error(message),
    };
    let right_loaded = match read_values_from_source(&right, "right", right_from) {
        Ok(loaded) => loaded,
        Err(message) => return input_usage_error(message),
    };

    let options = match sdiff::parse_options(value_diff_cap, key.as_deref(), &ignore_path) {
        Ok(options) => options,
        Err(error) => return input_usage_error(error.to_string()),
    };

    let report =
        match sdiff::execute_with_options(&left_loaded.values, &right_loaded.values, options) {
            Ok(report) => report,
            Err(error) => return input_usage_error(error.to_string()),
        };

    let exit_code = if fail_on_diff && report.values.total > 0 {
        2
    } else {
        0
    };
    let payload = match serde_json::to_value(report) {
        Ok(payload) => payload,
        Err(error) => return internal_error(format!("failed to serialize sdiff report: {error}")),
    };

    let mut execution = ToolExecution {
        exit_code,
        payload,
        pipeline: None,
    };

    if emit_pipeline {
        let pipeline = PipelineReport::new(
            "sdiff",
            PipelineInput::new(vec![
                pipeline_source("left", &left, left_loaded.format),
                pipeline_source("right", &right, right_loaded.format),
            ]),
            sdiff::pipeline_steps(),
            sdiff::deterministic_guards(),
        );
        execution.pipeline = pipeline_as_value(pipeline).ok();
    }

    execution
}

fn execute_diff_source(args: &Map<String, Value>) -> ToolExecution {
    let emit_pipeline = match parse_emit_pipeline(args) {
        Ok(value) => value,
        Err(message) => return input_usage_error(message),
    };
    let left = match parse_required_string(args, &["left"], "left") {
        Ok(value) => value,
        Err(message) => return input_usage_error(message),
    };
    let right = match parse_required_string(args, &["right"], "right") {
        Ok(value) => value,
        Err(message) => return input_usage_error(message),
    };
    let fail_on_diff = match parse_bool(args, &["fail_on_diff"], false, "fail_on_diff") {
        Ok(value) => value,
        Err(message) => return input_usage_error(message),
    };

    let execution_result = match diff::execute(left.as_str(), right.as_str()) {
        Ok(execution) => execution,
        Err(error) => return input_usage_error(error.to_string()),
    };
    let exit_code = if fail_on_diff && execution_result.report.values.total > 0 {
        2
    } else {
        0
    };

    let payload = match serde_json::to_value(diff::DiffSourceReport::new(
        execution_result.report,
        execution_result.sources,
    )) {
        Ok(payload) => payload,
        Err(error) => {
            return internal_error(format!("failed to serialize diff source report: {error}"));
        }
    };

    let mut execution = ToolExecution {
        exit_code,
        payload,
        pipeline: None,
    };

    if emit_pipeline {
        let mut pipeline = PipelineReport::new(
            "diff.source",
            PipelineInput::new(vec![
                PipelineInputSource {
                    label: "left".to_string(),
                    source: execution_result.left.metadata.kind.clone(),
                    path: Some(execution_result.left.metadata.path.clone()),
                    format: Some(execution_result.left.metadata.format.clone()),
                },
                PipelineInputSource {
                    label: "right".to_string(),
                    source: execution_result.right.metadata.kind.clone(),
                    path: Some(execution_result.right.metadata.path.clone()),
                    format: Some(execution_result.right.metadata.format.clone()),
                },
            ]),
            diff::pipeline_steps(),
            diff::deterministic_guards(),
        );
        for tool in &execution_result.used_tools {
            pipeline = pipeline.mark_external_tool_used(tool);
        }
        execution.pipeline = pipeline_as_value(pipeline).ok();
    }

    execution
}

fn execute_profile(args: &Map<String, Value>) -> ToolExecution {
    let emit_pipeline = match parse_emit_pipeline(args) {
        Ok(value) => value,
        Err(message) => return input_usage_error(message),
    };
    let from = match parse_optional_format(args, &["from"], "from") {
        Ok(value) => value,
        Err(message) => return input_usage_error(message),
    };
    let input = match parse_value_input(
        args,
        &["input_path", "input_file"],
        &["input", "input_inline", "input_value"],
        "input",
        true,
    ) {
        Ok(Some(source)) => source,
        Ok(None) => return input_usage_error("missing required `input`"),
        Err(message) => return input_usage_error(message),
    };

    let (response, input_format) = match &input {
        ValueInputSource::Path(path) => {
            let input_format = from.or_else(|| io::resolve_input_format(None, Some(path)).ok());
            (
                profile::run_with_stdin(
                    &profile::ProfileCommandArgs {
                        input: Some(path.clone()),
                        from,
                    },
                    Cursor::new(Vec::new()),
                ),
                input_format,
            )
        }
        ValueInputSource::Inline(values) => {
            if let Some(explicit) = from {
                if explicit != Format::Json {
                    return input_usage_error("inline profile input only supports `from=json`");
                }
            }
            (
                profile::run_with_stdin(
                    &profile::ProfileCommandArgs {
                        input: None,
                        from: Some(Format::Json),
                    },
                    Cursor::new(serialize_values_as_json_input(values)),
                ),
                Some(Format::Json),
            )
        }
    };

    let mut execution = ToolExecution {
        exit_code: response.exit_code,
        payload: response.payload,
        pipeline: None,
    };

    if emit_pipeline {
        let pipeline = PipelineReport::new(
            "profile",
            PipelineInput::new(vec![pipeline_source("input", &input, input_format)]),
            profile::pipeline_steps(),
            profile::deterministic_guards(),
        );
        execution.pipeline = pipeline_as_value(pipeline).ok();
    }

    execution
}

fn execute_ingest_doc(args: &Map<String, Value>) -> ToolExecution {
    let emit_pipeline = match parse_emit_pipeline(args) {
        Ok(value) => value,
        Err(message) => return input_usage_error(message),
    };
    let from = match parse_required_string(args, &["from"], "from") {
        Ok(value) => match parse_ingest_doc_format(value.as_str()) {
            Ok(format) => format,
            Err(message) => return input_usage_error(message),
        },
        Err(message) => return input_usage_error(message),
    };
    let input_path = match parse_optional_path(args, &["input_path", "input_file"], "input") {
        Ok(value) => value,
        Err(message) => return input_usage_error(message),
    };
    let input_text = match parse_optional_string_allow_empty(
        args,
        &["input", "input_text", "input_inline"],
        "input",
    ) {
        Ok(value) => value,
        Err(message) => return input_usage_error(message),
    };
    if input_path.is_some() && input_text.is_some() {
        return input_usage_error("`input` path and inline forms are mutually exclusive");
    }
    if input_path.is_none() && input_text.is_none() {
        return input_usage_error("missing required `input`");
    }
    if input_path.as_deref() == Some(Path::new("-")) {
        return input_usage_error(
            "`input` path `-` is not supported for `dataq.ingest.doc`; pass file path or inline `input`",
        );
    }

    let command_args = ingest::IngestDocCommandArgs {
        input: input_path.clone(),
        from,
    };
    let stdin_payload = input_text.unwrap_or_default().into_bytes();
    let response = ingest::run_with_stdin(&command_args, Cursor::new(stdin_payload));

    let mut execution = ToolExecution {
        exit_code: response.exit_code,
        payload: response.payload,
        pipeline: None,
    };

    if emit_pipeline {
        let source = if let Some(path) = input_path {
            PipelineInputSource::path("input", path.display().to_string(), Some(from.as_str()))
        } else {
            PipelineInputSource {
                label: "input".to_string(),
                source: "inline".to_string(),
                path: None,
                format: Some(from.as_str().to_string()),
            }
        };
        let report = PipelineReport::new(
            "ingest.doc",
            PipelineInput::new(vec![source]),
            ingest::pipeline_steps(),
            ingest::deterministic_guards(),
        )
        .mark_external_tool_used("pandoc")
        .mark_external_tool_used("jq");
        execution.pipeline = pipeline_as_value(report).ok();
    }

    execution
}

fn execute_ingest_notes(args: &Map<String, Value>) -> ToolExecution {
    let emit_pipeline = match parse_emit_pipeline(args) {
        Ok(value) => value,
        Err(message) => return input_usage_error(message),
    };
    let tags = match parse_string_list(args, &["tag", "tags"], "tag") {
        Ok(values) => values,
        Err(message) => return input_usage_error(message),
    };
    let since = match parse_optional_string(args, &["since"], "since") {
        Ok(value) => value,
        Err(message) => return input_usage_error(message),
    };
    let until = match parse_optional_string(args, &["until"], "until") {
        Ok(value) => value,
        Err(message) => return input_usage_error(message),
    };
    let to = match parse_optional_string(args, &["to"], "to") {
        Ok(value) => value,
        Err(message) => return input_usage_error(message),
    };
    if let Some(to) = to {
        if !matches!(to.as_str(), "json" | "jsonl") {
            return input_usage_error("`to` must be `json` or `jsonl`");
        }
    }

    let time_range = match ingest_engine::resolve_time_range(since.as_deref(), until.as_deref()) {
        Ok(range) => range,
        Err(error) => return input_usage_error(error.to_string()),
    };
    let command_args = ingest::IngestNotesCommandArgs {
        tags,
        since: time_range.since,
        until: time_range.until,
    };
    let (response, trace) = ingest::run_notes_with_trace(&command_args);

    let mut execution = ToolExecution {
        exit_code: response.exit_code,
        payload: response.payload,
        pipeline: None,
    };

    if emit_pipeline {
        let mut report = PipelineReport::new(
            "ingest.notes",
            PipelineInput::new(Vec::new()),
            ingest::notes_pipeline_steps(),
            ingest::notes_deterministic_guards(),
        );
        for used_tool in &trace.used_tools {
            report = report.mark_external_tool_used(used_tool);
        }
        report = report.with_stage_diagnostics(trace.stage_diagnostics);
        execution.pipeline = pipeline_as_value(report).ok();
    }

    execution
}

fn execute_ingest_book(args: &Map<String, Value>) -> ToolExecution {
    let emit_pipeline = match parse_emit_pipeline(args) {
        Ok(value) => value,
        Err(message) => return input_usage_error(message),
    };
    let root = match parse_optional_path(args, &["root", "root_path", "book_root"], "root") {
        Ok(Some(path)) => path,
        Ok(None) => return input_usage_error("missing required `root`"),
        Err(message) => return input_usage_error(message),
    };
    let include_files = match parse_bool(args, &["include_files"], false, "include_files") {
        Ok(value) => value,
        Err(message) => return input_usage_error(message),
    };
    let verify_mdbook_meta = match parse_bool(
        args,
        &["verify_mdbook_meta"],
        ingest::resolve_verify_mdbook_meta(),
        "verify_mdbook_meta",
    ) {
        Ok(value) => value,
        Err(message) => return input_usage_error(message),
    };

    let (response, trace) = ingest::run_book_with_trace(&ingest::IngestBookCommandArgs {
        root: root.clone(),
        include_files,
        verify_mdbook_meta,
    });

    let mut execution = ToolExecution {
        exit_code: response.exit_code,
        payload: response.payload,
        pipeline: None,
    };

    if emit_pipeline {
        let mut report = PipelineReport::new(
            "ingest.book",
            PipelineInput::new(vec![PipelineInputSource::path(
                "root",
                root.display().to_string(),
                None,
            )]),
            ingest::pipeline_steps_book(),
            ingest::deterministic_guards_book(),
        );
        for used_tool in &trace.used_tools {
            report = report.mark_external_tool_used(used_tool);
        }
        report = report.with_stage_diagnostics(trace.stage_diagnostics);
        execution.pipeline = pipeline_as_value(report).ok();
    }

    execution
}

fn execute_join(args: &Map<String, Value>) -> ToolExecution {
    let emit_pipeline = match parse_emit_pipeline(args) {
        Ok(value) => value,
        Err(message) => return input_usage_error(message),
    };
    let left = match parse_value_input(
        args,
        &["left_path", "left_file"],
        &["left", "left_inline", "left_value"],
        "left",
        true,
    ) {
        Ok(Some(source)) => source,
        Ok(None) => return input_usage_error("missing required `left`"),
        Err(message) => return input_usage_error(message),
    };
    let right = match parse_value_input(
        args,
        &["right_path", "right_file"],
        &["right", "right_inline", "right_value"],
        "right",
        true,
    ) {
        Ok(Some(source)) => source,
        Ok(None) => return input_usage_error("missing required `right`"),
        Err(message) => return input_usage_error(message),
    };
    let on = match parse_required_string(args, &["on"], "on") {
        Ok(value) => value,
        Err(message) => return input_usage_error(message),
    };
    let how = match parse_optional_string(args, &["how"], "how") {
        Ok(Some(value)) => match value.as_str() {
            "inner" => JoinHow::Inner,
            "left" => JoinHow::Left,
            _ => return input_usage_error("`how` must be `inner` or `left`"),
        },
        Ok(None) => JoinHow::Inner,
        Err(message) => return input_usage_error(message),
    };

    let left_format = source_format(&left);
    let right_format = source_format(&right);

    let command_args = join::JoinCommandArgs {
        left: to_join_input(left.clone()),
        right: to_join_input(right.clone()),
        on,
        how,
    };
    let (response, trace) = join::run_with_trace(&command_args);

    let mut execution = ToolExecution {
        exit_code: response.exit_code,
        payload: response.payload,
        pipeline: None,
    };

    if emit_pipeline {
        let mut report = PipelineReport::new(
            "join",
            PipelineInput::new(vec![
                pipeline_source("left", &left, left_format),
                pipeline_source("right", &right, right_format),
            ]),
            join::pipeline_steps(),
            join::deterministic_guards(),
        );
        for tool in &trace.used_tools {
            report = report.mark_external_tool_used(tool);
        }
        report = report.with_stage_diagnostics(trace.stage_diagnostics);
        execution.pipeline = pipeline_as_value(report).ok();
    }

    execution
}

fn execute_aggregate(args: &Map<String, Value>) -> ToolExecution {
    let emit_pipeline = match parse_emit_pipeline(args) {
        Ok(value) => value,
        Err(message) => return input_usage_error(message),
    };
    let input = match parse_value_input(
        args,
        &["input_path", "input_file"],
        &["input", "input_inline", "input_value"],
        "input",
        true,
    ) {
        Ok(Some(source)) => source,
        Ok(None) => return input_usage_error("missing required `input`"),
        Err(message) => return input_usage_error(message),
    };
    let group_by = match parse_required_string(args, &["group_by"], "group_by") {
        Ok(value) => value,
        Err(message) => return input_usage_error(message),
    };
    let target = match parse_required_string(args, &["target"], "target") {
        Ok(value) => value,
        Err(message) => return input_usage_error(message),
    };
    let metric = match parse_optional_string(args, &["metric"], "metric") {
        Ok(Some(value)) => match value.as_str() {
            "count" => AggregateMetric::Count,
            "sum" => AggregateMetric::Sum,
            "avg" => AggregateMetric::Avg,
            _ => return input_usage_error("`metric` must be one of `count`, `sum`, `avg`"),
        },
        Ok(None) => AggregateMetric::Count,
        Err(message) => return input_usage_error(message),
    };

    let input_format = source_format(&input);

    let command_args = aggregate::AggregateCommandArgs {
        input: to_aggregate_input(input.clone()),
        group_by,
        metric,
        target,
    };
    let (response, trace) = aggregate::run_with_trace(&command_args);

    let mut execution = ToolExecution {
        exit_code: response.exit_code,
        payload: response.payload,
        pipeline: None,
    };

    if emit_pipeline {
        let mut report = PipelineReport::new(
            "aggregate",
            PipelineInput::new(vec![pipeline_source("input", &input, input_format)]),
            aggregate::pipeline_steps(),
            aggregate::deterministic_guards(),
        );
        for tool in &trace.used_tools {
            report = report.mark_external_tool_used(tool);
        }
        report = report.with_stage_diagnostics(trace.stage_diagnostics);
        execution.pipeline = pipeline_as_value(report).ok();
    }

    execution
}

fn execute_scan_text(args: &Map<String, Value>) -> ToolExecution {
    let emit_pipeline = match parse_emit_pipeline(args) {
        Ok(value) => value,
        Err(message) => return input_usage_error(message),
    };
    let pattern = match parse_required_string(args, &["pattern"], "pattern") {
        Ok(value) => value,
        Err(message) => return input_usage_error(message),
    };
    let path = match parse_optional_path(args, &["path"], "path") {
        Ok(path) => path.unwrap_or_else(|| PathBuf::from(".")),
        Err(message) => return input_usage_error(message),
    };
    let glob = match parse_string_list(args, &["glob", "globs"], "glob") {
        Ok(value) => value,
        Err(message) => return input_usage_error(message),
    };
    let max_matches = match parse_optional_usize(args, &["max_matches"], "max_matches") {
        Ok(value) => value,
        Err(message) => return input_usage_error(message),
    };
    let policy_mode = match parse_bool(args, &["policy_mode"], false, "policy_mode") {
        Ok(value) => value,
        Err(message) => return input_usage_error(message),
    };
    let jq_project = match parse_bool(args, &["jq_project"], false, "jq_project") {
        Ok(value) => value,
        Err(message) => return input_usage_error(message),
    };

    let command_args = scan::ScanTextCommandArgs {
        pattern,
        path: path.clone(),
        glob,
        max_matches,
        policy_mode,
        jq_project,
    };
    let (response, trace) = scan::run_with_trace(&command_args);

    let mut execution = ToolExecution {
        exit_code: response.exit_code,
        payload: response.payload,
        pipeline: None,
    };

    if emit_pipeline {
        let mut report = PipelineReport::new(
            "scan",
            PipelineInput::new(vec![PipelineInputSource::path(
                "path",
                path.display().to_string(),
                None,
            )]),
            scan::pipeline_steps(),
            scan::deterministic_guards(),
        );
        if !report.external_tools.iter().any(|tool| tool.name == "rg") {
            report.external_tools.push(ExternalToolUsage {
                name: "rg".to_string(),
                used: false,
            });
        }
        for tool in &trace.used_tools {
            report = report.mark_external_tool_used(tool);
        }
        report = report.with_stage_diagnostics(trace.stage_diagnostics);
        execution.pipeline = pipeline_as_value(report).ok();
    }

    execution
}

fn execute_transform_rowset(args: &Map<String, Value>) -> ToolExecution {
    let emit_pipeline = match parse_emit_pipeline(args) {
        Ok(value) => value,
        Err(message) => return input_usage_error(message),
    };
    let input = match parse_value_input(
        args,
        &["input_path", "input_file"],
        &["input", "input_inline", "input_value"],
        "input",
        true,
    ) {
        Ok(Some(source)) => source,
        Ok(None) => return input_usage_error("missing required `input`"),
        Err(message) => return input_usage_error(message),
    };
    let jq_filter = match parse_required_string(args, &["jq_filter", "jq-filter"], "jq_filter") {
        Ok(value) => value,
        Err(message) => return input_usage_error(message),
    };
    let mlr = match parse_string_list(args, &["mlr", "mlr_args"], "mlr") {
        Ok(values) if values.is_empty() => return input_usage_error("missing required `mlr`"),
        Ok(values) => values,
        Err(message) => return input_usage_error(message),
    };

    let input_format = source_format(&input);
    let command_args = transform::TransformRowsetCommandArgs {
        input: to_transform_rowset_input(input.clone()),
        jq_filter,
        mlr,
    };
    let (response, trace) = transform::run_rowset_with_trace(&command_args);

    let mut execution = ToolExecution {
        exit_code: response.exit_code,
        payload: response.payload,
        pipeline: None,
    };

    if emit_pipeline {
        let mut report = PipelineReport::new(
            "transform.rowset",
            PipelineInput::new(vec![pipeline_source("input", &input, input_format)]),
            transform::pipeline_steps(),
            transform::deterministic_guards(),
        );
        for tool in &trace.used_tools {
            report = report.mark_external_tool_used(tool);
        }
        report = report.with_stage_diagnostics(trace.stage_diagnostics);
        execution.pipeline = pipeline_as_value(report).ok();
    }

    execution
}

fn execute_merge(args: &Map<String, Value>) -> ToolExecution {
    let emit_pipeline = match parse_emit_pipeline(args) {
        Ok(value) => value,
        Err(message) => return input_usage_error(message),
    };
    let base = match parse_document_input(
        args,
        &["base_path", "base_file"],
        &["base", "base_inline"],
        "base",
    ) {
        Ok(Some(source)) => source,
        Ok(None) => return input_usage_error("missing required `base`"),
        Err(message) => return input_usage_error(message),
    };

    let overlay_paths =
        match parse_path_list(args, &["overlay_paths", "overlay_path"], "overlay_paths") {
            Ok(paths) => paths,
            Err(message) => return input_usage_error(message),
        };
    let overlay_inline = match parse_value_list(
        args,
        &["overlays", "overlays_inline", "overlay_inline"],
        "overlays",
    ) {
        Ok(values) => values,
        Err(message) => return input_usage_error(message),
    };
    if !overlay_paths.is_empty() && !overlay_inline.is_empty() {
        return input_usage_error("`overlay_paths` and inline `overlays` are mutually exclusive");
    }

    let mut overlays = Vec::new();
    for path in overlay_paths {
        overlays.push(DocumentInputSource::Path(path));
    }
    for value in overlay_inline {
        overlays.push(DocumentInputSource::Inline(value));
    }
    if overlays.is_empty() {
        return input_usage_error("at least one overlay is required");
    }

    let policy = match parse_optional_string(args, &["policy"], "policy") {
        Ok(Some(raw)) => match MergePolicy::parse_cli_name(raw.as_str()) {
            Some(policy) => policy,
            None => {
                return input_usage_error(
                    "`policy` must be one of `last-wins`, `deep-merge`, `array-replace`",
                );
            }
        },
        Ok(None) => MergePolicy::LastWins,
        Err(message) => return input_usage_error(message),
    };
    let policy_paths =
        match parse_string_list(args, &["policy_path", "policy_paths"], "policy_path") {
            Ok(value) => value,
            Err(message) => return input_usage_error(message),
        };

    let command_args = merge::MergeCommandInputArgs {
        base: to_merge_input(base.clone()),
        overlays: overlays.iter().cloned().map(to_merge_input).collect(),
        policy,
    };

    let response = merge::run_with_policy_paths_from_inputs(&command_args, &policy_paths);

    let mut execution = ToolExecution {
        exit_code: response.exit_code,
        payload: response.payload,
        pipeline: None,
    };

    if emit_pipeline {
        let mut sources = Vec::with_capacity(1 + overlays.len());
        sources.push(document_pipeline_source("base", &base));
        for (idx, overlay) in overlays.iter().enumerate() {
            sources.push(document_pipeline_source(
                format!("overlay[{idx}]").as_str(),
                overlay,
            ));
        }
        let report = PipelineReport::new(
            "merge",
            PipelineInput::new(sources),
            merge::pipeline_steps(),
            merge::deterministic_guards(),
        );
        execution.pipeline = pipeline_as_value(report).ok();
    }

    execution
}

fn execute_doctor(args: &Map<String, Value>) -> ToolExecution {
    let emit_pipeline = match parse_emit_pipeline(args) {
        Ok(value) => value,
        Err(message) => return input_usage_error(message),
    };
    let capabilities = match parse_bool(args, &["capabilities"], false, "capabilities") {
        Ok(value) => value,
        Err(message) => return input_usage_error(message),
    };
    let profile = match parse_optional_doctor_profile(args) {
        Ok(value) => value,
        Err(message) => return input_usage_error(message),
    };
    let command_input = doctor::DoctorCommandInput {
        capabilities,
        profile,
    };

    let (response, _) = doctor::run_with_input_and_trace(command_input);
    let mut execution = ToolExecution {
        exit_code: response.exit_code,
        payload: response.payload,
        pipeline: None,
    };

    if emit_pipeline {
        let mut report = PipelineReport::new(
            "doctor",
            PipelineInput::new(Vec::new()),
            doctor::pipeline_steps(command_input.profile),
            doctor::deterministic_guards(command_input.profile),
        );
        for tool in doctor::pipeline_external_tools(command_input.profile) {
            report = report.mark_external_tool_used(&tool);
        }
        execution.pipeline = pipeline_as_value(report).ok();
    }

    execution
}

fn execute_contract(args: &Map<String, Value>) -> ToolExecution {
    let emit_pipeline = match parse_emit_pipeline(args) {
        Ok(value) => value,
        Err(message) => return input_usage_error(message),
    };
    let all = match parse_bool(args, &["all"], false, "all") {
        Ok(value) => value,
        Err(message) => return input_usage_error(message),
    };
    let command = match parse_optional_string(args, &["command"], "command") {
        Ok(value) => value,
        Err(message) => return input_usage_error(message),
    };

    let response = if all || command.is_none() {
        contract::run_all()
    } else if let Some(command) = command {
        let command = match contract_command_from_str(command.as_str()) {
            Ok(command) => command,
            Err(message) => return input_usage_error(message),
        };
        contract::run_for_command(command)
    } else {
        contract::run_all()
    };

    let mut execution = ToolExecution {
        exit_code: response.exit_code,
        payload: response.payload,
        pipeline: None,
    };

    if emit_pipeline {
        let report = PipelineReport::new(
            "contract",
            PipelineInput::new(Vec::new()),
            contract::pipeline_steps(),
            contract::deterministic_guards(),
        );
        execution.pipeline = pipeline_as_value(report).ok();
    }

    execution
}

fn execute_emit_plan(args: &Map<String, Value>) -> ToolExecution {
    let emit_pipeline = match parse_emit_pipeline(args) {
        Ok(value) => value,
        Err(message) => return input_usage_error(message),
    };
    let command = match parse_required_string(args, &["command"], "command") {
        Ok(value) => value,
        Err(message) => return input_usage_error(message),
    };
    let plan_args = match parse_optional_string_array(args, &["args"], "args") {
        Ok(Some(values)) => values,
        Ok(None) => Vec::new(),
        Err(message) => return input_usage_error(message),
    };

    let response = emit::run_plan(&emit::EmitPlanCommandArgs {
        command,
        args: plan_args.clone(),
    });

    let mut execution = ToolExecution {
        exit_code: response.exit_code,
        payload: response.payload,
        pipeline: None,
    };

    if emit_pipeline {
        let mut sources = vec![inline_source("command", Some(Format::Json))];
        if !plan_args.is_empty() {
            sources.push(inline_source("args", Some(Format::Json)));
        }
        let report = PipelineReport::new(
            "emit",
            PipelineInput::new(sources),
            emit::pipeline_steps(),
            emit::deterministic_guards(),
        );
        execution.pipeline = pipeline_as_value(report).ok();
    }

    execution
}

fn execute_recipe_run(args: &Map<String, Value>) -> ToolExecution {
    let emit_pipeline = match parse_emit_pipeline(args) {
        Ok(value) => value,
        Err(message) => return input_usage_error(message),
    };
    let file_path =
        match parse_optional_path(args, &["file_path", "file", "recipe_path"], "file_path") {
            Ok(value) => value,
            Err(message) => return input_usage_error(message),
        };
    let inline_recipe = match parse_inline_value(args, &["recipe", "recipe_inline"], "recipe") {
        Ok(value) => value,
        Err(message) => return input_usage_error(message),
    };
    if file_path.is_some() && inline_recipe.is_some() {
        return input_usage_error("`file_path` and inline `recipe` are mutually exclusive");
    }
    if file_path.is_none() && inline_recipe.is_none() {
        return input_usage_error("either `file_path` or inline `recipe` must be provided");
    }

    let base_dir = match parse_optional_path(args, &["base_dir"], "base_dir") {
        Ok(value) => value,
        Err(message) => return input_usage_error(message),
    };

    let (response, trace) = recipe::run_with_trace(&recipe::RecipeCommandArgs {
        file_path: file_path.clone(),
        recipe: inline_recipe,
        base_dir,
    });

    let mut execution = ToolExecution {
        exit_code: response.exit_code,
        payload: response.payload,
        pipeline: None,
    };

    if emit_pipeline {
        let steps = if trace.steps.is_empty() {
            vec![
                "load_recipe_file".to_string(),
                "validate_recipe_schema".to_string(),
            ]
        } else {
            trace.steps
        };
        let source = if let Some(path) = file_path {
            PipelineInputSource::path(
                "recipe",
                path.display().to_string(),
                io::resolve_input_format(None, Some(path.as_path()))
                    .ok()
                    .map(Format::as_str),
            )
        } else {
            inline_source("recipe", Some(Format::Json))
        };
        let report = PipelineReport::new(
            "recipe",
            PipelineInput::new(vec![source]),
            steps,
            recipe::deterministic_guards_run(),
        );
        execution.pipeline = pipeline_as_value(report).ok();
    }

    execution
}

fn execute_recipe_lock(args: &Map<String, Value>) -> ToolExecution {
    let emit_pipeline = match parse_emit_pipeline(args) {
        Ok(value) => value,
        Err(message) => return input_usage_error(message),
    };
    let file_path =
        match parse_optional_path(args, &["file_path", "file", "recipe_path"], "file_path") {
            Ok(Some(value)) => value,
            Ok(None) => return input_usage_error("missing required `file_path`"),
            Err(message) => return input_usage_error(message),
        };
    let out_path = match parse_optional_path(args, &["out_path", "out"], "out_path") {
        Ok(value) => value,
        Err(message) => return input_usage_error(message),
    };

    let (response, trace, serialized_lock) =
        recipe::lock_with_trace(&recipe::RecipeLockCommandArgs {
            file_path: file_path.clone(),
        });

    let mut execution = ToolExecution {
        exit_code: response.exit_code,
        payload: response.payload,
        pipeline: None,
    };

    if execution.exit_code == 0 {
        if let Some(out_path) = out_path {
            if let Some(serialized_lock) = serialized_lock {
                if let Err(error) = std::fs::write(&out_path, serialized_lock.as_slice()) {
                    execution.exit_code = 3;
                    execution.payload = json!({
                        "error": "input_usage_error",
                        "message": format!(
                            "failed to write recipe lock file `{}`: {error}",
                            out_path.display()
                        ),
                    });
                }
            } else {
                execution.exit_code = 1;
                execution.payload = json!({
                    "error": "internal_error",
                    "message": "recipe lock payload bytes were unavailable",
                });
            }
        }
    }

    if emit_pipeline {
        let steps = if trace.steps.is_empty() {
            vec![
                "recipe_lock_parse".to_string(),
                "recipe_lock_probe_tools".to_string(),
                "recipe_lock_fingerprint".to_string(),
            ]
        } else {
            trace.steps
        };
        let mut report = PipelineReport::new(
            "recipe",
            PipelineInput::new(vec![PipelineInputSource::path(
                "recipe",
                file_path.display().to_string(),
                io::resolve_input_format(None, Some(file_path.as_path()))
                    .ok()
                    .map(Format::as_str),
            )]),
            steps,
            recipe::deterministic_guards_lock(),
        );
        for tool_name in trace.tool_versions.keys() {
            report = report.mark_external_tool_used(tool_name);
        }
        execution.pipeline = pipeline_as_value(report).ok();
    }

    execution
}

fn execute_recipe_replay(args: &Map<String, Value>) -> ToolExecution {
    let emit_pipeline = match parse_emit_pipeline(args) {
        Ok(value) => value,
        Err(message) => return input_usage_error(message),
    };
    let file_path = match parse_optional_path(args, &["file_path", "file", "recipe_path"], "file") {
        Ok(Some(path)) => path,
        Ok(None) => return input_usage_error("missing required `file`"),
        Err(message) => return input_usage_error(message),
    };
    let lock_path = match parse_optional_path(args, &["lock_path", "lock"], "lock") {
        Ok(Some(path)) => path,
        Ok(None) => return input_usage_error("missing required `lock`"),
        Err(message) => return input_usage_error(message),
    };
    let strict = match parse_bool(args, &["strict"], false, "strict") {
        Ok(value) => value,
        Err(message) => return input_usage_error(message),
    };

    let (response, trace) = recipe::replay_with_trace(&recipe::RecipeReplayCommandArgs {
        file_path: file_path.clone(),
        lock_path: lock_path.clone(),
        strict,
    });

    let mut execution = ToolExecution {
        exit_code: response.exit_code,
        payload: response.payload,
        pipeline: None,
    };

    if emit_pipeline {
        let steps = if trace.steps.is_empty() {
            vec![
                "recipe_replay_parse".to_string(),
                "recipe_replay_verify_lock".to_string(),
                "recipe_replay_execute".to_string(),
            ]
        } else {
            trace.steps
        };
        let report = PipelineReport::new(
            "recipe",
            PipelineInput::new(vec![
                PipelineInputSource::path(
                    "recipe",
                    file_path.display().to_string(),
                    io::resolve_input_format(None, Some(file_path.as_path()))
                        .ok()
                        .map(Format::as_str),
                ),
                PipelineInputSource::path(
                    "lock",
                    lock_path.display().to_string(),
                    io::resolve_input_format(None, Some(lock_path.as_path()))
                        .ok()
                        .map(Format::as_str),
                ),
            ]),
            steps,
            recipe::deterministic_guards_replay(),
        );
        execution.pipeline = pipeline_as_value(report).ok();
    }

    execution
}

fn initialize_result() -> Value {
    json!({
        "protocolVersion": MCP_PROTOCOL_VERSION,
        "capabilities": {
            "tools": {
                "listChanged": false
            }
        },
        "serverInfo": {
            "name": "dataq",
            "version": env!("CARGO_PKG_VERSION")
        }
    })
}

fn tools_list_result() -> Value {
    let tools: Vec<Value> = TOOL_ORDER
        .iter()
        .map(|name| tool_definition(name))
        .collect();
    json!({ "tools": tools })
}

fn tool_definition(tool_name: &str) -> Value {
    let mut input_schema = json!({
        "type": "object",
        "properties": {
            "emit_pipeline": {
                "type": "boolean",
                "default": false
            }
        },
        "additionalProperties": true
    });
    if tool_name == "dataq.doctor" {
        input_schema["properties"]["capabilities"] = json!({
            "type": "boolean",
            "default": false
        });
        input_schema["properties"]["profile"] = json!({
            "type": "string",
            "enum": ["core", "ci-jobs", "doc", "api", "notes", "book", "scan"]
        });
    }

    json!({
        "name": tool_name,
        "description": format!("dataq MCP tool `{tool_name}`"),
        "inputSchema": input_schema
    })
}

fn tool_call_result(execution: ToolExecution) -> Value {
    let mut structured = json!({
        "exit_code": execution.exit_code,
        "payload": execution.payload,
    });
    if let Some(pipeline) = execution.pipeline {
        structured["pipeline"] = pipeline;
    }

    let text = serde_json::to_string(&structured)
        .unwrap_or_else(|_| "{\"error\":\"failed to serialize structuredContent\"}".to_string());

    json!({
        "content": [
            {
                "type": "text",
                "text": text
            }
        ],
        "structuredContent": structured,
        "isError": execution.exit_code != 0,
    })
}

fn success_response(id: Value, result: Value) -> Value {
    json!({
        "jsonrpc": JSONRPC_VERSION,
        "id": id,
        "result": result,
    })
}

fn error_response(id: Value, code: i64, message: &str) -> Value {
    json!({
        "jsonrpc": JSONRPC_VERSION,
        "id": id,
        "error": {
            "code": code,
            "message": message,
        }
    })
}

fn extract_error_id(object: &Map<String, Value>) -> Value {
    match object.get("id") {
        Some(Value::Null | Value::String(_) | Value::Number(_)) => {
            object.get("id").cloned().unwrap_or(Value::Null)
        }
        _ => Value::Null,
    }
}

fn input_usage_error(message: impl Into<String>) -> ToolExecution {
    ToolExecution {
        exit_code: 3,
        payload: json!({
            "error": "input_usage_error",
            "message": message.into(),
        }),
        pipeline: None,
    }
}

fn internal_error(message: impl Into<String>) -> ToolExecution {
    ToolExecution {
        exit_code: 1,
        payload: json!({
            "error": "internal_error",
            "message": message.into(),
        }),
        pipeline: None,
    }
}

fn parse_emit_pipeline(args: &Map<String, Value>) -> Result<bool, String> {
    parse_bool(args, &["emit_pipeline"], false, "emit_pipeline")
}

fn parse_bool(
    args: &Map<String, Value>,
    aliases: &[&str],
    default: bool,
    label: &str,
) -> Result<bool, String> {
    let value = find_alias(args, aliases, label)?;
    match value {
        None => Ok(default),
        Some(Value::Bool(flag)) => Ok(*flag),
        Some(_) => Err(format!("`{label}` must be a boolean")),
    }
}

fn parse_usize(
    args: &Map<String, Value>,
    aliases: &[&str],
    default: usize,
    label: &str,
) -> Result<usize, String> {
    let value = find_alias(args, aliases, label)?;
    match value {
        None => Ok(default),
        Some(Value::Number(number)) => number
            .as_u64()
            .map(|value| value as usize)
            .ok_or_else(|| format!("`{label}` must be a non-negative integer")),
        Some(_) => Err(format!("`{label}` must be a non-negative integer")),
    }
}

fn parse_optional_usize(
    args: &Map<String, Value>,
    aliases: &[&str],
    label: &str,
) -> Result<Option<usize>, String> {
    let value = find_alias(args, aliases, label)?;
    match value {
        None => Ok(None),
        Some(Value::Number(number)) => number
            .as_u64()
            .map(|value| value as usize)
            .map(Some)
            .ok_or_else(|| format!("`{label}` must be a non-negative integer")),
        Some(_) => Err(format!("`{label}` must be a non-negative integer")),
    }
}

fn parse_optional_u16(
    args: &Map<String, Value>,
    aliases: &[&str],
    label: &str,
) -> Result<Option<u16>, String> {
    let value = find_alias(args, aliases, label)?;
    match value {
        None => Ok(None),
        Some(Value::Number(number)) => number
            .as_u64()
            .and_then(|value| u16::try_from(value).ok())
            .map(Some)
            .ok_or_else(|| format!("`{label}` must be between 0 and 65535")),
        Some(Value::String(text)) => text
            .parse::<u16>()
            .map(Some)
            .map_err(|_| format!("`{label}` must be between 0 and 65535")),
        Some(_) => Err(format!("`{label}` must be a number or numeric string")),
    }
}

fn parse_required_string(
    args: &Map<String, Value>,
    aliases: &[&str],
    label: &str,
) -> Result<String, String> {
    parse_optional_string(args, aliases, label)?
        .ok_or_else(|| format!("missing required `{label}`"))
}

fn parse_optional_string(
    args: &Map<String, Value>,
    aliases: &[&str],
    label: &str,
) -> Result<Option<String>, String> {
    let value = find_alias(args, aliases, label)?;
    match value {
        None => Ok(None),
        Some(Value::String(text)) if text.trim().is_empty() => {
            Err(format!("`{label}` cannot be empty"))
        }
        Some(Value::String(text)) => Ok(Some(text.to_string())),
        Some(_) => Err(format!("`{label}` must be a string")),
    }
}

fn parse_optional_string_allow_empty(
    args: &Map<String, Value>,
    aliases: &[&str],
    label: &str,
) -> Result<Option<String>, String> {
    let value = find_alias(args, aliases, label)?;
    match value {
        None => Ok(None),
        Some(Value::String(text)) => Ok(Some(text.to_string())),
        Some(_) => Err(format!("`{label}` must be a string")),
    }
}

fn parse_optional_path(
    args: &Map<String, Value>,
    aliases: &[&str],
    label: &str,
) -> Result<Option<PathBuf>, String> {
    Ok(parse_optional_string(args, aliases, label)?.map(PathBuf::from))
}

fn parse_optional_format(
    args: &Map<String, Value>,
    aliases: &[&str],
    label: &str,
) -> Result<Option<Format>, String> {
    let raw = parse_optional_string(args, aliases, label)?;
    raw.map(|value| {
        Format::from_str(value.as_str())
            .map_err(|error| format!("invalid format for `{label}`: {error}"))
    })
    .transpose()
}

fn parse_optional_normalize_mode(
    args: &Map<String, Value>,
) -> Result<Option<AssertInputNormalizeMode>, String> {
    let raw = parse_optional_string(args, &["normalize"], "normalize")?;
    raw.map(|value| match value.as_str() {
        "github-actions-jobs" => Ok(AssertInputNormalizeMode::GithubActionsJobs),
        "gitlab-ci-jobs" => Ok(AssertInputNormalizeMode::GitlabCiJobs),
        _ => Err("`normalize` must be `github-actions-jobs` or `gitlab-ci-jobs`".to_string()),
    })
    .transpose()
}

fn parse_required_ingest_yaml_jobs_mode(
    args: &Map<String, Value>,
) -> Result<IngestYamlJobsMode, String> {
    let raw = parse_required_string(args, &["mode"], "mode")?;
    match raw.as_str() {
        "github-actions" => Ok(IngestYamlJobsMode::GithubActions),
        "gitlab-ci" => Ok(IngestYamlJobsMode::GitlabCi),
        "generic-map" => Ok(IngestYamlJobsMode::GenericMap),
        _ => Err("`mode` must be `github-actions`, `gitlab-ci`, or `generic-map`".to_string()),
    }
}

fn parse_optional_doctor_profile(
    args: &Map<String, Value>,
) -> Result<Option<doctor::DoctorProfile>, String> {
    let raw = parse_optional_string(args, &["profile"], "profile")?;
    raw.map(|value| doctor::DoctorProfile::from_str(value.as_str()))
        .transpose()
}

fn parse_string_list(
    args: &Map<String, Value>,
    aliases: &[&str],
    label: &str,
) -> Result<Vec<String>, String> {
    let value = find_alias(args, aliases, label)?;
    match value {
        None => Ok(Vec::new()),
        Some(Value::String(text)) => Ok(vec![text.to_string()]),
        Some(Value::Array(items)) => items
            .iter()
            .map(|item| {
                item.as_str()
                    .map(ToOwned::to_owned)
                    .ok_or_else(|| format!("`{label}` array must contain only strings"))
            })
            .collect(),
        Some(_) => Err(format!("`{label}` must be a string or array<string>")),
    }
}

fn parse_optional_string_array(
    args: &Map<String, Value>,
    aliases: &[&str],
    label: &str,
) -> Result<Option<Vec<String>>, String> {
    let value = find_alias(args, aliases, label)?;
    match value {
        None => Ok(None),
        Some(Value::Array(items)) => items
            .iter()
            .map(|item| {
                item.as_str()
                    .map(ToOwned::to_owned)
                    .ok_or_else(|| format!("`{label}` array must contain only strings"))
            })
            .collect::<Result<Vec<_>, _>>()
            .map(Some),
        Some(_) => Err(format!("`{label}` must be an array<string>")),
    }
}

fn parse_path_list(
    args: &Map<String, Value>,
    aliases: &[&str],
    label: &str,
) -> Result<Vec<PathBuf>, String> {
    parse_string_list(args, aliases, label)
        .map(|items| items.into_iter().map(PathBuf::from).collect())
}

fn parse_value_list(
    args: &Map<String, Value>,
    aliases: &[&str],
    label: &str,
) -> Result<Vec<Value>, String> {
    let value = find_alias(args, aliases, label)?;
    match value {
        None => Ok(Vec::new()),
        Some(Value::Array(items)) => Ok(items.clone()),
        Some(single) => Ok(vec![single.clone()]),
    }
}

fn parse_inline_value(
    args: &Map<String, Value>,
    aliases: &[&str],
    label: &str,
) -> Result<Option<Value>, String> {
    Ok(find_alias(args, aliases, label)?.cloned())
}

fn parse_optional_json_body(
    args: &Map<String, Value>,
    aliases: &[&str],
    label: &str,
) -> Result<Option<String>, String> {
    let value = find_alias(args, aliases, label)?;
    match value {
        None => Ok(None),
        Some(Value::String(text)) => Ok(Some(text.to_string())),
        Some(other) => serde_json::to_string(other)
            .map(Some)
            .map_err(|error| format!("failed to serialize `{label}`: {error}")),
    }
}

fn find_alias<'a>(
    args: &'a Map<String, Value>,
    aliases: &[&str],
    label: &str,
) -> Result<Option<&'a Value>, String> {
    let mut found: Option<&Value> = None;
    let mut found_name: Option<&str> = None;
    for alias in aliases {
        if let Some(value) = args.get(*alias) {
            if found.is_some() {
                return Err(format!(
                    "multiple aliases provided for `{label}` (`{}` and `{alias}`)",
                    found_name.unwrap_or(label)
                ));
            }
            found = Some(value);
            found_name = Some(alias);
        }
    }
    Ok(found)
}

fn parse_value_input(
    args: &Map<String, Value>,
    path_aliases: &[&str],
    inline_aliases: &[&str],
    label: &str,
    required: bool,
) -> Result<Option<ValueInputSource>, String> {
    let path = parse_optional_path(args, path_aliases, label)?;
    let inline = parse_inline_value(args, inline_aliases, label)?;

    if path.is_some() && inline.is_some() {
        return Err(format!(
            "`{label}` path and inline forms are mutually exclusive"
        ));
    }

    match (path, inline) {
        (Some(path), None) => Ok(Some(ValueInputSource::Path(path))),
        (None, Some(value)) => Ok(Some(ValueInputSource::Inline(value_to_rows(value)))),
        (None, None) if required => Err(format!("missing required `{label}`")),
        (None, None) => Ok(None),
        (Some(_), Some(_)) => unreachable!(),
    }
}

fn parse_document_input(
    args: &Map<String, Value>,
    path_aliases: &[&str],
    inline_aliases: &[&str],
    label: &str,
) -> Result<Option<DocumentInputSource>, String> {
    let path = parse_optional_path(args, path_aliases, label)?;
    let inline = parse_inline_value(args, inline_aliases, label)?;

    if path.is_some() && inline.is_some() {
        return Err(format!(
            "`{label}` path and inline forms are mutually exclusive"
        ));
    }

    Ok(match (path, inline) {
        (Some(path), None) => Some(DocumentInputSource::Path(path)),
        (None, Some(value)) => Some(DocumentInputSource::Inline(value)),
        (None, None) => None,
        (Some(_), Some(_)) => unreachable!(),
    })
}

fn read_values_from_source(
    source: &ValueInputSource,
    label: &str,
    explicit_format: Option<Format>,
) -> Result<LoadedValues, String> {
    match source {
        ValueInputSource::Path(path) => {
            let format = io::resolve_input_format(explicit_format, Some(path.as_path())).map_err(
                |error| {
                    format!(
                        "failed to resolve format for `{label}` from `{}`: {error}",
                        path.display()
                    )
                },
            )?;
            let file = File::open(path).map_err(|error| {
                format!(
                    "failed to open `{label}` file `{}`: {error}",
                    path.display()
                )
            })?;
            let values = io::reader::read_values(file, format)
                .map_err(|error| format!("failed to read `{label}` input: {error}"))?;
            Ok(LoadedValues {
                values,
                format: Some(format),
            })
        }
        ValueInputSource::Inline(values) => Ok(LoadedValues {
            values: values.clone(),
            format: Some(Format::Json),
        }),
    }
}

fn read_document_from_source(source: &DocumentInputSource, label: &str) -> Result<Value, String> {
    match source {
        DocumentInputSource::Path(path) => {
            let format = io::resolve_input_format(None, Some(path.as_path())).map_err(|error| {
                format!(
                    "failed to resolve format for `{label}` from `{}`: {error}",
                    path.display()
                )
            })?;
            let file = File::open(path).map_err(|error| {
                format!(
                    "failed to open `{label}` file `{}`: {error}",
                    path.display()
                )
            })?;
            let values = io::reader::read_values(file, format)
                .map_err(|error| format!("failed to read `{label}` input: {error}"))?;
            Ok(values_to_payload(values))
        }
        DocumentInputSource::Inline(value) => Ok(value.clone()),
    }
}

fn values_to_payload(values: Vec<Value>) -> Value {
    match values.as_slice() {
        [single] => single.clone(),
        _ => Value::Array(values),
    }
}

fn value_to_rows(value: Value) -> Vec<Value> {
    match value {
        Value::Array(values) => values,
        other => vec![other],
    }
}

fn serialize_values_as_json_input(values: &[Value]) -> Vec<u8> {
    let payload = values_to_payload(values.to_vec());
    serde_json::to_vec(&payload).unwrap_or_default()
}

fn to_join_input(source: ValueInputSource) -> join::JoinCommandInput {
    match source {
        ValueInputSource::Path(path) => join::JoinCommandInput::Path(path),
        ValueInputSource::Inline(values) => join::JoinCommandInput::Inline(values),
    }
}

fn to_ingest_yaml_jobs_input(source: ValueInputSource) -> ingest_yaml_jobs::IngestYamlJobsInput {
    match source {
        ValueInputSource::Path(path) if ingest_yaml_jobs::path_is_stdin(path.as_path()) => {
            ingest_yaml_jobs::IngestYamlJobsInput::Stdin
        }
        ValueInputSource::Path(path) => ingest_yaml_jobs::IngestYamlJobsInput::Path(path),
        ValueInputSource::Inline(values) => ingest_yaml_jobs::IngestYamlJobsInput::Inline(values),
    }
}

fn is_stdin_input_path_sentinel(path: &Path) -> bool {
    ingest_yaml_jobs::path_is_stdin(path) || path == Path::new("/dev/stdin")
}

fn to_aggregate_input(source: ValueInputSource) -> aggregate::AggregateCommandInput {
    match source {
        ValueInputSource::Path(path) => aggregate::AggregateCommandInput::Path(path),
        ValueInputSource::Inline(values) => aggregate::AggregateCommandInput::Inline(values),
    }
}

fn to_transform_rowset_input(source: ValueInputSource) -> transform::TransformRowsetCommandInput {
    match source {
        ValueInputSource::Path(path) => transform::TransformRowsetCommandInput::Path(path),
        ValueInputSource::Inline(values) => transform::TransformRowsetCommandInput::Inline(values),
    }
}

fn to_merge_input(source: DocumentInputSource) -> merge::MergeCommandInput {
    match source {
        DocumentInputSource::Path(path) => merge::MergeCommandInput::Path(path),
        DocumentInputSource::Inline(value) => merge::MergeCommandInput::Inline(value),
    }
}

fn source_format(source: &ValueInputSource) -> Option<Format> {
    match source {
        ValueInputSource::Path(path) => io::resolve_input_format(None, Some(path.as_path())).ok(),
        ValueInputSource::Inline(_) => Some(Format::Json),
    }
}

fn pipeline_source(
    label: &str,
    source: &ValueInputSource,
    format: Option<Format>,
) -> PipelineInputSource {
    match source {
        ValueInputSource::Path(path) => PipelineInputSource::path(
            label,
            path.display().to_string(),
            format.map(Format::as_str),
        ),
        ValueInputSource::Inline(_) => inline_source(label, format),
    }
}

fn document_pipeline_source(label: &str, source: &DocumentInputSource) -> PipelineInputSource {
    match source {
        DocumentInputSource::Path(path) => PipelineInputSource::path(
            label,
            path.display().to_string(),
            io::resolve_input_format(None, Some(path.as_path()))
                .ok()
                .map(Format::as_str),
        ),
        DocumentInputSource::Inline(_) => inline_source(label, Some(Format::Json)),
    }
}

fn inline_source(label: &str, format: Option<Format>) -> PipelineInputSource {
    PipelineInputSource {
        label: label.to_string(),
        source: "inline".to_string(),
        path: None,
        format: format.map(|value| value.as_str().to_string()),
    }
}

fn assert_pipeline_sources(
    input: &ValueInputSource,
    input_format: Option<Format>,
    rules_source: Option<&DocumentInputSource>,
    schema_source: Option<&DocumentInputSource>,
) -> Vec<PipelineInputSource> {
    let mut sources = Vec::new();
    if let Some(rules) = rules_source {
        sources.push(document_pipeline_source("rules", rules));
    }
    if let Some(schema) = schema_source {
        sources.push(document_pipeline_source("schema", schema));
    }
    sources.push(pipeline_source("input", input, input_format));
    sources
}

fn assert_pipeline_sources_for_paths(
    input_path: Option<&Path>,
    input_format: Option<Format>,
    rules_path: Option<&Path>,
    schema_path: Option<&Path>,
) -> Vec<PipelineInputSource> {
    let mut sources = Vec::new();
    if let Some(path) = rules_path {
        sources.push(PipelineInputSource::path(
            "rules",
            path.display().to_string(),
            io::resolve_input_format(None, Some(path))
                .ok()
                .map(Format::as_str),
        ));
    }
    if let Some(path) = schema_path {
        sources.push(PipelineInputSource::path(
            "schema",
            path.display().to_string(),
            io::resolve_input_format(None, Some(path))
                .ok()
                .map(Format::as_str),
        ));
    }
    if let Some(path) = input_path {
        sources.push(PipelineInputSource::path(
            "input",
            path.display().to_string(),
            input_format.map(Format::as_str),
        ));
    } else {
        sources.push(inline_source("input", input_format));
    }
    sources
}

fn pipeline_as_value(report: PipelineReport) -> Result<Value, String> {
    serde_json::to_value(report)
        .map_err(|error| format!("failed to serialize pipeline report: {error}"))
}

fn parse_ingest_doc_format(value: &str) -> Result<IngestDocInputFormat, String> {
    match value {
        "md" => Ok(IngestDocInputFormat::Md),
        "html" => Ok(IngestDocInputFormat::Html),
        "docx" => Ok(IngestDocInputFormat::Docx),
        "rst" => Ok(IngestDocInputFormat::Rst),
        "latex" => Ok(IngestDocInputFormat::Latex),
        _ => Err("`from` must be one of `md`, `html`, `docx`, `rst`, `latex`".to_string()),
    }
}

fn contract_command_from_str(value: &str) -> Result<contract::ContractCommand, String> {
    match value {
        "canon" => Ok(contract::ContractCommand::Canon),
        "ingest-api" => Ok(contract::ContractCommand::IngestApi),
        "ingest" => Ok(contract::ContractCommand::Ingest),
        "assert" => Ok(contract::ContractCommand::Assert),
        "gate-schema" => Ok(contract::ContractCommand::GateSchema),
        "gate" | "gate-policy" => Ok(contract::ContractCommand::Gate),
        "sdiff" => Ok(contract::ContractCommand::Sdiff),
        "diff-source" => Ok(contract::ContractCommand::DiffSource),
        "profile" => Ok(contract::ContractCommand::Profile),
        "ingest-doc" => Ok(contract::ContractCommand::IngestDoc),
        "ingest-notes" => Ok(contract::ContractCommand::IngestNotes),
        "ingest-book" => Ok(contract::ContractCommand::IngestBook),
        "scan" => Ok(contract::ContractCommand::Scan),
        "transform-rowset" | "transform.rowset" => Ok(contract::ContractCommand::TransformRowset),
        "merge" => Ok(contract::ContractCommand::Merge),
        "doctor" => Ok(contract::ContractCommand::Doctor),
        "recipe" | "recipe-run" => Ok(contract::ContractCommand::RecipeRun),
        "recipe-lock" => Ok(contract::ContractCommand::RecipeLock),
        _ => Err(format!("unsupported contract command `{value}`")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    use tempfile::tempdir;

    fn args(value: Value) -> Map<String, Value> {
        value.as_object().expect("args object").clone()
    }

    #[test]
    fn parse_request_validates_json_rpc_shape() {
        let mut output = Vec::new();
        let code = run_single_request(Cursor::new(b"{"), &mut output);
        assert_eq!(code, 0);
        let response: Value = serde_json::from_slice(&output).expect("json response");
        assert_eq!(response["error"]["code"], Value::from(JSONRPC_PARSE_ERROR));

        let invalid = parse_request_value(json!([])).expect_err("array is invalid request");
        assert_eq!(invalid.0, Value::Null);
        assert_eq!(invalid.1, JSONRPC_INVALID_REQUEST);
        assert_eq!(invalid.2, "request must be a JSON object");

        let unknown_field = parse_request_value(json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/list",
            "params": {},
            "extra": true
        }))
        .expect_err("unexpected field is invalid");
        assert_eq!(unknown_field.0, Value::from(1));
        assert_eq!(unknown_field.1, JSONRPC_INVALID_REQUEST);
        assert!(unknown_field.2.contains("unexpected request field"));

        let invalid_id = parse_request_value(json!({
            "jsonrpc": "2.0",
            "id": {"x": 1},
            "method": "tools/list"
        }))
        .expect_err("object id is invalid");
        assert_eq!(invalid_id.0, Value::Null);
        assert_eq!(invalid_id.1, JSONRPC_INVALID_REQUEST);
        assert_eq!(invalid_id.2, "`id` must be null, string, or number");

        let invalid_params = parse_request_value(json!({
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/list",
            "params": []
        }))
        .expect_err("params must be object");
        assert_eq!(invalid_params.0, Value::from(2));
        assert_eq!(invalid_params.1, JSONRPC_INVALID_REQUEST);
        assert_eq!(invalid_params.2, "`params` must be an object");

        let request = parse_request_value(json!({
            "jsonrpc": "2.0",
            "id": "abc",
            "method": "tools/list"
        }))
        .expect("valid request");
        assert_eq!(request.id, Value::from("abc"));
        assert_eq!(request.method, "tools/list");
        assert!(request.params.is_empty());
    }

    #[test]
    fn parse_alias_and_scalar_helpers_cover_success_and_errors() {
        let conflict = args(json!({"a": true, "b": false}));
        let err = parse_bool(&conflict, &["a", "b"], false, "flag").expect_err("alias conflict");
        assert!(err.contains("multiple aliases provided"));

        let none = args(json!({}));
        assert!(!parse_emit_pipeline(&none).expect("default emit_pipeline"));

        let bad_bool = args(json!({"emit_pipeline": "yes"}));
        assert_eq!(
            parse_emit_pipeline(&bad_bool).expect_err("invalid boolean"),
            "`emit_pipeline` must be a boolean"
        );

        let ints = args(json!({"n": 7, "u16n": 42, "u16s": "43"}));
        assert_eq!(parse_usize(&ints, &["n"], 0, "n").expect("usize"), 7);
        assert_eq!(
            parse_optional_u16(&ints, &["u16n"], "u16n").expect("u16 number"),
            Some(42)
        );
        assert_eq!(
            parse_optional_u16(&ints, &["u16s"], "u16s").expect("u16 string"),
            Some(43)
        );

        let bad_ints = args(json!({"n": -1, "u16n": 70000, "u16s": "x", "u16b": true}));
        assert!(parse_usize(&bad_ints, &["n"], 0, "n").is_err());
        assert!(parse_optional_u16(&bad_ints, &["u16n"], "u16n").is_err());
        assert!(parse_optional_u16(&bad_ints, &["u16s"], "u16s").is_err());
        assert!(parse_optional_u16(&bad_ints, &["u16b"], "u16b").is_err());

        let defaults = args(json!({}));
        assert_eq!(
            parse_optional_usize(&defaults, &["missing"], "missing").expect("optional usize"),
            None
        );
        assert_eq!(
            parse_optional_u16(&defaults, &["missing"], "missing").expect("optional u16"),
            None
        );
    }

    #[test]
    fn parse_string_and_list_helpers_cover_success_and_errors() {
        let values = args(json!({
            "name": "value",
            "empty": "",
            "arr": ["x", "y"],
            "scalar": "z",
            "json_body": {"k": "v"}
        }));

        assert_eq!(
            parse_required_string(&values, &["name"], "name").expect("required string"),
            "value"
        );
        assert_eq!(
            parse_optional_string_allow_empty(&values, &["empty"], "empty").expect("allow empty"),
            Some(String::new())
        );
        assert_eq!(
            parse_string_list(&values, &["arr"], "arr").expect("string list"),
            vec!["x".to_string(), "y".to_string()]
        );
        assert_eq!(
            parse_string_list(&values, &["scalar"], "scalar").expect("scalar list"),
            vec!["z".to_string()]
        );
        assert_eq!(
            parse_optional_string_array(&values, &["arr"], "arr").expect("string array"),
            Some(vec!["x".to_string(), "y".to_string()])
        );
        assert_eq!(
            parse_optional_json_body(&values, &["json_body"], "json_body").expect("json body"),
            Some("{\"k\":\"v\"}".to_string())
        );

        let invalid = args(json!({
            "name": true,
            "empty": "",
            "arr": ["x", 1],
            "array_only": "x",
            "format": "bogus",
            "normalize": "bogus",
            "mode": "bogus",
            "profile": "bogus",
            "path": "in.bin",
        }));
        assert!(parse_optional_string(&invalid, &["name"], "name").is_err());
        assert!(parse_optional_string(&invalid, &["empty"], "empty").is_err());
        assert!(parse_string_list(&invalid, &["arr"], "arr").is_err());
        assert!(parse_optional_string_array(&invalid, &["array_only"], "array_only").is_err());
        assert!(parse_optional_format(&invalid, &["format"], "format").is_err());
        assert!(parse_optional_normalize_mode(&invalid).is_err());
        assert!(parse_required_ingest_yaml_jobs_mode(&invalid).is_err());
        assert!(parse_optional_doctor_profile(&invalid).is_err());
        assert_eq!(
            parse_optional_path(&invalid, &["path"], "path").expect("path"),
            Some(PathBuf::from("in.bin"))
        );

        let normalize = args(json!({"normalize": "github-actions-jobs"}));
        assert_eq!(
            parse_optional_normalize_mode(&normalize).expect("normalize mode"),
            Some(AssertInputNormalizeMode::GithubActionsJobs)
        );
    }

    #[test]
    fn parse_input_and_document_sources_cover_variants() {
        let inline_rows = args(json!({"input": [{"id": 1}, {"id": 2}]}));
        let source = parse_value_input(&inline_rows, &["input_path"], &["input"], "input", true)
            .expect("value input")
            .expect("some source");
        match source {
            ValueInputSource::Inline(rows) => {
                assert_eq!(rows.len(), 2);
            }
            ValueInputSource::Path(path) => panic!("expected inline rows, got {}", path.display()),
        }

        let scalar_inline = args(json!({"input": {"id": 1}}));
        let source = parse_value_input(&scalar_inline, &["input_path"], &["input"], "input", true)
            .expect("value input")
            .expect("some source");
        match source {
            ValueInputSource::Inline(rows) => {
                assert_eq!(rows, vec![json!({"id": 1})]);
            }
            ValueInputSource::Path(path) => {
                panic!("expected inline scalar, got {}", path.display())
            }
        }

        let path_only = args(json!({"input_path": "values.json"}));
        let source = parse_value_input(&path_only, &["input_path"], &["input"], "input", true)
            .expect("value input")
            .expect("some source");
        match source {
            ValueInputSource::Path(path) => assert_eq!(path, PathBuf::from("values.json")),
            ValueInputSource::Inline(_) => panic!("expected path source"),
        }

        let missing = args(json!({}));
        assert!(parse_value_input(&missing, &["p"], &["i"], "input", true).is_err());

        let conflict = args(json!({"input_path": "values.json", "input": []}));
        assert!(parse_value_input(&conflict, &["input_path"], &["input"], "input", true).is_err());

        let doc_inline = args(json!({"schema": {"type": "object"}}));
        let source = parse_document_input(&doc_inline, &["schema_path"], &["schema"], "schema")
            .expect("document input")
            .expect("document source");
        match source {
            DocumentInputSource::Inline(value) => assert_eq!(value["type"], Value::from("object")),
            DocumentInputSource::Path(path) => {
                panic!("expected inline schema, got {}", path.display())
            }
        }

        let doc_path = args(json!({"schema_path": "schema.json"}));
        let source = parse_document_input(&doc_path, &["schema_path"], &["schema"], "schema")
            .expect("document input")
            .expect("document source");
        match source {
            DocumentInputSource::Path(path) => assert_eq!(path, PathBuf::from("schema.json")),
            DocumentInputSource::Inline(_) => panic!("expected path schema"),
        }
    }

    #[test]
    fn io_and_payload_helpers_cover_source_transforms() {
        let dir = tempdir().expect("tempdir");
        let values_path = dir.path().join("values.json");
        let doc_path = dir.path().join("doc.json");
        let unknown_path = dir.path().join("unknown.bin");

        fs::write(&values_path, b"[{\"id\":1},{\"id\":2}]").expect("write values");
        fs::write(&doc_path, b"{\"k\":\"v\"}").expect("write document");
        fs::write(&unknown_path, b"[]").expect("write unknown");

        let inline_loaded = read_values_from_source(
            &ValueInputSource::Inline(vec![json!({"id": 1})]),
            "input",
            None,
        )
        .expect("inline values");
        assert_eq!(inline_loaded.values, vec![json!({"id": 1})]);
        assert_eq!(inline_loaded.format, Some(Format::Json));

        let path_loaded =
            read_values_from_source(&ValueInputSource::Path(values_path.clone()), "input", None)
                .expect("path values");
        assert_eq!(path_loaded.values.len(), 2);
        assert_eq!(path_loaded.format, Some(Format::Json));

        let unknown_error =
            read_values_from_source(&ValueInputSource::Path(unknown_path.clone()), "input", None)
                .expect_err("unknown extension should fail");
        assert!(unknown_error.contains("failed to resolve format"));

        let document =
            read_document_from_source(&DocumentInputSource::Path(doc_path.clone()), "schema")
                .expect("read document");
        assert_eq!(document, json!({"k":"v"}));

        let inline_doc =
            read_document_from_source(&DocumentInputSource::Inline(json!([1, 2])), "schema")
                .expect("inline document");
        assert_eq!(inline_doc, json!([1, 2]));

        let payload = values_to_payload(vec![json!({"id": 1}), json!({"id": 2})]);
        assert_eq!(payload, json!([{"id": 1}, {"id": 2}]));
        let single_payload = values_to_payload(vec![json!({"id": 1})]);
        assert_eq!(single_payload, json!({"id": 1}));

        assert_eq!(value_to_rows(json!([1, 2])), vec![json!(1), json!(2)]);
        assert_eq!(value_to_rows(json!({"k": "v"})), vec![json!({"k":"v"})]);

        assert_eq!(
            serde_json::from_slice::<Value>(&serialize_values_as_json_input(&[json!({"id": 1})]))
                .expect("serialized json"),
            json!({"id": 1})
        );
    }

    #[test]
    fn pipeline_and_conversion_helpers_cover_all_variants() {
        let path_source = ValueInputSource::Path(PathBuf::from("rows.json"));
        let inline_values = ValueInputSource::Inline(vec![json!({"id": 1})]);
        let inline_document = DocumentInputSource::Inline(json!({"base": true}));
        let path_document = DocumentInputSource::Path(PathBuf::from("base.json"));

        match to_join_input(path_source.clone()) {
            join::JoinCommandInput::Path(path) => assert_eq!(path, PathBuf::from("rows.json")),
            join::JoinCommandInput::Inline(_) => panic!("expected path join input"),
        }
        match to_join_input(inline_values.clone()) {
            join::JoinCommandInput::Inline(values) => assert_eq!(values, vec![json!({"id": 1})]),
            join::JoinCommandInput::Path(path) => {
                panic!("expected inline join input, got {}", path.display())
            }
        }

        match to_ingest_yaml_jobs_input(ValueInputSource::Path(PathBuf::from("-"))) {
            ingest_yaml_jobs::IngestYamlJobsInput::Stdin => {}
            _ => panic!("expected stdin ingest_yaml_jobs input"),
        }
        match to_ingest_yaml_jobs_input(ValueInputSource::Path(PathBuf::from("jobs.yml"))) {
            ingest_yaml_jobs::IngestYamlJobsInput::Path(path) => {
                assert_eq!(path, PathBuf::from("jobs.yml"))
            }
            _ => panic!("expected path ingest_yaml_jobs input"),
        }
        match to_ingest_yaml_jobs_input(inline_values.clone()) {
            ingest_yaml_jobs::IngestYamlJobsInput::Inline(values) => {
                assert_eq!(values, vec![json!({"id": 1})])
            }
            _ => panic!("expected inline ingest_yaml_jobs input"),
        }

        assert!(is_stdin_input_path_sentinel(Path::new("-")));
        assert!(is_stdin_input_path_sentinel(Path::new("/dev/stdin")));
        assert!(!is_stdin_input_path_sentinel(Path::new("rows.json")));

        match to_aggregate_input(path_source.clone()) {
            aggregate::AggregateCommandInput::Path(path) => {
                assert_eq!(path, PathBuf::from("rows.json"))
            }
            _ => panic!("expected path aggregate input"),
        }
        match to_transform_rowset_input(inline_values.clone()) {
            transform::TransformRowsetCommandInput::Inline(values) => {
                assert_eq!(values, vec![json!({"id": 1})])
            }
            _ => panic!("expected inline transform input"),
        }
        match to_merge_input(path_document.clone()) {
            merge::MergeCommandInput::Path(path) => assert_eq!(path, PathBuf::from("base.json")),
            _ => panic!("expected path merge input"),
        }
        match to_merge_input(inline_document.clone()) {
            merge::MergeCommandInput::Inline(value) => assert_eq!(value, json!({"base": true})),
            _ => panic!("expected inline merge input"),
        }

        assert_eq!(source_format(&inline_values), Some(Format::Json));
        assert_eq!(source_format(&path_source), Some(Format::Json));

        let inline_pipeline = pipeline_source("input", &inline_values, Some(Format::Json));
        assert_eq!(inline_pipeline.source, "inline");
        let path_pipeline = pipeline_source("input", &path_source, Some(Format::Json));
        assert_eq!(path_pipeline.source, "path");
        assert_eq!(path_pipeline.path.as_deref(), Some("rows.json"));

        let document_inline_pipeline = document_pipeline_source("base", &inline_document);
        assert_eq!(document_inline_pipeline.source, "inline");
        let document_path_pipeline = document_pipeline_source("base", &path_document);
        assert_eq!(document_path_pipeline.source, "path");

        let sources = assert_pipeline_sources(
            &inline_values,
            Some(Format::Json),
            Some(&path_document),
            Some(&inline_document),
        );
        assert_eq!(sources.len(), 3);
        assert_eq!(sources[0].label, "rules");
        assert_eq!(sources[1].label, "schema");
        assert_eq!(sources[2].label, "input");

        let path_sources = assert_pipeline_sources_for_paths(
            Some(Path::new("input.json")),
            Some(Format::Json),
            Some(Path::new("rules.json")),
            None,
        );
        assert_eq!(path_sources.len(), 2);
        assert_eq!(path_sources[0].source, "path");
        assert_eq!(path_sources[1].path.as_deref(), Some("input.json"));

        let inline_sources =
            assert_pipeline_sources_for_paths(None, Some(Format::Json), None, None);
        assert_eq!(inline_sources.len(), 1);
        assert_eq!(inline_sources[0].source, "inline");

        let pipeline = PipelineReport::new(
            "assert",
            PipelineInput::new(Vec::new()),
            vec!["step".to_string()],
            vec!["guard".to_string()],
        );
        assert!(
            pipeline_as_value(pipeline)
                .expect("pipeline value")
                .is_object()
        );
    }

    #[test]
    fn protocol_and_parser_helpers_cover_commands_and_formats() {
        assert_eq!(
            parse_ingest_doc_format("md").expect("md"),
            IngestDocInputFormat::Md
        );
        assert_eq!(
            parse_ingest_doc_format("html").expect("html"),
            IngestDocInputFormat::Html
        );
        assert_eq!(
            parse_ingest_doc_format("docx").expect("docx"),
            IngestDocInputFormat::Docx
        );
        assert_eq!(
            parse_ingest_doc_format("rst").expect("rst"),
            IngestDocInputFormat::Rst
        );
        assert_eq!(
            parse_ingest_doc_format("latex").expect("latex"),
            IngestDocInputFormat::Latex
        );
        assert!(parse_ingest_doc_format("txt").is_err());

        assert_eq!(
            contract_command_from_str("gate-policy").expect("gate-policy alias"),
            contract::ContractCommand::Gate
        );
        assert_eq!(
            contract_command_from_str("transform.rowset").expect("transform alias"),
            contract::ContractCommand::TransformRowset
        );
        assert!(contract_command_from_str("unknown").is_err());

        let tools = tools_list_result();
        let tool_names: Vec<&str> = tools["tools"]
            .as_array()
            .expect("tools array")
            .iter()
            .map(|entry| entry["name"].as_str().expect("tool name"))
            .collect();
        assert_eq!(tool_names, TOOL_ORDER);

        let doctor_definition = tool_definition("dataq.doctor");
        assert!(doctor_definition["inputSchema"]["properties"]["capabilities"].is_object());
        assert!(doctor_definition["inputSchema"]["properties"]["profile"].is_object());

        let canon_definition = tool_definition("dataq.canon");
        assert!(canon_definition["inputSchema"]["properties"]["emit_pipeline"].is_object());
        assert!(canon_definition["inputSchema"]["properties"]["profile"].is_null());

        let execution = ToolExecution {
            exit_code: 2,
            payload: json!({"error":"x"}),
            pipeline: Some(json!({"command":"assert"})),
        };
        let result = tool_call_result(execution);
        assert_eq!(result["isError"], Value::Bool(true));
        assert!(result["structuredContent"]["pipeline"].is_object());
        assert!(
            result["content"][0]["text"]
                .as_str()
                .unwrap_or("")
                .contains("\"exit_code\":2")
        );

        let success = success_response(Value::from(1), json!({"ok": true}));
        assert_eq!(success["result"]["ok"], Value::Bool(true));
        let error = error_response(Value::from("id"), JSONRPC_METHOD_NOT_FOUND, "not found");
        assert_eq!(
            error["error"]["code"],
            Value::from(JSONRPC_METHOD_NOT_FOUND)
        );

        let invalid_id_object =
            serde_json::Map::from_iter(vec![("id".to_string(), json!({"x":1}))]);
        assert_eq!(extract_error_id(&invalid_id_object), Value::Null);
        let valid_id_object = serde_json::Map::from_iter(vec![("id".to_string(), json!("ok"))]);
        assert_eq!(extract_error_id(&valid_id_object), Value::from("ok"));

        let usage = input_usage_error("bad input");
        assert_eq!(usage.exit_code, 3);
        assert_eq!(usage.payload["error"], Value::from("input_usage_error"));
        let internal = internal_error("boom");
        assert_eq!(internal.exit_code, 1);
        assert_eq!(internal.payload["error"], Value::from("internal_error"));

        let request = JsonRpcRequest {
            id: Value::from(7),
            method: "tools/list".to_string(),
            params: Map::new(),
        };
        let handled = handle_request(request);
        assert!(handled["result"]["tools"].is_array());
    }

    #[test]
    fn execute_functions_validate_inputs_before_runtime_execution() {
        let exec = execute_canon(&args(json!({})));
        assert_eq!(exec.exit_code, 3);
        assert_eq!(exec.payload["error"], json!("input_usage_error"));

        let exec = execute_ingest_api(&args(json!({
            "url": "https://example.test",
            "method": "TRACE"
        })));
        assert_eq!(exec.exit_code, 3);
        assert!(
            exec.payload["message"]
                .as_str()
                .unwrap_or("")
                .contains("method")
        );

        let exec = execute_ingest_yaml_jobs(&args(json!({
            "mode": "generic-map",
            "input_path": "-"
        })));
        assert_eq!(exec.exit_code, 3);
        assert!(
            exec.payload["message"]
                .as_str()
                .unwrap_or("")
                .contains("stdin sentinels")
        );

        let exec = execute_assert(&args(json!({
            "input": [{"id": 1}]
        })));
        assert_eq!(exec.exit_code, 3);
        assert!(
            exec.payload["message"]
                .as_str()
                .unwrap_or("")
                .contains("exactly one")
        );

        let exec = execute_assert(&args(json!({
            "input": [{"id": 1}],
            "rules": {"required_keys": [], "forbid_keys": [], "fields": {}},
            "schema": {"type": "object"}
        })));
        assert_eq!(exec.exit_code, 3);
        assert!(
            exec.payload["message"]
                .as_str()
                .unwrap_or("")
                .contains("mutually exclusive")
        );

        let exec = execute_assert(&args(json!({
            "input": [{"id": 1}],
            "rules": {"required_keys": [], "forbid_keys": [], "fields": {}},
            "normalize": "github-actions-jobs"
        })));
        assert_eq!(exec.exit_code, 3);
        assert!(
            exec.payload["message"]
                .as_str()
                .unwrap_or("")
                .contains("only supported")
        );

        let exec = execute_gate_schema(&args(json!({
            "input": [{"id": 1}],
            "schema": {"type": "object"}
        })));
        assert_eq!(exec.exit_code, 3);
        assert!(
            exec.payload["message"]
                .as_str()
                .unwrap_or("")
                .contains("inline `schema`")
        );

        let exec = execute_gate_schema(&args(json!({
            "from": "github-actions-raw",
            "input": [{"id": 1}],
            "schema_path": "schema.json"
        })));
        assert_eq!(exec.exit_code, 3);
        assert_eq!(exec.payload["error"], json!("input_usage_error"));

        let exec = execute_gate_policy(&args(json!({
            "rules_path": "rules.json",
            "input_path": "-"
        })));
        assert_eq!(exec.exit_code, 3);
        assert!(
            exec.payload["message"]
                .as_str()
                .unwrap_or("")
                .contains("stdin sentinel")
        );

        let exec = execute_sdiff(&args(json!({
            "left": [{"id": 1}],
            "right": [{"id": 1}],
            "left_from": "invalid"
        })));
        assert_eq!(exec.exit_code, 3);
        assert!(
            exec.payload["message"]
                .as_str()
                .unwrap_or("")
                .contains("format")
        );

        let exec = execute_diff_source(&args(json!({
            "left": "left.json",
            "right": "right.json",
            "fail_on_diff": "yes"
        })));
        assert_eq!(exec.exit_code, 3);
        assert!(
            exec.payload["message"]
                .as_str()
                .unwrap_or("")
                .contains("boolean")
        );

        let exec = execute_profile(&args(json!({
            "input": [{"id": 1}],
            "from": "csv"
        })));
        assert_eq!(exec.exit_code, 3);
        assert!(
            exec.payload["message"]
                .as_str()
                .unwrap_or("")
                .contains("from=json")
        );

        let exec = execute_ingest_doc(&args(json!({
            "from": "txt",
            "input": "# title"
        })));
        assert_eq!(exec.exit_code, 3);
        assert!(
            exec.payload["message"]
                .as_str()
                .unwrap_or("")
                .contains("must be one of")
        );

        let exec = execute_ingest_doc(&args(json!({
            "from": "md"
        })));
        assert_eq!(exec.exit_code, 3);
        assert!(
            exec.payload["message"]
                .as_str()
                .unwrap_or("")
                .contains("missing required")
        );

        let exec = execute_ingest_doc(&args(json!({
            "from": "md",
            "input": "# title",
            "input_path": "doc.md"
        })));
        assert_eq!(exec.exit_code, 3);
        assert!(
            exec.payload["message"]
                .as_str()
                .unwrap_or("")
                .contains("mutually exclusive")
        );

        let exec = execute_ingest_doc(&args(json!({
            "from": "md",
            "input_path": "-"
        })));
        assert_eq!(exec.exit_code, 3);
        assert!(
            exec.payload["message"]
                .as_str()
                .unwrap_or("")
                .contains("not supported")
        );

        let exec = execute_ingest_notes(&args(json!({
            "to": "xml"
        })));
        assert_eq!(exec.exit_code, 3);
        assert!(
            exec.payload["message"]
                .as_str()
                .unwrap_or("")
                .contains("json")
        );

        let exec = execute_ingest_notes(&args(json!({
            "since": "2025-02-01T00:00:00Z",
            "until": "2025-01-01T00:00:00Z"
        })));
        assert_eq!(exec.exit_code, 3);
        assert!(
            exec.payload["message"]
                .as_str()
                .unwrap_or("")
                .contains("must be less than or equal")
        );

        let exec = execute_ingest_book(&args(json!({
            "root": ".",
            "include_files": "yes"
        })));
        assert_eq!(exec.exit_code, 3);
        assert!(
            exec.payload["message"]
                .as_str()
                .unwrap_or("")
                .contains("boolean")
        );

        let exec = execute_join(&args(json!({
            "left": [{"id": 1}],
            "right": [{"id": 1}],
            "on": "id",
            "how": "outer"
        })));
        assert_eq!(exec.exit_code, 3);
        assert!(
            exec.payload["message"]
                .as_str()
                .unwrap_or("")
                .contains("inner")
        );

        let exec = execute_aggregate(&args(json!({
            "input": [{"team": "a", "value": 1}],
            "group_by": "team",
            "target": "value",
            "metric": "median"
        })));
        assert_eq!(exec.exit_code, 3);
        assert!(
            exec.payload["message"]
                .as_str()
                .unwrap_or("")
                .contains("count")
        );

        let exec = execute_scan_text(&args(json!({
            "pattern": "x",
            "max_matches": "many"
        })));
        assert_eq!(exec.exit_code, 3);
        assert!(
            exec.payload["message"]
                .as_str()
                .unwrap_or("")
                .contains("non-negative integer")
        );

        let exec = execute_transform_rowset(&args(json!({
            "input": [{"id": 1}],
            "jq_filter": ".",
            "mlr": []
        })));
        assert_eq!(exec.exit_code, 3);
        assert!(
            exec.payload["message"]
                .as_str()
                .unwrap_or("")
                .contains("missing required")
        );

        let exec = execute_merge(&args(json!({
            "base": {"a": 1},
            "overlay_paths": ["a.json"],
            "overlays": [{"a": 2}]
        })));
        assert_eq!(exec.exit_code, 3);
        assert!(
            exec.payload["message"]
                .as_str()
                .unwrap_or("")
                .contains("mutually exclusive")
        );

        let exec = execute_merge(&args(json!({
            "base": {"a": 1}
        })));
        assert_eq!(exec.exit_code, 3);
        assert!(
            exec.payload["message"]
                .as_str()
                .unwrap_or("")
                .contains("at least one overlay")
        );

        let exec = execute_doctor(&args(json!({
            "capabilities": "yes"
        })));
        assert_eq!(exec.exit_code, 3);
        assert!(
            exec.payload["message"]
                .as_str()
                .unwrap_or("")
                .contains("boolean")
        );

        let exec = execute_contract(&args(json!({
            "command": "unknown-command"
        })));
        assert_eq!(exec.exit_code, 3);
        assert!(
            exec.payload["message"]
                .as_str()
                .unwrap_or("")
                .contains("unsupported contract command")
        );

        let exec = execute_emit_plan(&args(json!({})));
        assert_eq!(exec.exit_code, 3);
        assert!(
            exec.payload["message"]
                .as_str()
                .unwrap_or("")
                .contains("missing required")
        );

        let exec = execute_recipe_run(&args(json!({})));
        assert_eq!(exec.exit_code, 3);
        assert!(
            exec.payload["message"]
                .as_str()
                .unwrap_or("")
                .contains("must be provided")
        );

        let exec = execute_recipe_run(&args(json!({
            "file_path": "recipe.json",
            "recipe": {"version":"dataq.recipe.v1","steps":[]}
        })));
        assert_eq!(exec.exit_code, 3);
        assert!(
            exec.payload["message"]
                .as_str()
                .unwrap_or("")
                .contains("mutually exclusive")
        );

        let exec = execute_recipe_lock(&args(json!({})));
        assert_eq!(exec.exit_code, 3);
        assert!(
            exec.payload["message"]
                .as_str()
                .unwrap_or("")
                .contains("missing required")
        );

        let exec = execute_recipe_replay(&args(json!({
            "file_path": "recipe.json"
        })));
        assert_eq!(exec.exit_code, 3);
        assert!(
            exec.payload["message"]
                .as_str()
                .unwrap_or("")
                .contains("missing required")
        );

        let exec = execute_recipe_replay(&args(json!({
            "file_path": "recipe.json",
            "lock_path": "recipe.lock.json",
            "strict": "yes"
        })));
        assert_eq!(exec.exit_code, 3);
        assert!(
            exec.payload["message"]
                .as_str()
                .unwrap_or("")
                .contains("boolean")
        );
    }
}
