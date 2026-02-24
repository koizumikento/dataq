use std::fs::File;
use std::io::{Cursor, Read, Write};
use std::path::{Path, PathBuf};
use std::str::FromStr;

use serde_json::{Map, Value, json};

use crate::cmd::{
    aggregate,
    r#assert::{self as assert_cmd, AssertInputNormalizeMode},
    canon, contract, doctor, emit, gate, join, merge, profile, recipe, sdiff,
};
use crate::domain::report::{PipelineInput, PipelineInputSource, PipelineReport};
use crate::domain::rules::AssertRules;
use crate::engine::aggregate::AggregateMetric;
use crate::engine::r#assert as assert_engine;
use crate::engine::canon::{CanonOptions, canonicalize_values};
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
const TOOL_ORDER: [&str; 12] = [
    "dataq.canon",
    "dataq.assert",
    "dataq.gate.schema",
    "dataq.sdiff",
    "dataq.profile",
    "dataq.join",
    "dataq.aggregate",
    "dataq.merge",
    "dataq.doctor",
    "dataq.contract",
    "dataq.emit.plan",
    "dataq.recipe.run",
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
        "dataq.assert" => execute_assert(args),
        "dataq.gate.schema" => execute_gate_schema(args),
        "dataq.sdiff" => execute_sdiff(args),
        "dataq.profile" => execute_profile(args),
        "dataq.join" => execute_join(args),
        "dataq.aggregate" => execute_aggregate(args),
        "dataq.merge" => execute_merge(args),
        "dataq.doctor" => execute_doctor(args),
        "dataq.contract" => execute_contract(args),
        "dataq.emit.plan" => execute_emit_plan(args),
        "dataq.recipe.run" => execute_recipe_run(args),
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
        gate::pipeline_steps(),
        gate::deterministic_guards(),
    );
    for used_tool in &trace.used_tools {
        report = report.mark_external_tool_used(used_tool);
    }
    report = report.with_stage_diagnostics(trace.stage_diagnostics);
    execution.pipeline = pipeline_as_value(report).ok();
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
            recipe::deterministic_guards(),
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

fn to_aggregate_input(source: ValueInputSource) -> aggregate::AggregateCommandInput {
    match source {
        ValueInputSource::Path(path) => aggregate::AggregateCommandInput::Path(path),
        ValueInputSource::Inline(values) => aggregate::AggregateCommandInput::Inline(values),
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

fn contract_command_from_str(value: &str) -> Result<contract::ContractCommand, String> {
    match value {
        "canon" => Ok(contract::ContractCommand::Canon),
        "assert" => Ok(contract::ContractCommand::Assert),
        "gate-schema" => Ok(contract::ContractCommand::GateSchema),
        "sdiff" => Ok(contract::ContractCommand::Sdiff),
        "profile" => Ok(contract::ContractCommand::Profile),
        "merge" => Ok(contract::ContractCommand::Merge),
        "doctor" => Ok(contract::ContractCommand::Doctor),
        "recipe" => Ok(contract::ContractCommand::Recipe),
        _ => Err(format!("unsupported contract command `{value}`")),
    }
}
