use std::collections::BTreeMap;

use chrono::{DateTime, SecondsFormat, Utc};
use serde::Serialize;
use serde_json::{Value, json};

use crate::adapters::{jq, xh};
use crate::domain::report::PipelineStageDiagnostic;
use crate::util::time::normalize_rfc3339_utc;

/// Input arguments for `ingest api` command execution API.
#[derive(Debug, Clone)]
pub struct IngestApiCommandArgs {
    pub url: String,
    pub method: IngestApiMethod,
    pub headers: Vec<String>,
    pub body: Option<String>,
    pub expect_status: Option<u16>,
}

/// HTTP methods accepted by `ingest api`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IngestApiMethod {
    Get,
    Post,
    Put,
    Patch,
    Delete,
}

impl IngestApiMethod {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Get => "GET",
            Self::Post => "POST",
            Self::Put => "PUT",
            Self::Patch => "PATCH",
            Self::Delete => "DELETE",
        }
    }
}

impl From<IngestApiMethod> for xh::HttpMethod {
    fn from(value: IngestApiMethod) -> Self {
        match value {
            IngestApiMethod::Get => xh::HttpMethod::Get,
            IngestApiMethod::Post => xh::HttpMethod::Post,
            IngestApiMethod::Put => xh::HttpMethod::Put,
            IngestApiMethod::Patch => xh::HttpMethod::Patch,
            IngestApiMethod::Delete => xh::HttpMethod::Delete,
        }
    }
}

/// Structured command response that carries exit-code mapping and JSON payload.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct IngestApiCommandResponse {
    pub exit_code: i32,
    pub payload: Value,
}

/// Trace details used by `--emit-pipeline` for API ingest stages.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct IngestApiPipelineTrace {
    pub used_tools: Vec<String>,
    pub stage_diagnostics: Vec<PipelineStageDiagnostic>,
}

impl IngestApiPipelineTrace {
    fn mark_tool_used(&mut self, tool: &'static str) {
        if self.used_tools.iter().any(|used| used == tool) {
            return;
        }
        self.used_tools.push(tool.to_string());
    }
}

pub fn run(args: &IngestApiCommandArgs) -> IngestApiCommandResponse {
    run_with_trace(args).0
}

pub fn run_with_trace(
    args: &IngestApiCommandArgs,
) -> (IngestApiCommandResponse, IngestApiPipelineTrace) {
    match execute(args) {
        Ok((payload, exit_code, trace)) => (IngestApiCommandResponse { exit_code, payload }, trace),
        Err(error) => {
            let response = match error.kind {
                CommandErrorKind::InputUsage(message) => IngestApiCommandResponse {
                    exit_code: 3,
                    payload: json!({
                        "error": "input_usage_error",
                        "message": message,
                    }),
                },
                CommandErrorKind::Internal(message) => IngestApiCommandResponse {
                    exit_code: 1,
                    payload: json!({
                        "error": "internal_error",
                        "message": message,
                    }),
                },
            };
            (response, error.trace)
        }
    }
}

/// Ordered pipeline-step names used for `--emit-pipeline` diagnostics.
pub fn pipeline_steps() -> Vec<String> {
    vec![
        "ingest_api_xh_fetch".to_string(),
        "ingest_api_jq_normalize".to_string(),
    ]
}

/// Determinism guards applied by `ingest api`.
pub fn deterministic_guards() -> Vec<String> {
    vec![
        "explicit_ingest_header_allowlist".to_string(),
        "projected_headers_sorted_lexicographically".to_string(),
        "fetched_at_normalized_rfc3339_utc".to_string(),
        "no_shell_interpolation_for_user_input".to_string(),
    ]
}

#[derive(Debug)]
struct CommandError {
    kind: CommandErrorKind,
    trace: IngestApiPipelineTrace,
}

#[derive(Debug)]
enum CommandErrorKind {
    InputUsage(String),
    Internal(String),
}

fn execute(
    args: &IngestApiCommandArgs,
) -> Result<(Value, i32, IngestApiPipelineTrace), CommandError> {
    let mut trace = IngestApiPipelineTrace::default();

    validate_url(args.url.as_str()).map_err(|message| CommandError {
        kind: CommandErrorKind::InputUsage(message),
        trace: trace.clone(),
    })?;

    let parsed_headers = parse_headers(&args.headers).map_err(|message| CommandError {
        kind: CommandErrorKind::InputUsage(message),
        trace: trace.clone(),
    })?;
    let normalized_body = normalize_body(args.body.as_deref()).map_err(|message| CommandError {
        kind: CommandErrorKind::InputUsage(message),
        trace: trace.clone(),
    })?;

    trace.mark_tool_used("xh");
    let fetched = match xh::fetch(&xh::XhRequest {
        url: args.url.clone(),
        method: args.method.into(),
        headers: parsed_headers,
        body: normalized_body,
    }) {
        Ok(response) => {
            trace
                .stage_diagnostics
                .push(PipelineStageDiagnostic::success(
                    1,
                    "ingest_api_xh_fetch",
                    "xh",
                    1,
                    1,
                ));
            response
        }
        Err(error) => {
            trace
                .stage_diagnostics
                .push(PipelineStageDiagnostic::failure(
                    1,
                    "ingest_api_xh_fetch",
                    "xh",
                    1,
                ));
            return Err(CommandError {
                kind: map_xh_error(error),
                trace,
            });
        }
    };

    let raw_payload = json!({
        "source": {
            "kind": "api",
            "url": args.url,
            "method": args.method.as_str(),
        },
        "status": fetched.status,
        "headers": fetched.headers,
        "body": fetched.body,
        "fetched_at": derive_fetched_at(&fetched.headers),
    });

    trace.mark_tool_used("jq");
    let payload = match jq::normalize_ingest_api_response(&raw_payload) {
        Ok(payload) => {
            trace
                .stage_diagnostics
                .push(PipelineStageDiagnostic::success(
                    2,
                    "ingest_api_jq_normalize",
                    "jq",
                    1,
                    1,
                ));
            payload
        }
        Err(error) => {
            trace
                .stage_diagnostics
                .push(PipelineStageDiagnostic::failure(
                    2,
                    "ingest_api_jq_normalize",
                    "jq",
                    1,
                ));
            return Err(CommandError {
                kind: map_jq_error(error),
                trace,
            });
        }
    };

    let actual_status = payload
        .get("status")
        .and_then(Value::as_u64)
        .and_then(|value| u16::try_from(value).ok())
        .ok_or_else(|| CommandError {
            kind: CommandErrorKind::Internal(
                "normalized ingest payload did not include integer `status`".to_string(),
            ),
            trace: trace.clone(),
        })?;

    let exit_code = if let Some(expect_status) = args.expect_status {
        if actual_status == expect_status { 0 } else { 2 }
    } else {
        0
    };

    Ok((payload, exit_code, trace))
}

fn map_xh_error(error: xh::XhError) -> CommandErrorKind {
    match error {
        xh::XhError::Unavailable => {
            CommandErrorKind::InputUsage("ingest api requires `xh` in PATH".to_string())
        }
        xh::XhError::Execution(message) => {
            CommandErrorKind::InputUsage(format!("failed to fetch API response with xh: {message}"))
        }
        xh::XhError::Spawn(error) => {
            CommandErrorKind::Internal(format!("failed to execute xh: {error}"))
        }
        xh::XhError::OutputDecode(error) => {
            CommandErrorKind::InputUsage(format!("xh output is not valid UTF-8: {error}"))
        }
        xh::XhError::Parse(message) => {
            CommandErrorKind::InputUsage(format!("failed to parse xh response: {message}"))
        }
    }
}

fn map_jq_error(error: jq::JqError) -> CommandErrorKind {
    match error {
        jq::JqError::Unavailable => {
            CommandErrorKind::InputUsage("ingest api requires `jq` in PATH".to_string())
        }
        jq::JqError::Execution(message) => CommandErrorKind::Internal(format!(
            "failed to normalize API response with jq: {message}"
        )),
        jq::JqError::Spawn(error) => {
            CommandErrorKind::Internal(format!("failed to execute jq: {error}"))
        }
        jq::JqError::Stdin(error) => {
            CommandErrorKind::Internal(format!("failed to write jq stdin: {error}"))
        }
        jq::JqError::Parse(error) => {
            CommandErrorKind::Internal(format!("jq output is not valid JSON: {error}"))
        }
        jq::JqError::OutputShape | jq::JqError::OutputObjectShape => {
            CommandErrorKind::Internal("jq normalization output shape was invalid".to_string())
        }
        jq::JqError::Serialize(error) => {
            CommandErrorKind::Internal(format!("failed to serialize jq input: {error}"))
        }
    }
}

fn validate_url(url: &str) -> Result<(), String> {
    let trimmed = url.trim();
    if trimmed.is_empty() {
        return Err("`--url` cannot be empty".to_string());
    }
    let lower = trimmed.to_ascii_lowercase();
    let prefix_len = if lower.starts_with("https://") {
        8
    } else if lower.starts_with("http://") {
        7
    } else {
        return Err("`--url` must start with `http://` or `https://`".to_string());
    };
    let host = &trimmed[prefix_len..];
    if host.is_empty() || host.starts_with('/') {
        return Err("`--url` must include a host component".to_string());
    }
    Ok(())
}

fn parse_headers(raw_headers: &[String]) -> Result<Vec<(String, String)>, String> {
    raw_headers
        .iter()
        .map(|entry| parse_single_header(entry))
        .collect()
}

fn parse_single_header(raw: &str) -> Result<(String, String), String> {
    let (name, value) = raw
        .split_once(':')
        .ok_or_else(|| format!("invalid `--header` value `{raw}` (expected `key:value`)"))?;
    let normalized_name = name.trim().to_ascii_lowercase();
    if normalized_name.is_empty() {
        return Err(format!(
            "invalid `--header` value `{raw}` (header name cannot be empty)"
        ));
    }
    if !normalized_name
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || ch == '-')
    {
        return Err(format!(
            "invalid `--header` name `{}` (allowed: [A-Za-z0-9-])",
            name.trim()
        ));
    }
    Ok((normalized_name, value.trim().to_string()))
}

fn normalize_body(body: Option<&str>) -> Result<Option<String>, String> {
    match body {
        None => Ok(None),
        Some(raw) => {
            let parsed: Value = serde_json::from_str(raw)
                .map_err(|error| format!("`--body` must be valid JSON: {error}"))?;
            serde_json::to_string(&parsed)
                .map(Some)
                .map_err(|error| format!("failed to serialize `--body`: {error}"))
        }
    }
}

fn derive_fetched_at(headers: &BTreeMap<String, String>) -> String {
    if let Some(date_header) = headers.get("date") {
        if let Ok(parsed) = DateTime::parse_from_rfc2822(date_header) {
            return parsed
                .with_timezone(&Utc)
                .to_rfc3339_opts(SecondsFormat::Secs, true);
        }
        if let Some(normalized) = normalize_rfc3339_utc(date_header) {
            return normalized;
        }
    }
    "1970-01-01T00:00:00Z".to_string()
}

#[cfg(test)]
mod tests {
    use super::{IngestApiMethod, derive_fetched_at, normalize_body, parse_headers, validate_url};
    use std::collections::BTreeMap;

    #[test]
    fn validates_http_and_https_urls() {
        assert!(validate_url("https://example.com").is_ok());
        assert!(validate_url("http://example.com/path").is_ok());
        assert!(validate_url("ftp://example.com").is_err());
    }

    #[test]
    fn parses_headers_with_lowercased_names() {
        let headers =
            parse_headers(&["Content-Type: application/json".to_string()]).expect("parse headers");
        assert_eq!(
            headers,
            vec![("content-type".to_string(), "application/json".to_string())]
        );
    }

    #[test]
    fn normalizes_body_json_representation() {
        let normalized = normalize_body(Some("{\"z\":1,\"a\":2}"))
            .expect("normalize body")
            .expect("body");
        assert_eq!(normalized, r#"{"z":1,"a":2}"#);
    }

    #[test]
    fn derives_fetched_at_from_http_date() {
        let mut headers = BTreeMap::new();
        headers.insert(
            "date".to_string(),
            "Mon, 24 Feb 2025 10:00:00 GMT".to_string(),
        );

        let fetched_at = derive_fetched_at(&headers);
        assert_eq!(fetched_at, "2025-02-24T10:00:00Z");
    }

    #[test]
    fn method_string_mapping_is_stable() {
        assert_eq!(IngestApiMethod::Get.as_str(), "GET");
        assert_eq!(IngestApiMethod::Post.as_str(), "POST");
        assert_eq!(IngestApiMethod::Put.as_str(), "PUT");
        assert_eq!(IngestApiMethod::Patch.as_str(), "PATCH");
        assert_eq!(IngestApiMethod::Delete.as_str(), "DELETE");
    }
}
