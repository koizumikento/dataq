use std::collections::BTreeMap;
use std::process::{Command, Stdio};

use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HttpMethod {
    Get,
    Post,
    Put,
    Patch,
    Delete,
}

impl HttpMethod {
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct XhRequest {
    pub url: String,
    pub method: HttpMethod,
    pub headers: Vec<(String, String)>,
    pub body: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct XhResponse {
    pub status: u16,
    pub headers: BTreeMap<String, String>,
    pub body: String,
}

#[derive(Debug, Error)]
pub enum XhError {
    #[error("`xh` is not available in PATH")]
    Unavailable,
    #[error("failed to spawn xh: {0}")]
    Spawn(std::io::Error),
    #[error("xh execution failed: {0}")]
    Execution(String),
    #[error("xh output is not valid UTF-8: {0}")]
    OutputDecode(std::str::Utf8Error),
    #[error("failed to parse xh response: {0}")]
    Parse(String),
}

pub fn fetch(request: &XhRequest) -> Result<XhResponse, XhError> {
    let xh_bin = std::env::var("DATAQ_XH_BIN").unwrap_or_else(|_| "xh".to_string());
    let mut command = Command::new(&xh_bin);
    command
        .arg("--ignore-stdin")
        .arg("--pretty=none")
        .arg("--print=hb")
        .arg(request.method.as_str())
        .arg(request.url.as_str());

    for (name, value) in &request.headers {
        command.arg(format!("{name}:{value}"));
    }
    if let Some(body) = request.body.as_ref() {
        command.arg("--raw").arg(body);
    }

    let output = match command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
    {
        Ok(child) => child.wait_with_output().map_err(XhError::Spawn)?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Err(XhError::Unavailable);
        }
        Err(error) => return Err(XhError::Spawn(error)),
    };

    if !output.status.success() {
        let stderr = String::from_utf8(output.stderr)
            .unwrap_or_else(|_| "failed to decode xh stderr".to_string());
        return Err(XhError::Execution(stderr.trim().to_string()));
    }

    parse_response(&output.stdout)
}

fn parse_response(raw: &[u8]) -> Result<XhResponse, XhError> {
    let text = std::str::from_utf8(raw).map_err(XhError::OutputDecode)?;
    let normalized = text.replace("\r\n", "\n");
    let (head, body) = split_head_and_body(normalized.as_str());

    let mut lines = head.lines();
    let status_line = lines
        .next()
        .ok_or_else(|| XhError::Parse("missing HTTP status line".to_string()))?;
    let status = parse_status_line(status_line)?;

    let mut headers: BTreeMap<String, String> = BTreeMap::new();
    for line in lines {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let (name, value) = trimmed
            .split_once(':')
            .ok_or_else(|| XhError::Parse(format!("invalid header line `{trimmed}`")))?;
        let key = name.trim().to_ascii_lowercase();
        if key.is_empty() {
            return Err(XhError::Parse("header name cannot be empty".to_string()));
        }
        let value = value.trim().to_string();
        headers
            .entry(key)
            .and_modify(|existing| {
                if !existing.is_empty() {
                    existing.push_str(", ");
                }
                existing.push_str(value.as_str());
            })
            .or_insert(value);
    }

    Ok(XhResponse {
        status,
        headers,
        body: body.to_string(),
    })
}

fn split_head_and_body(input: &str) -> (&str, &str) {
    if let Some((head, body)) = input.split_once("\n\n") {
        (head, body)
    } else {
        (input, "")
    }
}

fn parse_status_line(status_line: &str) -> Result<u16, XhError> {
    let mut tokens = status_line.split_ascii_whitespace();
    let http_version = tokens
        .next()
        .ok_or_else(|| XhError::Parse("missing HTTP version in status line".to_string()))?;
    if !http_version.starts_with("HTTP/") {
        return Err(XhError::Parse(format!(
            "status line must start with HTTP version, got `{status_line}`"
        )));
    }
    let status_raw = tokens
        .next()
        .ok_or_else(|| XhError::Parse("missing status code in status line".to_string()))?;
    status_raw
        .parse::<u16>()
        .map_err(|_| XhError::Parse(format!("invalid status code `{status_raw}`")))
}

#[cfg(test)]
mod tests {
    use super::{parse_response, split_head_and_body};

    #[test]
    fn splits_head_and_body() {
        let (head, body) = split_head_and_body("HTTP/1.1 200 OK\nA: b\n\n{\"ok\":true}");
        assert_eq!(head, "HTTP/1.1 200 OK\nA: b");
        assert_eq!(body, "{\"ok\":true}");
    }

    #[test]
    fn parses_response_status_headers_and_body() {
        let response = parse_response(
            br#"HTTP/1.1 200 OK
Date: Mon, 24 Feb 2025 10:00:00 GMT
Content-Type: application/json
X-Custom: one
X-Custom: two

{"ok":true}"#,
        )
        .expect("parse response");

        assert_eq!(response.status, 200);
        assert_eq!(
            response.headers.get("content-type").map(String::as_str),
            Some("application/json")
        );
        assert_eq!(
            response.headers.get("x-custom").map(String::as_str),
            Some("one, two")
        );
        assert_eq!(response.body, r#"{"ok":true}"#);
    }
}
