use std::io::Write;
use std::process::{Command, Stdio};

use serde_json::Value;
use thiserror::Error;

/// Supported document input formats accepted by pandoc.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PandocInputFormat {
    Markdown,
    Html,
    Docx,
    Rst,
    Latex,
}

impl PandocInputFormat {
    pub fn as_pandoc_name(self) -> &'static str {
        match self {
            Self::Markdown => "markdown",
            Self::Html => "html",
            Self::Docx => "docx",
            Self::Rst => "rst",
            Self::Latex => "latex",
        }
    }

    pub fn as_cli_name(self) -> &'static str {
        match self {
            Self::Markdown => "md",
            Self::Html => "html",
            Self::Docx => "docx",
            Self::Rst => "rst",
            Self::Latex => "latex",
        }
    }

    pub fn requires_utf8(self) -> bool {
        !matches!(self, Self::Docx)
    }
}

/// Errors returned while invoking pandoc for AST conversion.
#[derive(Debug, Error)]
pub enum PandocError {
    #[error("`pandoc` is not available in PATH")]
    Unavailable,
    #[error("failed to spawn pandoc: {0}")]
    Spawn(std::io::Error),
    #[error("failed to write pandoc stdin: {0}")]
    Stdin(std::io::Error),
    #[error("pandoc execution failed: {0}")]
    Execution(String),
    #[error("pandoc output is not valid JSON: {0}")]
    Parse(serde_json::Error),
}

/// Convert raw document bytes to pandoc JSON AST.
pub fn to_json_ast(input: &[u8], from: PandocInputFormat) -> Result<Value, PandocError> {
    let pandoc_bin = std::env::var("DATAQ_PANDOC_BIN").unwrap_or_else(|_| "pandoc".to_string());
    let mut child = match Command::new(&pandoc_bin)
        .arg("-f")
        .arg(from.as_pandoc_name())
        .arg("-t")
        .arg("json")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
    {
        Ok(child) => child,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            return Err(PandocError::Unavailable);
        }
        Err(err) => return Err(PandocError::Spawn(err)),
    };

    if let Some(stdin) = child.stdin.as_mut() {
        stdin.write_all(input).map_err(PandocError::Stdin)?;
    } else {
        return Err(PandocError::Execution(
            "pandoc stdin was not piped as expected".to_string(),
        ));
    }

    let output = child.wait_with_output().map_err(PandocError::Spawn)?;
    if !output.status.success() {
        let stderr = String::from_utf8(output.stderr)
            .unwrap_or_else(|_| "failed to decode pandoc stderr".to_string());
        return Err(PandocError::Execution(stderr.trim().to_string()));
    }

    serde_json::from_slice(&output.stdout).map_err(PandocError::Parse)
}
