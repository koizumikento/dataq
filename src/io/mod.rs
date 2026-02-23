pub mod error;
pub mod format;
pub mod reader;
pub mod writer;

use std::fmt;
use std::path::Path;
use std::str::FromStr;

pub use error::IoError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Format {
    Json,
    Yaml,
    Csv,
    Jsonl,
}

impl Format {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Json => "json",
            Self::Yaml => "yaml",
            Self::Csv => "csv",
            Self::Jsonl => "jsonl",
        }
    }
}

impl fmt::Display for Format {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for Format {
    type Err = IoError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "json" => Ok(Self::Json),
            "yaml" | "yml" => Ok(Self::Yaml),
            "csv" => Ok(Self::Csv),
            "jsonl" | "ndjson" => Ok(Self::Jsonl),
            other => Err(IoError::UnsupportedFormat {
                format: other.to_string(),
            }),
        }
    }
}

pub fn resolve_input_format(
    explicit: Option<Format>,
    input: Option<&Path>,
) -> Result<Format, IoError> {
    resolve_format(explicit, input, "input")
}

pub fn autodetect_stdin_input_format(input: &[u8]) -> Result<Format, IoError> {
    reader::autodetect_stdin_format(input)
}

pub fn resolve_output_format(
    explicit: Option<Format>,
    output: Option<&Path>,
) -> Result<Format, IoError> {
    resolve_format(explicit, output, "output")
}

fn resolve_format(
    explicit: Option<Format>,
    path: Option<&Path>,
    kind: &'static str,
) -> Result<Format, IoError> {
    if let Some(format) = explicit {
        return Ok(format);
    }
    let Some(path) = path else {
        return Err(IoError::UnresolvedFormat { kind });
    };
    format_from_path(path).ok_or_else(|| IoError::UnsupportedPathExtension {
        kind,
        path: path.to_string_lossy().into_owned(),
    })
}

fn format_from_path(path: &Path) -> Option<Format> {
    let ext = path.extension()?.to_string_lossy().to_ascii_lowercase();
    match ext.as_str() {
        "json" => Some(Format::Json),
        "yaml" | "yml" => Some(Format::Yaml),
        "csv" => Some(Format::Csv),
        "jsonl" | "ndjson" => Some(Format::Jsonl),
        _ => None,
    }
}
