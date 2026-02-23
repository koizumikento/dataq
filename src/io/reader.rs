use std::io::{Cursor, Read};

use serde_json::Value;

use crate::io::format::jsonl::JsonlStreamError;
use crate::io::format::{csv, json, jsonl, yaml};
use crate::io::{Format, IoError};

pub fn read_values<R: Read>(reader: R, format: Format) -> Result<Vec<Value>, IoError> {
    match format {
        Format::Json => json::read_json(reader),
        Format::Yaml => yaml::read_yaml(reader),
        Format::Csv => csv::read_csv(reader),
        Format::Jsonl => jsonl::read_jsonl(reader),
    }
}

pub fn autodetect_stdin_format(input: &[u8]) -> Result<Format, IoError> {
    if input.iter().all(u8::is_ascii_whitespace) {
        return Err(IoError::StdinAutodetectFailed);
    }
    if jsonl::looks_like_jsonl(input) {
        return Ok(Format::Jsonl);
    }
    if json::read_json(Cursor::new(input)).is_ok() {
        return Ok(Format::Json);
    }
    if looks_like_yaml(input) {
        return Ok(Format::Yaml);
    }
    if csv::looks_like_csv(input) {
        return Ok(Format::Csv);
    }
    Err(IoError::StdinAutodetectFailed)
}

pub fn read_jsonl_stream<R: Read, F, E>(reader: R, emit: F) -> Result<(), JsonlStreamError<E>>
where
    F: FnMut(Value) -> Result<(), E>,
{
    jsonl::read_jsonl_stream(reader, emit)
}

fn looks_like_yaml(input: &[u8]) -> bool {
    let Ok(text) = std::str::from_utf8(input) else {
        return false;
    };
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return false;
    }
    let has_yaml_hint = trimmed.contains(':')
        || trimmed
            .lines()
            .any(|line| line.trim_start().starts_with("- "));
    if !has_yaml_hint {
        return false;
    }
    yaml::read_yaml(Cursor::new(input)).is_ok()
}
