use std::io::Write;

use serde_json::Value;

use crate::io::format::{csv, json, jsonl, yaml};
use crate::io::{Format, IoError};

pub fn write_values<W: Write>(writer: W, format: Format, values: &[Value]) -> Result<(), IoError> {
    match format {
        Format::Json => json::write_json(writer, values),
        Format::Yaml => yaml::write_yaml(writer, values),
        Format::Csv => csv::write_csv(writer, values),
        Format::Jsonl => jsonl::write_jsonl(writer, values),
    }
}
