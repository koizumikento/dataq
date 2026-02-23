use std::io::Read;

use serde_json::Value;

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
