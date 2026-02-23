use std::io::{BufRead, BufReader, Read, Write};

use serde_json::Value;

use crate::io::IoError;

pub fn read_jsonl<R: Read>(reader: R) -> Result<Vec<Value>, IoError> {
    let mut values = Vec::new();
    let reader = BufReader::new(reader);
    for line in reader.lines() {
        let line = line?;
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let value: Value = serde_json::from_str(trimmed)?;
        values.push(value);
    }
    Ok(values)
}

pub fn write_jsonl<W: Write>(mut writer: W, values: &[Value]) -> Result<(), IoError> {
    for value in values {
        serde_json::to_writer(&mut writer, value)?;
        writer.write_all(b"\n")?;
    }
    Ok(())
}
