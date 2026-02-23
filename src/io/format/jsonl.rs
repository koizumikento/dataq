use std::io::{BufRead, BufReader, Read, Write};

use serde_json::Value;

use crate::io::IoError;

#[derive(Debug)]
pub enum JsonlStreamError<E> {
    Read(IoError),
    Emit(E),
}

pub fn looks_like_jsonl(input: &[u8]) -> bool {
    let mut has_value = false;
    let result = read_jsonl_stream(input, |_| {
        has_value = true;
        Ok::<(), ()>(())
    });
    matches!(result, Ok(())) && has_value
}

pub fn non_empty_line_count(input: &[u8]) -> usize {
    input
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.iter().all(u8::is_ascii_whitespace))
        .count()
}

pub fn read_jsonl_stream<R: Read, F, E>(reader: R, mut emit: F) -> Result<(), JsonlStreamError<E>>
where
    F: FnMut(Value) -> Result<(), E>,
{
    let reader = BufReader::new(reader);
    for line in reader.lines() {
        let line = line
            .map_err(IoError::from)
            .map_err(JsonlStreamError::Read)?;
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let value: Value = serde_json::from_str(trimmed)
            .map_err(IoError::from)
            .map_err(JsonlStreamError::Read)?;
        emit(value).map_err(JsonlStreamError::Emit)?;
    }
    Ok(())
}

pub fn read_jsonl<R: Read>(reader: R) -> Result<Vec<Value>, IoError> {
    let mut values = Vec::new();
    read_jsonl_stream(reader, |value| {
        values.push(value);
        Ok::<(), std::convert::Infallible>(())
    })
    .map_err(|error| match error {
        JsonlStreamError::Read(source) => source,
        JsonlStreamError::Emit(never) => match never {},
    })?;
    Ok(values)
}

pub fn write_jsonl_value<W: Write>(mut writer: W, value: &Value) -> Result<(), IoError> {
    serde_json::to_writer(&mut writer, value)?;
    writer.write_all(b"\n")?;
    Ok(())
}

pub fn write_jsonl<W: Write>(mut writer: W, values: &[Value]) -> Result<(), IoError> {
    for value in values {
        write_jsonl_value(&mut writer, value)?;
    }
    Ok(())
}
