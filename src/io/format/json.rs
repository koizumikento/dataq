use std::io::{Read, Write};

use serde_json::Value;

use crate::io::IoError;

pub fn read_json<R: Read>(reader: R) -> Result<Vec<Value>, IoError> {
    let value: Value = serde_json::from_reader(reader)?;
    Ok(match value {
        Value::Array(items) => items,
        single => vec![single],
    })
}

pub fn write_json<W: Write>(writer: W, values: &[Value]) -> Result<(), IoError> {
    if values.len() == 1 {
        serde_json::to_writer(writer, &values[0])?;
    } else {
        serde_json::to_writer(writer, values)?;
    }
    Ok(())
}
