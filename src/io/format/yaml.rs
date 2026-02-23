use std::io::{Read, Write};

use serde_json::Value;

use crate::io::IoError;

pub fn read_yaml<R: Read>(reader: R) -> Result<Vec<Value>, IoError> {
    let yaml_value: serde_yaml::Value = serde_yaml::from_reader(reader)?;
    let json_value = serde_json::to_value(yaml_value)?;
    Ok(match json_value {
        Value::Array(items) => items,
        single => vec![single],
    })
}

pub fn write_yaml<W: Write>(writer: W, values: &[Value]) -> Result<(), IoError> {
    if values.len() == 1 {
        serde_yaml::to_writer(writer, &values[0])?;
    } else {
        serde_yaml::to_writer(writer, values)?;
    }
    Ok(())
}
