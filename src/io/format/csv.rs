use std::collections::BTreeSet;
use std::io::{Read, Write};

use serde_json::{Map, Value};

use crate::io::IoError;

pub fn read_csv<R: Read>(reader: R) -> Result<Vec<Value>, IoError> {
    let mut csv_reader = csv::ReaderBuilder::new().from_reader(reader);
    let headers = csv_reader.headers()?.clone();
    let mut out = Vec::new();
    for row in csv_reader.records() {
        let record = row?;
        let mut map = Map::new();
        for (index, cell) in record.iter().enumerate() {
            let key = headers
                .get(index)
                .map(ToOwned::to_owned)
                .unwrap_or_else(|| format!("col_{index}"));
            map.insert(key, Value::String(cell.to_string()));
        }
        out.push(Value::Object(map));
    }
    Ok(out)
}

pub fn write_csv<W: Write>(writer: W, values: &[Value]) -> Result<(), IoError> {
    let mut headers = BTreeSet::new();
    for value in values {
        if let Value::Object(map) = value {
            headers.extend(map.keys().cloned());
        }
    }
    let headers: Vec<String> = headers.into_iter().collect();
    let mut csv_writer = csv::WriterBuilder::new().from_writer(writer);
    if !headers.is_empty() {
        csv_writer.write_record(&headers)?;
    }
    for (index, value) in values.iter().enumerate() {
        let Value::Object(map) = value else {
            return Err(IoError::InvalidCsvRow { index });
        };
        let row: Vec<String> = headers
            .iter()
            .map(|key| map.get(key).map(stringify_csv_value).unwrap_or_default())
            .collect();
        csv_writer.write_record(row)?;
    }
    csv_writer.flush()?;
    Ok(())
}

fn stringify_csv_value(value: &Value) -> String {
    match value {
        Value::Null => String::new(),
        Value::Bool(v) => v.to_string(),
        Value::Number(v) => v.to_string(),
        Value::String(v) => v.clone(),
        other => serde_json::to_string(other).unwrap_or_default(),
    }
}
