use std::io::Cursor;

use dataq::io::Format;
use dataq::io::format::jsonl::write_jsonl_value;
use dataq::io::reader::{read_jsonl_stream, read_values};
use dataq::io::writer::write_values;
use serde_json::json;

#[test]
fn json_roundtrip() {
    let values = vec![json!({"a": 1}), json!({"a": 2})];
    let mut out = Vec::new();
    write_values(&mut out, Format::Json, &values).expect("write json");
    let read_back = read_values(Cursor::new(out), Format::Json).expect("read json");
    assert_eq!(read_back, values);
}

#[test]
fn jsonl_roundtrip() {
    let values = vec![json!({"a": 1}), json!({"a": 2})];
    let mut out = Vec::new();
    write_values(&mut out, Format::Jsonl, &values).expect("write jsonl");
    let read_back = read_values(Cursor::new(out), Format::Jsonl).expect("read jsonl");
    assert_eq!(read_back, values);
}

#[test]
fn yaml_roundtrip() {
    let values = vec![json!({"a": "x"}), json!({"a": "y"})];
    let mut out = Vec::new();
    write_values(&mut out, Format::Yaml, &values).expect("write yaml");
    let read_back = read_values(Cursor::new(out), Format::Yaml).expect("read yaml");
    assert_eq!(read_back, values);
}

#[test]
fn csv_roundtrip_for_object_rows() {
    let values = vec![
        json!({"a": "1", "b": "x"}),
        json!({"a": "2", "b": "y"}),
        json!({"a": "3", "b": ""}),
    ];
    let mut out = Vec::new();
    write_values(&mut out, Format::Csv, &values).expect("write csv");
    let read_back = read_values(Cursor::new(out), Format::Csv).expect("read csv");
    assert_eq!(read_back, values);
}

#[test]
fn jsonl_stream_read_preserves_record_order() {
    let input = br#"{"id":"2"}
{"id":"1"}
"#;
    let mut out = Vec::new();
    read_jsonl_stream(Cursor::new(input), |value| {
        out.push(value);
        Ok::<(), ()>(())
    })
    .expect("stream read jsonl");
    assert_eq!(out, vec![json!({"id":"2"}), json!({"id":"1"})]);
}

#[test]
fn jsonl_stream_write_matches_batch_writer_output() {
    let values = vec![json!({"a": 1}), json!({"a": 2})];
    let mut streamed = Vec::new();
    for value in &values {
        write_jsonl_value(&mut streamed, value).expect("stream write jsonl");
    }

    let mut batch = Vec::new();
    write_values(&mut batch, Format::Jsonl, &values).expect("batch write jsonl");
    assert_eq!(streamed, batch);
}
