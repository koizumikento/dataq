use std::io::Cursor;

use dataq::cmd::sdiff;
use dataq::io::Format;
use dataq::io::reader::read_values;
use serde_json::json;

#[test]
fn value_diff_cap_truncates_with_default_limit() {
    let left_input = build_jsonl(120, 0);
    let right_input = build_jsonl(120, 1);

    let left = read_values(Cursor::new(left_input), Format::Jsonl).expect("read left jsonl");
    let right = read_values(Cursor::new(right_input), Format::Jsonl).expect("read right jsonl");

    let report = sdiff::execute(&left, &right);
    let actual = serde_json::to_value(report).expect("serialize report");

    assert_eq!(
        actual["counts"],
        json!({"left": 120, "right": 120, "delta": 0, "equal": true})
    );
    assert_eq!(
        actual["keys"],
        json!({"left_only": [], "right_only": [], "shared": ["$[\"v\"]"]})
    );
    assert_eq!(actual["ignored_paths"], json!([]));
    assert_eq!(actual["values"]["total"], json!(120));
    assert_eq!(actual["values"]["truncated"], json!(true));
    assert_eq!(
        actual["values"]["items"].as_array().map(Vec::len),
        Some(100)
    );
    assert_eq!(actual["values"]["items"][0]["path"], json!("$[0][\"v\"]"));
    assert_eq!(actual["values"]["items"][99]["path"], json!("$[99][\"v\"]"));
}

#[test]
fn key_mode_and_ignore_path_work_with_jsonl_flow() {
    let left = read_values(
        Cursor::new("{\"id\":2,\"v\":\"same\",\"ts\":\"2025-01-01\"}\n{\"id\":1,\"v\":\"left\",\"ts\":\"2025-01-01\"}"),
        Format::Jsonl,
    )
    .expect("read left");
    let right = read_values(
        Cursor::new("{\"id\":1,\"v\":\"right\",\"ts\":\"2025-02-01\"}\n{\"id\":2,\"v\":\"same\",\"ts\":\"2025-02-01\"}"),
        Format::Jsonl,
    )
    .expect("read right");

    let options = sdiff::parse_options(1, Some(r#"$["id"]"#), &[r#"$["ts"]"#.to_string()])
        .expect("parse options");
    let report = sdiff::execute_with_options(&left, &right, options).expect("run sdiff");
    let actual = serde_json::to_value(report).expect("serialize report");

    assert_eq!(actual["ignored_paths"], json!([r#"$["ts"]"#]));
    assert_eq!(actual["values"]["total"], json!(1));
    assert_eq!(actual["values"]["truncated"], json!(false));
    assert_eq!(
        actual["values"]["items"],
        json!([{
            "path": "$[0][\"v\"]",
            "left": "left",
            "right": "right"
        }])
    );
}

fn build_jsonl(rows: usize, offset: i64) -> String {
    (0..rows)
        .map(|i| format!("{{\"v\":{}}}", i as i64 + offset))
        .collect::<Vec<String>>()
        .join("\n")
}
