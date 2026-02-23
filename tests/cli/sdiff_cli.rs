use dataq::cmd::sdiff;
use serde_json::json;

#[test]
fn no_diff_report_is_empty_and_deterministic() {
    let left = vec![json!({"a": 1}), json!({"a": 2})];
    let right = vec![json!({"a": 1}), json!({"a": 2})];

    let report = sdiff::execute(&left, &right);
    let actual = serde_json::to_value(report).expect("serialize report");

    let expected = json!({
        "counts": {
            "left": 2,
            "right": 2,
            "delta": 0,
            "equal": true
        },
        "keys": {
            "left_only": [],
            "right_only": [],
            "shared": ["$.a"]
        },
        "values": {
            "total": 0,
            "truncated": false,
            "items": []
        }
    });

    assert_eq!(actual, expected);
}

#[test]
fn count_diff_is_reported() {
    let left = vec![json!({"a": 1})];
    let right = vec![json!({"a": 1}), json!({"a": 2})];

    let report = sdiff::execute(&left, &right);
    let actual = serde_json::to_value(report).expect("serialize report");

    let expected = json!({
        "counts": {
            "left": 1,
            "right": 2,
            "delta": 1,
            "equal": false
        },
        "keys": {
            "left_only": [],
            "right_only": [],
            "shared": ["$.a"]
        },
        "values": {
            "total": 0,
            "truncated": false,
            "items": []
        }
    });

    assert_eq!(actual, expected);
}

#[test]
fn key_diff_is_reported() {
    let left = vec![json!({"a": 1, "only_left": true})];
    let right = vec![json!({"a": 1, "only_right": true})];

    let report = sdiff::execute(&left, &right);
    let actual = serde_json::to_value(report).expect("serialize report");

    let expected = json!({
        "counts": {
            "left": 1,
            "right": 1,
            "delta": 0,
            "equal": true
        },
        "keys": {
            "left_only": ["$.only_left"],
            "right_only": ["$.only_right"],
            "shared": ["$.a"]
        },
        "values": {
            "total": 2,
            "truncated": false,
            "items": [
                {
                    "path": "$[0].only_left",
                    "left": true,
                    "right": null
                },
                {
                    "path": "$[0].only_right",
                    "left": null,
                    "right": true
                }
            ]
        }
    });

    assert_eq!(actual, expected);
}
