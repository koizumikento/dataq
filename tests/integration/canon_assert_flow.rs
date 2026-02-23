use std::collections::BTreeMap;

use dataq::domain::rules::{AssertRules, CountRule, NumericRangeRule, RuleType};
use dataq::engine::r#assert::execute_assert;
use dataq::util::sort::sort_value_keys;
use serde_json::{Number, json};

#[test]
fn canon_then_assert_success_flow() {
    let raw = [
        json!({"score": 1.2, "id": 1}),
        json!({"id": 2, "score": 2.4}),
    ];
    let canonized: Vec<_> = raw.iter().map(sort_value_keys).collect();

    let mut types = BTreeMap::new();
    types.insert("id".to_string(), RuleType::Integer);
    types.insert("score".to_string(), RuleType::Number);

    let mut ranges = BTreeMap::new();
    ranges.insert(
        "score".to_string(),
        NumericRangeRule {
            min: Some(float_number(1.0)),
            max: Some(float_number(3.0)),
        },
    );

    let rules = AssertRules {
        required_keys: vec!["id".to_string(), "score".to_string()],
        types,
        count: CountRule {
            min: Some(2),
            max: Some(2),
        },
        ranges,
    };

    let report = execute_assert(&canonized, &rules).expect("assert result");
    assert!(report.matched);
    assert_eq!(report.mismatch_count, 0);
    assert!(report.mismatches.is_empty());
}

#[test]
fn mismatch_report_order_is_deterministic() {
    let values = vec![json!({"id": "x", "score": 10}), json!({"score": -1})];

    let mut types = BTreeMap::new();
    types.insert("id".to_string(), RuleType::Integer);

    let mut ranges = BTreeMap::new();
    ranges.insert(
        "score".to_string(),
        NumericRangeRule {
            min: Some(float_number(0.0)),
            max: Some(float_number(5.0)),
        },
    );

    let rules = AssertRules {
        required_keys: vec!["id".to_string(), "score".to_string()],
        types,
        count: CountRule::default(),
        ranges,
    };

    let report = execute_assert(&values, &rules).expect("assert result");
    assert!(!report.matched);
    assert_eq!(report.mismatch_count, 5);
    let as_json = serde_json::to_string(&report).expect("serialize");
    assert_eq!(
        as_json,
        r#"{"matched":false,"mismatch_count":5,"mismatches":[{"path":"$[0].id","reason":"type_mismatch","actual":"string","expected":"integer"},{"path":"$[0].score","reason":"above_max","actual":10,"expected":5.0},{"path":"$[1].id","reason":"missing_key","actual":null,"expected":"integer"},{"path":"$[1].id","reason":"missing_key","actual":null,"expected":"present"},{"path":"$[1].score","reason":"below_min","actual":-1,"expected":0.0}]}"#
    );
}

fn float_number(value: f64) -> Number {
    Number::from_f64(value).expect("finite float")
}
