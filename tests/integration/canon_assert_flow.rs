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
        ..AssertRules::default()
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
        ..AssertRules::default()
    };

    let report = execute_assert(&values, &rules).expect("assert result");
    assert!(!report.matched);
    assert_eq!(report.mismatch_count, 5);
    let as_json = serde_json::to_string(&report).expect("serialize");
    assert_eq!(
        as_json,
        r#"{"matched":false,"mismatch_count":5,"mismatches":[{"path":"$[0].id","rule_kind":"types","reason":"type_mismatch","actual":"string","expected":"integer"},{"path":"$[0].score","rule_kind":"ranges","reason":"above_max","actual":10,"expected":5.0},{"path":"$[1].id","rule_kind":"required_keys","reason":"missing_key","actual":null,"expected":"present"},{"path":"$[1].id","rule_kind":"types","reason":"missing_key","actual":null,"expected":"integer"},{"path":"$[1].score","rule_kind":"ranges","reason":"below_min","actual":-1,"expected":0.0}]}"#
    );
}

#[test]
fn mixed_rule_report_order_is_stable() {
    let values =
        vec![json!({"id": null, "name": "User-1", "status": "pending", "meta": {"blocked": true}})];

    let mut types = BTreeMap::new();
    types.insert("id".to_string(), RuleType::Integer);

    let mut nullable = BTreeMap::new();
    nullable.insert("id".to_string(), false);

    let mut enum_values = BTreeMap::new();
    enum_values.insert("status".to_string(), vec![json!("ok"), json!("done")]);

    let mut patterns = BTreeMap::new();
    patterns.insert("name".to_string(), "^[a-z]+_[0-9]+$".to_string());

    let rules = AssertRules {
        forbid_keys: vec!["meta.blocked".to_string()],
        types,
        nullable,
        enum_values,
        patterns,
        ..AssertRules::default()
    };

    let report = execute_assert(&values, &rules).expect("assert result");
    assert!(!report.matched);
    assert_eq!(report.mismatch_count, 5);

    let as_json = serde_json::to_string(&report).expect("serialize");
    assert_eq!(
        as_json,
        r#"{"matched":false,"mismatch_count":5,"mismatches":[{"path":"$[0].meta.blocked","rule_kind":"forbid_keys","reason":"forbidden_key","actual":true,"expected":"absent"},{"path":"$[0].id","rule_kind":"types","reason":"type_mismatch","actual":"null","expected":"integer"},{"path":"$[0].id","rule_kind":"nullable","reason":"null_not_allowed","actual":null,"expected":false},{"path":"$[0].status","rule_kind":"enum","reason":"enum_mismatch","actual":"pending","expected":["ok","done"]},{"path":"$[0].name","rule_kind":"pattern","reason":"pattern_mismatch","actual":"User-1","expected":"^[a-z]+_[0-9]+$"}]}"#
    );
}

fn float_number(value: f64) -> Number {
    Number::from_f64(value).expect("finite float")
}
