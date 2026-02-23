use dataq::util::sort::sort_value_keys;
use serde_json::json;

#[test]
fn sorts_keys_recursively_and_deterministically() {
    let input = json!({
        "z": 0,
        "a": {
            "d": 4,
            "b": 2,
            "c": [
                {"k": 2, "a": 1}
            ]
        }
    });

    let sorted = sort_value_keys(&input);
    let as_json = serde_json::to_string(&sorted).expect("serialize");
    assert_eq!(as_json, r#"{"a":{"b":2,"c":[{"a":1,"k":2}],"d":4},"z":0}"#);

    let sorted_again = sort_value_keys(&sorted);
    assert_eq!(sorted, sorted_again);
}
