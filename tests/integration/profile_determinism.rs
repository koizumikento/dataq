use std::io::Cursor;

use dataq::cmd::profile::{ProfileCommandArgs, run_with_stdin};
use dataq::io::Format;

#[test]
fn profile_is_deterministic_for_identical_input() {
    let input = r#"[{"a":1,"z":"x"},{"z":"x","a":1},{"a":null}]"#;
    let args = ProfileCommandArgs {
        input: None,
        from: Some(Format::Json),
    };

    let first = run_with_stdin(&args, Cursor::new(input));
    let second = run_with_stdin(&args, Cursor::new(input));

    assert_eq!(first.exit_code, 0);
    assert_eq!(second.exit_code, 0);

    let first_bytes = serde_json::to_vec(&first.payload).expect("serialize first payload");
    let second_bytes = serde_json::to_vec(&second.payload).expect("serialize second payload");
    assert_eq!(first_bytes, second_bytes);
}

#[test]
fn profile_numeric_stats_are_deterministic_for_same_input_bytes() {
    let input = r#"[{"score":1.25},{"score":2.5},{"score":null},{"score":4.75}]"#;
    let args = ProfileCommandArgs {
        input: None,
        from: Some(Format::Json),
    };

    let first = run_with_stdin(&args, Cursor::new(input));
    let second = run_with_stdin(&args, Cursor::new(input));

    assert_eq!(first.exit_code, 0);
    assert_eq!(second.exit_code, 0);

    let first_bytes = serde_json::to_vec(&first.payload).expect("serialize first payload");
    let second_bytes = serde_json::to_vec(&second.payload).expect("serialize second payload");
    assert_eq!(first_bytes, second_bytes);
}
