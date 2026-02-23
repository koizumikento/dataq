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

    let first_json = serde_json::to_string(&first.payload).expect("serialize first payload");
    let second_json = serde_json::to_string(&second.payload).expect("serialize second payload");
    assert_eq!(first_json, second_json);
}
