use std::io::{Read, Write};

use crate::domain::error::CanonError;
use crate::engine::canon::{CanonOptions, canonicalize_values};
use crate::io::{Format, reader, writer};

/// Command-level options for canonicalization execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CanonCommandOptions {
    /// Sort object keys lexicographically. If `false`, preserve input key order.
    pub sort_keys: bool,
    /// Normalize RFC3339 timestamps to UTC (`Z`) when enabled.
    pub normalize_time: bool,
}

impl Default for CanonCommandOptions {
    fn default() -> Self {
        Self {
            sort_keys: true,
            normalize_time: false,
        }
    }
}

impl From<CanonCommandOptions> for CanonOptions {
    fn from(value: CanonCommandOptions) -> Self {
        Self {
            sort_keys: value.sort_keys,
            normalize_time: value.normalize_time,
        }
    }
}

/// Ordered pipeline-step names used for `--emit-pipeline` diagnostics.
pub fn pipeline_steps() -> Vec<String> {
    vec![
        "read_input_values".to_string(),
        "canonicalize_values".to_string(),
        "write_output_values".to_string(),
    ]
}

/// Determinism guards applied by the `canon` command.
pub fn deterministic_guards(options: CanonCommandOptions) -> Vec<String> {
    let mut guards = vec![
        "rust_native_execution".to_string(),
        "no_shell_interpolation_for_user_input".to_string(),
    ];
    if options.sort_keys {
        guards.push("object_keys_sorted_lexicographically".to_string());
    } else {
        guards.push("input_object_key_order_preserved".to_string());
    }
    if options.normalize_time {
        guards.push("timestamps_normalized_rfc3339_utc".to_string());
    }
    guards
}

/// Execute `canon` from input stream to output stream.
///
/// This function is intentionally thin: it only coordinates I/O and delegates
/// canonicalization to the engine layer.
pub fn run<R: Read, W: Write>(
    input: R,
    output: W,
    input_format: Format,
    output_format: Format,
    options: CanonCommandOptions,
) -> Result<(), CanonError> {
    let values =
        reader::read_values(input, input_format).map_err(|source| CanonError::ReadInput {
            format: input_format,
            source,
        })?;
    let canonical = canonicalize_values(values, options.into());
    writer::write_values(output, output_format, &canonical).map_err(|source| {
        CanonError::WriteOutput {
            format: output_format,
            source,
        }
    })
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use serde_json::json;

    use super::{CanonCommandOptions, run};
    use crate::io::Format;

    #[test]
    fn runs_pipeline_with_default_sorting() {
        let input = br#"{"z":"false","a":{"n":"42"}}"#;
        let mut output = Vec::new();
        run(
            Cursor::new(input),
            &mut output,
            Format::Json,
            Format::Json,
            CanonCommandOptions::default(),
        )
        .expect("canon run should succeed");

        let out_value: serde_json::Value = serde_json::from_slice(&output).expect("parse output");
        assert_eq!(out_value, json!({"a":{"n":42},"z":false}));
    }

    #[test]
    fn run_is_deterministic_for_same_input() {
        let input = br#"{"b":{"z":"2","a":"1"},"a":"true"}"#;
        let options = CanonCommandOptions {
            sort_keys: true,
            normalize_time: true,
        };

        let mut first = Vec::new();
        run(
            Cursor::new(input),
            &mut first,
            Format::Json,
            Format::Json,
            options,
        )
        .expect("first run should succeed");

        let mut second = Vec::new();
        run(
            Cursor::new(input),
            &mut second,
            Format::Json,
            Format::Json,
            options,
        )
        .expect("second run should succeed");

        assert_eq!(first, second);
    }

    #[test]
    fn preserves_input_key_order_when_sorting_disabled() {
        let input = br#"{"z":"false","a":{"n":"42","m":"1"}}"#;
        let mut output = Vec::new();
        run(
            Cursor::new(input),
            &mut output,
            Format::Json,
            Format::Json,
            CanonCommandOptions {
                sort_keys: false,
                normalize_time: false,
            },
        )
        .expect("canon run should succeed");

        let out = String::from_utf8(output).expect("output should be utf8");
        assert_eq!(out, r#"{"z":false,"a":{"n":42,"m":1}}"#);
    }
}
