use std::io::{Read, Write};

use crate::domain::error::CanonError;
use crate::engine::canon::{CanonOptions, canonicalize_values};
use crate::io::{Format, reader, writer};

/// Command-level options for canonicalization execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CanonCommandOptions {
    pub sort_keys: bool,
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
}
