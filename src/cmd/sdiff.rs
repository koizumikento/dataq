use serde_json::Value;
use thiserror::Error;

use crate::engine::sdiff::{self, SdiffOptions, SdiffReport};
use crate::{
    domain::value_path::{ValuePath, ValuePathError},
    engine::sdiff::SdiffError,
};

pub use crate::engine::sdiff::DEFAULT_VALUE_DIFF_CAP;

/// Ordered pipeline-step names used for `--emit-pipeline` diagnostics.
pub fn pipeline_steps() -> Vec<String> {
    vec![
        "resolve_input_formats".to_string(),
        "read_left_values".to_string(),
        "read_right_values".to_string(),
        "compute_structural_diff".to_string(),
        "write_diff_report".to_string(),
    ]
}

/// Determinism guards applied by the `sdiff` command.
pub fn deterministic_guards() -> Vec<String> {
    vec![
        "rust_native_execution".to_string(),
        "no_shell_interpolation_for_user_input".to_string(),
        "canonical_json_path_escaping".to_string(),
        "deterministic_diff_item_ordering".to_string(),
    ]
}

/// Runs structural diff with default options.
///
/// This function expects both datasets to already be parsed into JSON values
/// (for example via `io::reader::read_values`).
pub fn execute(left: &[Value], right: &[Value]) -> SdiffReport {
    execute_with_options(left, right, SdiffOptions::default())
        .expect("default sdiff options must be valid")
}

/// Runs structural diff with an explicit value-diff cap.
pub fn execute_with_value_diff_cap(
    left: &[Value],
    right: &[Value],
    value_diff_cap: usize,
) -> SdiffReport {
    execute_with_options(left, right, SdiffOptions::new(value_diff_cap))
        .expect("value-only sdiff options must be valid")
}

/// Runs structural diff with explicit engine options.
pub fn execute_with_options(
    left: &[Value],
    right: &[Value],
    options: SdiffOptions,
) -> Result<SdiffReport, SdiffCommandError> {
    sdiff::structural_diff(left, right, options).map_err(SdiffCommandError::Engine)
}

/// Parses CLI-facing option values into validated `sdiff` options.
pub fn parse_options(
    value_diff_cap: usize,
    key_path: Option<&str>,
    ignore_paths: &[String],
) -> Result<SdiffOptions, SdiffCommandError> {
    let parsed_key = key_path
        .map(ValuePath::parse_canonical)
        .transpose()
        .map_err(|source| SdiffCommandError::InvalidKeyPath {
            path: key_path.unwrap_or_default().to_string(),
            source,
        })?;

    let mut parsed_ignored_paths = Vec::with_capacity(ignore_paths.len());
    for raw_path in ignore_paths {
        let parsed = ValuePath::parse_canonical(raw_path).map_err(|source| {
            SdiffCommandError::InvalidIgnorePath {
                path: raw_path.clone(),
                source,
            }
        })?;
        parsed_ignored_paths.push(parsed);
    }

    parsed_ignored_paths.sort();
    parsed_ignored_paths.dedup();

    Ok(SdiffOptions::new(value_diff_cap)
        .with_key_path(parsed_key)
        .with_ignore_paths(parsed_ignored_paths))
}

#[derive(Debug, Error)]
pub enum SdiffCommandError {
    #[error("invalid `--key` path `{path}`: {source}")]
    InvalidKeyPath {
        path: String,
        source: ValuePathError,
    },
    #[error("invalid `--ignore-path` path `{path}`: {source}")]
    InvalidIgnorePath {
        path: String,
        source: ValuePathError,
    },
    #[error("{0}")]
    Engine(#[from] SdiffError),
}
