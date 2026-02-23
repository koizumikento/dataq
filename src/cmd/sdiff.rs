use serde_json::Value;

use crate::engine::sdiff::{self, SdiffOptions, SdiffReport};

pub use crate::engine::sdiff::DEFAULT_VALUE_DIFF_CAP;

/// Runs structural diff with default options.
///
/// This function expects both datasets to already be parsed into JSON values
/// (for example via `io::reader::read_values`).
pub fn execute(left: &[Value], right: &[Value]) -> SdiffReport {
    execute_with_options(left, right, SdiffOptions::default())
}

/// Runs structural diff with an explicit value-diff cap.
pub fn execute_with_value_diff_cap(
    left: &[Value],
    right: &[Value],
    value_diff_cap: usize,
) -> SdiffReport {
    execute_with_options(left, right, SdiffOptions::new(value_diff_cap))
}

/// Runs structural diff with explicit engine options.
pub fn execute_with_options(left: &[Value], right: &[Value], options: SdiffOptions) -> SdiffReport {
    sdiff::structural_diff(left, right, options)
}
