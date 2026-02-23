pub mod compare;

use serde::Serialize;
use serde_json::Value;

/// Default maximum number of value diff items included in the report.
pub const DEFAULT_VALUE_DIFF_CAP: usize = 100;

/// Options for structural diff execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SdiffOptions {
    pub value_diff_cap: usize,
}

impl SdiffOptions {
    pub const fn new(value_diff_cap: usize) -> Self {
        Self { value_diff_cap }
    }
}

impl Default for SdiffOptions {
    fn default() -> Self {
        Self::new(DEFAULT_VALUE_DIFF_CAP)
    }
}

/// Deterministic structural diff report.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct SdiffReport {
    pub counts: CountDiff,
    pub keys: KeyDiff,
    pub values: ValueDiffSection,
}

/// Record-count comparison section.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct CountDiff {
    pub left: usize,
    pub right: usize,
    pub delta: i64,
    pub equal: bool,
}

/// Structural key comparison section.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct KeyDiff {
    pub left_only: Vec<String>,
    pub right_only: Vec<String>,
    pub shared: Vec<String>,
}

/// Value difference section.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct ValueDiffSection {
    pub total: usize,
    pub truncated: bool,
    pub items: Vec<ValueDiffItem>,
}

/// One value difference entry at a stable path.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct ValueDiffItem {
    pub path: String,
    pub left: Value,
    pub right: Value,
}

/// Compares two datasets and returns a deterministic structural diff report.
pub fn structural_diff(left: &[Value], right: &[Value], options: SdiffOptions) -> SdiffReport {
    compare::compare_datasets(left, right, options)
}
