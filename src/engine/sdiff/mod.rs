pub mod compare;

use serde::Serialize;
use serde_json::Value;
use thiserror::Error;

use crate::domain::value_path::ValuePath;

/// Default maximum number of value diff items included in the report.
pub const DEFAULT_VALUE_DIFF_CAP: usize = 100;

/// Options for structural diff execution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SdiffOptions {
    pub value_diff_cap: usize,
    pub key_path: Option<ValuePath>,
    pub ignore_paths: Vec<ValuePath>,
}

impl SdiffOptions {
    pub fn new(value_diff_cap: usize) -> Self {
        Self {
            value_diff_cap,
            key_path: None,
            ignore_paths: Vec::new(),
        }
    }

    pub fn with_key_path(mut self, key_path: Option<ValuePath>) -> Self {
        self.key_path = key_path;
        self
    }

    pub fn with_ignore_paths(mut self, ignore_paths: Vec<ValuePath>) -> Self {
        self.ignore_paths = ignore_paths;
        self
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
    pub ignored_paths: Vec<String>,
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

/// Errors for invalid `sdiff` comparison modes and data shape.
#[derive(Debug, Clone, Error, PartialEq)]
pub enum SdiffError {
    #[error("key path `{key_path}` is missing on {side} row index {row_index}")]
    MissingKeyValue {
        side: &'static str,
        row_index: usize,
        key_path: String,
    },
    #[error(
        "duplicate key value at `{key_path}` on {side} rows {first_index} and {second_index}: {key_literal}"
    )]
    DuplicateKeyValue {
        side: &'static str,
        key_path: String,
        key_literal: String,
        first_index: usize,
        second_index: usize,
    },
}

/// Compares two datasets and returns a deterministic structural diff report.
pub fn structural_diff(
    left: &[Value],
    right: &[Value],
    options: SdiffOptions,
) -> Result<SdiffReport, SdiffError> {
    compare::compare_datasets(left, right, options)
}
