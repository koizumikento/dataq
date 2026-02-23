use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// Deterministic profile report for `profile` command output.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ProfileReport {
    pub record_count: usize,
    pub field_count: usize,
    pub fields: BTreeMap<String, ProfileFieldReport>,
}

/// Deterministic per-field statistics.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ProfileFieldReport {
    pub null_ratio: f64,
    pub unique_count: usize,
    pub type_distribution: ProfileTypeDistribution,
}

/// Deterministic type distribution for one field path.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProfileTypeDistribution {
    pub null: usize,
    pub boolean: usize,
    pub number: usize,
    pub string: usize,
    pub array: usize,
    pub object: usize,
}
