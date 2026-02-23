use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::{Number, Value};

/// Rule schema for the `assert` command MVP.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct AssertRules {
    #[serde(default)]
    pub required_keys: Vec<String>,
    #[serde(default)]
    pub types: BTreeMap<String, RuleType>,
    #[serde(default)]
    pub count: CountRule,
    #[serde(default)]
    pub ranges: BTreeMap<String, NumericRangeRule>,
}

/// Record count boundaries.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(default, deny_unknown_fields)]
pub struct CountRule {
    pub min: Option<usize>,
    pub max: Option<usize>,
}

/// Expected JSON value type for a field.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RuleType {
    String,
    Number,
    Integer,
    Boolean,
    Object,
    Array,
    Null,
}

impl RuleType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::String => "string",
            Self::Number => "number",
            Self::Integer => "integer",
            Self::Boolean => "boolean",
            Self::Object => "object",
            Self::Array => "array",
            Self::Null => "null",
        }
    }

    pub fn matches(&self, value: &Value) -> bool {
        match self {
            Self::String => value.is_string(),
            Self::Number => value.is_number(),
            Self::Integer => value.as_i64().is_some() || value.as_u64().is_some(),
            Self::Boolean => value.is_boolean(),
            Self::Object => value.is_object(),
            Self::Array => value.is_array(),
            Self::Null => value.is_null(),
        }
    }
}

/// Numeric range boundaries for a field.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(default, deny_unknown_fields)]
pub struct NumericRangeRule {
    pub min: Option<Number>,
    pub max: Option<Number>,
}

/// Single mismatch in assert output.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MismatchEntry {
    pub path: String,
    pub reason: String,
    pub actual: Value,
    pub expected: Value,
}

/// Deterministic report produced by assert validation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AssertReport {
    pub matched: bool,
    pub mismatch_count: usize,
    pub mismatches: Vec<MismatchEntry>,
}
