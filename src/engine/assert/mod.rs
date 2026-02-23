pub mod validator;

use serde_json::Value;

use crate::domain::rules::{AssertReport, AssertRules};

pub use validator::AssertValidationError;

/// Executes assert validation against loaded input values and parsed rules.
pub fn execute_assert(
    values: &[Value],
    rules: &AssertRules,
) -> Result<AssertReport, AssertValidationError> {
    validator::validate(values, rules)
}
