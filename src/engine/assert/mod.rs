pub mod schema;
pub mod validator;

use serde_json::Value;

use crate::domain::rules::{AssertReport, AssertRules};

pub use schema::SchemaValidationEngine;
pub use validator::AssertValidationError;

/// Executes assert validation against loaded input values and parsed rules.
pub fn execute_assert(
    values: &[Value],
    rules: &AssertRules,
) -> Result<AssertReport, AssertValidationError> {
    validator::validate(values, rules)
}

/// Executes assert validation against loaded input values and JSON Schema.
pub fn execute_assert_with_schema(
    values: &[Value],
    schema: &Value,
) -> Result<AssertReport, AssertValidationError> {
    self::schema::validate(values, schema)
}

/// Executes assert validation against loaded input values and JSON Schema
/// with an explicit schema-validation engine.
pub fn execute_assert_with_schema_and_engine(
    values: &[Value],
    schema: &Value,
    engine: SchemaValidationEngine,
) -> Result<AssertReport, AssertValidationError> {
    self::schema::validate_with_engine(values, schema, engine)
}
