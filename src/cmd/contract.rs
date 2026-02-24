use serde::Serialize;
use serde_json::{Value, json};

/// Supported command names in deterministic order.
pub const ORDERED_COMMANDS: [ContractCommand; 7] = [
    ContractCommand::Canon,
    ContractCommand::Assert,
    ContractCommand::Sdiff,
    ContractCommand::Profile,
    ContractCommand::Merge,
    ContractCommand::Doctor,
    ContractCommand::Recipe,
];

/// Subcommand identifier accepted by `dataq contract --command`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContractCommand {
    Canon,
    Assert,
    Sdiff,
    Profile,
    Merge,
    Doctor,
    Recipe,
}

/// Structured command response that carries exit-code mapping and JSON payload.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct ContractCommandResponse {
    pub exit_code: i32,
    pub payload: Value,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
struct CommandContract<'a> {
    command: &'a str,
    schema: &'a str,
    output_fields: &'a [&'a str],
    exit_codes: ExitCodeContract<'a>,
    notes: &'a [&'a str],
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
struct ExitCodeContract<'a> {
    #[serde(rename = "0")]
    success: &'a str,
    #[serde(rename = "2")]
    validation_mismatch: &'a str,
    #[serde(rename = "3")]
    input_usage_error: &'a str,
    #[serde(rename = "1")]
    internal_error: &'a str,
}

const NO_FIXED_ROOT_FIELDS: &[&str] = &[];
const ASSERT_FIELDS: &[&str] = &["matched", "mismatch_count", "mismatches"];
const SDIFF_FIELDS: &[&str] = &["counts", "keys", "ignored_paths", "values"];
const PROFILE_FIELDS: &[&str] = &["record_count", "field_count", "fields"];
const DOCTOR_FIELDS: &[&str] = &["tools"];
const RECIPE_FIELDS: &[&str] = &["matched", "exit_code", "steps"];

const CANON_NOTES: &[&str] = &[
    "Output is the canonicalized root JSON value.",
    "Top-level keys are input-dependent and therefore not fixed.",
];
const ASSERT_NOTES: &[&str] = &[
    "Validation mismatch details are emitted in `mismatches`.",
    "`--rules-help` and `--schema-help` have dedicated schema IDs.",
];
const SDIFF_NOTES: &[&str] = &[
    "`values.total` is the full diff count before truncation.",
    "`--value-diff-cap` only limits `values.items`.",
];
const PROFILE_NOTES: &[&str] = &[
    "`fields` keys are canonical JSON paths in deterministic order.",
    "`numeric_stats` is omitted when no numeric samples exist.",
];
const MERGE_NOTES: &[&str] = &[
    "Output is the merged root JSON value.",
    "Top-level keys are input-dependent and therefore not fixed.",
];
const DOCTOR_NOTES: &[&str] = &[
    "Tool reports are always ordered as `jq`, `yq`, `mlr`.",
    "`--capabilities` adds capability probes; `--profile` adds `capabilities` and `profile` with static requirement table versioning.",
    "Exit code 3 means missing/non-executable `jq|yq|mlr` without `--profile`, or unsatisfied selected profile requirements with `--profile`.",
];
const DOCTOR_EXIT_CODE_3: &str = "without `--profile`: missing/non-executable `jq|yq|mlr`; with `--profile`: selected profile requirements are unsatisfied";
const RECIPE_NOTES: &[&str] = &[
    "`steps` preserves recipe definition order.",
    "Step-level unmatched results map to exit code 2.",
];

pub fn run_for_command(command: ContractCommand) -> ContractCommandResponse {
    let payload = command_contract(command);
    serialize_payload(&payload)
}

pub fn run_all() -> ContractCommandResponse {
    let payload: Vec<CommandContract<'static>> =
        ORDERED_COMMANDS.into_iter().map(command_contract).collect();
    serialize_payload(&payload)
}

/// Ordered pipeline-step names used for `--emit-pipeline` diagnostics.
pub fn pipeline_steps() -> Vec<String> {
    vec![
        "resolve_contract_target".to_string(),
        "load_contract_metadata".to_string(),
        "write_contract_output".to_string(),
    ]
}

/// Determinism guards planned for the `contract` command.
pub fn deterministic_guards() -> Vec<String> {
    vec![
        "rust_native_execution".to_string(),
        "static_contract_schema_ids".to_string(),
        "fixed_contract_order_for_all".to_string(),
    ]
}

fn command_contract(command: ContractCommand) -> CommandContract<'static> {
    match command {
        ContractCommand::Canon => CommandContract {
            command: "canon",
            schema: "dataq.canon.output.v1",
            output_fields: NO_FIXED_ROOT_FIELDS,
            exit_codes: exit_codes("validation mismatch is not used by this command"),
            notes: CANON_NOTES,
        },
        ContractCommand::Assert => CommandContract {
            command: "assert",
            schema: "dataq.assert.output.v1",
            output_fields: ASSERT_FIELDS,
            exit_codes: exit_codes("validation mismatch against rules or JSON Schema"),
            notes: ASSERT_NOTES,
        },
        ContractCommand::Sdiff => CommandContract {
            command: "sdiff",
            schema: "dataq.sdiff.output.v1",
            output_fields: SDIFF_FIELDS,
            exit_codes: exit_codes("diff detected when `--fail-on-diff` is enabled"),
            notes: SDIFF_NOTES,
        },
        ContractCommand::Profile => CommandContract {
            command: "profile",
            schema: "dataq.profile.output.v1",
            output_fields: PROFILE_FIELDS,
            exit_codes: exit_codes("validation mismatch is not used by this command"),
            notes: PROFILE_NOTES,
        },
        ContractCommand::Merge => CommandContract {
            command: "merge",
            schema: "dataq.merge.output.v1",
            output_fields: NO_FIXED_ROOT_FIELDS,
            exit_codes: exit_codes("validation mismatch is not used by this command"),
            notes: MERGE_NOTES,
        },
        ContractCommand::Doctor => CommandContract {
            command: "doctor",
            schema: "dataq.doctor.output.v1",
            output_fields: DOCTOR_FIELDS,
            exit_codes: exit_codes_with_code_three(
                "validation mismatch is not used by this command",
                DOCTOR_EXIT_CODE_3,
            ),
            notes: DOCTOR_NOTES,
        },
        ContractCommand::Recipe => CommandContract {
            command: "recipe",
            schema: "dataq.recipe.run.output.v1",
            output_fields: RECIPE_FIELDS,
            exit_codes: exit_codes("at least one step reported `matched=false`"),
            notes: RECIPE_NOTES,
        },
    }
}

fn exit_codes(validation_mismatch: &'static str) -> ExitCodeContract<'static> {
    exit_codes_with_code_three(validation_mismatch, "input/usage error")
}

fn exit_codes_with_code_three(
    validation_mismatch: &'static str,
    code_three: &'static str,
) -> ExitCodeContract<'static> {
    ExitCodeContract {
        success: "success",
        validation_mismatch,
        input_usage_error: code_three,
        internal_error: "internal/unexpected error",
    }
}

fn serialize_payload<T: Serialize>(payload: &T) -> ContractCommandResponse {
    match serde_json::to_value(payload) {
        Ok(payload) => ContractCommandResponse {
            exit_code: 0,
            payload,
        },
        Err(error) => ContractCommandResponse {
            exit_code: 1,
            payload: json!({
                "error": "internal_error",
                "message": format!("failed to serialize contract payload: {error}"),
            }),
        },
    }
}
