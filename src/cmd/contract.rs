use serde::Serialize;
use serde_json::{Value, json};

/// Supported command names in deterministic order.
pub const ORDERED_COMMANDS: [ContractCommand; 26] = [
    ContractCommand::Canon,
    ContractCommand::IngestApi,
    ContractCommand::Ingest,
    ContractCommand::IngestJc,
    ContractCommand::IngestTabular,
    ContractCommand::Assert,
    ContractCommand::GateSchema,
    ContractCommand::Gate,
    ContractCommand::SchemaInfer,
    ContractCommand::Sdiff,
    ContractCommand::DiffSource,
    ContractCommand::Profile,
    ContractCommand::IngestDoc,
    ContractCommand::IngestNotes,
    ContractCommand::IngestBook,
    ContractCommand::Join,
    ContractCommand::Aggregate,
    ContractCommand::Scan,
    ContractCommand::TransformRowset,
    ContractCommand::TransformSql,
    ContractCommand::Merge,
    ContractCommand::Doctor,
    ContractCommand::RecipeRun,
    ContractCommand::RecipeLock,
    ContractCommand::RecipeReplay,
    ContractCommand::EmitPlan,
];

/// Subcommand identifier accepted by `dataq contract --command`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContractCommand {
    Canon,
    IngestApi,
    Ingest,
    IngestJc,
    IngestTabular,
    Assert,
    GateSchema,
    Gate,
    SchemaInfer,
    Sdiff,
    DiffSource,
    Profile,
    IngestDoc,
    IngestNotes,
    IngestBook,
    Join,
    Aggregate,
    Scan,
    TransformRowset,
    TransformSql,
    Merge,
    Doctor,
    RecipeRun,
    RecipeLock,
    RecipeReplay,
    EmitPlan,
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
const INGEST_API_FIELDS: &[&str] = &["source", "status", "headers", "body", "fetched_at"];
const INGEST_JC_FIELDS: &[&str] = &["source", "parser", "result_type", "record_count", "records"];
const ASSERT_FIELDS: &[&str] = &["matched", "mismatch_count", "mismatches"];
const GATE_FIELDS: &[&str] = &["matched", "violations", "details"];
const SDIFF_FIELDS: &[&str] = &["counts", "keys", "ignored_paths", "values"];
const DIFF_SOURCE_FIELDS: &[&str] = &["counts", "keys", "ignored_paths", "values", "sources"];
const PROFILE_FIELDS: &[&str] = &[
    "record_count",
    "field_count",
    "truncated",
    "returned_field_count",
    "fields",
    "missing_fields",
];
const INGEST_DOC_FIELDS: &[&str] = &["meta", "headings", "links", "tables", "code_blocks"];
const INGEST_NOTES_FIELDS: &[&str] = &[
    "id",
    "title",
    "body",
    "tags",
    "created_at",
    "updated_at",
    "metadata",
];
const INGEST_BOOK_FIELDS: &[&str] = &["book", "summary"];
const SCAN_FIELDS: &[&str] = &["matches", "summary"];
const TRANSFORM_SQL_FIELDS: &[&str] = &[];
const DOCTOR_FIELDS: &[&str] = &["tools"];
const RECIPE_RUN_FIELDS: &[&str] = &["matched", "exit_code", "steps"];
const RECIPE_LOCK_FIELDS: &[&str] = &[
    "version",
    "command_graph_hash",
    "args_hash",
    "tool_versions",
    "dataq_version",
];
const RECIPE_REPLAY_FIELDS: &[&str] = &["matched", "exit_code", "lock_check", "steps"];
const EMIT_PLAN_FIELDS: &[&str] = &["command", "args", "stages", "tools"];

const CANON_NOTES: &[&str] = &[
    "Output is the canonicalized root JSON value.",
    "Top-level keys are input-dependent and therefore not fixed.",
];
const INGEST_API_NOTES: &[&str] = &[
    "Fetch stage uses `xh` and normalize stage uses `jq`.",
    "`headers` output is projected by explicit allowlist and deterministic order.",
];
const INGEST_YAML_JOBS_NOTES: &[&str] = &[
    "Output is a JSON array of normalized job records.",
    "Mode-specific row schemas: github-actions, gitlab-ci, generic-map.",
];
const INGEST_JC_NOTES: &[&str] = &[
    "Output is a deterministic envelope around `jc` parser output.",
    "`records` is always an array; non-array `jc` output is wrapped as one record.",
    "Record object keys are sorted recursively.",
];
const INGEST_TABULAR_NOTES: &[&str] = &[
    "Output is a JSON array of tabular rows.",
    "Row fields are input-dependent and therefore not fixed.",
    "`csvkit` stages convert input to CSV then JSON with inference disabled.",
];
const ASSERT_NOTES: &[&str] = &[
    "Validation mismatch details are emitted in `mismatches`.",
    "`--rules-help` and `--schema-help` have dedicated schema IDs.",
    "`--schema` defaults to built-in `jsonschema`; optional engines are `ajv` and `checkjs`.",
];
const GATE_SCHEMA_NOTES: &[&str] = &[
    "JSON output shape is aligned with `assert --schema`.",
    "`--from` resolves ingest presets with explicit validation errors.",
];
const GATE_NOTES: &[&str] = &[
    "Policy violation details are emitted in `details`.",
    "`details` are sorted by `path` then `rule_id` for deterministic output.",
];
const SCHEMA_INFER_NOTES: &[&str] = &[
    "Output is the JSON Schema value emitted by `qsv schema`.",
    "Top-level schema keys are qsv output-dependent and therefore not fixed.",
];
const SDIFF_NOTES: &[&str] = &[
    "`values.total` is the full diff count before truncation.",
    "`--value-diff-cap` only limits `values.items`.",
];
const DIFF_SOURCE_NOTES: &[&str] = &[
    "`sources.left` and `sources.right` include resolved input metadata.",
    "Preset sources must be specified as `preset:<preset-name>:<path>`.",
];
const PROFILE_NOTES: &[&str] = &[
    "`fields` keys are canonical JSON paths in deterministic order.",
    "`numeric_stats` is omitted when no numeric samples exist.",
    "`--field` projects `fields` after profile computation; `field_count` remains the total input field count.",
    "`returned_field_count` is emitted only when projection is requested.",
    "`missing_fields` is emitted when `--allow-missing-fields` is used.",
    "`--from csv` can normalize qsv adapter profile/stats CSV rows into the same output schema.",
    "`--brief` emits compact LLM-oriented fields with `path`, `null_ratio`, `unique_count`, `dominant_type`, and nullable `numeric`.",
    "`--max-fields` caps brief fields after projection and sorting; brief output always includes `truncated`.",
    "`--sort-fields` accepts `path`, `unique_count`, or `null_ratio` for brief field ordering.",
];
const INGEST_DOC_NOTES: &[&str] = &[
    "Extraction runs as `pandoc -t json` followed by jq projection.",
    "`headings`, `links`, `tables`, and `code_blocks` preserve source order.",
];
const INGEST_NOTES_NOTES: &[&str] = &[
    "Output is a JSON array of normalized note records sorted by `created_at` then `id`.",
    "`created_at` and `updated_at` are normalized to RFC3339 UTC when present.",
];
const INGEST_BOOK_NOTES: &[&str] = &[
    "`summary.order` preserves `SUMMARY.md` chapter ordering.",
    "`--include-files` controls optional chapter `file` metadata fields.",
];
const SCAN_NOTES: &[&str] = &[
    "`matches` is deterministically sorted by `path`, `line`, `column`.",
    "When `policy_mode=true`, exit code 2 indicates forbidden patterns were found.",
];
const TRANSFORM_ROWSET_NOTES: &[&str] = &[
    "Output is always a JSON array.",
    "`jq` runs first, then `mlr`, with stage-level diagnostics in `--emit-pipeline`.",
];
const TRANSFORM_SQL_NOTES: &[&str] = &[
    "Output is always a JSON array.",
    "DuckDB result row order is preserved; use `ORDER BY` in SQL when deterministic ordering is required.",
    "Object keys inside each output row are sorted recursively and float values are normalized.",
    "`duckdb` executes the SQL stage with explicit argument-array invocation.",
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
const INGEST_YAML_JOBS_EXIT_CODE_3: &str =
    "input/usage error (malformed YAML, unknown mode, or missing `jq`/`yq`/`mlr`)";
const RECIPE_RUN_NOTES: &[&str] = &[
    "This contract describes `recipe run` output.",
    "`steps` preserves recipe definition order.",
    "Step-level unmatched results map to exit code 2.",
];
const RECIPE_LOCK_NOTES: &[&str] = &[
    "`tool_versions` keys are deterministically sorted by tool name (`jq`, `mlr`, `yq`).",
    "Lock output is canonicalized before write/emit.",
];
const RECIPE_REPLAY_NOTES: &[&str] = &[
    "`lock_check` reports strict mode, match status, mismatch count, and mismatch details.",
    "When `strict=true`, lock mismatches map to exit code 2 before step execution.",
    "`steps` preserves recipe definition order when execution runs.",
];
const EMIT_PLAN_NOTES: &[&str] = &[
    "`stages` preserves deterministic stage order with explicit dependencies.",
    "`tools` reports expected external-tool usage in fixed tool order.",
    "The plan is resolved statically and does not execute the target command.",
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
        ContractCommand::IngestApi => CommandContract {
            command: "ingest-api",
            schema: "dataq.ingest.api.output.v1",
            output_fields: INGEST_API_FIELDS,
            exit_codes: exit_codes("`--expect-status` mismatch"),
            notes: INGEST_API_NOTES,
        },
        ContractCommand::Ingest => CommandContract {
            command: "ingest yaml-jobs",
            schema: "dataq.ingest.yaml_jobs.output.v1",
            output_fields: NO_FIXED_ROOT_FIELDS,
            exit_codes: exit_codes_with_code_three(
                "validation mismatch is not used by this command",
                INGEST_YAML_JOBS_EXIT_CODE_3,
            ),
            notes: INGEST_YAML_JOBS_NOTES,
        },
        ContractCommand::IngestJc => CommandContract {
            command: "ingest-jc",
            schema: "dataq.ingest.jc.output.v1",
            output_fields: INGEST_JC_FIELDS,
            exit_codes: exit_codes_with_code_three(
                "validation mismatch is not used by this command",
                "input/usage error or missing `jc`",
            ),
            notes: INGEST_JC_NOTES,
        },
        ContractCommand::IngestTabular => CommandContract {
            command: "ingest-tabular",
            schema: "dataq.ingest.tabular.output.v1",
            output_fields: NO_FIXED_ROOT_FIELDS,
            exit_codes: exit_codes_with_code_three(
                "validation mismatch is not used by this command",
                "input/usage error or missing `csvkit`",
            ),
            notes: INGEST_TABULAR_NOTES,
        },
        ContractCommand::Assert => CommandContract {
            command: "assert",
            schema: "dataq.assert.output.v1",
            output_fields: ASSERT_FIELDS,
            exit_codes: exit_codes("validation mismatch against rules or JSON Schema"),
            notes: ASSERT_NOTES,
        },
        ContractCommand::GateSchema => CommandContract {
            command: "gate-schema",
            schema: "dataq.gate.schema.output.v1",
            output_fields: ASSERT_FIELDS,
            exit_codes: exit_codes("validation mismatch against JSON Schema"),
            notes: GATE_SCHEMA_NOTES,
        },
        ContractCommand::Gate => CommandContract {
            command: "gate",
            schema: "dataq.gate.policy.output.v1",
            output_fields: GATE_FIELDS,
            exit_codes: exit_codes("policy violations detected"),
            notes: GATE_NOTES,
        },
        ContractCommand::SchemaInfer => CommandContract {
            command: "schema-infer",
            schema: "dataq.schema.infer.output.v1",
            output_fields: NO_FIXED_ROOT_FIELDS,
            exit_codes: exit_codes_with_code_three(
                "validation mismatch is not used by this command",
                "input/usage error or missing `qsv`",
            ),
            notes: SCHEMA_INFER_NOTES,
        },
        ContractCommand::Sdiff => CommandContract {
            command: "sdiff",
            schema: "dataq.sdiff.output.v1",
            output_fields: SDIFF_FIELDS,
            exit_codes: exit_codes("diff detected when `--fail-on-diff` is enabled"),
            notes: SDIFF_NOTES,
        },
        ContractCommand::DiffSource => CommandContract {
            command: "diff-source",
            schema: "dataq.diff.source.output.v1",
            output_fields: DIFF_SOURCE_FIELDS,
            exit_codes: exit_codes("diff detected when `--fail-on-diff` is enabled"),
            notes: DIFF_SOURCE_NOTES,
        },
        ContractCommand::Profile => CommandContract {
            command: "profile",
            schema: "dataq.profile.output.v1",
            output_fields: PROFILE_FIELDS,
            exit_codes: exit_codes("validation mismatch is not used by this command"),
            notes: PROFILE_NOTES,
        },
        ContractCommand::Join => CommandContract {
            command: "join",
            schema: "dataq.join.output.v1",
            output_fields: NO_FIXED_ROOT_FIELDS,
            exit_codes: exit_codes_with_code_three(
                "validation mismatch is not used by this command",
                "input/usage error or missing `mlr`",
            ),
            notes: &[
                "Output is a JSON array of joined rows.",
                "Row fields are input-dependent and therefore not fixed.",
                "`mlr` performs the join stage with explicit argument-array invocation.",
            ],
        },
        ContractCommand::Aggregate => CommandContract {
            command: "aggregate",
            schema: "dataq.aggregate.output.v1",
            output_fields: NO_FIXED_ROOT_FIELDS,
            exit_codes: exit_codes_with_code_three(
                "validation mismatch is not used by this command",
                "input/usage error or missing `mlr`",
            ),
            notes: &[
                "Output is a JSON array of grouped aggregate rows.",
                "Row fields depend on `--group-by`, `--metric`, and `--target`.",
                "`--sort-by metric --order desc --limit <n>` returns top-k groups by aggregate metric.",
                "`mlr` performs the aggregate stage with explicit argument-array invocation.",
            ],
        },
        ContractCommand::IngestDoc => CommandContract {
            command: "ingest.doc",
            schema: "dataq.ingest.doc.output.v1",
            output_fields: INGEST_DOC_FIELDS,
            exit_codes: exit_codes_with_code_three(
                "validation mismatch is not used by this command",
                "input/usage error or missing `pandoc`/`jq`",
            ),
            notes: INGEST_DOC_NOTES,
        },
        ContractCommand::IngestNotes => CommandContract {
            command: "ingest.notes",
            schema: "dataq.ingest.notes.output.v1",
            output_fields: INGEST_NOTES_FIELDS,
            exit_codes: exit_codes_with_code_three(
                "validation mismatch is not used by this command",
                "input/usage error or missing `nb`/`jq`",
            ),
            notes: INGEST_NOTES_NOTES,
        },
        ContractCommand::IngestBook => CommandContract {
            command: "ingest-book",
            schema: "dataq.ingest.book.output.v1",
            output_fields: INGEST_BOOK_FIELDS,
            exit_codes: exit_codes_with_code_three(
                "validation mismatch is not used by this command",
                "input/usage error or missing `jq`/`mdbook`",
            ),
            notes: INGEST_BOOK_NOTES,
        },
        ContractCommand::Scan => CommandContract {
            command: "scan",
            schema: "dataq.scan.text.output.v1",
            output_fields: SCAN_FIELDS,
            exit_codes: exit_codes("forbidden-pattern matches when `policy_mode` is enabled"),
            notes: SCAN_NOTES,
        },
        ContractCommand::TransformRowset => CommandContract {
            command: "transform-rowset",
            schema: "dataq.transform.rowset.output.v1",
            output_fields: NO_FIXED_ROOT_FIELDS,
            exit_codes: exit_codes("validation mismatch is not used by this command"),
            notes: TRANSFORM_ROWSET_NOTES,
        },
        ContractCommand::TransformSql => CommandContract {
            command: "transform-sql",
            schema: "dataq.transform.sql.output.v1",
            output_fields: TRANSFORM_SQL_FIELDS,
            exit_codes: exit_codes("validation mismatch is not used by this command"),
            notes: TRANSFORM_SQL_NOTES,
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
        ContractCommand::RecipeRun => CommandContract {
            command: "recipe-run",
            schema: "dataq.recipe.run.output.v1",
            output_fields: RECIPE_RUN_FIELDS,
            exit_codes: exit_codes("at least one step reported `matched=false`"),
            notes: RECIPE_RUN_NOTES,
        },
        ContractCommand::RecipeLock => CommandContract {
            command: "recipe-lock",
            schema: "dataq.recipe.lock.output.v1",
            output_fields: RECIPE_LOCK_FIELDS,
            exit_codes: exit_codes("validation mismatch is not used by this command"),
            notes: RECIPE_LOCK_NOTES,
        },
        ContractCommand::RecipeReplay => CommandContract {
            command: "recipe-replay",
            schema: "dataq.recipe.replay.output.v1",
            output_fields: RECIPE_REPLAY_FIELDS,
            exit_codes: exit_codes("strict lock mismatch or step-level validation mismatch"),
            notes: RECIPE_REPLAY_NOTES,
        },
        ContractCommand::EmitPlan => CommandContract {
            command: "emit-plan",
            schema: "dataq.emit.plan.output.v1",
            output_fields: EMIT_PLAN_FIELDS,
            exit_codes: exit_codes("validation mismatch is not used by this command"),
            notes: EMIT_PLAN_NOTES,
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

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{ContractCommand, run_for_command};

    #[test]
    fn assert_contract_keeps_expected_output_fields() {
        let response = run_for_command(ContractCommand::Assert);
        assert_eq!(response.exit_code, 0);
        assert_eq!(
            response.payload["output_fields"],
            json!(["matched", "mismatch_count", "mismatches"])
        );
    }

    #[test]
    fn assert_contract_mentions_optional_schema_engines() {
        let response = run_for_command(ContractCommand::Assert);
        let notes = response.payload["notes"].as_array().expect("notes array");
        assert!(notes.iter().any(|note| {
            note.as_str()
                .is_some_and(|text| text.contains("optional engines are `ajv` and `checkjs`"))
        }));
    }
}
