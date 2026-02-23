/// Ordered pipeline-step names used for `--emit-pipeline` diagnostics.
pub fn pipeline_steps() -> Vec<String> {
    vec![
        "resolve_merge_inputs".to_string(),
        "read_merge_values".to_string(),
        "apply_merge_policy".to_string(),
        "write_merged_output".to_string(),
    ]
}

/// Determinism guards planned for the `merge` command.
pub fn deterministic_guards() -> Vec<String> {
    vec![
        "rust_native_execution".to_string(),
        "no_shell_interpolation_for_user_input".to_string(),
        "deterministic_merge_precedence".to_string(),
    ]
}
