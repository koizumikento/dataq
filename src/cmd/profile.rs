/// Ordered pipeline-step names used for `--emit-pipeline` diagnostics.
pub fn pipeline_steps() -> Vec<String> {
    vec![
        "resolve_profile_input".to_string(),
        "read_profile_values".to_string(),
        "compute_profile_summary".to_string(),
        "write_profile_report".to_string(),
    ]
}

/// Determinism guards planned for the `profile` command.
pub fn deterministic_guards() -> Vec<String> {
    vec![
        "rust_native_execution".to_string(),
        "no_shell_interpolation_for_user_input".to_string(),
        "deterministic_summary_key_ordering".to_string(),
    ]
}
