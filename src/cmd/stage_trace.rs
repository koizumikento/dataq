use std::time::Instant;

use serde_json::Value;

use crate::domain::report::{PipelineStageDiagnostic, PipelineStageMetrics};

pub(crate) fn run_value_stage<E, F>(
    order: usize,
    step: &'static str,
    tool: &'static str,
    input_sets: &[&[Value]],
    run_stage: F,
) -> (Result<Vec<Value>, E>, PipelineStageDiagnostic)
where
    F: FnOnce() -> Result<Vec<Value>, E>,
{
    let input_records = input_sets.iter().map(|rows| rows.len()).sum();
    let input_bytes = input_sets
        .iter()
        .map(|rows| value_array_json_bytes(rows))
        .sum();
    let started = Instant::now();
    let result = run_stage();
    let duration_ms = elapsed_ms(started);

    let diagnostic = match &result {
        Ok(rows) => PipelineStageDiagnostic::success_with_metrics(
            order,
            step,
            tool,
            input_records,
            rows.len(),
            PipelineStageMetrics {
                input_bytes,
                output_bytes: value_array_json_bytes(rows),
                duration_ms,
            },
        ),
        Err(_) => PipelineStageDiagnostic::failure_with_metrics(
            order,
            step,
            tool,
            input_records,
            PipelineStageMetrics {
                input_bytes,
                output_bytes: 0,
                duration_ms,
            },
        ),
    };

    (result, diagnostic)
}

fn value_array_json_bytes(rows: &[Value]) -> usize {
    serde_json::to_vec(rows).map_or(0, |bytes| bytes.len())
}

fn elapsed_ms(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX)
}
