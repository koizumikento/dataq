use thiserror::Error;

use crate::io::{Format, IoError};

/// Errors produced by the `canon` command boundary.
#[derive(Debug, Error)]
pub enum CanonError {
    /// Input could not be parsed in the declared format.
    #[error("failed to read {format} input: {source}")]
    ReadInput {
        format: Format,
        #[source]
        source: IoError,
    },

    /// Output could not be serialized in the declared format.
    #[error("failed to write {format} output: {source}")]
    WriteOutput {
        format: Format,
        #[source]
        source: IoError,
    },
}
