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

/// Errors produced by the `profile` command boundary.
#[derive(Debug, Error)]
pub enum ProfileError {
    /// Input format could not be resolved from flags or input path.
    #[error("failed to resolve input format: {source}")]
    ResolveInput {
        #[source]
        source: IoError,
    },

    /// Input file could not be opened.
    #[error("failed to open input file `{path}`: {source}")]
    OpenInput {
        path: String,
        #[source]
        source: std::io::Error,
    },

    /// Input could not be parsed in the declared format.
    #[error("failed to read {format} input: {source}")]
    ReadInput {
        format: Format,
        #[source]
        source: IoError,
    },

    /// Structured report could not be serialized.
    #[error("failed to serialize profile report: {source}")]
    SerializeReport {
        #[source]
        source: serde_json::Error,
    },
}
