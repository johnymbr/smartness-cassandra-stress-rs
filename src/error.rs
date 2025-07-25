use std::{error::Error, fmt::Debug};
use thiserror::Error;

#[derive(Error)]
pub enum SmartnessError {
    #[error("failed to create process runtime with Tokio")]
    ProcessRuntimeBuildError(#[source] std::io::Error),
    #[error("failed to create metrics runtime with Tokio")]
    MetricsRuntimeBuildError(#[source] std::io::Error),
    #[error("failed to open dataset file")]
    DatasetFileOpenError(#[source] std::io::Error),
    #[error("dataset file does not exist")]
    DatasetFileDoesNotExist,
    #[error("workload file does not exist")]
    WorkloadFileDoesNotExist,
    #[error("failed to open workload file")]
    WorkloadFileOpenError(#[source] std::io::Error),
    #[error("failed to deserialize workload file")]
    WorkloadFileDeserializationError(#[source] serde_json::Error),
    #[error("it is required set cycles or running_time")]
    CyclesOrRunningTimeRequired,
    #[error("it is required set write_script")]
    WriteScriptRequired,
    #[error("it is required set read_script")]
    ReadScriptRequired,
    #[error(
        "it is required set startup_create_schema_script and startup_drop_table_script and startup_create_table_script"
    )]
    StartuptScriptsRequired,
    #[error("it is required set warmup_qty_ops")]
    WarmupQtyOpsRequired,
}

impl Debug for SmartnessError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "{}", self)?;
        if let Some(source) = self.source() {
            writeln!(f, "Caused by:\n\t{}", source)?;
        }
        Ok(())
    }
}
