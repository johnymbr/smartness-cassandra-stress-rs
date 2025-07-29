use scylla::errors::{ExecutionError, NewSessionError};
use std::{error::Error, fmt::Debug};
use thiserror::Error;

#[derive(Error)]
pub enum SmartnessError {
    #[error("failed to create process runtime with Tokio")]
    ProcessRuntimeBuildError(#[source] std::io::Error),
    #[error("failed to create metrics runtime with Tokio")]
    MetricsRuntimeBuildError(#[source] std::io::Error),
    #[error("failed to create metrics parent path")]
    MetricsParentPathCreateError(#[source] std::io::Error),
    #[error("failed to create metrics file")]
    MetricsFileCreateError(#[source] std::io::Error),
    #[error("failed to write headers into metrics file")]
    MetricsFileWriteHeadersError(#[source] csv::Error),
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
    #[error("it is required set cassandra_host")]
    CassandraHostRequired,
    #[error("it is required set cassandra_port")]
    CassandraPortRequired,
    #[error("it is required set cassandra_username and cassandra_password")]
    CassandraUsernameAndPasswordAreRequired,
    #[error("error when create a ScyllaDB session")]
    ScyllaSessionError(#[source] NewSessionError),
    #[error("error when run create keyspace script")]
    CsqlCreateKeyspaceError(#[source] ExecutionError),
    #[error("error when run drop table script")]
    CsqlDropTableError(#[source] ExecutionError),
    #[error("error when run create table script")]
    CsqlCreateTableError(#[source] ExecutionError),
    #[error("error when insert a record via warmup")]
    WarmupInsertOpError(#[source] ExecutionError),
    #[error("error when execute a write operation")]
    CsqlWriteOpError(#[source] ExecutionError),
    #[error("error when execute a read operation")]
    CsqlReadOpError(#[source] ExecutionError),
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
