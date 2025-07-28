use std::{fs::File, io::BufReader, path::Path};

use serde::{Deserialize, Serialize};

use crate::error::SmartnessError;

#[derive(Serialize, Deserialize, Debug)]
pub struct SmartnessSettings {
    /// quantity of workers to process tasks
    pub workers: Option<usize>,
    /// path to load csv dataset
    pub dataset_path: String,
    /// dir to save metrics
    pub metrics_dir: String,
    /// quantity of cycles to run tests
    pub cycles: Option<i64>,
    /// time in minutes to run tests
    pub running_time: Option<i64>,
    /// quantity of tasks that will start in a second, default 100
    pub tasks_per_sec: Option<i32>,
    /// percentage of read tasks, default 0.1
    pub reads_rate: Option<f32>,
    /// read task interval, computed using tasks_per_sec and reads_rate
    pub reads_interval: Option<i64>,
    /// task interval, computed using tasks_per_sec and 1000ms
    pub task_interval: Option<i64>,
    /// quantity of columns that we will use, if -1 all columns will be used.
    pub cols_qty: Option<i64>,
    /// host to connect to cassandra
    pub cassandra_host: Option<String>,
    /// port to connect to cassandra
    pub cassandra_port: Option<i32>,
    /// username to connect to cassandra
    pub cassandra_username: Option<String>,
    /// password to connect to cassandra
    pub cassandra_password: Option<String>,
    /// script to use in write tasks
    pub write_script: Option<String>,
    /// script to use in read tasks
    pub read_script: Option<String>,
    /// if true, we will use script to create schema, dropt table and create table
    pub startup_enabled: Option<bool>,
    ///script to create a schema on cassandra
    pub startup_create_schema_script: Option<String>,
    ///script to a table on cassandra
    pub startup_drop_table_script: Option<String>,
    ///script to a table on cassandra
    pub startup_create_table_script: Option<String>,
    ///if true, we will use quantity of warmup operations to insert some records
    pub warmup_enabled: Option<bool>,
    /// warmup operations to insert some records
    pub warmup_qty_ops: Option<i64>,
}

// TODO change this name to smartnesssettings
impl SmartnessSettings {
    pub fn new(workload_path: String) -> Result<Self, SmartnessError> {
        let workload_path = Path::new(&workload_path);
        if !workload_path.exists() {
            return Err(SmartnessError::WorkloadFileDoesNotExist);
        }

        let workload_file =
            File::open(workload_path).map_err(SmartnessError::WorkloadFileOpenError)?;
        let reader = BufReader::new(workload_file);

        // Read the JSON contents of the file as an instance of `User`.
        let mut smartness_config: SmartnessSettings = serde_json::from_reader(reader)
            .map_err(SmartnessError::WorkloadFileDeserializationError)?;

        if smartness_config.cycles.is_none() && smartness_config.running_time.is_none() {
            return Err(SmartnessError::CyclesOrRunningTimeRequired);
        }

        if smartness_config.cassandra_host.is_none() {
            return Err(SmartnessError::CassandraHostRequired);
        }

        if smartness_config.cassandra_port.is_none() {
            return Err(SmartnessError::CassandraPortRequired);
        }

        if smartness_config.cassandra_username.is_none()
            || smartness_config.cassandra_password.is_none()
        {
            return Err(SmartnessError::CassandraUsernameAndPasswordAreRequired);
        }

        if smartness_config.tasks_per_sec.is_none() {
            smartness_config.tasks_per_sec = Some(100);
        }

        if smartness_config.reads_rate.is_none() {
            smartness_config.reads_rate = Some(0.1);
        }

        let reads_ops = (smartness_config.tasks_per_sec.unwrap() as f32
            * smartness_config.reads_rate.unwrap())
        .floor();

        let writes_ops = smartness_config.tasks_per_sec.unwrap() as f32 - reads_ops;

        let reads_interval = if reads_ops != 0.0 {
            (writes_ops / reads_ops).floor()
        } else {
            reads_ops
        };

        smartness_config.reads_interval = Some(reads_interval as i64);
        smartness_config.task_interval =
            Some((1_000_000_000.0 / smartness_config.tasks_per_sec.unwrap() as f32).floor() as i64);

        if smartness_config.cols_qty.is_none() {
            smartness_config.cols_qty = Some(-1);
        }

        if smartness_config.write_script.is_none() {
            return Err(SmartnessError::WriteScriptRequired);
        }

        if smartness_config.read_script.is_none() {
            return Err(SmartnessError::ReadScriptRequired);
        }

        if smartness_config.startup_enabled.is_some() && smartness_config.startup_enabled.unwrap() {
            if smartness_config.startup_create_schema_script.is_none()
                || smartness_config.startup_drop_table_script.is_none()
                || smartness_config.startup_create_table_script.is_none()
            {
                return Err(SmartnessError::StartuptScriptsRequired);
            }
        }

        if smartness_config.warmup_enabled.is_some()
            && smartness_config.warmup_enabled.unwrap()
            && smartness_config.warmup_qty_ops.is_none()
        {
            return Err(SmartnessError::WarmupQtyOpsRequired);
        }

        // println!("Config: {:?}", smartness_config);

        Ok(smartness_config)
    }
}
