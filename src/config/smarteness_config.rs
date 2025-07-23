use std::{fs::File, io::BufReader, path::Path};

use serde::{Deserialize, Serialize};

use crate::error::SmartnessError;

#[derive(Serialize, Deserialize, Debug)]
pub struct SmartnessConfig {
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
    /// quantity of tasks that will start in a second
    pub tasks_per_sec: Option<i64>,
    /// percentage of read tasks
    pub reads_rate: Option<f32>,
}

impl SmartnessConfig {
    pub fn new(workload_path: String) -> Result<Self, SmartnessError> {
        let workload_path = Path::new(&workload_path);
        if !workload_path.exists() {
            return Err(SmartnessError::WorkloadFileDoesNotExist);
        }

        let workload_file =
            File::open(workload_path).map_err(SmartnessError::WorkloadFileOpenError)?;
        let reader = BufReader::new(workload_file);

        // Read the JSON contents of the file as an instance of `User`.
        let mut smartness_config: SmartnessConfig = serde_json::from_reader(reader)
            .map_err(SmartnessError::WorkloadFileDeserializationError)?;

        println!("Config: {:?}", smartness_config);

        if smartness_config.cycles.is_none() && smartness_config.running_time.is_none() {
            return Err(SmartnessError::CyclesOrRunningTimeRequired);
        }

        if smartness_config.tasks_per_sec.is_none() {
            smartness_config.tasks_per_sec = Some(100);
        }

        if smartness_config.reads_rate.is_none() {
            smartness_config.reads_rate = Some(0.1);
        }

        Ok(smartness_config)
    }
}
