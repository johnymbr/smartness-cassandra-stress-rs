use chrono::Utc;
use tokio::{
    runtime::Runtime,
    time::{self},
};

use crate::{config::smarteness_config::SmartnessConfig, error::SmartnessError};

pub fn create_runtime(_smartness_settings: &SmartnessConfig) -> Result<Runtime, SmartnessError> {
    let metrics_runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .thread_name("cassandra-metrics-pool")
        .enable_all()
        .build()
        .map_err(SmartnessError::MetricsRuntimeBuildError)?;

    // TODO: here we need to change to consume Session metrics and save it on csv files...
    metrics_runtime.spawn(async move {
        let mut count = 0;
        let mut interval = time::interval(time::Duration::from_secs(1));
        loop {
            println!(
                "Writing metrics on csv file {} - Timestamp: {}",
                count,
                Utc::now().timestamp()
            );
            count += 1;

            interval.tick().await;
        }
    });

    Ok(metrics_runtime)
}
