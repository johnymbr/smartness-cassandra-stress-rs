use std::sync::Arc;

use chrono::Utc;
use scylla::client::session::Session;
use tokio::{
    runtime::Runtime,
    time::{self},
};

use crate::{config::smarteness_settings::SmartnessSettings, error::SmartnessError};

pub fn create_runtime(
    _smartness_settings: &SmartnessSettings,
    session: Arc<Session>,
) -> Result<Runtime, SmartnessError> {
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
            let metrics = session.get_metrics();
            println!(
                "Timestamp: {} | Count: {} [Queries Num: {}, Queries Requested: {}, Errors Occurred: {}, Iter Errors Occurred: {}, Average Latency: {}, 99.9 latency percentile: {},
                Mean Rate: {}, One Minute Rate: {}, Five Minute Rate: {}, Fifteen Minute Rate: {}, Total Connections: {}, Connection Timeouts: {}, Requests Timeouts: {}] ",
                Utc::now().timestamp_millis(),
                count,
                metrics.get_queries_num(), metrics.get_queries_iter_num(), metrics.get_errors_num(), metrics.get_errors_iter_num(),
                metrics.get_latency_avg_ms().unwrap_or(0), metrics.get_latency_percentile_ms(99.9).unwrap_or(0),
                metrics.get_mean_rate(), metrics.get_one_minute_rate(), metrics.get_five_minute_rate(), metrics.get_fifteen_minute_rate(),
                metrics.get_total_connections(), metrics.get_connection_timeouts(), metrics.get_request_timeouts()
            );

            if let Ok(snapshot) = metrics.get_snapshot() {
                println!("Timestamp: {} | Count: {} [Snapshot Min: {}, Snapshot Max: {}, Snapshot Mean: {}, Snapshot Std Dev: {}, Snapshot Median: {}, Snapshot 75th percentile: {}, Snapshot 95th percentile: {},
                    Snapshot 98th percentile: {}, Snapshot 99th percentile: {}, Snapshot 99.9th percentile: {}]",
                    Utc::now().timestamp_millis(), count, snapshot.min, snapshot.max, snapshot.mean, snapshot.stddev, snapshot.median, snapshot.percentile_75,
                    snapshot.percentile_95, snapshot.percentile_98, snapshot.percentile_99, snapshot.percentile_99_9
                );
            }

            count += 1;

            interval.tick().await;
        }
    });

    Ok(metrics_runtime)
}
