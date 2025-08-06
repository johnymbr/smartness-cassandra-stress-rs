use std::{
    fs::{self, File},
    path::Path,
    sync::Arc,
};

use chrono::Utc;
use csv::Writer;
use scylla::client::session::Session;
use tokio::{
    runtime::Runtime,
    time::{self},
};

use crate::{config::smarteness_settings::SmartnessSettings, error::SmartnessError};

pub fn create_runtime(
    smartness_settings: &SmartnessSettings,
    write_session: Arc<Session>,
    read_session: Arc<Session>,
) -> Result<Runtime, SmartnessError> {
    let file_name = Utc::now().format("%Y%m%d_%H%M%S%3f").to_string();

    println!(
        "Metrics file: Write = {}_w.csv / Read = {}_r.csv ",
        file_name, file_name
    );

    let mut write_file = create_file(format!(
        "{}/{}_w.csv",
        &smartness_settings.metrics_dir.clone(),
        file_name
    ))?;

    let mut read_file = create_file(format!(
        "{}/{}_r.csv",
        &smartness_settings.metrics_dir.clone(),
        file_name
    ))?;

    let metrics_runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .thread_name("cassandra-metrics-pool")
        .enable_all()
        .build()
        .map_err(SmartnessError::MetricsRuntimeBuildError)?;

    metrics_runtime.spawn(async move {
        let mut interval = time::interval(time::Duration::from_secs(1));

        loop {
            write_metrics(&mut write_file, &write_session);
            write_metrics(&mut read_file, &read_session);

            interval.tick().await;
        }
    });

    Ok(metrics_runtime)
}

fn create_file(file_name: String) -> Result<Writer<File>, SmartnessError> {
    let metrics_path = Path::new(&file_name);

    if let Some(parent) = metrics_path.parent() {
        fs::create_dir_all(parent).map_err(SmartnessError::MetricsParentPathCreateError)?;
    }

    let file = File::create(metrics_path).map_err(SmartnessError::MetricsFileCreateError)?;
    let mut wtr = csv::Writer::from_writer(file);
    wtr.write_record(&[
        "timestamp",
        "queries_num",
        "queries_requested",
        "errors_occurred",
        "iter_errors_occurred",
        "average_latency",
        "99_9_latency_percentile",
        "mean_rate",
        "one_minute_rate",
        "five_minute_rate",
        "fifteen_minute_rate",
        "total_connections",
        "connection_timeouts",
        "requests_timeouts",
        "snapshot_min",
        "snapshot_max",
        "snapshot_mean",
        "snapshot_std_dev",
        "snapshot_median",
        "snapshot_75th_percentile",
        "snapshot_95th_percentile",
        "snapshot_98th_percentile",
        "snapshot_99th_percentile",
        "snapshot_99_9th_percentile",
    ])
    .map_err(SmartnessError::MetricsFileWriteHeadersError)?;

    Ok(wtr)
}

fn write_metrics(csv_file: &mut Writer<File>, session: &Session) {
    let metrics = session.get_metrics();

    let mut metric_values = Vec::<String>::new();
    metric_values.push(Utc::now().timestamp().to_string());
    metric_values.push(metrics.get_queries_num().to_string());
    metric_values.push(metrics.get_queries_iter_num().to_string());
    metric_values.push(metrics.get_errors_num().to_string());
    metric_values.push(metrics.get_errors_iter_num().to_string());
    metric_values.push(metrics.get_latency_avg_ms().unwrap_or(0).to_string());
    metric_values.push(
        metrics
            .get_latency_percentile_ms(99.9)
            .unwrap_or(0)
            .to_string(),
    );
    metric_values.push(metrics.get_mean_rate().to_string());
    metric_values.push(metrics.get_one_minute_rate().to_string());
    metric_values.push(metrics.get_five_minute_rate().to_string());
    metric_values.push(metrics.get_fifteen_minute_rate().to_string());
    metric_values.push(metrics.get_total_connections().to_string());
    metric_values.push(metrics.get_connection_timeouts().to_string());
    metric_values.push(metrics.get_request_timeouts().to_string());

    if let Ok(snapshot) = metrics.get_snapshot() {
        metric_values.push(snapshot.min.to_string());
        metric_values.push(snapshot.max.to_string());
        metric_values.push(snapshot.mean.to_string());
        metric_values.push(snapshot.stddev.to_string());
        metric_values.push(snapshot.median.to_string());
        metric_values.push(snapshot.percentile_75.to_string());
        metric_values.push(snapshot.percentile_95.to_string());
        metric_values.push(snapshot.percentile_98.to_string());
        metric_values.push(snapshot.percentile_99.to_string());
        metric_values.push(snapshot.percentile_99_9.to_string());
    } else {
        metric_values.push("-1".to_owned());
        metric_values.push("-1".to_owned());
        metric_values.push("-1".to_owned());
        metric_values.push("-1".to_owned());
        metric_values.push("-1".to_owned());
        metric_values.push("-1".to_owned());
        metric_values.push("-1".to_owned());
        metric_values.push("-1".to_owned());
        metric_values.push("-1".to_owned());
        metric_values.push("-1".to_owned());
    }

    if let Err(error) = csv_file.write_record(metric_values) {
        println!("Error when write a metrics record: {}", error);
    }

    if let Err(error) = csv_file.flush() {
        println!("Error when flush a metrics record: {}", error);
    }
}
