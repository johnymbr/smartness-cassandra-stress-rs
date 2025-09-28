use std::sync::{
    Mutex,
    atomic::{AtomicU64, Ordering},
};

use statrs::statistics::{Data, Distribution, Max, Min, OrderStatistics};

use crate::{config::smarteness_settings::SmartnessSettings, metrics::metrics_store::MetricsStore};

const ORDER_TYPE: Ordering = Ordering::Relaxed;

pub struct MetricSnapshot {
    pub count: u64,
    pub min: f64,
    pub max: f64,
    pub mean: f64,
    pub std_dev: f64,
    pub median: f64,
    pub p_75th: f64,
    pub p_95th: f64,
    pub p_98th: f64,
    pub p_99th: f64,
    pub p_99_9th: f64,
    pub d_min: f64,
    pub d_max: f64,
    pub d_mean: f64,
    pub d_std_dev: f64,
    pub d_median: f64,
    pub d_p_75th: f64,
    pub d_p_95th: f64,
    pub d_p_98th: f64,
    pub d_p_99th: f64,
    pub d_p_99_9th: f64,
    pub w_min: f64,
    pub w_max: f64,
    pub w_mean: f64,
    pub w_std_dev: f64,
    pub w_median: f64,
    pub w_p_75th: f64,
    pub w_p_95th: f64,
    pub w_p_98th: f64,
    pub w_p_99th: f64,
    pub w_p_99_9th: f64,
}

pub struct MetricsManager {
    pub count: AtomicU64,
    pub metrics_store: Mutex<MetricsStore>,
    pub disabled: bool,
}

impl MetricsManager {
    pub fn new(smartness_settings: &SmartnessSettings) -> Self {
        MetricsManager {
            count: AtomicU64::new(0),
            metrics_store: Mutex::new(MetricsStore::new(smartness_settings)),
            disabled: smartness_settings.no_metrics.unwrap(),
        }
    }

    pub fn add_latency(&self, latency: f64) {
        if !self.disabled {
            let old_count = self.count.fetch_add(1, ORDER_TYPE);

            let mut metrics_store = self.metrics_store.lock().unwrap();
            metrics_store.add_latency(latency, old_count);
        }
    }

    /// copy values from all vectors and generate snapshot...
    pub fn generate_snapshot(&self) -> MetricSnapshot {
        let count = self.count.load(ORDER_TYPE);

        let (latency_vec, drained_vec, windowed_vec) = self.get_latencies();

        // latency_vec
        let mut data_latency_vec = Data::new(latency_vec);

        // drained_vec
        let mut data_drained_vec = Data::new(drained_vec);

        // windowed_vec
        let mut data_windowed_vec = Data::new(windowed_vec);

        // get min, max, mean, std_dev, median, 75perc, 95perc, 98perc, 99perc, 99_9perc
        MetricSnapshot {
            count,
            min: if data_latency_vec.min().is_nan() {
                0.0
            } else {
                data_latency_vec.min()
            },
            max: if data_latency_vec.max().is_nan() {
                0.0
            } else {
                data_latency_vec.max()
            },
            mean: if data_latency_vec.mean().unwrap_or(0.0).is_nan() {
                0.0
            } else {
                data_latency_vec.mean().unwrap_or(0.0)
            },
            std_dev: if data_latency_vec.std_dev().unwrap_or(0.0).is_nan() {
                0.0
            } else {
                data_latency_vec.std_dev().unwrap_or(0.0)
            },
            median: if data_latency_vec.median().is_nan() {
                0.0
            } else {
                data_latency_vec.median()
            },
            p_75th: if data_latency_vec.quantile(0.75).is_nan() {
                0.0
            } else {
                data_latency_vec.quantile(0.75)
            },
            p_95th: if data_latency_vec.quantile(0.95).is_nan() {
                0.0
            } else {
                data_latency_vec.quantile(0.95)
            },
            p_98th: if data_latency_vec.quantile(0.98).is_nan() {
                0.0
            } else {
                data_latency_vec.quantile(0.98)
            },
            p_99th: if data_latency_vec.quantile(0.99).is_nan() {
                0.0
            } else {
                data_latency_vec.quantile(0.99)
            },
            p_99_9th: if data_latency_vec.quantile(0.999).is_nan() {
                0.0
            } else {
                data_latency_vec.quantile(0.999)
            },
            d_min: if data_drained_vec.min().is_nan() {
                0.0
            } else {
                data_drained_vec.min()
            },
            d_max: if data_drained_vec.max().is_nan() {
                0.0
            } else {
                data_drained_vec.max()
            },
            d_mean: if data_drained_vec.mean().unwrap_or(0.0).is_nan() {
                0.0
            } else {
                data_drained_vec.mean().unwrap_or(0.0)
            },
            d_std_dev: if data_drained_vec.std_dev().unwrap_or(0.0).is_nan() {
                0.0
            } else {
                data_drained_vec.std_dev().unwrap_or(0.0)
            },
            d_median: if data_drained_vec.median().is_nan() {
                0.0
            } else {
                data_drained_vec.median()
            },
            d_p_75th: if data_drained_vec.quantile(0.75).is_nan() {
                0.0
            } else {
                data_drained_vec.quantile(0.75)
            },
            d_p_95th: if data_drained_vec.quantile(0.95).is_nan() {
                0.0
            } else {
                data_drained_vec.quantile(0.95)
            },
            d_p_98th: if data_drained_vec.quantile(0.98).is_nan() {
                0.0
            } else {
                data_drained_vec.quantile(0.98)
            },
            d_p_99th: if data_drained_vec.quantile(0.99).is_nan() {
                0.0
            } else {
                data_drained_vec.quantile(0.99)
            },
            d_p_99_9th: if data_drained_vec.quantile(0.999).is_nan() {
                0.0
            } else {
                data_drained_vec.quantile(0.999)
            },
            w_min: if data_windowed_vec.min().is_nan() {
                0.0
            } else {
                data_windowed_vec.min()
            },
            w_max: if data_windowed_vec.max().is_nan() {
                0.0
            } else {
                data_windowed_vec.max()
            },
            w_mean: if data_windowed_vec.mean().unwrap_or(0.0).is_nan() {
                0.0
            } else {
                data_windowed_vec.mean().unwrap_or(0.0)
            },
            w_std_dev: if data_windowed_vec.std_dev().unwrap_or(0.0).is_nan() {
                0.0
            } else {
                data_windowed_vec.std_dev().unwrap_or(0.0)
            },
            w_median: if data_windowed_vec.median().is_nan() {
                0.0
            } else {
                data_windowed_vec.median()
            },
            w_p_75th: if data_windowed_vec.quantile(0.75).is_nan() {
                0.0
            } else {
                data_windowed_vec.quantile(0.75)
            },
            w_p_95th: if data_windowed_vec.quantile(0.95).is_nan() {
                0.0
            } else {
                data_windowed_vec.quantile(0.95)
            },
            w_p_98th: if data_windowed_vec.quantile(0.98).is_nan() {
                0.0
            } else {
                data_windowed_vec.quantile(0.98)
            },
            w_p_99th: if data_windowed_vec.quantile(0.99).is_nan() {
                0.0
            } else {
                data_windowed_vec.quantile(0.99)
            },
            w_p_99_9th: if data_windowed_vec.quantile(0.999).is_nan() {
                0.0
            } else {
                data_windowed_vec.quantile(0.999)
            },
        }
    }

    fn get_latencies(&self) -> (Vec<f64>, Vec<f64>, Vec<f64>) {
        let metrics_store = self.metrics_store.lock().unwrap();
        (
            metrics_store.get_latency_vec_clone(),
            metrics_store.get_drained_vec_clone(),
            metrics_store.get_windowed_vec_clone(),
        )
    }
}
