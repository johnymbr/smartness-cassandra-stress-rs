use std::sync::atomic::{AtomicU64, Ordering};

use crate::config::smarteness_settings::SmartnessSettings;

const ORDER_TYPE: Ordering = Ordering::Relaxed;

pub struct MetricsStore {
    pub latency_vec: Vec<f64>,
    pub drained_vec: Vec<f64>,
    pub windowed_vec: Vec<f64>,
    pub windowed_size: i32,
    pub drain_interval_min: u64,
    pub start_time: std::time::Instant,
    pub last_tick: AtomicU64,
}

impl MetricsStore {
    pub fn new(smartness_settings: &SmartnessSettings) -> Self {
        let now = std::time::Instant::now();

        MetricsStore {
            latency_vec: Vec::new(),
            drained_vec: Vec::new(),
            windowed_vec: Vec::with_capacity(
                smartness_settings.metrics_window_size.unwrap() as usize
            ),
            windowed_size: smartness_settings.metrics_window_size.unwrap(),
            drain_interval_min: smartness_settings.metrics_drain_interval_minutes.unwrap() as u64,
            start_time: now,
            last_tick: AtomicU64::new(now.elapsed().as_secs() as u64),
        }
    }

    pub fn add_latency(&mut self, latency: f64, old_count: u64) {
        self.latency_vec.push(latency);
        self.add_to_drainec_vec(latency);
        self.add_to_windowed_vec(latency, old_count);
    }

    pub fn get_latency_vec_clone(&self) -> Vec<f64> {
        self.latency_vec.clone()
    }

    pub fn get_drained_vec_clone(&self) -> Vec<f64> {
        self.drained_vec.clone()
    }

    pub fn get_windowed_vec_clone(&self) -> Vec<f64> {
        self.windowed_vec.clone()
    }

    fn add_to_drainec_vec(&mut self, latency: f64) {
        // Multiple threads could read the same `old_tick`...
        let old_tick = self.last_tick.load(ORDER_TYPE);
        let new_tick = self.start_time.elapsed().as_secs() as u64;
        let elapsed = new_tick - old_tick;

        if elapsed > self.drain_interval_min * 60 {
            let new_interval_start_tick = new_tick - elapsed % (self.drain_interval_min * 60);
            // But then only one will succeed in the following COMPARE EXCHANGE operation.
            if self
                .last_tick
                .compare_exchange(old_tick, new_interval_start_tick, ORDER_TYPE, ORDER_TYPE)
                .is_ok()
            {
                self.drained_vec.clear();
            }
        }

        self.drained_vec.push(latency);
    }

    fn add_to_windowed_vec(&mut self, latency: f64, count: u64) {
        let position = count % self.windowed_size as u64;
        if count < self.windowed_size as u64 {
            self.windowed_vec.push(latency);
        } else {
            self.windowed_vec[position as usize] = latency;
        }
    }
}
