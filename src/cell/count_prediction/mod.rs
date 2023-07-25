//! count prediction based cell

use crate::{
    model::{self, Slot},
    platform::Platform,
    scheduler::cell::Cell,
    util,
};
use anyhow::{anyhow, Result};
use dashmap::DashMap;
use futures::{
    future::{select, Either},
    Future,
};
use parking_lot::Mutex;
use std::{
    collections::VecDeque,
    sync::{
        atomic::{AtomicI64, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{sync::Notify, time::timeout};

pub struct CountPredictionSource {
    pub average_create_ms: u64,
    pub current_time_stamp: u64,
    pub last_free_slot_count: u64,
}

pub struct CountPredictionResult {
    pub assign_count: u64,
    pub idle_count: u64,
}

#[tonic::async_trait]
pub trait CountPredictionStrategy {
    async fn set_schedule_info(&mut self, interval_ms: u64);
    async fn predicate(&mut self, source: CountPredictionSource) -> CountPredictionResult;
    async fn learn(&mut self, result: CountPredictionResult);
    async fn update_metric(&mut self, predicted: CountPredictionResult, real: CountPredictionResult);
}

pub struct CountPredictionMetric {
    pub count: u64,
    pub average_create_ms: u64,
    pub last_free_slot_count: u64,
}

impl CountPredictionMetric {
    pub fn new() -> CountPredictionMetric {
        CountPredictionMetric {
            average_create_ms: 0,
            count: 0,
            last_free_slot_count: 0,
        }
    }

    pub fn update(&mut self, create_ms: u64, free_slot_count: u64) {
        if self.average_create_ms == 0 {
            self.average_create_ms = create_ms;
        } else {
            self.average_create_ms =
                (self.average_create_ms as f64 * 0.4 + create_ms as f64 * 0.6) as u64;
        }
        self.last_free_slot_count = free_slot_count;
    }

    pub fn get_prediction_source(&self) -> CountPredictionSource {
        CountPredictionSource {
            last_free_slot_count: self.last_free_slot_count,
            average_create_ms: self.average_create_ms,
            current_time_stamp: util::current_timestamp(),
        }
    }
}
