use std::sync::atomic::{AtomicU64, Ordering};

use crate::util;

#[derive(Debug)]
pub struct SlotMetric {
    pub create_at_ms: u64,
    pub last_used_at_ms: u64,
    pub total_used_time_ms: u64,
    pub total_used_time_ms_in_period: u64,
}

impl SlotMetric {
    pub fn new() -> SlotMetric {
        SlotMetric {
            create_at_ms: util::current_timestamp_ms(),
            last_used_at_ms: util::current_timestamp_ms(),
            total_used_time_ms: 0,
            total_used_time_ms_in_period: 0,
        }
    }

    pub fn period_reset(&mut self) {
        self.total_used_time_ms_in_period = 0;
    }

    pub fn update_total_used_time(&mut self) {
        self.total_used_time_ms += self.time_since_last_used();
        self.total_used_time_ms_in_period += self.time_since_last_used();
    }

    pub fn update_last_used(&mut self) {
        self.last_used_at_ms = util::current_timestamp_ms();
    }

    pub fn time_since_last_used(&self) -> u64 {
        util::current_timestamp_ms() - self.last_used_at_ms
    }

    pub fn time_since_create(&self) -> u64 {
        util::current_timestamp_ms() - self.create_at_ms
    }
}

pub struct ScheduleTimeRecoder<'a> {
    metric: &'a CellMetric,
    start: u64,
}

impl<'a> ScheduleTimeRecoder<'a> {
    pub fn new(metric: &'a CellMetric) -> ScheduleTimeRecoder<'a> {
        ScheduleTimeRecoder {
            metric,
            start: util::current_timestamp_ms(),
        }
    }
}

impl<'a> Drop for ScheduleTimeRecoder<'a> {
    fn drop(&mut self) {
        let duration = util::current_timestamp_ms() - self.start;
        self.metric.update_schedule_time(duration);
    }
}

#[derive(Debug)]
pub struct CellPerfScore {
    pub schedule: f64,
    pub resource: f64,
}

#[derive(Debug)]
pub struct CellMetric {
    pub total_slot_time_ms: AtomicU64,
    pub total_used_time_ms: AtomicU64,
    pub total_schedule_time_ms: AtomicU64,
    pub expected_execute_time_ms: AtomicU64,
    pub assign_request_count_in_period: AtomicU64,
    pub idle_request_count_in_period: AtomicU64,
}

impl CellMetric {
    pub fn new() -> CellMetric {
        CellMetric {
            total_slot_time_ms: AtomicU64::new(0),
            total_used_time_ms: AtomicU64::new(0),
            total_schedule_time_ms: AtomicU64::new(0),
            expected_execute_time_ms: AtomicU64::new(0),
            assign_request_count_in_period: AtomicU64::new(0),
            idle_request_count_in_period: AtomicU64::new(0),
        }
    }

    pub fn period_reset(&self) {
        self.assign_request_count_in_period
            .store(0, Ordering::SeqCst);
        self.idle_request_count_in_period.store(0, Ordering::SeqCst);
    }

    pub fn schedule_time_recoder(&self) -> ScheduleTimeRecoder {
        ScheduleTimeRecoder::new(self)
    }

    pub fn update_with_slot(&self, slot: &SlotMetric) {
        self.total_slot_time_ms
            .fetch_add(slot.time_since_create(), Ordering::SeqCst);
        self.total_used_time_ms
            .fetch_add(slot.total_used_time_ms, Ordering::SeqCst);
    }

    pub fn update_schedule_time(&self, duration: u64) {
        self.total_schedule_time_ms
            .fetch_add(duration, Ordering::SeqCst);
    }

    pub fn update_expected_execute_time(&self, duration: u64) {
        let _ =
            self.expected_execute_time_ms
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |old| {
                    let old = if old == 0 { duration } else { old };
                    let new = old as f64 * 0.7 + duration as f64 * 0.3;
                    Some(new as u64)
                });
    }

    pub fn update_assign_request_count(&self) {
        self.assign_request_count_in_period
            .fetch_add(1, Ordering::SeqCst);
    }

    pub fn update_idle_request_count(&self) {
        self.idle_request_count_in_period
            .fetch_add(1, Ordering::SeqCst);
    }
}
