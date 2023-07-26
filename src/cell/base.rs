use crate::{
    model::{self, Slot},
    platform::Platform,
    scheduler::cell::Cell,
    util,
};
use anyhow::{anyhow, Result};
use crossbeam_utils::atomic::AtomicCell;
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
    time::{Duration, Instant},
};
use tokio::{sync::Notify, time::timeout_at};
use crate::cell::metric::{SlotMetric, CellMetric};

pub struct SlotWithMetric {
    slot: Slot,
    metric: SlotMetric,
}

pub struct BaseCell {
    meta: model::Meta,
    client: Arc<Platform>,
    free_slots: Mutex<VecDeque<SlotWithMetric>>,
    free_slots_notify: Notify,
    // request_id -> slot
    occupied_slots: DashMap<String, SlotWithMetric>,
    cell_metric: CellMetric,
}


impl BaseCell {
    fn new(meta: model::Meta, client: Arc<Platform>) -> BaseCell {
        BaseCell {
            meta,
            client,
            free_slots: Mutex::new(VecDeque::new()),
            free_slots_notify: Notify::new(),
            occupied_slots: DashMap::new(),
            cell_metric: CellMetric::new(),
        }
    }

    fn try_get_free_slot(&self) -> Option<SlotWithMetric> {
        let mut free_slots = self.free_slots.lock();
        free_slots.pop_back()
    }

    fn put_free_slot_fresh(&self, mut info: SlotWithMetric) {
        info.metric.update_last_used();
        self.put_free_slot(info)
    }

    fn put_free_slot(&self, info: SlotWithMetric) {
        let mut free_slots = self.free_slots.lock();
        free_slots.push_back(info);
        drop(free_slots);
        self.free_slots_notify.notify_one();
    }

    async fn wait_for_free_slot(self: Arc<Self>) -> Option<SlotWithMetric> {
        self.free_slots_notify.notified().await;
        self.try_get_free_slot()
    }

    async fn destroy_slot(&self, slot: &SlotWithMetric) -> Result<()> {
        let raw_slot = &slot.slot;
        self.client
            .destroy_slot(&raw_slot.instance_id, &raw_slot.id)
            .await
    }
}