use crate::{
    cell::metric::{CellMetric, SlotMetric},
    model::{self, Slot},
    platform::Platform,
};
use anyhow::Result;
use dashmap::DashMap;
use parking_lot::Mutex;
use std::{collections::VecDeque, sync::Arc};
use tokio::sync::Notify;

pub struct SlotWithMetric {
    pub slot: Slot,
    pub metric: SlotMetric,
}

pub struct BaseCell {
    pub meta: model::Meta,
    pub client: Arc<Platform>,
    pub free_slots: Mutex<VecDeque<SlotWithMetric>>,
    pub free_slots_notify: Notify,
    // request_id -> slot
    pub occupied_slots: DashMap<String, SlotWithMetric>,
    pub cell_metric: CellMetric,
}

impl BaseCell {
    pub fn new(meta: model::Meta, client: Arc<Platform>) -> BaseCell {
        BaseCell {
            meta,
            client,
            free_slots: Mutex::new(VecDeque::new()),
            free_slots_notify: Notify::new(),
            occupied_slots: DashMap::new(),
            cell_metric: CellMetric::new(),
        }
    }

    pub fn try_get_free_slot(&self) -> Option<SlotWithMetric> {
        let mut free_slots = self.free_slots.lock();
        free_slots.pop_back()
    }

    pub fn put_free_slot_fresh(&self, mut info: SlotWithMetric) {
        info.metric.update_last_used();
        self.put_free_slot(info)
    }

    pub fn put_free_slot(&self, info: SlotWithMetric) {
        let mut free_slots = self.free_slots.lock();
        free_slots.push_back(info);
        drop(free_slots);
        self.free_slots_notify.notify_one();
    }

    pub async fn wait_for_free_slot(self: Arc<Self>) -> Option<SlotWithMetric> {
        self.free_slots_notify.notified().await;
        self.try_get_free_slot()
    }

   pub  async fn create_free_slot(self: Arc<Self>) -> Result<SlotWithMetric> {
        let slot = self
            .client
            .create_slot(
                model::ResourceConfig {
                    memory_in_megabytes: self.meta.memory_in_mb,
                },
                self.meta.clone(),
            )
            .await?;
        Ok(SlotWithMetric {
            slot,
            metric: SlotMetric::new(),
        })
    }

    pub async fn destroy_slot(&self, slot: &SlotWithMetric) -> Result<()> {
        let raw_slot = &slot.slot;
        self.client
            .destroy_slot(&raw_slot.instance_id, &raw_slot.id)
            .await
    }
}
