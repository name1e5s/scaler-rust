//! Naive policy cell: just pool them, and add a GC.
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

const OUTDATED_SLOT_GC_SEC: u64 = 300;
const OUTDATED_SLOT_GC_INTERVAL_SEC: u64 = 5;
const OUTDATED_SLOT_LEN: usize = 5;

struct SlotInfo {
    slot: Slot,
    last_used: u64,
}

impl SlotInfo {
    fn update_last_used(&mut self) {
        self.last_used = util::current_timestamp();
    }

    fn time_since_last_used(&self) -> u64 {
        util::current_timestamp() - self.last_used
    }
}

#[derive(Default, Clone)]
struct MetaInfo {
    expected_execute_time: f64,
}

impl MetaInfo {
    fn update_with(&mut self, info: &SlotInfo) {
        self.expected_execute_time =
            self.expected_execute_time * 0.7 + info.time_since_last_used() as f64 * 0.3;
    }
}

pub struct NaiveCell {
    meta: model::Meta,
    meta_info: Mutex<MetaInfo>,
    client: Arc<Platform>,
    free_slots: Mutex<VecDeque<SlotInfo>>,
    free_slots_notify: Notify,
    // request_id -> slot
    occupied_slots: DashMap<String, SlotInfo>,
    expected_release_count: AtomicI64,
}

impl NaiveCell {
    fn new(meta: model::Meta, client: Arc<Platform>) -> NaiveCell {
        NaiveCell {
            meta,
            client,
            meta_info: Mutex::new(MetaInfo::default()),
            free_slots: Mutex::new(VecDeque::new()),
            free_slots_notify: Notify::new(),
            occupied_slots: DashMap::new(),
            expected_release_count: AtomicI64::new(0),
        }
    }

    fn get_meta_info(&self) -> MetaInfo {
        self.meta_info.lock().clone()
    }

    fn try_get_free_slot(&self) -> Option<SlotInfo> {
        let mut free_slots = self.free_slots.lock();
        free_slots.pop_back()
    }

    fn put_free_slot_fresh(&self, mut info: SlotInfo) {
        info.update_last_used();
        self.put_free_slot(info)
    }

    fn put_free_slot(&self, info: SlotInfo) {
        let mut free_slots = self.free_slots.lock();
        free_slots.push_back(info);
        drop(free_slots);
        self.free_slots_notify.notify_one();
    }

    fn update_expected_release_count(&self) -> i64 {
        let expected_execute_time = self.get_meta_info().expected_execute_time;
        let mut val = { self.free_slots.lock().len() as i64 };
        for item in &self.occupied_slots {
            if (item.value().time_since_last_used() as f64) < expected_execute_time {
                val += 1;
            }
        }
        self.expected_release_count.store(val, Ordering::SeqCst);
        val
    }

    async fn wait_for_free_slot(self: Arc<Self>) -> Option<SlotInfo> {
        self.free_slots_notify.notified().await;
        self.try_get_free_slot()
    }

    async fn create_free_slot(self: Arc<Self>) -> Result<SlotInfo> {
        let slot = self
            .client
            .create_slot(
                model::ResourceConfig {
                    memory_in_megabytes: self.meta.memory_in_mb,
                },
                self.meta.clone(),
            )
            .await?;
        Ok(SlotInfo { slot, last_used: 0 })
    }

    fn create_slot_in_background<Fut>(self: Arc<Self>, fut: Fut)
    where
        Fut: Future<Output = Result<SlotInfo>> + Send + 'static,
    {
        tokio::spawn(async move {
            match fut.await {
                Ok(slot) => {
                    self.put_free_slot_fresh(slot);
                }
                Err(e) => {
                    log::error!("create slot failed: {:?}", e);
                }
            }
        });
    }

    async fn get_or_create_free_slot(self: Arc<Self>) -> Result<SlotInfo> {
        if let Some(slot) = self.try_get_free_slot() {
            self.expected_release_count.fetch_sub(1, Ordering::SeqCst);
            return Ok(slot);
        }

        if self.expected_release_count.fetch_sub(1, Ordering::SeqCst) > 0 {
            if let Some(slot) = timeout(
                Duration::from_secs(OUTDATED_SLOT_GC_INTERVAL_SEC),
                self.clone().wait_for_free_slot(),
            )
            .await
            .ok()
            .flatten()
            {
                return Ok(slot);
            }
        }

        match select(
            Box::pin(self.clone().wait_for_free_slot()),
            Box::pin(self.clone().create_free_slot()),
        )
        .await
        {
            Either::Left((slot, fut)) => {
                if let Some(slot) = slot {
                    self.clone().create_slot_in_background(fut);
                    Ok(slot)
                } else {
                    fut.await
                }
            }
            Either::Right((slot, _)) => slot,
        }
    }

    fn select_outdated_slots(&self) -> Vec<SlotInfo> {
        let mut to_free = Vec::new();
        let mut free_slots = self.free_slots.lock();
        while let Some(info) = free_slots.front() {
            if free_slots.len() < OUTDATED_SLOT_LEN
                && info.time_since_last_used() < OUTDATED_SLOT_GC_SEC
            {
                break;
            }
            to_free.push(free_slots.pop_front().expect("peeked"));
        }
        to_free
    }

    async fn destroy_slot(&self, slot: &SlotInfo) -> Result<()> {
        let raw_slot = &slot.slot;
        self.client
            .destroy_slot(&raw_slot.instance_id, &raw_slot.id)
            .await
    }

    async fn destroy_outdated_slots(self: Arc<Self>) -> usize {
        let to_free = self.select_outdated_slots();
        let mut result = 0;
        let mut futs = vec![];
        for slot in to_free {
            let cloned_self = self.clone();
            let fut = tokio::spawn(async move {
                if let Err(e) = cloned_self.destroy_slot(&slot).await {
                    log::error!("destroy slot failed: {:?}, will retry", e);
                    cloned_self.put_free_slot(slot);
                    return None;
                }
                Some(())
            });
            futs.push(fut);
        }
        for fut in futs {
            if fut.await.ok().flatten().is_some() {
                result += 1;
            }
        }
        result
    }
}

#[tonic::async_trait]
impl Cell for NaiveCell {
    fn new(meta: model::Meta, client: Arc<Platform>) -> Arc<Self> {
        let cell = Arc::new(NaiveCell::new(meta, client));
        let weak_cell = Arc::downgrade(&cell);
        tokio::spawn(async move {
            while let Some(cell) = weak_cell.upgrade() {
                let key = cell.meta.key.clone();
                let count = cell.clone().destroy_outdated_slots().await;
                if count != 0 {
                    log::info!("cell {}, destroyd {} outdated slots", key, count);
                }
                let count = cell.update_expected_release_count();
                if count != 0 {
                    log::info!("cell {}, expected_release_count {}", key, count);
                }
                tokio::time::sleep(tokio::time::Duration::from_secs(
                    OUTDATED_SLOT_GC_INTERVAL_SEC,
                ))
                .await;
            }
        });
        cell
    }
    async fn assign(
        self: Arc<Self>,
        request_id: String,
        _timestamp: u64,
    ) -> Result<model::Assignment> {
        let mut slot = self.clone().get_or_create_free_slot().await?;
        slot.update_last_used();
        let instance_id = slot.slot.instance_id.clone();
        self.occupied_slots.insert(request_id.clone(), slot);
        Ok(model::Assignment {
            request_id,
            instance_id,
            meta_key: self.meta.key.clone(),
        })
    }
    async fn idle(
        self: Arc<Self>,
        assignment: model::Assignment,
        idle_reason: model::IdleReason,
    ) -> Result<()> {
        let slot = self
            .occupied_slots
            .remove(&assignment.request_id)
            .ok_or_else(|| anyhow!("occupied slot, request_id: {}", assignment.request_id))?
            .1;
        {
            self.meta_info.lock().update_with(&slot);
        }
        if idle_reason.need_destroy {
            self.destroy_slot(&slot).await
        } else {
            self.put_free_slot_fresh(slot);
            Ok(())
        }
    }
}
