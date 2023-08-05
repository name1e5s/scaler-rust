//! Freeless cell, expected to got a 50
use crate::{
    model::{self, Slot},
    platform::Platform,
    scheduler::cell::{Cell, CellFactory},
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
    collections::BinaryHeap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{sync::Notify, time::timeout};

const OUTDATED_SLOT_GC_INTERVAL_SEC: u64 = 60;

struct SlotInfo {
    slot: Slot,
    last_used: u64,
}

impl PartialEq for SlotInfo {
    fn eq(&self, other: &Self) -> bool {
        self.last_used == other.last_used
    }
}

impl Eq for SlotInfo {}

impl PartialOrd for SlotInfo {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.last_used.partial_cmp(&other.last_used)
    }
}

impl Ord for SlotInfo {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.last_used.cmp(&other.last_used)
    }
}

pub struct FreelessCell {
    meta: model::Meta,
    client: Arc<Platform>,
    outdated_gc_sec: u64,
    free_slots: Mutex<BinaryHeap<SlotInfo>>,
    free_slots_notify: Notify,
    // request_id -> slot
    occupied_slots: DashMap<String, SlotInfo>,
    assign_count: AtomicU64,
}

impl FreelessCell {
    fn new(meta: model::Meta, client: Arc<Platform>, outdated_gc_sec: u64) -> FreelessCell {
        FreelessCell {
            meta,
            client,
            outdated_gc_sec,
            free_slots: Mutex::new(BinaryHeap::new()),
            free_slots_notify: Notify::new(),
            occupied_slots: DashMap::new(),
            assign_count: AtomicU64::new(0),
        }
    }

    fn try_get_free_slot(&self) -> Option<SlotInfo> {
        let mut free_slots = self.free_slots.lock();
        free_slots.pop()
    }

    fn put_free_slot_fresh(&self, mut info: SlotInfo) {
        info.last_used = util::current_timestamp();
        self.put_free_slot(info)
    }

    fn put_free_slot(&self, info: SlotInfo) {
        let mut free_slots = self.free_slots.lock();
        free_slots.push(info);
        drop(free_slots);
        self.free_slots_notify.notify_one();
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
            return Ok(slot);
        }

        if let Some(slot) = timeout(Duration::from_secs(3), self.clone().wait_for_free_slot())
            .await
            .ok()
            .flatten()
        {
            return Ok(slot);
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

    fn select_outdated_slots(&self, all: bool) -> Vec<SlotInfo> {
        let current = util::current_timestamp();
        let mut to_free = Vec::new();
        let mut free_slots = self.free_slots.lock();
        while let Some(info) = free_slots.peek() {
            if !all && current - info.last_used < self.outdated_gc_sec {
                break;
            }
            to_free.push(free_slots.pop().expect("peeked"));
        }
        to_free
    }

    async fn destroy_slot(&self, slot: &SlotInfo) -> Result<()> {
        let raw_slot = &slot.slot;
        self.client
            .destroy_slot(&raw_slot.instance_id, &raw_slot.id)
            .await
    }

    fn destroy_outdated_slots(self: Arc<Self>, all: bool) -> usize {
        let to_free = self.select_outdated_slots(all);
        let result = to_free.len();
        tokio::spawn(async move {
            for slot in to_free {
                if let Err(e) = self.destroy_slot(&slot).await {
                    log::error!("destroy slot failed: {:?}, will retry", e);
                    self.put_free_slot(slot);
                }
            }
        });
        result
    }
}

#[tonic::async_trait]
impl Cell for FreelessCell {
    async fn assign(
        self: Arc<Self>,
        request_id: String,
        _timestamp: u64,
    ) -> Result<model::Assignment> {
        self.assign_count.fetch_add(1, Ordering::SeqCst);
        let slot = self.clone().get_or_create_free_slot().await?;
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
        if idle_reason.need_destroy {
            self.destroy_slot(&slot).await
        } else {
            self.put_free_slot_fresh(slot);
            Ok(())
        }
    }
}

pub struct FreelessCellFactory;

impl CellFactory<FreelessCell> for FreelessCellFactory {
    fn new(&self, meta: model::Meta, client: Arc<Platform>) -> Arc<FreelessCell> {
        let cell = Arc::new(FreelessCell::new(meta, client, 1800));
        for _ in 0..200 {
            cell.clone()
                .create_slot_in_background(cell.clone().create_free_slot());
        }
        let weak_cell = Arc::downgrade(&cell);
        tokio::spawn(async move {
            let mut all_zero_count = 0;
            while let Some(cell) = weak_cell.upgrade() {
                if cell.assign_count.swap(0, Ordering::SeqCst) == 0 {
                    all_zero_count += 1;
                } else {
                    all_zero_count = 0;
                }
                let key = cell.meta.key.clone();
                let count = cell.destroy_outdated_slots(all_zero_count > 6);
                if count != 0 {
                    log::info!("cell {}, destroyd {} outdated slots", key, count);
                }
                tokio::time::sleep(tokio::time::Duration::from_secs(
                    OUTDATED_SLOT_GC_INTERVAL_SEC,
                ))
                .await;
            }
        });
        cell
    }
}
