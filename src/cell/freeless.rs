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
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{
    sync::Notify,
    task::JoinHandle,
    time::{sleep, timeout},
};

const OUTDATED_SLOT_GC_INTERVAL_SEC: u64 = 60;
const FACTOR_MAX: u64 = 3900;
const FACTORS: &[(&str, u64)] = &[
    ("roles1", 3100),
    ("rolebindings1", 3900),
    ("certificatesigningrequests1", 3300),
    ("roles2", 3200),
    ("rolebindings2", 3900),
    ("certificatesigningrequests2", 3300),
];

fn req_per_min_to_slots(meta_key: &str, req: u64) -> u64 {
    let factor = {
        let mut result = FACTOR_MAX;
        for (key, factor) in FACTORS {
            if meta_key == *key {
                result = *factor;
            }
        }
        result
    };
    (req * factor) / (60 * 1000)
}

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
    pre_allocate: u64,
    try_allocate: AtomicU64,
    init_done: AtomicBool,
}

impl FreelessCell {
    fn new(
        meta: model::Meta,
        client: Arc<Platform>,
        outdated_gc_sec: u64,
        pre_allocate: u64,
    ) -> FreelessCell {
        FreelessCell {
            meta,
            client,
            outdated_gc_sec,
            pre_allocate,
            free_slots: Mutex::new(BinaryHeap::new()),
            free_slots_notify: Notify::new(),
            occupied_slots: DashMap::new(),
            assign_count: AtomicU64::new(0),
            try_allocate: AtomicU64::new(0),
            init_done: AtomicBool::new(false),
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

    fn create_slot_in_background<Fut>(self: Arc<Self>, fut: Fut) -> JoinHandle<()>
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
        })
    }

    async fn loop_wait_for_free_slot(self: Arc<Self>) -> Option<SlotInfo> {
        loop {
            if self.init_done.load(Ordering::Relaxed) {
                return None;
            }
            if let Some(slot) = self.try_get_free_slot() {
                return Some(slot);
            }
            if let Some(slot) = self.clone().wait_for_free_slot().await {
                return Some(slot);
            }
            sleep(Duration::from_secs(1)).await;
        }
    }

    async fn get_or_create_free_slot(self: Arc<Self>) -> Result<SlotInfo> {
        if let Some(slot) = self.try_get_free_slot() {
            return Ok(slot);
        }

        if !self.init_done.load(Ordering::Relaxed)
            && self.try_allocate.fetch_add(1, Ordering::SeqCst) < self.pre_allocate * 6
        {
            if let Some(slot) = timeout(
                Duration::from_secs(40),
                self.clone().loop_wait_for_free_slot(),
            )
            .await
            .ok()
            .flatten()
            {
                return Ok(slot);
            }
        }

        if let Some(slot) = timeout(Duration::from_secs(2), self.clone().wait_for_free_slot())
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
        let key = meta.key.clone();
        let expected_max_req = if key.ends_with("1") { 750 } else { 1075 };
        let pre_allocate = req_per_min_to_slots(&meta.key, expected_max_req);
        let outdate_gc_sec = if key.ends_with("1") { 1300 } else { 1500 };
        let cell = Arc::new(FreelessCell::new(
            meta,
            client,
            outdate_gc_sec,
            pre_allocate,
        ));
        let mut handles = Vec::new();
        for _ in 0..pre_allocate {
            let h = cell
                .clone()
                .create_slot_in_background(cell.clone().create_free_slot());
            handles.push(h);
        }
        let cloned_cell = cell.clone();
        tokio::spawn(async move {
            for h in handles {
                let _ = h.await;
            }
            cloned_cell.init_done.store(true, Ordering::SeqCst);
        });
        let weak_cell = Arc::downgrade(&cell);
        let all_zero_max = if key.ends_with("1") { 7 } else { 4 };
        tokio::spawn(async move {
            let mut all_zero_count = 0;
            while let Some(cell) = weak_cell.upgrade() {
                if cell.assign_count.swap(0, Ordering::SeqCst) == 0 {
                    all_zero_count += 1;
                } else {
                    all_zero_count = 0;
                }
                let key = cell.meta.key.clone();
                let count = cell.destroy_outdated_slots(all_zero_count > all_zero_max);
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
