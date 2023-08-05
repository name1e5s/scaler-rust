//! Memory based policy cell: just pool them, and add a GC.
use crate::{
    model::{self, Slot},
    platform::Platform,
    scheduler::cell::{Cell, CellFactory},
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
    cmp::{max, min},
    collections::VecDeque,
    sync::{
        atomic::{AtomicI64, AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use tokio::{sync::Notify, time::timeout_at};

const OUTDATED_SLOT_GC_SEC: u64 = 15;
const OUTDATED_SLOT_GC_BASE_MEM_MB: u64 = 128;
const OUTDATED_SLOT_GC_INTERVAL_SEC: u64 = 5;

struct SlotInfo {
    slot: Slot,
    last_used: u64,
}

impl SlotInfo {
    fn update_last_used(&mut self) {
        self.last_used = util::current_timestamp();
    }

    fn time_between_last_used(&self, current: u64) -> u64 {
        current - self.last_used
    }

    fn time_since_last_used(&self) -> u64 {
        self.time_between_last_used(util::current_timestamp())
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

pub struct MemoryCell {
    meta: model::Meta,
    meta_info: Mutex<MetaInfo>,
    client: Arc<Platform>,
    free_slots: Mutex<VecDeque<SlotInfo>>,
    free_slots_notify: Notify,
    // request_id -> slot
    occupied_slots: DashMap<String, SlotInfo>,
    expected_release_count: AtomicI64,
    expected_deadline: AtomicCell<Instant>,
    assign_count: AtomicI64,
    memory_limit: AtomicU64,
}

impl MemoryCell {
    fn new(meta: model::Meta, client: Arc<Platform>) -> MemoryCell {
        MemoryCell {
            meta,
            client,
            meta_info: Mutex::new(MetaInfo::default()),
            free_slots: Mutex::new(VecDeque::new()),
            free_slots_notify: Notify::new(),
            occupied_slots: DashMap::new(),
            expected_release_count: AtomicI64::new(0),
            expected_deadline: AtomicCell::new(Instant::now()),
            assign_count: AtomicI64::new(0),
            memory_limit: AtomicU64::new(OUTDATED_SLOT_GC_BASE_MEM_MB * OUTDATED_SLOT_GC_SEC),
        }
    }

    fn update_expected_deadline(&self, interval: Duration) {
        let deadline = Instant::now() + interval;
        self.expected_deadline.store(deadline);
    }

    fn update_memory_limit(&self, hint: i64) {
        let current = self.memory_limit.load(Ordering::SeqCst);
        let limit_base = OUTDATED_SLOT_GC_BASE_MEM_MB * OUTDATED_SLOT_GC_SEC;
        let limit_max = self.meta.memory_in_mb * OUTDATED_SLOT_GC_SEC * 2;
        let mut new = current;
        if hint > 10 {
            new = current + limit_base * (hint as f64 / 5.).ln().ceil() as u64;
        } else if hint < -10 {
            new = current - limit_base * (-hint as f64 / 5.).ln().ceil() as u64;
        }
        new = max(new, limit_base);
        new = min(new, limit_max);
        self.memory_limit.store(new, Ordering::SeqCst);
    }

    fn is_expired(&self, time_s: u64) -> bool {
        time_s * self.meta.memory_in_mb > self.memory_limit.load(Ordering::SeqCst)
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
        let current = util::current_timestamp();
        let expected_execute_time = self.get_meta_info().expected_execute_time;
        let mut val = { self.free_slots.lock().len() as i64 };
        for item in &self.occupied_slots {
            if (item.value().time_between_last_used(current) + OUTDATED_SLOT_GC_INTERVAL_SEC) as f64
                > expected_execute_time
            {
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
            if let Some(slot) = timeout_at(
                self.expected_deadline.load().into(),
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
        let current = util::current_timestamp();
        let mut to_free = Vec::new();
        let mut free_slots = self.free_slots.lock();
        while let Some(info) = free_slots.front() {
            if !self.is_expired(info.time_between_last_used(current)) {
                break;
            }
            to_free.push(free_slots.pop_front().expect("peeked"));
        }
        to_free
    }

    fn select_n_slots(&self, mut n: i64) -> Vec<SlotInfo> {
        let mut to_free = Vec::new();
        let mut free_slots = self.free_slots.lock();
        while n > 0 {
            if let Some(_) = free_slots.front() {
                to_free.push(free_slots.pop_front().expect("peeked"));
            } else {
                break;
            }
            n -= 1;
        }
        to_free
    }

    async fn destroy_slot(&self, slot: &SlotInfo) -> Result<()> {
        let raw_slot = &slot.slot;
        self.client
            .destroy_slot(&raw_slot.instance_id, &raw_slot.id)
            .await
    }

    async fn destroy_outdated_slots(self: Arc<Self>, to_free: Vec<SlotInfo>) -> usize {
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
impl Cell for MemoryCell {
    async fn assign(
        self: Arc<Self>,
        request_id: String,
        _timestamp: u64,
    ) -> Result<model::Assignment> {
        self.assign_count.fetch_add(1, Ordering::SeqCst);
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

pub struct MemoryCellFactory;

impl CellFactory<MemoryCell> for MemoryCellFactory {
    fn new(&self, meta: model::Meta, client: Arc<Platform>) -> Arc<MemoryCell> {
        let cell = Arc::new(MemoryCell::new(meta, client));
        let weak_cell = Arc::downgrade(&cell);
        tokio::spawn(async move {
            let mut last_period_assign_count = 0;
            while let Some(cell) = weak_cell.upgrade() {
                let assign_count = cell.assign_count.swap(0, Ordering::SeqCst);
                let old_release_count = cell.expected_release_count.load(Ordering::SeqCst);
                cell.update_memory_limit(assign_count - last_period_assign_count);
                let interval = Duration::from_secs(OUTDATED_SLOT_GC_INTERVAL_SEC);
                cell.update_expected_deadline(interval);
                let key = cell.meta.key.clone();
                let to_free = {
                    let mut vec = cell.clone().select_outdated_slots();
                    let more_free = cell
                        .clone()
                        .select_n_slots(old_release_count / 2 - vec.len() as i64);
                    vec.extend(more_free);
                    vec
                };
                let cloned_cell = cell.clone();
                let cloned_key = key.clone();
                tokio::spawn(async move {
                    let count = cloned_cell.destroy_outdated_slots(to_free).await;
                    if count != 0 {
                        log::info!("cell {}, destroyd {} outdated slots", cloned_key, count);
                    }
                });
                let count = cell.update_expected_release_count();
                if count != 0 {
                    log::info!("cell {}, expected_release_count {}", key, count);
                }
                last_period_assign_count = assign_count;
                tokio::time::sleep(interval).await;
            }
        });
        cell
    }
}
