//! stork style cell

use std::{sync::Arc, time::Duration};

use crate::{
    cell::BaseCell,
    model,
    platform::Platform,
    scheduler::cell::{Cell, CellFactory},
};
use anyhow::Result;
use futures::{
    future::{select, Either},
    Future,
};
use tokio::time::timeout;

use super::base::SlotWithMetric;

const STORK_SCHEDULE_INTERVAL_SEC: u64 = 5;

pub struct StorkCell {
    base: Arc<BaseCell>,
}

impl StorkCell {
    fn new(meta: model::Meta, client: Arc<Platform>) -> StorkCell {
        StorkCell {
            base: Arc::new(BaseCell::new(meta, client)),
        }
    }

    fn is_empty(&self) -> bool {
        self.base.occupied_slots.is_empty() && self.base.free_slots.lock().is_empty()
    }

    fn create_slot_in_background<Fut>(self: Arc<Self>, fut: Fut)
    where
        Fut: Future<Output = Result<SlotWithMetric>> + Send + 'static,
    {
        tokio::spawn(async move {
            match fut.await {
                Ok(mut slot) => {
                    slot.metric.update_last_used();
                    self.base.put_free_slot_fresh(slot);
                }
                Err(e) => {
                    log::error!("create slot failed: {:?}", e);
                }
            }
        });
    }

    async fn get_or_create_free_slot(self: Arc<Self>) -> Result<SlotWithMetric> {
        // force new slot when empty
        if self.is_empty() {
            return self.base.clone().create_free_slot().await;
        }
        // fast path
        if let Some(slot) = self.base.try_get_free_slot() {
            return Ok(slot);
        }
        // wait for scheduler
        if let Some(slot) = timeout(
            Duration::from_secs(STORK_SCHEDULE_INTERVAL_SEC),
            self.base.clone().wait_for_free_slot(),
        )
        .await
        .ok()
        .flatten()
        {
            return Ok(slot);
        }
        // wait and create
        match select(
            Box::pin(self.clone().base.clone().wait_for_free_slot()),
            Box::pin(self.clone().base.clone().create_free_slot()),
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
}

#[tonic::async_trait]
impl Cell for StorkCell {
    async fn assign(
        self: Arc<Self>,
        request_id: String,
        _timestamp: u64,
    ) -> Result<model::Assignment> {
        let mut slot = {
            let _ctr = self.base.cell_metric.schedule_time_recoder();
            self.clone().get_or_create_free_slot().await?
        };
        slot.metric.update_last_used();
        let instance_id = slot.slot.instance_id.clone();
        self.base.occupied_slots.insert(request_id.clone(), slot);
        self.base.cell_metric.update_assign_request_count();
        Ok(model::Assignment {
            request_id,
            instance_id,
            meta_key: self.base.meta.key.clone(),
        })
    }
    async fn idle(
        self: Arc<Self>,
        _assignment: model::Assignment,
        _idle_reason: model::IdleReason,
    ) -> Result<()> {
        Ok(())
    }
}

pub struct StorkCellFactory;

impl CellFactory<StorkCell> for StorkCellFactory {
    fn new(&self, meta: model::Meta, client: Arc<Platform>) -> Arc<StorkCell> {
        let cell = Arc::new(StorkCell::new(meta, client));
        cell
    }
}
