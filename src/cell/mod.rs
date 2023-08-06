mod base;
mod freeless;
mod memory;
mod metric;
mod naive;
mod naive_set_1;
mod naive_set_2;
mod stork;

pub mod mixed;

pub use base::BaseCell;
pub use freeless::{FreelessCell, FreelessCellFactory};
pub use memory::{MemoryCell, MemoryCellFactory};
pub use metric::CellMetric;
pub use naive::{NaiveCell, NaiveCellFactory};
pub use naive_set_1::{NaiveSet1Cell, NaiveSet1CellFactory};
pub use naive_set_2::{NaiveSet2Cell, NaiveSet2CellFactory};

use crate::{
    model,
    platform::Platform,
    scheduler::cell::{Cell, CellFactory},
};
use anyhow::Result;
use dashmap::DashMap;
use std::sync::Arc;

pub struct DirectCell {
    meta: model::Meta,
    client: Arc<Platform>,
    // request_id -> instance_id
    map: DashMap<String, model::Slot>,
}

impl DirectCell {
    fn new(meta: model::Meta, client: Arc<Platform>) -> DirectCell {
        DirectCell {
            meta,
            client,
            map: DashMap::new(),
        }
    }
}

#[tonic::async_trait]
impl Cell for DirectCell {
    async fn assign(self: Arc<Self>, request_id: String, _: u64) -> Result<model::Assignment> {
        let meta_key = self.meta.key.clone();
        let slot = self
            .client
            .create_slot(
                model::ResourceConfig {
                    memory_in_megabytes: self.meta.memory_in_mb,
                },
                self.meta.clone(),
            )
            .await?;
        self.map.insert(request_id.clone(), slot.clone());
        Ok(model::Assignment {
            request_id,
            meta_key,
            instance_id: slot.instance_id,
        })
    }
    async fn idle(
        self: Arc<Self>,
        assignment: model::Assignment,
        _: model::IdleReason,
    ) -> Result<()> {
        let slot = self.map.get(&assignment.request_id).unwrap().clone();
        self.client
            .destroy_slot(&slot.instance_id, &slot.id)
            .await?;
        Ok(())
    }
}

pub struct DirectCellFactory;

impl CellFactory<DirectCell> for DirectCellFactory {
    fn new(&self, meta: model::Meta, client: Arc<Platform>) -> Arc<DirectCell> {
        Arc::new(DirectCell::new(meta, client))
    }
}
