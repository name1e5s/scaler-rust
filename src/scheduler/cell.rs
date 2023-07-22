use crate::{model, platform::Platform};
use anyhow::{anyhow, Result};
use dashmap::DashMap;
use std::sync::Arc;

use super::Scheduler;

#[tonic::async_trait]
pub trait Cell: Send + Sync + 'static {
    fn new(meta: model::Meta, client: Arc<Platform>) -> Arc<Self>;
    async fn assign(
        self: Arc<Self>,
        request_id: String,
        timestamp: u64,
    ) -> Result<model::Assignment>;
    async fn idle(
        self: Arc<Self>,
        assignment: model::Assignment,
        idle_reason: model::IdleReason,
    ) -> Result<()>;
}

pub struct CellScheduler<T>
where
    T: Cell,
{
    client: Arc<Platform>,
    cell_map: DashMap<String, Arc<T>>,
}

impl<T> CellScheduler<T>
where
    T: Cell,
{
    pub fn new(client: Platform) -> CellScheduler<T> {
        CellScheduler {
            client: Arc::new(client),
            cell_map: DashMap::new(),
        }
    }

    fn try_get_key(&self, key: &str) -> Option<Arc<T>> {
        self.cell_map.get(key).map(|v| v.clone())
    }

    fn get_or_insert(&self, meta: &model::Meta) -> Arc<T> {
        let key = &meta.key;
        if let Some(cell) = self.cell_map.get(key) {
            cell.clone()
        } else {
            let cell = T::new(meta.clone(), self.client.clone());
            self.cell_map.insert(meta.key.clone(), cell.clone());
            cell
        }
    }
}

#[tonic::async_trait]
impl<T> Scheduler for CellScheduler<T>
where
    T: Cell,
{
    async fn assign(
        &self,
        request_id: String,
        timestamp: u64,
        meta: Option<model::Meta>,
    ) -> Result<model::Assignment> {
        let meta = meta.ok_or_else(|| anyhow!("meta is None"))?;

        self.get_or_insert(&meta)
            .assign(request_id, timestamp)
            .await
    }
    async fn idle(
        &self,
        assignment: Option<model::Assignment>,
        idle_reason: Option<model::IdleReason>,
    ) -> Result<()> {
        let assignment = assignment.ok_or_else(|| anyhow!("assignment is None"))?;
        let idle_reason = idle_reason.ok_or_else(|| anyhow!("idle_reason is None"))?;

        self.try_get_key(&assignment.meta_key)
            .ok_or_else(|| anyhow!("meta_key not found at idle"))?
            .idle(assignment, idle_reason)
            .await
    }
}
