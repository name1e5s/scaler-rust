use crate::model;
use crate::platform::Platform;
use anyhow::{Result, anyhow};
use dashmap::DashMap;

#[tonic::async_trait]
pub trait Scheduler {
    async fn assign(
        &self,
        request_id: String,
        timestamp: u64,
        meta: Option<model::Meta>,
    ) -> Result<model::Assignment>;
    async fn idle(
        &self,
        assignment: Option<model::Assignment>,
        idle_reason: Option<model::IdleReason>,
    ) -> Result<()>;
}

pub struct DirectScheduler {
    client: Platform,
    // request_id -> instance_id
    map: DashMap<String, model::Slot>,
}

impl DirectScheduler {
    pub async fn new() -> Result<DirectScheduler> {
        Ok(DirectScheduler {
            client: Platform::new().await?,
            map: DashMap::new(),
        })
    }
}

#[tonic::async_trait]
impl Scheduler for DirectScheduler {
    async fn assign(
        &self,
        request_id: String,
        _: u64,
        meta: Option<model::Meta>,
    ) -> Result<model::Assignment> {
        let meta = meta.ok_or_else(|| anyhow!("meta is None"))?;
        let meta_key = meta.key.clone();
        let slot = self.client.create_slot(model::ResourceConfig {
            memory_in_megabytes: meta.memory_in_mb,
        }, meta).await?;
        self.map.insert(request_id.clone(), slot.clone());
        Ok(model::Assignment {
            request_id,
            meta_key,
            instance_id: slot.instance_id,
        })
    }
    async fn idle(
        &self,
        assignment: Option<model::Assignment>,
        _: Option<model::IdleReason>,
    ) -> Result<()> {
        let assignment = assignment.ok_or_else(|| anyhow!("assignment is None"))?;
        let slot = self.map.get(&assignment.request_id).unwrap().clone();
        self.client.destroy_slot(&slot.instance_id, &slot.id).await?;
        Ok(())
    }
}
