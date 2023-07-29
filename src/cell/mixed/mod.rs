mod simple;

pub use simple::SimpleMixedCellFactory;

use crate::{model, scheduler::cell::Cell};
use anyhow::Result;
use std::sync::Arc;

pub struct MixedCell {
    inner: Arc<dyn Cell>,
}

impl MixedCell {
    pub fn new(inner: Arc<dyn Cell>) -> Arc<MixedCell> {
        Arc::new(MixedCell { inner })
    }
}

#[tonic::async_trait]
impl Cell for MixedCell {
    async fn assign(
        self: Arc<Self>,
        request_id: String,
        timestamp: u64,
    ) -> Result<model::Assignment> {
        self.inner.clone().assign(request_id, timestamp).await
    }
    async fn idle(
        self: Arc<Self>,
        assignment: model::Assignment,
        idle_reason: model::IdleReason,
    ) -> Result<()> {
        self.inner.clone().idle(assignment, idle_reason).await
    }
}
