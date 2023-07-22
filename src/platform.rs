use crate::{
    config::global_config,
    model,
    rpc::{self, platform_client::PlatformClient},
    server::TonicResult,
};

use anyhow::{anyhow, bail, Result};
use std::future::Future;
use tonic::transport::{Channel, Endpoint};
use uuid::Uuid;

pub struct Platform {
    inner: PlatformClient<Channel>,
}

impl Platform {
    pub async fn new() -> Result<Platform> {
        let config = global_config();
        let channel = Endpoint::try_from(config.client_addr)?.connect_lazy();
        let inner = PlatformClient::new(channel);
        Ok(Platform { inner })
    }

    async fn run_with_retry<'a, 'b, F, G, Fut, Req, Resp>(
        &'a self,
        f: F,
        retry: G,
        req: Req,
    ) -> TonicResult<Resp>
    where
        'b: 'a,
        F: Fn(&'a Platform, Req) -> Fut,
        G: Fn(&Resp) -> bool,
        Fut: Future<Output = TonicResult<Resp>> + 'b,
        Req: Clone,
    {
        let mut count = 0;
        loop {
            let resp = f(self, req.clone()).await?;
            if count < 3 && retry(resp.get_ref()) {
                count += 1;
                continue;
            } else {
                return Ok(resp);
            }
        }
    }

    pub async fn create_slot(&self, config: model::ResourceConfig, meta: model::Meta) -> Result<model::Slot> {
        let instance_id = Uuid::new_v4().to_string();
        let resp = self
            .run_with_retry(
                |platform, req| async {
                    let mut client = platform.inner.clone();
                    client.create_slot(req).await
                },
                |resp| resp.status == rpc::Status::Throttle.into(),
                rpc::CreateSlotRequest {
                    request_id: instance_id.clone(),
                    resource_config: Some(rpc::ResourceConfig {
                        memory_in_megabytes: config.memory_in_megabytes,
                    }),
                },
            )
            .await?
            .into_inner();
        if resp.status() != rpc::Status::Ok {
            bail!(
                "create_slot failed, resp: {} {}",
                resp.status,
                resp.error_message
            );
        }
        let slot = resp.slot.ok_or_else(|| anyhow!("slot is None"))?;
        let resource_config = slot
            .resource_config
            .ok_or_else(|| anyhow!("resource_config is None"))?;
        let init = self.init(&instance_id, &slot.id, meta).await?;
        Ok(model::Slot {
            instance_id,
            id: slot.id,
            resource_config: resource_config.into(),
            create_time: slot.create_time,
            create_duration_in_ms: slot.create_duration_in_ms,
            init_time: init.create_time,
            init_duration_in_ms: init.init_duration_in_ms,
        })
    }

    pub async fn destroy_slot(&self, instance_id: &str, id: &str) -> Result<()> {
        let resp = self
            .run_with_retry(
                |platform, req| async {
                    let mut client = platform.inner.clone();
                    client.destroy_slot(req).await
                },
                |resp| resp.status == rpc::Status::Throttle.into(),
                rpc::DestroySlotRequest {
                    request_id: instance_id.to_string(),
                    id: id.to_string(),
                    reason: "".to_string(),
                },
            )
            .await?
            .into_inner();
        if resp.status() != rpc::Status::Ok {
            bail!(
                "destroy_slot failed, resp: {} {}",
                resp.status,
                resp.error_message
            );
        }
        Ok(())
    }

    async fn init(
        &self,
        instance_id: &str,
        id: &str,
        meta: model::Meta,
    ) -> Result<rpc::InitReply> {
        let resp = self
            .run_with_retry(
                |platform, req| async {
                    let mut client = platform.inner.clone();
                    client.init(req).await
                },
                |resp| resp.status == rpc::Status::Throttle.into(),
                rpc::InitRequest {
                    request_id: instance_id.to_string(),
                    instance_id: instance_id.to_string(),
                    slot_id: id.to_string(),
                    meta_data: Some(rpc::Meta {
                        key: meta.key,
                        runtime: meta.runtime,
                        timeout_in_secs: meta.timeout_in_secs,
                        memory_in_mb: meta.memory_in_mb,
                    }),
                },
            )
            .await?
            .into_inner();
        if resp.status() != rpc::Status::Ok {
            bail!(
                "init failed, resp: {} {}",
                resp.status,
                resp.error_message
            );
        }
        Ok(resp)
    }
}
