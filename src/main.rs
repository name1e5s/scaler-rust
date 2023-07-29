use anyhow::Result;
use env_logger::{Builder, Env};
use scaler::{
    cell::mixed::SimpleMixedCellFactory, platform::Platform, rpc::scaler_server::ScalerServer,
    scheduler::cell::CellScheduler, server::ScalerImpl,
};
use tonic::transport::Server;

#[tokio::main]
async fn main() -> Result<()> {
    Builder::from_env(Env::default().default_filter_or("debug")).init();

    let addr = "127.0.0.1:9001".parse()?;
    let platform = Platform::new()?;
    let scheduler = CellScheduler::new(SimpleMixedCellFactory, platform);
    let server = ScalerImpl::new(scheduler);

    Server::builder()
        .add_service(ScalerServer::new(server))
        .serve(addr)
        .await?;

    Ok(())
}
