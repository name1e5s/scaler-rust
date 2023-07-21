use anyhow::Result;
use scaler::rpc::scaler_server::ScalerServer;
use scaler::server::ScalerImpl;
use scaler::scheduler::DirectScheduler;
use tonic::transport::Server;

#[tokio::main]
async fn main() -> Result<()> {
    let addr = "127.0.0.1:9001".parse()?;
    let scheduler = DirectScheduler::new().await?;
    let server = ScalerImpl::new(scheduler);

    Server::builder()
        .add_service(ScalerServer::new(server))
        .serve(addr)
        .await?;

    Ok(())
}
