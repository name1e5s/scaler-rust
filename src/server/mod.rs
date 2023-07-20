pub mod platform;

use crate::rpc::scaler_server::Scaler;
use crate::rpc::{AssignReply, AssignRequest, IdleReply, IdleRequest};
use tonic::{Request, Response, Status};

type TonicResult<T> = std::result::Result<Response<T>, Status>;

pub struct ScalerImpl;

#[tonic::async_trait]
impl Scaler for ScalerImpl {
    async fn assign(&self, _: Request<AssignRequest>) -> TonicResult<AssignReply> {
        println!("assign");
        Ok(Response::new(AssignReply {
            ..Default::default()
        }))
    }
    async fn idle(&self, _: Request<IdleRequest>) -> TonicResult<IdleReply> {
        println!("idle");
        Ok(Response::new(IdleReply {
            ..Default::default()
        }))
    }
}
