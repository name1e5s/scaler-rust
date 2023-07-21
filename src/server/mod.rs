use crate::rpc::scaler_server::Scaler;
use crate::rpc::{self, AssignReply, AssignRequest, IdleReply, IdleRequest};
use crate::scheduler::Scheduler;
use tonic::{Request, Response, Status};

pub type TonicResult<T> = std::result::Result<Response<T>, Status>;

fn error_to_tonic<E: ToString>(e: E) -> Status {
    Status::internal(e.to_string())
}

pub struct ScalerImpl<T> {
    scheduler: T,
}

impl<T> ScalerImpl<T> {
    pub fn new(scheduler: T) -> ScalerImpl<T> {
        ScalerImpl { scheduler }
    }
}

#[tonic::async_trait]
impl<T> Scaler for ScalerImpl<T>
where
    T: Scheduler + Send + Sync + 'static,
{
    async fn assign(&self, req: Request<AssignRequest>) -> TonicResult<AssignReply> {
        let req = req.into_inner();
        self.scheduler
            .assign(req.request_id, req.timestamp, req.meta_data.map(Into::into))
            .await
            .map(|v| {
                Response::new(AssignReply {
                    status: rpc::Status::Ok.into(),
                    assigment: Some(v),
                    ..Default::default()
                })
            })
            .map_err(error_to_tonic)
    }
    async fn idle(&self, req: Request<IdleRequest>) -> TonicResult<IdleReply> {
        let req = req.into_inner();
        self.scheduler
            .idle(req.assigment, req.result)
            .await
            .map_err(error_to_tonic)?;
        Ok(Response::new(IdleReply {
            status: rpc::Status::Ok.into(),
            ..Default::default()
        }))
    }
}
