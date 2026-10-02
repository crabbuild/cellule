use http_body_util::Full;
use hyper::{
    Method, Request, Response, StatusCode, body::Bytes, server::conn::http1, service::service_fn,
};
use hyper_util::rt::{TokioIo, TokioTimer};
use std::{
    io::Read as _,
    net::{Ipv4Addr, SocketAddr},
    path::PathBuf,
    sync::Arc,
    time::Duration,
};
use tokio::{net::TcpListener, sync::Semaphore, task::JoinSet};
use tokio_util::sync::CancellationToken;
#[derive(Debug, thiserror::Error)]
pub(crate) enum TargetError {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Join(#[from] tokio::task::JoinError),
}
fn read(path: &std::path::Path) -> std::io::Result<bool> {
    let mut bytes = Vec::new();
    std::fs::File::open(path)?.take(9).read_to_end(&mut bytes)?;
    match bytes.as_slice() {
        b"up\n" => Ok(true),
        b"down\n" => Ok(false),
        _ => Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "synthetic target state must be up or down",
        )),
    }
}
pub(crate) async fn bind(port: u16) -> Result<TcpListener, TargetError> {
    Ok(TcpListener::bind((Ipv4Addr::LOCALHOST, port)).await?)
}
pub(crate) fn address(listener: &TcpListener) -> Result<SocketAddr, TargetError> {
    Ok(listener.local_addr()?)
}
pub(crate) async fn serve(
    listener: TcpListener,
    path: PathBuf,
    cancel: CancellationToken,
) -> Result<(), TargetError> {
    let slots = Arc::new(Semaphore::new(4));
    let mut tasks = JoinSet::new();
    let mut failure = None;
    loop {
        tokio::select! {biased;
            ()=cancel.cancelled()=>break,
            Some(joined)=tasks.join_next(),if !tasks.is_empty()=>{if let Err(source)=joined{failure=Some(TargetError::Join(source));break;}},
            accepted=listener.accept()=>{
                let (stream,_)=match accepted{Ok(value)=>value,Err(source)=>{failure=Some(source.into());break;}};
                let permit=match slots.clone().try_acquire_owned(){Ok(value)=>value,Err(_)=>continue};
                let path=path.clone();
                tasks.spawn(async move{
                    let _permit=permit;
                    let mut builder=http1::Builder::new();builder.keep_alive(false).max_headers(16).max_buf_size(16<<10).timer(TokioTimer::new()).header_read_timeout(Duration::from_secs(2));
                    let handler=service_fn(move|request:Request<hyper::body::Incoming>|{
                        let path=path.clone();async move{
                            let (status,body)=if request.method()!=Method::GET||request.uri()!="/probe"{(StatusCode::NOT_FOUND,"unknown route\n")}else{
                                match tokio::task::spawn_blocking(move||read(&path)).await{
                                    Ok(Ok(true))=>(StatusCode::OK,"up\n"),
                                    Ok(Ok(false))=>(StatusCode::SERVICE_UNAVAILABLE,"down\n"),
                                    Ok(Err(source))=>{tracing::debug!(error=%source,"synthetic target state unavailable");(StatusCode::SERVICE_UNAVAILABLE,"state unavailable\n")},
                                    Err(source)=>{tracing::error!(error=%source,"synthetic target read task failed");(StatusCode::SERVICE_UNAVAILABLE,"state unavailable\n")},
                                }
                            };
                            let mut response=Response::new(Full::new(Bytes::from_static(body.as_bytes())));*response.status_mut()=status;
                            Ok::<_,std::convert::Infallible>(response)
                        }
                    });
                    if let Err(source)=builder.serve_connection(TokioIo::new(stream),handler).await{tracing::debug!(error=%source,"synthetic target connection closed");}
                });
            }
        }
    }
    drop(listener);
    while let Some(result) = tasks.join_next().await {
        if let Err(source) = result {
            if failure.is_none() {
                failure = Some(TargetError::Join(source));
            } else {
                tracing::error!(error=%source,"additional target drain failure");
            }
        }
    }
    match failure {
        Some(source) => Err(source),
        None => Ok(()),
    }
}
