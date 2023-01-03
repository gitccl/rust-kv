use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use crate::{KvEngine, KvError, Request, Response, Result, ThreadPool};
use futures_util::{SinkExt, TryStreamExt};
use log::{error, info};
use tokio::{
    net::{TcpListener, TcpStream},
    select, signal,
    sync::oneshot,
};
use tokio_serde::{formats::SymmetricalJson, SymmetricallyFramed};
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};

/// The server of a key value store.
pub struct KvServer<E: KvEngine, T: ThreadPool> {
    engine: E,
    pool: T,
}

impl<E: KvEngine, T: ThreadPool> KvServer<E, T> {
    /// create a `KvServer` with a given storage engine.
    pub fn new(engine: E, pool: T) -> KvServer<E, T> {
        KvServer { engine, pool }
    }

    /// Run the server listening on the given address
    pub fn run(&mut self, addr: String, is_stop: Arc<AtomicBool>) -> Result<()> {
        let rt = tokio::runtime::Runtime::new()?;
        rt.block_on(async {
            select! {
                res = async {
                    let listener = TcpListener::bind(addr).await?;
                    loop {
                        let (client, client_addr) = listener.accept().await?;
                        if is_stop.load(Ordering::SeqCst) {
                            break;
                        }
                        let engine = self.engine.clone();
                        let pool = self.pool.clone();
                        tokio::spawn(async move {
                            if let Err(err) = handle_request(engine, client, pool).await {
                                error!("failed to handle request from {}: {}", client_addr, err);
                            }
                        });
                    }
                    info!("server is stopping...");
                    Ok::<_, std::io::Error>(())
                } => {
                    if let Err(err) = res {
                        error!("server error: {}", err);
                    }
                }
                _ = signal::ctrl_c() => {
                    info!("receive ctrl-c, server is stopping...");
                }
            };
        });
        info!("server exited");
        Ok(())
    }
}

async fn handle_request<E: KvEngine, T: ThreadPool>(
    engine: E,
    stream: TcpStream,
    pool: T,
) -> Result<()> {
    let client_addr = stream.peer_addr()?;
    info!("handle request from {}", client_addr);

    let (read_half, write_half) = stream.into_split();
    let frame_reader = FramedRead::new(read_half, LengthDelimitedCodec::new());
    let frame_writer = FramedWrite::new(write_half, LengthDelimitedCodec::new());

    let mut req_reader = SymmetricallyFramed::<_, Request, _>::new(
        frame_reader,
        SymmetricalJson::<Request>::default(),
    );
    let mut resp_writer = SymmetricallyFramed::<_, Response, _>::new(
        frame_writer,
        SymmetricalJson::<Response>::default(),
    );

    loop {
        let request = match req_reader.try_next().await? {
            Some(req) => req,
            None => {
                info!("client {} closed", client_addr);
                break;
            }
        };

        let (tx, rx) = oneshot::channel();

        let mut engine = engine.clone();
        pool.spawn(move || {
            let resp = match request {
                Request::Get(key) => match engine.get(key) {
                    Ok(value) => Response::Ok(value),
                    Err(err) => Response::Err(format!("{}", err)),
                },
                Request::Set(key, value) => match engine.set(key, value) {
                    Ok(_) => Response::Ok(None),
                    Err(err) => Response::Err(format!("{}", err)),
                },
                Request::Remove(key) => match engine.remove(key) {
                    Ok(_) => Response::Ok(None),
                    Err(err) => Response::Err(format!("{}", err)),
                },
            };
            if tx.send(resp).is_err() {
                error!("Receiving end is dropped");
            }
        });

        let resp = rx
            .await
            .map_err(|e| KvError::StringError(format!("{}", e)))?;
        resp_writer.send(resp).await?;
    }

    Ok(())
}
