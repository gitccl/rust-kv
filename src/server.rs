use std::{
    io::{BufWriter, Write},
    net::{TcpListener, TcpStream},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use crate::{KvEngine, Request, Response, Result, ThreadPool};
use log::{error, info};
use serde_json::Deserializer;

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
        let listener = TcpListener::bind(addr)?;
        for stream in listener.incoming() {
            if is_stop.load(Ordering::SeqCst) {
                break;
            }
            let engine = self.engine.clone();
            self.pool.spawn(move || {
                match stream {
                    Ok(stream) => {
                        if let Err(err) = handle_request(engine, stream) {
                            error!("handle request error: {}", err);
                        }
                    }
                    Err(err) => error!("connect error: {}", err),
                };
            });
        }
        Ok(())
    }
}

fn handle_request<E: KvEngine>(mut engine: E, stream: TcpStream) -> Result<()> {
    let client_addr = stream.peer_addr()?;
    info!("handle request from {}", client_addr);

    let mut writer = BufWriter::new(&stream);
    let req_reader = Deserializer::from_reader(&stream).into_iter::<Request>();
    for request in req_reader {
        let resp = match request? {
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

        serde_json::to_writer(&mut writer, &resp)?;
        writer.flush()?;
    }
    Ok(())
}
