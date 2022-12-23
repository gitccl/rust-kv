use std::{
    env::current_dir,
    fmt::Display,
    fs,
    io::{BufWriter, Write},
    net::{TcpListener, TcpStream},
    process::exit,
};

use clap::{Parser, ValueEnum};
use log::{error, info, LevelFilter};
use rust_kv::{KvEngine, KvStore, Request, Response, Result, SledStore};
use serde_json::Deserializer;

const DEFAULT_LISTENING_ADDRESS: &str = "127.0.0.1:4000";
const DEFAULT_ENGINE: Engine = Engine::Kvs;

fn main() -> Result<()> {
    env_logger::builder().filter_level(LevelFilter::Info).init();

    let mut args = Arg::parse();
    let curr_engine = current_engine()?;
    if args.engine.is_none() {
        args.engine = curr_engine
    } else if curr_engine.is_some() && args.engine != curr_engine {
        error!("engine type not match, current: {}", curr_engine.unwrap());
        exit(-1)
    }

    if let Err(err) = run(args.engine.unwrap_or(DEFAULT_ENGINE), args.addr) {
        error!("{}", err);
        exit(-1)
    }
    Ok(())
}

fn run(engine: Engine, addr: String) -> Result<()> {
    let engine_path = current_dir()?.join("engine");
    fs::write(engine_path, format!("{}", engine))?;

    info!("kv-server {}", env!("CARGO_PKG_VERSION"));
    info!("Storage engine: {}", engine);
    info!("Listening on: {}", addr);

    match engine {
        Engine::Kvs => run_server(KvStore::open(current_dir()?)?, addr),
        Engine::Sled => run_server(SledStore::open(current_dir()?)?, addr),
    }
}

fn run_server<E: KvEngine>(kv_engine: E, addr: String) -> Result<()> {
    let mut server = KvServer::new(kv_engine);
    server.run(addr)
}

/// retrieve engine from db dir
fn current_engine() -> Result<Option<Engine>> {
    let engine_path = current_dir()?.join("engine");
    if !engine_path.exists() {
        return Ok(None);
    }
    let engine_str = fs::read_to_string(engine_path)?;
    if engine_str == format!("{}", Engine::Kvs) {
        return Ok(Some(Engine::Kvs));
    } else if engine_str == format!("{}", Engine::Sled) {
        return Ok(Some(Engine::Sled));
    }
    Ok(None)
}

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Arg {
    /// The address that server listening
    #[arg(short, long, default_value=DEFAULT_LISTENING_ADDRESS)]
    addr: String,
    /// The storage engine that server use.
    /// Can be retrieved from the db dir. Default to kvs.
    #[arg(value_enum, short, long)]
    engine: Option<Engine>,
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
enum Engine {
    Kvs,
    Sled,
}

impl Display for Engine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Engine::Kvs => write!(f, "kvs"),
            Engine::Sled => write!(f, "sled"),
        }
    }
}

/// The server of a key value store.
struct KvServer<E: KvEngine> {
    engine: E,
}

impl<E: KvEngine> KvServer<E> {
    /// create a `KvServer` with a given storage engine.
    pub fn new(engine: E) -> KvServer<E> {
        KvServer { engine }
    }

    /// Run the server listening on the given address
    pub fn run(&mut self, addr: String) -> Result<()> {
        let listener = TcpListener::bind(addr)?;
        for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    if let Err(err) = self.handle_request(stream) {
                        error!("handle request error: {}", err);
                    }
                }
                Err(err) => error!("connect error: {}", err),
            };
        }
        Ok(())
    }

    fn handle_request(&mut self, stream: TcpStream) -> Result<()> {
        let client_addr = stream.peer_addr()?;
        info!("handle request from {}", client_addr);

        let mut writer = BufWriter::new(&stream);
        let req_reader = Deserializer::from_reader(&stream).into_iter::<Request>();
        for request in req_reader {
            let resp = match request? {
                Request::Get(key) => match self.engine.get(key) {
                    Ok(value) => Response::Ok(value),
                    Err(err) => Response::Err(format!("{}", err)),
                },
                Request::Set(key, value) => match self.engine.set(key, value) {
                    Ok(_) => Response::Ok(None),
                    Err(err) => Response::Err(format!("{}", err)),
                },
                Request::Remove(key) => match self.engine.remove(key) {
                    Ok(_) => Response::Ok(None),
                    Err(err) => Response::Err(format!("{}", err)),
                },
            };

            serde_json::to_writer(&mut writer, &resp)?;
            writer.flush()?;
        }
        Ok(())
    }
}
