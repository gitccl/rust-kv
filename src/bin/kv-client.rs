use std::{
    io::{BufReader, BufWriter, Write},
    net::TcpStream,
};

use clap::{arg, Command};
use rust_kv::{KvError, Request, Response, Result};
use serde::Deserialize;
use serde_json::{de::IoRead, Deserializer};

const DEFAULT_LISTENING_ADDRESS: &str = "127.0.0.1:4000";

fn main() -> Result<()> {
    let matches = Command::new(env!("CARGO_PKG_NAME"))
        .version(env!("CARGO_PKG_VERSION"))
        .author(env!("CARGO_PKG_AUTHORS"))
        .about(env!("CARGO_PKG_DESCRIPTION"))
        .disable_help_subcommand(true)
        .arg_required_else_help(true)
        .subcommand(
            Command::new("set")
                .about("Set the value of a string key to a string")
                .arg(arg!(<key> "A string key"))
                .arg(arg!(<value> "The string value of the key"))
                .arg(
                    arg!(--addr <IP_PORT> "The address of the server")
                        .default_value(DEFAULT_LISTENING_ADDRESS),
                ),
        )
        .subcommand(
            Command::new("get")
                .about("Get the string value of a given string key")
                .arg(arg!(<key> "A string key"))
                .arg(
                    arg!(--addr <IP_PORT> "The address of the server")
                        .default_value(DEFAULT_LISTENING_ADDRESS),
                ),
        )
        .subcommand(
            Command::new("rm")
                .about("Remove a given key")
                .arg(arg!(<key> "A string key"))
                .arg(
                    arg!(--addr <IP_PORT> "The address of the server")
                        .default_value(DEFAULT_LISTENING_ADDRESS),
                ),
        )
        .get_matches();

    match matches.subcommand() {
        Some(("set", sub_matches)) => {
            let key = sub_matches.get_one::<String>("key").unwrap().clone();
            let value = sub_matches.get_one::<String>("value").unwrap().clone();
            let addr = sub_matches.get_one::<String>("addr").unwrap();
            let mut client = KvClient::new(addr)?;
            client.set(key, value)?;
        }
        Some(("get", sub_matches)) => {
            let key = sub_matches.get_one::<String>("key").unwrap().clone();
            let addr = sub_matches.get_one::<String>("addr").unwrap();
            let mut client = KvClient::new(addr)?;
            if let Some(value) = client.get(key)? {
                println!("{}", value);
            } else {
                println!("Key not found");
            }
        }
        Some(("rm", sub_matches)) => {
            let key = sub_matches.get_one::<String>("key").unwrap().clone();
            let addr = sub_matches.get_one::<String>("addr").unwrap();
            let mut client = KvClient::new(addr)?;
            client.remove(key)?;
        }
        _ => unreachable!(),
    }
    Ok(())
}

struct KvClient {
    reader: Deserializer<IoRead<BufReader<TcpStream>>>,
    writer: BufWriter<TcpStream>,
}

impl KvClient {
    // create a KvClient with server addr
    pub fn new(addr: &String) -> Result<KvClient> {
        let tcp_reader = TcpStream::connect(addr)?;
        let tcp_writer = tcp_reader.try_clone()?;
        Ok(KvClient {
            reader: Deserializer::from_reader(BufReader::new(tcp_reader)),
            writer: BufWriter::new(tcp_writer),
        })
    }

    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        self.request(Request::Get(key))
    }

    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        self.request(Request::Set(key, value))?;
        Ok(())
    }

    pub fn remove(&mut self, key: String) -> Result<()> {
        self.request(Request::Remove(key))?;
        Ok(())
    }

    fn request(&mut self, req: Request) -> Result<Option<String>> {
        serde_json::to_writer(&mut self.writer, &req)?;
        self.writer.flush()?;
        match Response::deserialize(&mut self.reader)? {
            Response::Ok(resp) => Ok(resp),
            Response::Err(msg) => Err(KvError::StringError(msg)),
        }
    }
}
