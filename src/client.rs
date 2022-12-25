use std::{
    io::{BufReader, BufWriter, Write},
    net::TcpStream,
};

use crate::{KvError, Request, Response, Result};
use serde::Deserialize;
use serde_json::{de::IoRead, Deserializer};

pub struct KvClient {
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
