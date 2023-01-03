use crate::{KvError, Request, Response, Result};
use futures_util::{Future, SinkExt, TryFutureExt, TryStreamExt};
use lazy_static::lazy_static;
use tokio::net::{
    tcp::{OwnedReadHalf, OwnedWriteHalf},
    TcpStream,
};
use tokio_serde::{
    formats::{Json, SymmetricalJson},
    Framed, SymmetricallyFramed,
};
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};

lazy_static! {
    static ref RT: tokio::runtime::Runtime = tokio::runtime::Runtime::new().unwrap();
}

pub struct KvClient {
    read_json: Framed<
        FramedRead<OwnedReadHalf, LengthDelimitedCodec>,
        Response,
        Response,
        Json<Response, Response>,
    >,
    write_json: Framed<
        FramedWrite<OwnedWriteHalf, LengthDelimitedCodec>,
        Request,
        Request,
        Json<Request, Request>,
    >,
}

impl KvClient {
    // create a KvClient with server addr
    pub async fn new(addr: String) -> Result<KvClient> {
        let stream = TcpStream::connect(addr).await?;
        let (read_half, write_half) = stream.into_split();
        let frame_reader = FramedRead::new(read_half, LengthDelimitedCodec::new());
        let frame_writer = FramedWrite::new(write_half, LengthDelimitedCodec::new());

        let read_json = SymmetricallyFramed::<_, Response, _>::new(
            frame_reader,
            SymmetricalJson::<Response>::default(),
        );
        let write_json = SymmetricallyFramed::<_, Request, _>::new(
            frame_writer,
            SymmetricalJson::<Request>::default(),
        );
        Ok(KvClient {
            read_json,
            write_json,
        })
    }

    pub fn new_v2(addr: String) -> impl Future<Output = Result<KvClient>> {
        async move {
            let stream = TcpStream::connect(addr)
                .map_err(KvError::from)
                .map_ok(|tcp| {
                    let (read_half, write_half) = tcp.into_split();
                    let frame_reader = FramedRead::new(read_half, LengthDelimitedCodec::new());
                    let frame_writer = FramedWrite::new(write_half, LengthDelimitedCodec::new());

                    let read_json = SymmetricallyFramed::<_, Response, _>::new(
                        frame_reader,
                        SymmetricalJson::<Response>::default(),
                    );
                    let write_json = SymmetricallyFramed::<_, Request, _>::new(
                        frame_writer,
                        SymmetricalJson::<Request>::default(),
                    );
                    KvClient {
                        read_json,
                        write_json,
                    }
                });
            stream.await
        }
    }

    pub async fn new_v3(addr: String) -> Result<KvClient> {
        let stream = TcpStream::connect(addr)
            .map_err(KvError::from)
            .map_ok(|tcp| {
                let (read_half, write_half) = tcp.into_split();
                let frame_reader = FramedRead::new(read_half, LengthDelimitedCodec::new());
                let frame_writer = FramedWrite::new(write_half, LengthDelimitedCodec::new());

                let read_json = SymmetricallyFramed::<_, Response, _>::new(
                    frame_reader,
                    SymmetricalJson::<Response>::default(),
                );
                let write_json = SymmetricallyFramed::<_, Request, _>::new(
                    frame_writer,
                    SymmetricalJson::<Request>::default(),
                );
                KvClient {
                    read_json,
                    write_json,
                }
            });
        stream.await
    }

    pub fn new_v4(addr: String) -> impl Future<Output = Result<KvClient>> {
        let stream = TcpStream::connect(addr)
            .map_err(KvError::from)
            .map_ok(|tcp| {
                let (read_half, write_half) = tcp.into_split();
                let frame_reader = FramedRead::new(read_half, LengthDelimitedCodec::new());
                let frame_writer = FramedWrite::new(write_half, LengthDelimitedCodec::new());

                let read_json = SymmetricallyFramed::<_, Response, _>::new(
                    frame_reader,
                    SymmetricalJson::<Response>::default(),
                );
                let write_json = SymmetricallyFramed::<_, Request, _>::new(
                    frame_writer,
                    SymmetricalJson::<Request>::default(),
                );
                KvClient {
                    read_json,
                    write_json,
                }
            });
        stream
    }

    pub async fn get(&mut self, key: String) -> Result<Option<String>> {
        self.request(Request::Get(key)).await
    }

    pub async fn set(&mut self, key: String, value: String) -> Result<()> {
        self.request(Request::Set(key, value)).await?;
        Ok(())
    }

    pub async fn remove(&mut self, key: String) -> Result<()> {
        self.request(Request::Remove(key)).await?;
        Ok(())
    }

    async fn request(&mut self, req: Request) -> Result<Option<String>> {
        self.write_json.send(req).await?;
        let resp = self
            .read_json
            .try_next()
            .await?
            .expect("Response cannot be none");
        match resp {
            Response::Ok(resp) => Ok(resp),
            Response::Err(msg) => Err(KvError::StringError(msg)),
        }
    }
}
