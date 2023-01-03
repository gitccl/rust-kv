use std::{
    net::TcpStream,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Once,
    },
    thread,
    time::Duration,
};

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use crossbeam_utils::sync::WaitGroup;
use log::{warn, LevelFilter};
use rust_kv::{
    KvClient, KvServer, KvStore, RayonThreadPool, SharedQueueThreadPool, SledStore, ThreadPool,
};
use tempfile::TempDir;

static LOGGER_INIT: Once = Once::new();
const THREAD_COUNT: [usize; 4] = [1, 2, 4, 8];
const ENTRY_COUNT: usize = 100;

fn write_queued_kvstore(c: &mut Criterion) {
    LOGGER_INIT.call_once(|| {
        env_logger::builder().filter_level(LevelFilter::Warn).init();
    });
    let mut group = c.benchmark_group("write_queued_kvstore");
    for thread_num in THREAD_COUNT {
        group.bench_with_input(
            BenchmarkId::from_parameter(thread_num),
            &thread_num,
            |b, &thread_num| {
                let addr = "127.0.0.1:8888";
                let temp_dir = TempDir::new().unwrap();
                let pool = SharedQueueThreadPool::new(thread_num).unwrap();
                let engine = KvStore::open(temp_dir.path()).unwrap();
                let mut server = KvServer::new(engine, pool);
                let is_stop = Arc::new(AtomicBool::new(false));

                let is_stop_clone = is_stop.clone();
                let child_handle = thread::spawn(move || {
                    server
                        .run(addr.to_owned(), is_stop_clone)
                        .expect("kv server failed");
                });

                let values = String::from("value");
                let keys: Vec<String> = (0..ENTRY_COUNT).map(|i| format!("key{}", i)).collect();
                let client_pool = RayonThreadPool::new(ENTRY_COUNT).unwrap();

                thread::sleep(Duration::from_secs(1));
                b.iter(|| {
                    let wg = WaitGroup::new();
                    for i in 0..ENTRY_COUNT {
                        let key = keys[i].clone();
                        let value = values.clone();
                        let wg = wg.clone();
                        client_pool.spawn(move || {
                            let rt = tokio::runtime::Runtime::new().unwrap();
                            rt.block_on(async move {
                                match KvClient::new(addr.to_owned()).await {
                                    Ok(mut client) => {
                                        client.set(key, value).await.expect("client set error");
                                    }
                                    Err(err) => {
                                        warn!("failed to new kv client: {}", err);
                                    }
                                }
                            });
                            drop(wg);
                        });
                    }

                    wg.wait();
                });

                is_stop.store(true, Ordering::SeqCst);

                // trigger server stop
                let _ = TcpStream::connect(addr).unwrap();

                child_handle.join().expect("child thread err");
            },
        );
    }

    group.finish();
}

fn read_queued_kvstore(c: &mut Criterion) {
    LOGGER_INIT.call_once(|| {
        env_logger::builder().filter_level(LevelFilter::Warn).init();
    });
    let mut group = c.benchmark_group("read_queued_kvstore");
    for thread_num in THREAD_COUNT {
        group.bench_with_input(
            BenchmarkId::from_parameter(thread_num),
            &thread_num,
            |b, &thread_num| {
                let addr = "127.0.0.1:8888";
                let temp_dir = TempDir::new().unwrap();
                let pool = SharedQueueThreadPool::new(thread_num).unwrap();
                let engine = KvStore::open(temp_dir.path()).unwrap();
                let mut server = KvServer::new(engine, pool);
                let is_stop = Arc::new(AtomicBool::new(false));

                let is_stop_clone = is_stop.clone();
                let child_handle = thread::spawn(move || {
                    server
                        .run(addr.to_owned(), is_stop_clone)
                        .expect("kv server failed");
                });

                let values = String::from("value");
                let keys: Vec<String> = (0..ENTRY_COUNT).map(|i| format!("key{}", i)).collect();
                let client_pool = RayonThreadPool::new(ENTRY_COUNT).unwrap();

                thread::sleep(Duration::from_secs(1));

                let rt = tokio::runtime::Runtime::new().unwrap();
                rt.block_on(async {
                    let mut client = KvClient::new(addr.to_owned()).await.unwrap();
                    for i in 0..ENTRY_COUNT {
                        client.set(keys[i].clone(), values.clone()).await.unwrap();
                    }
                });

                b.iter(|| {
                    let wg = WaitGroup::new();
                    for i in 0..ENTRY_COUNT {
                        let key = keys[i].clone();
                        let wg = wg.clone();
                        client_pool.spawn(move || {
                            let rt = tokio::runtime::Runtime::new().unwrap();
                            rt.block_on(async move {
                                match KvClient::new(addr.to_owned()).await {
                                    Ok(mut client) => {
                                        client.get(key).await.expect("client get error");
                                    }
                                    Err(err) => {
                                        warn!("failed to new kv client: {}", err);
                                    }
                                }
                            });
                            drop(wg);
                        });
                    }

                    wg.wait();
                });

                is_stop.store(true, Ordering::SeqCst);

                // trigger server stop
                let _ = TcpStream::connect(addr).unwrap();

                child_handle.join().expect("child thread err");
            },
        );
    }

    group.finish();
}

fn write_rayon_kvstore(c: &mut Criterion) {
    LOGGER_INIT.call_once(|| {
        env_logger::builder().filter_level(LevelFilter::Warn).init();
    });
    let mut group = c.benchmark_group("write_rayon_kvstore");
    for thread_num in THREAD_COUNT {
        group.bench_with_input(
            BenchmarkId::from_parameter(thread_num),
            &thread_num,
            |b, &thread_num| {
                let addr = "127.0.0.1:8888";
                let temp_dir = TempDir::new().unwrap();
                let pool = RayonThreadPool::new(thread_num).unwrap();
                let engine = KvStore::open(temp_dir.path()).unwrap();
                let mut server = KvServer::new(engine, pool);
                let is_stop = Arc::new(AtomicBool::new(false));

                let is_stop_clone = is_stop.clone();
                let child_handle = thread::spawn(move || {
                    server
                        .run(addr.to_owned(), is_stop_clone)
                        .expect("kv server failed");
                });

                let values = String::from("value");
                let keys: Vec<String> = (0..ENTRY_COUNT).map(|i| format!("key{}", i)).collect();
                let client_pool = RayonThreadPool::new(ENTRY_COUNT).unwrap();

                thread::sleep(Duration::from_secs(1));
                b.iter(|| {
                    let wg = WaitGroup::new();
                    for i in 0..ENTRY_COUNT {
                        let key = keys[i].clone();
                        let value = values.clone();
                        let wg = wg.clone();
                        client_pool.spawn(move || {
                            let rt = tokio::runtime::Runtime::new().unwrap();
                            rt.block_on(async move {
                                match KvClient::new(addr.to_owned()).await {
                                    Ok(mut client) => {
                                        client.set(key, value).await.expect("client set error");
                                    }
                                    Err(err) => {
                                        warn!("failed to new kv client: {}", err);
                                    }
                                }
                            });
                            drop(wg);
                        });
                    }

                    wg.wait();
                });

                is_stop.store(true, Ordering::SeqCst);

                // trigger server stop
                let _ = TcpStream::connect(addr).unwrap();

                child_handle.join().expect("child thread err");
            },
        );
    }

    group.finish();
}

fn read_rayon_kvstore(c: &mut Criterion) {
    LOGGER_INIT.call_once(|| {
        env_logger::builder().filter_level(LevelFilter::Warn).init();
    });
    let mut group = c.benchmark_group("read_rayon_kvstore");
    for thread_num in THREAD_COUNT {
        group.bench_with_input(
            BenchmarkId::from_parameter(thread_num),
            &thread_num,
            |b, &thread_num| {
                let addr = "127.0.0.1:8888";
                let temp_dir = TempDir::new().unwrap();
                let pool = RayonThreadPool::new(thread_num).unwrap();
                let engine = KvStore::open(temp_dir.path()).unwrap();
                let mut server = KvServer::new(engine, pool);
                let is_stop = Arc::new(AtomicBool::new(false));

                let is_stop_clone = is_stop.clone();
                let child_handle = thread::spawn(move || {
                    server
                        .run(addr.to_owned(), is_stop_clone)
                        .expect("kv server failed");
                });

                let values = String::from("value");
                let keys: Vec<String> = (0..ENTRY_COUNT).map(|i| format!("key{}", i)).collect();
                let client_pool = RayonThreadPool::new(ENTRY_COUNT).unwrap();

                thread::sleep(Duration::from_secs(1));

                let rt = tokio::runtime::Runtime::new().unwrap();
                rt.block_on(async {
                    let mut client = KvClient::new(addr.to_owned()).await.unwrap();
                    for i in 0..ENTRY_COUNT {
                        client.set(keys[i].clone(), values.clone()).await.unwrap();
                    }
                });

                b.iter(|| {
                    let wg = WaitGroup::new();
                    for i in 0..ENTRY_COUNT {
                        let key = keys[i].clone();
                        let wg = wg.clone();
                        client_pool.spawn(move || {
                            let rt = tokio::runtime::Runtime::new().unwrap();
                            rt.block_on(async move {
                                match KvClient::new(addr.to_owned()).await {
                                    Ok(mut client) => {
                                        client.get(key).await.expect("client get error");
                                    }
                                    Err(err) => {
                                        warn!("failed to new kv client: {}", err);
                                    }
                                }
                            });
                            drop(wg);
                        });
                    }

                    wg.wait();
                });

                is_stop.store(true, Ordering::SeqCst);

                // trigger server stop
                let _ = TcpStream::connect(addr).unwrap();

                child_handle.join().expect("child thread err");
            },
        );
    }

    group.finish();
}

fn write_rayon_sledstore(c: &mut Criterion) {
    LOGGER_INIT.call_once(|| {
        env_logger::builder().filter_level(LevelFilter::Warn).init();
    });
    let mut group = c.benchmark_group("write_rayon_sledstore");
    for thread_num in THREAD_COUNT {
        group.bench_with_input(
            BenchmarkId::from_parameter(thread_num),
            &thread_num,
            |b, &thread_num| {
                let addr = "127.0.0.1:8888";
                let temp_dir = TempDir::new().unwrap();
                let pool = RayonThreadPool::new(thread_num).unwrap();
                let engine = SledStore::open(temp_dir.path()).unwrap();
                let mut server = KvServer::new(engine, pool);
                let is_stop = Arc::new(AtomicBool::new(false));

                let is_stop_clone = is_stop.clone();
                let child_handle = thread::spawn(move || {
                    server
                        .run(addr.to_owned(), is_stop_clone)
                        .expect("kv server failed");
                });

                let values = String::from("value");
                let keys: Vec<String> = (0..ENTRY_COUNT).map(|i| format!("key{}", i)).collect();
                let client_pool = RayonThreadPool::new(ENTRY_COUNT).unwrap();

                thread::sleep(Duration::from_secs(1));
                b.iter(|| {
                    let wg = WaitGroup::new();
                    for i in 0..ENTRY_COUNT {
                        let key = keys[i].clone();
                        let value = values.clone();
                        let wg = wg.clone();
                        client_pool.spawn(move || {
                            let rt = tokio::runtime::Runtime::new().unwrap();
                            rt.block_on(async move {
                                match KvClient::new(addr.to_owned()).await {
                                    Ok(mut client) => {
                                        client.set(key, value).await.expect("client set error");
                                    }
                                    Err(err) => {
                                        warn!("failed to new kv client: {}", err);
                                    }
                                }
                            });
                            drop(wg);
                        });
                    }

                    wg.wait();
                });

                is_stop.store(true, Ordering::SeqCst);

                // trigger server stop
                let _ = TcpStream::connect(addr).unwrap();

                child_handle.join().expect("child thread err");
            },
        );
    }

    group.finish();
}

fn read_rayon_sledstore(c: &mut Criterion) {
    LOGGER_INIT.call_once(|| {
        env_logger::builder().filter_level(LevelFilter::Warn).init();
    });
    let mut group = c.benchmark_group("read_rayon_sledstore");
    for thread_num in THREAD_COUNT {
        group.bench_with_input(
            BenchmarkId::from_parameter(thread_num),
            &thread_num,
            |b, &thread_num| {
                let addr = "127.0.0.1:8888";
                let temp_dir = TempDir::new().unwrap();
                let pool = RayonThreadPool::new(thread_num).unwrap();
                let engine = SledStore::open(temp_dir.path()).unwrap();
                let mut server = KvServer::new(engine, pool);
                let is_stop = Arc::new(AtomicBool::new(false));

                let is_stop_clone = is_stop.clone();
                let child_handle = thread::spawn(move || {
                    server
                        .run(addr.to_owned(), is_stop_clone)
                        .expect("kv server failed");
                });

                let values = String::from("value");
                let keys: Vec<String> = (0..ENTRY_COUNT).map(|i| format!("key{}", i)).collect();
                let client_pool = RayonThreadPool::new(ENTRY_COUNT).unwrap();

                thread::sleep(Duration::from_secs(1));

                let rt = tokio::runtime::Runtime::new().unwrap();
                rt.block_on(async {
                    let mut client = KvClient::new(addr.to_owned()).await.unwrap();
                    for i in 0..ENTRY_COUNT {
                        client.set(keys[i].clone(), values.clone()).await.unwrap();
                    }
                });

                b.iter(|| {
                    let wg = WaitGroup::new();
                    for i in 0..ENTRY_COUNT {
                        let key = keys[i].clone();
                        let wg = wg.clone();
                        client_pool.spawn(move || {
                            let rt = tokio::runtime::Runtime::new().unwrap();
                            rt.block_on(async move {
                                match KvClient::new(addr.to_owned()).await {
                                    Ok(mut client) => {
                                        client.get(key).await.expect("client get error");
                                    }
                                    Err(err) => {
                                        warn!("failed to new kv client: {}", err);
                                    }
                                }
                            });
                            drop(wg);
                        });
                    }

                    wg.wait();
                });

                is_stop.store(true, Ordering::SeqCst);

                // trigger server stop
                let _ = TcpStream::connect(addr).unwrap();

                child_handle.join().expect("child thread err");
            },
        );
    }

    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(10);
    targets = write_queued_kvstore, read_queued_kvstore, write_rayon_kvstore,
                read_rayon_kvstore, write_rayon_sledstore, read_rayon_sledstore
}

criterion_main!(benches);
