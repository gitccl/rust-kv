use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use rand::{seq::IteratorRandom, thread_rng};
use rust_kv::{KvEngine, KvStore, SledStore};
use tempfile::TempDir;

fn set_bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("set_bench");
    let rng = &mut thread_rng();
    let set_range = (0..100000).choose_multiple(rng, 1000);

    group.bench_function("kvs", |b| {
        b.iter_batched(
            || {
                let temp_dir = TempDir::new().expect("failed to new temp dir");
                KvStore::open(temp_dir.path()).expect("failed to open KvStore")
            },
            |mut kv_store| {
                for &i in &set_range {
                    kv_store
                        .set(format!("key{}", i), format!("value{}", i))
                        .expect("failed to set");
                }
            },
            BatchSize::SmallInput,
        )
    });

    group.bench_function("sled", |b| {
        b.iter_batched(
            || {
                let temp_dir = TempDir::new().expect("failed to new temp dir");
                SledStore::open(temp_dir.path()).expect("failed to open SledStore")
            },
            |mut kv_store| {
                for &i in &set_range {
                    kv_store
                        .set(format!("key{}", i), format!("value{}", i))
                        .expect("failed to set");
                }
            },
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

fn get_bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("get_bench");
    let rng = &mut thread_rng();
    let set_range = (0..100000).choose_multiple(rng, 1000);
    let get_range = set_range.iter().choose_multiple(rng, 300);

    group.bench_function("kvs", |b| {
        b.iter_batched(
            || {
                let temp_dir = TempDir::new().expect("failed to new temp dir");
                let mut kv_store = KvStore::open(temp_dir.path()).expect("failed to open KvStore");
                for &i in &set_range {
                    kv_store
                        .set(format!("key{}", i), format!("value{}", i))
                        .expect("failed to set");
                }
                kv_store
            },
            |mut kv_store| {
                for &&i in &get_range {
                    kv_store
                        .get(format!("key{}", i))
                        .expect("failed to get key")
                        .expect("the value cannot be None");
                }
            },
            BatchSize::SmallInput,
        )
    });

    group.bench_function("sled", |b| {
        b.iter_batched(
            || {
                let temp_dir = TempDir::new().expect("failed to new temp dir");
                let mut kv_store =
                    SledStore::open(temp_dir.path()).expect("failed to open SledStore");
                for &i in &set_range {
                    kv_store
                        .set(format!("key{}", i), format!("value{}", i))
                        .expect("failed to set");
                }
                kv_store
            },
            |mut kv_store| {
                for &&i in &get_range {
                    kv_store
                        .get(format!("key{}", i))
                        .expect("failed to get key")
                        .expect("the value cannot be None");
                }
            },
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

criterion_group!(benches, set_bench, get_bench);
criterion_main!(benches);
