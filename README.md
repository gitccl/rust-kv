- [Introduction](#introduction)
- [Storage design](#storage-design)
  - [`set` operation](#set-operation)
  - [`get` operation](#get-operation)
  - [`rm` operation](#rm-operation)
  - [Merge and Compaction](#merge-and-compaction)
- [Getting Started](#getting-started)
  - [Build](#build)
  - [Run Server](#run-server)
  - [Run Client](#run-client)
- [Tests](#tests)
- [Benchmarks](#benchmarks)

## Introduction
Rust-KV is a networked simple key-value database written in rust, with multithreading and asynchronous I/O. It is a simple log-structured storage inspired by [bitcask](https://github.com/basho/bitcask/blob/develop/doc/bitcask-intro.pdf).

Rust-KV includes two parts: client and server, corresponding to [kv-server](./src/bin/kv-server.rs) and [kv-client](./src/bin/kv-client.rs) cli respectively. The `kv-server` is an asynchronous server based on the [tokio](https://tokio.rs/) asynchronous runtime, which can concurrently process a large number of requests from clients.

Rust-KV support three operations(commands) similar to redis:
- set key value
- get key
- rm key

## Storage design
The storage engine is log-structured, which is inspired by [bitcask](https://github.com/basho/bitcask/blob/develop/doc/bitcask-intro.pdf). There is a hash table in memory and some data files on disk. 

Data files are append-only log files that hold the KV pairs. A single database instance could have many data files, out of which just one will be active and opened for writing, while the others are considered immutable and are only used for reads. 

The in-memory hash table stores all the keys present in the database and maps it to the offset in the datafile where the value resides, thus facilitating the point lookups. The mapped value in the hash table is a structure that holds `file_id`, `offset` and `length`.

### `set` operation
When a new KV pair is submitted to be stored, the engine first appends it to the active datafile and then creates a new entry in the hash table specifying the offset and file where the value is stored. Putting a new KV pair requires just one atomic operation encapsulating one disk write and a few in-memory access and updates. Since the active datafile is an append-only file, the disk write operation does not have to perform any disk seek, thus providing a high write throughput.

Updating an existing Key Value is very similar to putting a new KV pair, instead of creating an entry in hash table, the existing entry is updated with the new position of that value. The entry corresponding to the old value is now dangling and will be garbage collected explicitly during merging and compaction.

### `get` operation
Reading a KV pair from the store requires the engine to first access the hash table to find the datafile and the offset within it for the given key. Then the engine performs one disk read from the corresponding datafile at the offset to retrieve the KV pair. 

### `rm` operation
Removing a key is a special operation where the engine atomically appends a new entry in the active datafile with value equalling a tombstone value, denoting deletion, and deleting the entry from the in-memory hash table. In remove operation as well, the older entries corresponding to the deleted keys are left dangling and will be garbage collected explicitly during merging and compaction.

### Merge and Compaction
As we have seen during Update and Remove operations the old entries corresponding to the key remain untouched and dangling and this leads to database consuming a lot of disk space. In order to make things efficient for the disk utilization, the engine once a while compacts the older closed datafiles into one or many merged files having the same structure as the existing datafiles.

The merge process iterates over all the immutable files in the database and produces a set of datafiles having only live and latest versions of each present key. This way the unused and non-existent keys are ignored from the newer datafiles saving a bunch of disk space. Since the record now exists in a different merged datafile and at a new offset, its entry in hash table needs an atomic updation.

## Getting Started
### Build
```
cargo build
```

### Run Server
Run the `kv-server`, the `--addr` option specifies the address that the server listens to.
```sh
$ ./target/debug/kv-server --addr 127.0.0.1:8000
```

### Run Client
Run the `kv-client`, the `--addr` option specifies the address of the `kv-server`.
```sh
$ ./target/debug/kv-client --addr 127.0.0.1:8000
```
Then you can input command like this:
```txt
Use \help to get usage.
> \help
set <key> <value>: set the value of a string key
get <key>: get the string value of a given string key
rm <key>: remove a given key
exit: exit the client
> get name
Key not found
> set name ccl
Ok
> get name
ccl
> rm name
Ok
> exit
client exited...
```

## Tests
Run `cargo test` to run the tests.
- [cli.rs](./tests/cli.rs) tests the `kv-server` cli and `kv-client` cli.
- [kv_store.rs](./tests/kv_store.rs) tests the KV store engine. 
- [thread_pool.rs](./tests/thread_pool.rs) tests the thread_pool.

## Benchmarks
Run `cargo bench` to run the benchmark. The benchmark results are plotted as charts, open `target/criterion/report/index.html` file to view the results.  

- [kv_engine_bench.rs](./benches/kv_engine_bench.rs) benchmarks the raw read/write performance of the kv engine.
- [thread_pool.rs](./benches/thread_pool.rs) benchmarks the read/write performance of the server which uses thread pool and asynchronous network.