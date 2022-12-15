//! A simple key/value store.

mod error;
mod kv;

pub use error::{KvError, Result};
pub use kv::KvStore;
