use std::path::PathBuf;

use crate::{KvEngine, KvError, Result};
use sled::Db;

/// Sled KV storage engine
#[derive(Clone)]
pub struct SledStore {
    db: Db,
}

impl SledStore {
    pub fn open(dir_path: impl Into<PathBuf>) -> Result<SledStore> {
        Ok(SledStore {
            db: sled::open(dir_path.into())?,
        })
    }
}

impl KvEngine for SledStore {
    fn set(&mut self, key: String, value: String) -> Result<()> {
        self.db.insert(key.as_str(), value.as_str())?;
        self.db.flush()?;
        Ok(())
    }

    fn get(&mut self, key: String) -> Result<Option<String>> {
        let value = self
            .db
            .get(key.as_str())?
            .map(|ivec| String::from_utf8(ivec.to_vec()))
            .transpose()?;
        Ok(value)
    }

    fn remove(&mut self, key: String) -> Result<()> {
        self.db.remove(&key)?.ok_or(KvError::KeyNotFound)?;
        self.db.flush()?;
        Ok(())
    }
}
