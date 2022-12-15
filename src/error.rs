use std::io;

use failure::Fail;

/// Result type for kvs.
pub type Result<T> = std::result::Result<T, KvError>;

/// Error type for kvs.
#[derive(Fail, Debug)]
pub enum KvError {
    /// IO error.
    #[fail(display = "{}", _0)]
    Io(#[cause] io::Error),

    /// Serialization or deserialization error.
    #[fail(display = "{}", _0)]
    Serde(#[cause] serde_json::Error),

    /// Removing non-existent key error.
    #[fail(display = "Key not found")]
    KeyNotFound,

    /// Unexpected command type error in log.
    /// It indicated a corrupted log or a program bug.
    #[fail(display = "Unexpected command type")]
    UnexpectedCommandType,
}

impl From<io::Error> for KvError {
    fn from(error: io::Error) -> Self {
        KvError::Io(error)
    }
}

impl From<serde_json::Error> for KvError {
    fn from(error: serde_json::Error) -> Self {
        KvError::Serde(error)
    }
}
