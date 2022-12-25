use std::{io, string};

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

    /// Error with a string message
    #[fail(display = "{}", _0)]
    StringError(String),

    /// Sled store error.
    #[fail(display = "{}", _0)]
    Sled(#[cause] sled::Error),

    /// Key or value is invalid UTF-8 sequence
    #[fail(display = "{}", _0)]
    Utf8(#[cause] string::FromUtf8Error),

    /// rayon ThreadPool build error
    #[fail(display = "{}", _0)]
    ThreadPool(#[cause] rayon::ThreadPoolBuildError),
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

impl From<sled::Error> for KvError {
    fn from(error: sled::Error) -> Self {
        KvError::Sled(error)
    }
}

impl From<string::FromUtf8Error> for KvError {
    fn from(error: string::FromUtf8Error) -> Self {
        KvError::Utf8(error)
    }
}

impl From<rayon::ThreadPoolBuildError> for KvError {
    fn from(error: rayon::ThreadPoolBuildError) -> Self {
        KvError::ThreadPool(error)
    }
}
