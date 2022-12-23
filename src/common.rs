use serde::{Deserialize, Serialize};

// The request struct that client use to send request
#[derive(Debug, Serialize, Deserialize)]
pub enum Request {
    // get key
    Get(String),
    // set key value
    Set(String, String),
    // remove key
    Remove(String),
}

// The repsone struct that server return
#[derive(Debug, Serialize, Deserialize)]
pub enum Response {
    // Successful request
    // For Set and Remove request, there is no need to consider the value in Ok
    Ok(Option<String>),
    // Failed request
    Err(String),
}
