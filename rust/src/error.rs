#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid location {location:?}: {reason}")]
    InvalidLocation { location: String, reason: String },
    #[error("object store error: {0}")]
    Store(#[from] object_store::Error),
    #[error("index {0} is not valid UTF-8")]
    IndexEncoding(String),
    #[error("index {0} has malformed lines; refusing to derive byte ranges from it")]
    MalformedIndex(String),
    #[error("failed to decode message {message_index}: {reason}")]
    Decode { message_index: usize, reason: String },
    #[error("arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
}

pub type Result<T> = std::result::Result<T, Error>;
