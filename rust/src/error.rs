#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid location {location:?}: {reason}")]
    InvalidLocation { location: String, reason: String },
    #[error("object store error: {0}")]
    Store(#[from] object_store::Error),
    #[error("failed to decode message {message_index}: {reason}")]
    Decode { message_index: usize, reason: String },
    #[error("unsupported: {0}")]
    Unsupported(String),
    #[error("the levels option matches .idx level text, so it needs an .idx sidecar")]
    LevelsNeedIndex,
    #[error("invalid option {name}: {reason}")]
    InvalidOption { name: String, reason: String },
    #[error("arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
}

pub type Result<T> = std::result::Result<T, Error>;
