//! Evaluation spike for a Rust GRIB reader: idx-driven byte-range reads from
//! object storage, decoded with grib-rs and returned as Arrow.

pub mod batch;
pub mod decode;
pub mod error;
pub mod idx;
#[cfg(feature = "python")]
mod python;
pub mod reader;

pub use decode::DecodedField;
pub use error::{Error, Result};
pub use reader::{ReadStats, Selection, read};
