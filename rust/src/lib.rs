//! A Rust GRIB2 reader with the Scala DSv2 reader's semantics: idx-driven
//! byte-range reads from object storage, decoded with grib-rs, named with
//! netCDF-Java's tables, and returned as Arrow in the same schemas.

pub mod batch;
pub mod decode;
pub mod error;
pub mod filters;
pub mod geometry;
pub mod idx;
pub mod index_levels;
pub mod product;
#[cfg(feature = "python")]
mod python;
pub mod reader;
pub mod scan;
pub mod selection;
pub mod tables;

pub use decode::DecodedField;
pub use error::{Error, Result};
pub use reader::{FilePlan, GribFile, ReadStats, list_files, plan_file, read, read_plan};
pub use selection::Selection;
