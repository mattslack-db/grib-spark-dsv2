//! Python bindings: `grib_idx.read(...)` returns a `pyarrow.RecordBatch`.

use std::collections::HashMap;
use std::sync::OnceLock;

use arrow::pyarrow::ToPyArrow;
use pyo3::exceptions::{PyIOError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyDict;

use crate::batch::to_record_batch;
use crate::error::Error;
use crate::reader::{ReadStats, Selection};

fn runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread().enable_all().build().expect("failed to start tokio runtime")
    })
}

fn to_py_err(e: Error) -> PyErr {
    match e {
        Error::InvalidLocation { .. } | Error::MalformedIndex(_) | Error::IndexEncoding(_) => {
            PyValueError::new_err(e.to_string())
        }
        _ => PyIOError::new_err(e.to_string()),
    }
}

fn read_impl<'py>(
    py: Python<'py>,
    location: &str,
    variables: Option<Vec<String>>,
    levels: Option<Vec<String>>,
    storage_options: Option<HashMap<String, String>>,
) -> PyResult<(Bound<'py, PyAny>, ReadStats)> {
    let selection = Selection { variables, levels };
    let options = storage_options.unwrap_or_default();
    let (batch, stats) = py
        .detach(|| {
            let (fields, stats) = runtime().block_on(crate::reader::read(location, &options, &selection))?;
            Ok((to_record_batch(location, &fields)?, stats))
        })
        .map_err(to_py_err)?;
    Ok((batch.to_pyarrow(py)?, stats))
}

/// Read the GRIB2 fields matching `variables` / `levels` (exact matches against
/// the `.idx` sidecar; `None` means all) from `location`, fetching only the
/// selected messages' byte ranges.
///
/// `location` is an `s3://`, `az://`, `abfss://`, `gs://`, `https://` or
/// `file://` URL, or a local path such as `/Volumes/...`. `storage_options`
/// are passed to the object store, e.g. `{"skip_signature": "true",
/// "region": "us-east-1"}` for public NOAA buckets.
#[pyfunction]
#[pyo3(signature = (location, variables=None, levels=None, storage_options=None))]
fn read<'py>(
    py: Python<'py>,
    location: &str,
    variables: Option<Vec<String>>,
    levels: Option<Vec<String>>,
    storage_options: Option<HashMap<String, String>>,
) -> PyResult<Bound<'py, PyAny>> {
    read_impl(py, location, variables, levels, storage_options).map(|(batch, _)| batch)
}

/// As `read`, also returning a dict describing the access pattern
/// (`file_size`, `messages_in_index`, `messages_read`, `bytes_read`).
#[pyfunction]
#[pyo3(signature = (location, variables=None, levels=None, storage_options=None))]
fn read_with_stats<'py>(
    py: Python<'py>,
    location: &str,
    variables: Option<Vec<String>>,
    levels: Option<Vec<String>>,
    storage_options: Option<HashMap<String, String>>,
) -> PyResult<(Bound<'py, PyAny>, Bound<'py, PyDict>)> {
    let (batch, stats) = read_impl(py, location, variables, levels, storage_options)?;
    let dict = PyDict::new(py);
    dict.set_item("file_size", stats.file_size)?;
    dict.set_item("messages_in_index", stats.messages_in_index)?;
    dict.set_item("messages_read", stats.messages_read)?;
    dict.set_item("bytes_read", stats.bytes_read)?;
    Ok((batch, dict))
}

#[pymodule]
fn grib_idx(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(read, m)?)?;
    m.add_function(wrap_pyfunction!(read_with_stats, m)?)?;
    Ok(())
}
