//! The `grib_idx._native` extension module: listing, planning and reading,
//! returning pyarrow batches. The Python package (`python/grib_idx`) builds
//! the user-facing `read` and the Spark data source on top of these.

use std::collections::{HashMap, VecDeque};
use std::sync::OnceLock;

use arrow::array::RecordBatch;
use arrow::pyarrow::ToPyArrow;
use pyo3::exceptions::{PyIOError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyTuple};
use rayon::prelude::*;

use crate::batch::{self, LongitudeRange, OutputMode};
use crate::error::Error;
use crate::filters::{Column, Comparison, Predicate};
use crate::reader::{self, FilePlan, GribFile};
use crate::scan::Scan;
use crate::selection::Selection;

/// A message as Python sees it: `(message_index, offset, length, [(idx variable, idx level), ...])`.
type PyMessage = (usize, u64, u64, Vec<(String, String)>);

fn runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RUNTIME.get_or_init(|| tokio::runtime::Builder::new_multi_thread().enable_all().build().expect("failed to start tokio runtime"))
}

fn to_py_err(e: Error) -> PyErr {
    match e {
        Error::InvalidLocation { .. } | Error::InvalidOption { .. } | Error::LevelsNeedIndex | Error::Unsupported(_) => {
            PyValueError::new_err(e.to_string())
        }
        _ => PyIOError::new_err(e.to_string()),
    }
}

fn options(storage_options: Option<HashMap<String, String>>) -> HashMap<String, String> {
    storage_options.unwrap_or_default()
}

/// Parse a predicate tuple: `(op, column, value)` for `eq`, `in`, `gt`, `ge`,
/// `lt`, `le`; `("and"|"or", p, q)`; `("not", p)`. Shapes the reader can't
/// evaluate exactly raise ValueError.
fn predicate(obj: &Bound<'_, PyAny>) -> PyResult<Predicate> {
    let tuple = obj.cast::<PyTuple>().map_err(|_| PyValueError::new_err(format!("predicate must be a tuple, got {obj}")))?;
    let op: String = tuple.get_item(0)?.extract()?;
    let unsupported = || PyValueError::new_err(format!("unsupported predicate {obj}"));
    let column = || -> PyResult<Column> { Column::parse(&tuple.get_item(1)?.extract::<String>()?).ok_or_else(unsupported) };
    let comparison = |cmp| -> PyResult<Predicate> {
        match column()? {
            Column::LevelValue => Ok(Predicate::LevelCmp(cmp, tuple.get_item(2)?.extract()?)),
            _ => Err(unsupported()),
        }
    };
    match op.as_str() {
        "eq" => match column()? {
            c @ (Column::VariableShortName | Column::LevelType) => Ok(Predicate::StrEq(c, tuple.get_item(2)?.extract()?)),
            Column::Discipline => Ok(Predicate::DisciplineEq(tuple.get_item(2)?.extract()?)),
            Column::LevelValue => Ok(Predicate::LevelIn(vec![tuple.get_item(2)?.extract()?])),
        },
        "in" => match column()? {
            c @ (Column::VariableShortName | Column::LevelType) => Ok(Predicate::StrIn(c, tuple.get_item(2)?.extract()?)),
            Column::LevelValue => Ok(Predicate::LevelIn(tuple.get_item(2)?.extract()?)),
            Column::Discipline => Err(unsupported()),
        },
        "gt" => comparison(Comparison::Gt),
        "ge" => comparison(Comparison::Ge),
        "lt" => comparison(Comparison::Lt),
        "le" => comparison(Comparison::Le),
        "and" => Ok(Predicate::And(Box::new(predicate(&tuple.get_item(1)?)?), Box::new(predicate(&tuple.get_item(2)?)?))),
        "or" => Ok(Predicate::Or(Box::new(predicate(&tuple.get_item(1)?)?), Box::new(predicate(&tuple.get_item(2)?)?))),
        "not" => Ok(Predicate::Not(Box::new(predicate(&tuple.get_item(1)?)?))),
        _ => Err(unsupported()),
    }
}

fn selection(
    variables: Option<Vec<String>>,
    levels: Option<Vec<String>>,
    level_type: Option<String>,
    predicate_tuple: Option<&Bound<'_, PyAny>>,
) -> PyResult<Selection> {
    Ok(Selection { variables, levels, level_type, predicate: predicate_tuple.map(predicate).transpose()? })
}

/// Check that a predicate tuple is one the reader evaluates exactly.
#[pyfunction]
fn is_supported_predicate(obj: &Bound<'_, PyAny>) -> bool {
    predicate(obj).is_ok()
}

/// The pyarrow schema of `output_mode` ("flat" or "gridded").
#[pyfunction]
fn schema<'py>(py: Python<'py>, output_mode: &str) -> PyResult<Bound<'py, PyAny>> {
    batch::schema(OutputMode::parse(output_mode).map_err(to_py_err)?).to_pyarrow(py)
}

/// The GRIB files at `location` (a file, directory or glob), as
/// `(location, size, has_idx)` tuples; `has_idx` is None when unknown.
#[pyfunction]
#[pyo3(signature = (location, storage_options=None))]
fn list_files(
    py: Python<'_>,
    location: &str,
    storage_options: Option<HashMap<String, String>>,
) -> PyResult<Vec<(String, u64, Option<bool>)>> {
    let options = options(storage_options);
    let files = py.detach(|| runtime().block_on(reader::list_files(location, &options))).map_err(to_py_err)?;
    Ok(files.into_iter().map(|f| (f.location, f.size, f.has_idx)).collect())
}

/// Plan one file: a dict with `file_size`, `messages` (list of message tuples,
/// or None to read the file whole), `messages_in_index` and `warnings`.
#[pyfunction]
#[pyo3(signature = (location, size, has_idx=None, variables=None, levels=None, level_type=None, predicate=None, use_index=true, storage_options=None))]
#[allow(clippy::too_many_arguments)]
fn plan_file<'py>(
    py: Python<'py>,
    location: String,
    size: u64,
    has_idx: Option<bool>,
    variables: Option<Vec<String>>,
    levels: Option<Vec<String>>,
    level_type: Option<String>,
    predicate: Option<&Bound<'py, PyAny>>,
    use_index: bool,
    storage_options: Option<HashMap<String, String>>,
) -> PyResult<Bound<'py, PyDict>> {
    let selection = selection(variables, levels, level_type, predicate)?;
    let options = options(storage_options);
    let file = GribFile { location, size, has_idx };
    let plan = py.detach(|| runtime().block_on(reader::plan_file(&file, &options, &selection, use_index))).map_err(to_py_err)?;
    let dict = PyDict::new(py);
    dict.set_item("file_size", plan.file_size)?;
    let messages: Option<Vec<PyMessage>> = plan.messages.map(|ms| {
        ms.into_iter()
            .map(|m| (m.message_index, m.offset, m.length, m.entries.into_iter().map(|e| (e.variable_name, e.level_description)).collect()))
            .collect()
    });
    dict.set_item("messages", messages)?;
    dict.set_item("messages_in_index", plan.messages_in_index)?;
    dict.set_item("warnings", plan.warnings)?;
    Ok(dict)
}

/// Read a planned file, as an iterator of pyarrow batches decoded a chunk of
/// messages at a time: in gridded mode one batch per chunk, in flat mode one
/// per field. After iterating, `stats` has `messages_read`, `bytes_read` and
/// `decode_failures` (messages skipped because they couldn't be decoded).
#[pyfunction]
#[pyo3(signature = (location, file_size, messages=None, variables=None, levels=None, level_type=None, predicate=None, output_mode="gridded", longitude_range="0to360", storage_options=None))]
#[allow(clippy::too_many_arguments)]
fn read_plan<'py>(
    py: Python<'py>,
    location: String,
    file_size: u64,
    messages: Option<Vec<PyMessage>>,
    variables: Option<Vec<String>>,
    levels: Option<Vec<String>>,
    level_type: Option<String>,
    predicate: Option<&Bound<'py, PyAny>>,
    output_mode: &str,
    longitude_range: &str,
    storage_options: Option<HashMap<String, String>>,
) -> PyResult<PlanScan> {
    let selection = selection(variables, levels, level_type, predicate)?;
    let mode = OutputMode::parse(output_mode).map_err(to_py_err)?;
    let longitudes = LongitudeRange::parse(longitude_range).map_err(to_py_err)?;
    let options = options(storage_options);
    let plan = FilePlan {
        messages: messages
            .map(|ms| ms.into_iter().map(|(i, o, l, f)| reader::indexed_message(i, o, l, f, file_size)).collect::<Result<Vec<_>, _>>())
            .transpose()
            .map_err(to_py_err)?,
        location,
        file_size,
        messages_in_index: None,
        warnings: vec![],
    };
    let scan = py.detach(|| runtime().block_on(Scan::open(&plan, &options, selection, mode == OutputMode::Flat))).map_err(to_py_err)?;
    Ok(PlanScan { scan, location: plan.location, mode, longitudes, pending: VecDeque::new() })
}

#[pyclass]
struct PlanScan {
    scan: Scan,
    location: String,
    mode: OutputMode,
    longitudes: LongitudeRange,
    pending: VecDeque<RecordBatch>,
}

#[pymethods]
impl PlanScan {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__<'py>(&mut self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyAny>>> {
        loop {
            if let Some(batch) = self.pending.pop_front() {
                return batch.to_pyarrow(py).map(Some);
            }
            let (scan, location, mode, longitudes) = (&mut self.scan, &self.location, self.mode, self.longitudes);
            let batches = py
                .detach(|| -> Option<Result<Vec<RecordBatch>, Error>> {
                    let fields = match scan.next_chunk(rayon::current_num_threads())? {
                        Ok(fields) => fields,
                        Err(e) => return Some(Err(e)),
                    };
                    Some(match mode {
                        OutputMode::Gridded if fields.is_empty() => Ok(vec![]),
                        OutputMode::Gridded => batch::gridded_batch(location, &fields, longitudes).map(|b| vec![b]),
                        OutputMode::Flat => fields.par_iter().map(|f| batch::flat_batch(location, f, longitudes)).collect(),
                    })
                })
                .transpose()
                .map_err(to_py_err)?;
            match batches {
                Some(batches) => self.pending.extend(batches),
                None => return Ok(None),
            }
        }
    }

    #[getter]
    fn stats<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let stats = &self.scan.stats;
        let dict = PyDict::new(py);
        dict.set_item("messages_read", stats.messages_read)?;
        dict.set_item("bytes_read", stats.bytes_read)?;
        dict.set_item("decode_failures", stats.decode_failures.clone())?;
        Ok(dict)
    }
}

#[pymodule]
fn _native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(list_files, m)?)?;
    m.add_function(wrap_pyfunction!(plan_file, m)?)?;
    m.add_function(wrap_pyfunction!(read_plan, m)?)?;
    m.add_function(wrap_pyfunction!(schema, m)?)?;
    m.add_function(wrap_pyfunction!(is_supported_predicate, m)?)?;
    Ok(())
}
