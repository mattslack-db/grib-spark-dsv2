//! Arrow output in the Scala reader's schemas (`GribSchemas.scala`): flat
//! (one row per grid point) or gridded (one row per field).

use std::sync::{Arc, OnceLock};

use arrow::array::{
    ArrayRef, Float64Array, Int32Array, ListArray, RecordBatch, StringArray, StructArray, TimestampMicrosecondArray,
    UInt32Array,
};
use arrow::compute::take;
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef, TimeUnit};

use crate::decode::DecodedField;
use crate::error::{Error, Result};
use crate::geometry::to_signed_longitude;
use crate::product::ProductAttributes;

const MICROS_PER_MILLI: i64 = 1_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputMode {
    Flat,
    Gridded,
}

impl OutputMode {
    pub fn parse(value: &str) -> Result<Self> {
        match value.trim().to_lowercase().as_str() {
            "flat" => Ok(Self::Flat),
            "gridded" => Ok(Self::Gridded),
            other => Err(invalid("outputMode", format!("'{other}'. Must be 'flat' or 'gridded'."))),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LongitudeRange {
    ZeroTo360,
    Signed,
}

impl LongitudeRange {
    pub fn parse(value: &str) -> Result<Self> {
        match value.trim() {
            "0to360" => Ok(Self::ZeroTo360),
            "-180to180" => Ok(Self::Signed),
            other => Err(invalid("longitudeRange", format!("'{other}'. Must be '0to360' or '-180to180'."))),
        }
    }

    fn apply(self, lon: f64) -> f64 {
        match self {
            Self::ZeroTo360 => lon,
            Self::Signed => to_signed_longitude(lon),
        }
    }
}

fn invalid(name: &str, reason: String) -> Error {
    Error::InvalidOption { name: name.into(), reason }
}

fn int(name: &str, nullable: bool) -> Field {
    Field::new(name, DataType::Int32, nullable)
}

fn double(name: &str, nullable: bool) -> Field {
    Field::new(name, DataType::Float64, nullable)
}

fn string(name: &str, nullable: bool) -> Field {
    Field::new(name, DataType::Utf8, nullable)
}

fn timestamp(name: &str) -> Field {
    Field::new(name, DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), true)
}

fn product_fields() -> &'static Fields {
    static FIELDS: OnceLock<Fields> = OnceLock::new();
    FIELDS.get_or_init(|| {
        let strukt = |name: &str, fields: Vec<Field>| Field::new(name, DataType::Struct(fields.into()), true);
        Fields::from(vec![
            strukt("aerosol", vec![
                int("type", false),
                string("type_name", true),
                int("size_interval_type", true),
                double("size_first_m", true),
                double("size_second_m", true),
                int("wavelength_interval_type", true),
                double("wavelength_first_m", true),
                double("wavelength_second_m", true),
            ]),
            strukt("ensemble", vec![
                int("perturbation_type", false),
                string("perturbation_type_name", true),
                int("member", false),
                int("ensemble_size", false),
            ]),
            strukt("ensemble_derived", vec![int("type", false), string("type_name", true), int("ensemble_size", false)]),
            strukt("probability", vec![
                int("type", false),
                string("type_name", true),
                double("lower_limit", true),
                double("upper_limit", true),
            ]),
            int("percentile", true),
            strukt("statistic", vec![int("process", false), string("process_name", true), double("interval_hours", true)]),
        ])
    })
}

/// Columns shared by both schemas, before the mode-specific ones.
fn leading_fields() -> Vec<Field> {
    vec![
        string("file_path", false),
        int("message_index", false),
        int("discipline", true),
        string("variable_short_name", true),
        string("variable_name", true),
        string("units", true),
        string("level_type", true),
        double("level_value", true),
        timestamp("reference_time"),
        timestamp("forecast_time"),
    ]
}

fn trailing_fields() -> Vec<Field> {
    vec![
        int("pds_template", true),
        double("level_value2", true),
        Field::new("product", DataType::Struct(product_fields().clone()), true),
    ]
}

pub fn schema(mode: OutputMode) -> SchemaRef {
    let middle = match mode {
        OutputMode::Flat => vec![double("latitude", false), double("longitude", false), double("value", true)],
        OutputMode::Gridded => vec![
            int("nx", false),
            int("ny", false),
            double("lat_start", false),
            double("lon_start", false),
            double("lat_increment", false),
            double("lon_increment", false),
            Field::new("values", DataType::List(Arc::new(double("element", true))), false),
        ],
    };
    Arc::new(Schema::new([leading_fields(), middle, trailing_fields()].concat()))
}

/// Gridded mode: one batch, one row per field.
pub fn gridded_batch(file_path: &str, fields: &[DecodedField], longitudes: LongitudeRange) -> Result<RecordBatch> {
    let rows: Vec<&DecodedField> = fields.iter().collect();
    let ints = |f: fn(&DecodedField) -> i32| Arc::new(fields.iter().map(f).collect::<Int32Array>()) as ArrayRef;
    let doubles = |f: &dyn Fn(&DecodedField) -> f64| Arc::new(fields.iter().map(f).collect::<Float64Array>()) as ArrayRef;
    let offsets = OffsetBuffer::from_lengths(fields.iter().map(|f| f.values.len()));
    let flat: Float64Array = fields.iter().flat_map(|f| f.values.iter().map(|&v| f64::from(v))).collect();
    let values = ListArray::new(Arc::new(double("element", true)), offsets, Arc::new(flat), None);

    let middle: Vec<ArrayRef> = vec![
        ints(|f| f.nx),
        ints(|f| f.ny),
        doubles(&|f| f.geometry.lat_start),
        // The Scala reader shifts the gridded origin like any longitude.
        doubles(&|f| longitudes.apply(f.geometry.lon_start)),
        doubles(&|f| f.geometry.lat_increment),
        doubles(&|f| f.geometry.lon_increment),
        Arc::new(values),
    ];
    let columns = [leading_columns(file_path, &rows), middle, trailing_columns(&rows)?].concat();
    Ok(RecordBatch::try_new(schema(OutputMode::Gridded), columns)?)
}

/// Flat mode: one batch per field, one row per grid point. The field's
/// constant columns are built once and repeated with Arrow's `take` kernel.
pub fn flat_batch(file_path: &str, field: &DecodedField, longitudes: LongitudeRange) -> Result<RecordBatch> {
    let (lats, lons) = field.lat_lons.as_ref().ok_or_else(|| Error::Unsupported("flat mode needs lat/lons decoded".into()))?;
    let points = field.values.len();
    let first_row = UInt32Array::from(vec![0u32; points]);
    let repeat = |columns: Vec<ArrayRef>| -> Result<Vec<ArrayRef>> {
        columns.iter().map(|c| Ok(take(c.as_ref(), &first_row, None)?)).collect()
    };
    let middle: Vec<ArrayRef> = vec![
        Arc::new(Float64Array::from(lats.clone())),
        Arc::new(lons.iter().map(|&lon| longitudes.apply(lon)).collect::<Float64Array>()),
        Arc::new(field.values.iter().map(|&v| f64::from(v)).collect::<Float64Array>()),
    ];
    let columns = [repeat(leading_columns(file_path, &[field]))?, middle, repeat(trailing_columns(&[field])?)?].concat();
    Ok(RecordBatch::try_new(schema(OutputMode::Flat), columns)?)
}

fn leading_columns(file_path: &str, rows: &[&DecodedField]) -> Vec<ArrayRef> {
    let strings = |f: fn(&DecodedField) -> &str| Arc::new(rows.iter().map(|r| Some(f(r))).collect::<StringArray>()) as ArrayRef;
    let timestamps = |f: fn(&DecodedField) -> i64| {
        let micros: TimestampMicrosecondArray = rows.iter().map(|r| Some(f(r) * MICROS_PER_MILLI)).collect();
        Arc::new(micros.with_timezone("UTC")) as ArrayRef
    };
    vec![
        Arc::new(StringArray::from(vec![file_path; rows.len()])),
        Arc::new(rows.iter().map(|r| r.message_index).collect::<Int32Array>()),
        Arc::new(rows.iter().map(|r| r.discipline).collect::<Int32Array>()),
        strings(|f| &f.variable_short_name),
        strings(|f| &f.variable_name),
        strings(|f| &f.units),
        strings(|f| &f.level_type),
        Arc::new(rows.iter().map(|r| r.level_value).collect::<Float64Array>()),
        timestamps(|f| f.reference_time_ms),
        timestamps(|f| f.forecast_time_ms),
    ]
}

fn trailing_columns(rows: &[&DecodedField]) -> Result<Vec<ArrayRef>> {
    let products: Vec<&ProductAttributes> = rows.iter().map(|r| &r.product).collect();
    Ok(vec![
        Arc::new(rows.iter().map(|r| Some(r.pds_template)).collect::<Int32Array>()),
        Arc::new(rows.iter().map(|r| r.level_value2).collect::<Float64Array>()),
        Arc::new(product_array(&products)?),
    ])
}

/// A struct column whose rows are present where `present` is true.
fn struct_column(fields: &Fields, columns: Vec<ArrayRef>, present: Vec<bool>) -> Result<StructArray> {
    Ok(StructArray::try_new(fields.clone(), columns, Some(NullBuffer::from(present)))?)
}

fn sub_fields(name: &str) -> &'static Fields {
    match product_fields().find(name).map(|(_, f)| f.data_type()) {
        Some(DataType::Struct(fields)) => fields,
        _ => unreachable!("product field {name} is a struct"),
    }
}

fn product_array(products: &[&ProductAttributes]) -> Result<StructArray> {
    macro_rules! column {
        ($items:expr, $array:ty, $get:expr) => {
            Arc::new($items.iter().map($get).collect::<$array>()) as ArrayRef
        };
    }
    let aerosols: Vec<_> = products.iter().map(|p| p.aerosol.as_ref()).collect();
    let aerosol = struct_column(sub_fields("aerosol"), vec![
        column!(aerosols, Int32Array, |a| a.map(|a| a.aerosol_type)),
        column!(aerosols, StringArray, |a| a.and_then(|a| a.type_name.as_deref())),
        column!(aerosols, Int32Array, |a| a.and_then(|a| a.size_interval_type)),
        column!(aerosols, Float64Array, |a| a.and_then(|a| a.size_first_m)),
        column!(aerosols, Float64Array, |a| a.and_then(|a| a.size_second_m)),
        column!(aerosols, Int32Array, |a| a.and_then(|a| a.wavelength_interval_type)),
        column!(aerosols, Float64Array, |a| a.and_then(|a| a.wavelength_first_m)),
        column!(aerosols, Float64Array, |a| a.and_then(|a| a.wavelength_second_m)),
    ], aerosols.iter().map(Option::is_some).collect())?;

    let ensembles: Vec<_> = products.iter().map(|p| p.ensemble.as_ref()).collect();
    let ensemble = struct_column(sub_fields("ensemble"), vec![
        column!(ensembles, Int32Array, |e| e.map(|e| e.perturbation_type)),
        column!(ensembles, StringArray, |e| e.and_then(|e| e.perturbation_type_name.as_deref())),
        column!(ensembles, Int32Array, |e| e.map(|e| e.member)),
        column!(ensembles, Int32Array, |e| e.map(|e| e.ensemble_size)),
    ], ensembles.iter().map(Option::is_some).collect())?;

    let deriveds: Vec<_> = products.iter().map(|p| p.ensemble_derived.as_ref()).collect();
    let ensemble_derived = struct_column(sub_fields("ensemble_derived"), vec![
        column!(deriveds, Int32Array, |d| d.map(|d| d.derived_type)),
        column!(deriveds, StringArray, |d| d.and_then(|d| d.type_name.as_deref())),
        column!(deriveds, Int32Array, |d| d.map(|d| d.ensemble_size)),
    ], deriveds.iter().map(Option::is_some).collect())?;

    let probabilities: Vec<_> = products.iter().map(|p| p.probability.as_ref()).collect();
    let probability = struct_column(sub_fields("probability"), vec![
        column!(probabilities, Int32Array, |p| p.map(|p| p.probability_type)),
        column!(probabilities, StringArray, |p| p.and_then(|p| p.type_name.as_deref())),
        column!(probabilities, Float64Array, |p| p.and_then(|p| p.lower_limit)),
        column!(probabilities, Float64Array, |p| p.and_then(|p| p.upper_limit)),
    ], probabilities.iter().map(Option::is_some).collect())?;

    let statistics: Vec<_> = products.iter().map(|p| p.statistic.as_ref()).collect();
    let statistic = struct_column(sub_fields("statistic"), vec![
        column!(statistics, Int32Array, |s| s.map(|s| s.process)),
        column!(statistics, StringArray, |s| s.and_then(|s| s.process_name.as_deref())),
        column!(statistics, Float64Array, |s| s.and_then(|s| s.interval_hours)),
    ], statistics.iter().map(Option::is_some).collect())?;

    let percentile = column!(products, Int32Array, |p| p.percentile);
    let columns: Vec<ArrayRef> =
        vec![Arc::new(aerosol), Arc::new(ensemble), Arc::new(ensemble_derived), Arc::new(probability), percentile, Arc::new(statistic)];
    Ok(StructArray::try_new(product_fields().clone(), columns, None)?)
}
