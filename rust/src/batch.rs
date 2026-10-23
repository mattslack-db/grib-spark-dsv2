//! Converts decoded fields to an Arrow `RecordBatch` in the Scala reader's
//! gridded layout (one row per field), plus a few columns the spike adds.

use std::sync::Arc;

use arrow::array::{
    ArrayRef, Float32Array, Float64Array, Int32Array, ListArray, RecordBatch, StringArray, TimestampMicrosecondArray,
};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};

use crate::decode::DecodedField;
use crate::error::Result;

const MICROS_PER_MILLI: i64 = 1_000;

pub fn schema() -> SchemaRef {
    let timestamp = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
    let values_item = Arc::new(Field::new("item", DataType::Float32, true));
    Arc::new(Schema::new(vec![
        Field::new("file_path", DataType::Utf8, false),
        Field::new("message_index", DataType::Int32, false),
        Field::new("discipline", DataType::Int32, true),
        Field::new("parameter_category", DataType::Int32, true),
        Field::new("parameter_number", DataType::Int32, true),
        Field::new("variable_short_name", DataType::Utf8, true),
        Field::new("variable_name", DataType::Utf8, true),
        Field::new("level_description", DataType::Utf8, true),
        Field::new("level_type_code", DataType::Int32, true),
        Field::new("level_type", DataType::Utf8, true),
        Field::new("level_value", DataType::Float64, true),
        Field::new("level_value2", DataType::Float64, true),
        Field::new("reference_time", timestamp.clone(), true),
        Field::new("forecast_time", timestamp, true),
        Field::new("pds_template", DataType::Int32, true),
        Field::new("grid_template", DataType::Int32, true),
        Field::new("nx", DataType::Int32, false),
        Field::new("ny", DataType::Int32, false),
        Field::new("lat_start", DataType::Float64, true),
        Field::new("lon_start", DataType::Float64, true),
        Field::new("lat_increment", DataType::Float64, true),
        Field::new("lon_increment", DataType::Float64, true),
        Field::new("values", DataType::List(values_item), false),
    ]))
}

pub fn to_record_batch(file_path: &str, fields: &[DecodedField]) -> Result<RecordBatch> {
    let ints = |f: fn(&DecodedField) -> Option<i32>| Arc::new(fields.iter().map(f).collect::<Int32Array>()) as ArrayRef;
    let floats =
        |f: fn(&DecodedField) -> Option<f64>| Arc::new(fields.iter().map(f).collect::<Float64Array>()) as ArrayRef;
    let strings =
        |f: fn(&DecodedField) -> Option<&str>| Arc::new(fields.iter().map(f).collect::<StringArray>()) as ArrayRef;
    let timestamps = |f: fn(&DecodedField) -> Option<i64>| {
        let micros: TimestampMicrosecondArray = fields.iter().map(|d| f(d).map(|ms| ms * MICROS_PER_MILLI)).collect();
        Arc::new(micros.with_timezone("UTC")) as ArrayRef
    };

    let columns: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(vec![file_path; fields.len()])),
        ints(|f| Some(f.message_index)),
        ints(|f| Some(f.discipline)),
        ints(|f| f.parameter_category),
        ints(|f| f.parameter_number),
        strings(|f| Some(f.variable_short_name.as_str())),
        strings(|f| f.variable_name.as_deref()),
        strings(|f| Some(f.level_description.as_str())),
        ints(|f| f.level_type_code),
        strings(|f| f.level_type.as_deref()),
        floats(|f| f.level_value),
        floats(|f| f.level_value2),
        timestamps(|f| f.reference_time_ms),
        timestamps(|f| f.forecast_time_ms),
        ints(|f| Some(f.pds_template)),
        ints(|f| Some(f.grid_template)),
        ints(|f| Some(f.nx)),
        ints(|f| Some(f.ny)),
        floats(|f| f.lat_start),
        floats(|f| f.lon_start),
        floats(|f| f.lat_increment),
        floats(|f| f.lon_increment),
        values_column(fields),
    ];
    Ok(RecordBatch::try_new(schema(), columns)?)
}

/// The grid values as `list<float32>`, NaN for missing points (as the Scala
/// reader emits them); no per-element null buffer.
fn values_column(fields: &[DecodedField]) -> ArrayRef {
    let offsets = OffsetBuffer::from_lengths(fields.iter().map(|f| f.values.len()));
    let flat: Float32Array = fields.iter().flat_map(|f| f.values.iter().copied()).collect::<Vec<_>>().into();
    let item = Arc::new(Field::new("item", DataType::Float32, true));
    Arc::new(ListArray::new(item, offsets, Arc::new(flat), None))
}
