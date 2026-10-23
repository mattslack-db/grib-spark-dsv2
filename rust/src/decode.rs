//! Decodes one physical GRIB2 message (held in memory) into its fields.

use grib::codetables::{ArrayLookup, CodeTable4_2, CodeTable4_5};
use grib::{FixedSurface, LatLons};

use crate::error::{Error, Result};
use crate::idx::IndexedMessage;

/// GRIB2 code table 4.5: "missing" type of fixed surface.
const MISSING_SURFACE_TYPE: u8 = 255;
/// GRIB2 grid template 3.0: regular latitude/longitude.
const REGULAR_LAT_LON_TEMPLATE: u16 = 0;

/// One decoded field, mirroring the Scala reader's gridded-mode row.
#[derive(Debug, Clone, PartialEq)]
pub struct DecodedField {
    pub message_index: i32,
    pub discipline: i32,
    pub parameter_category: Option<i32>,
    pub parameter_number: Option<i32>,
    /// From the `.idx` line (wgrib2 abbreviation, e.g. "TMP"); grib-rs code
    /// tables carry long names only.
    pub variable_short_name: String,
    pub variable_name: Option<String>,
    pub level_description: String,
    pub level_type_code: Option<i32>,
    pub level_type: Option<String>,
    pub level_value: Option<f64>,
    pub level_value2: Option<f64>,
    pub reference_time_ms: Option<i64>,
    pub forecast_time_ms: Option<i64>,
    pub pds_template: i32,
    pub grid_template: i32,
    pub nx: i32,
    pub ny: i32,
    /// Grid origin and signed increments: regular lat/lon grids only.
    pub lat_start: Option<f64>,
    pub lon_start: Option<f64>,
    pub lat_increment: Option<f64>,
    pub lon_increment: Option<f64>,
    /// Missing (bitmapped) points are NaN.
    pub values: Vec<f32>,
}

/// Decode every field of `message` from its bytes, pairing the n-th GRIB
/// submessage with the n-th `.idx` entry.
pub fn decode_message(bytes: &[u8], message: &IndexedMessage) -> Result<Vec<DecodedField>> {
    let grib2 = grib::from_bytes(bytes).map_err(|e| decode_error(message, &e))?;
    let fields: Vec<DecodedField> = grib2
        .iter()
        .zip(&message.entries)
        .enumerate()
        .map(|(position, ((_, submessage), entry))| {
            decode_submessage(submessage, message.message_index + position, &entry.variable_name, &entry.level_description)
                .map_err(|e| decode_error(message, &e))
        })
        .collect::<Result<_>>()?;

    if fields.len() != message.entries.len() {
        return Err(Error::Decode {
            message_index: message.message_index,
            reason: format!("idx lists {} fields, message holds {}", message.entries.len(), fields.len()),
        });
    }
    Ok(fields)
}

fn decode_submessage<R: grib::Grib2Read>(
    submessage: grib::SubMessage<'_, R>,
    message_index: usize,
    short_name: &str,
    level_description: &str,
) -> std::result::Result<DecodedField, grib::GribError> {
    let discipline = submessage.indicator().discipline;
    let prod_def = submessage.prod_def();
    let category = prod_def.parameter_category();
    let number = prod_def.parameter_number();
    let surfaces = prod_def.fixed_surfaces();
    let first_surface = surfaces.as_ref().map(|(first, _)| first);
    let second_surface = surfaces.as_ref().map(|(_, second)| second);
    let temporal = submessage.temporal_info();
    let grid_template = submessage.grid_def().grid_tmpl_num();
    let pds_template = prod_def.prod_tmpl_num();
    let (nx, ny) = submessage.grid_shape()?;
    let geometry = if grid_template == REGULAR_LAT_LON_TEMPLATE {
        regular_grid_geometry(&submessage, nx)?
    } else {
        None
    };

    let values: Vec<f32> = grib::Grib2SubmessageDecoder::from(submessage)?.dispatch()?.collect();

    Ok(DecodedField {
        message_index: to_i32(message_index),
        discipline: i32::from(discipline),
        parameter_category: category.map(i32::from),
        parameter_number: number.map(i32::from),
        variable_short_name: short_name.to_string(),
        variable_name: category.zip(number).and_then(|(c, n)| table_name(&CodeTable4_2::new(discipline, c), n)),
        level_description: level_description.to_string(),
        level_type_code: first_surface.map(|s| i32::from(s.surface_type)),
        level_type: first_surface.and_then(|s| table_name(&CodeTable4_5, s.surface_type)),
        level_value: first_surface.map(FixedSurface::value).filter(|v| !v.is_nan()),
        level_value2: second_surface
            .filter(|s| s.surface_type != MISSING_SURFACE_TYPE)
            .map(FixedSurface::value)
            .filter(|v| !v.is_nan()),
        reference_time_ms: temporal.ref_time.map(|t| t.timestamp_millis()),
        forecast_time_ms: temporal.forecast_time_target.map(|t| t.timestamp_millis()),
        pds_template: i32::from(pds_template),
        grid_template: i32::from(grid_template),
        nx: to_i32(nx),
        ny: to_i32(ny),
        lat_start: geometry.map(|g| g.lat_start),
        lon_start: geometry.map(|g| g.lon_start),
        lat_increment: geometry.map(|g| g.lat_increment),
        lon_increment: geometry.map(|g| g.lon_increment),
        values,
    })
}

#[derive(Debug, Clone, Copy)]
struct GridGeometry {
    lat_start: f64,
    lon_start: f64,
    lat_increment: f64,
    lon_increment: f64,
}

/// Origin and signed increments of a regular lat/lon grid, read off the first
/// point, its row neighbour and its column neighbour. Longitudes are left
/// unnormalised (GFS runs 0..360), matching netCDF-Java's `startx`.
fn regular_grid_geometry<R: grib::Grib2Read>(
    submessage: &grib::SubMessage<'_, R>,
    nx: usize,
) -> std::result::Result<Option<GridGeometry>, grib::GribError> {
    let points: Vec<(f64, f64)> = submessage.latlons_unchecked()?.take(nx + 1).collect();
    let (Some(&(lat0, lon0)), Some(&(_, lon1)), Some(&(lat_next_row, _))) =
        (points.first(), points.get(1), points.get(nx))
    else {
        return Ok(None);
    };
    Ok(Some(GridGeometry {
        lat_start: lat0,
        lon_start: lon0,
        lat_increment: lat_next_row - lat0,
        lon_increment: lon1 - lon0,
    }))
}

/// A WMO code table entry, or `None` for codes grib-rs doesn't define (notably
/// NCEP's local-use range, 192..=254), rather than its "not implemented" text.
fn table_name(table: &impl ArrayLookup, code: u8) -> Option<String> {
    table.data().get(usize::from(code)).filter(|name| !name.is_empty()).map(|name| name.to_string())
}

fn to_i32(n: usize) -> i32 {
    i32::try_from(n).expect("GRIB counts fit in i32")
}

fn decode_error(message: &IndexedMessage, e: &dyn std::fmt::Display) -> Error {
    Error::Decode { message_index: message.message_index, reason: e.to_string() }
}
