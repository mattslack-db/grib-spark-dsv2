//! Decodes GRIB2 messages held in memory into fields carrying the same values
//! as the Scala reader's `DecodedGribMessage`.

use grib::FixedSurface;

use crate::error::{Error, Result};
use crate::geometry::{self, Geometry};
use crate::product::{self, ProductAttributes};
use crate::tables::{self, CodeTable};

/// GRIB2 code table 4.5: "missing" type of fixed surface.
const MISSING_SURFACE_TYPE: u8 = 255;
const GRIB_MAGIC: &[u8; 4] = b"GRIB";
const EDITION_OCTET: usize = 7;
const GRIB1: u8 = 1;

/// One decoded field.
#[derive(Debug, Clone, PartialEq)]
pub struct DecodedField {
    pub message_index: i32,
    pub discipline: i32,
    pub variable_short_name: String,
    pub variable_name: String,
    pub units: String,
    pub level_type: String,
    /// NaN when the first fixed surface has no value (as netCDF-Java reports it).
    pub level_value: f64,
    /// The second fixed surface, for layers.
    pub level_value2: Option<f64>,
    pub reference_time_ms: i64,
    /// For statistically processed fields, the end of the interval.
    pub forecast_time_ms: i64,
    pub pds_template: i32,
    pub nx: i32,
    pub ny: i32,
    pub geometry: Geometry,
    /// Per-point latitudes and longitudes (0..360), when requested (flat mode).
    pub lat_lons: Option<(Vec<f64>, Vec<f64>)>,
    /// Missing (bitmapped) points are NaN.
    pub values: Vec<f32>,
    pub product: ProductAttributes,
}

/// The fields decoded from a buffer, and the messages that couldn't be decoded
/// (skipped, as the Scala reader skips them with a warning).
#[derive(Debug, Default)]
pub struct Decoded {
    pub fields: Vec<DecodedField>,
    pub failures: Vec<String>,
    /// Fields in the buffer, decoded or not.
    pub submessages: usize,
}

/// Decode every field in `bytes`, which hold one or more whole GRIB2 messages,
/// numbering them from `first_index`.
pub fn decode_all(bytes: &[u8], first_index: usize, with_lat_lons: bool) -> Result<Decoded> {
    // Unlike netCDF-Java's scanner, which silently finds nothing, fail on bytes
    // that aren't GRIB (e.g. a non-GRIB file, or `.idx` offsets that don't
    // match the file).
    if !bytes.starts_with(GRIB_MAGIC) {
        return Err(Error::Decode { message_index: first_index, reason: "not a GRIB message (no GRIB header)".into() });
    }
    if bytes.get(EDITION_OCTET) == Some(&GRIB1) {
        return Err(Error::Unsupported("GRIB1 messages (grib-rs decodes GRIB2 only)".into()));
    }
    let grib2 = grib::from_bytes(bytes).map_err(|e| Error::Decode { message_index: first_index, reason: e.to_string() })?;
    let mut decoded = Decoded::default();
    for (position, (_, submessage)) in grib2.iter().enumerate() {
        let message_index = first_index + position;
        match decode_submessage(submessage, message_index, with_lat_lons) {
            Ok(field) => decoded.fields.push(field),
            Err(e) => decoded.failures.push(format!("message {message_index}: {e}")),
        }
        decoded.submessages += 1;
    }
    Ok(decoded)
}

fn decode_submessage<R: grib::Grib2Read>(
    submessage: grib::SubMessage<'_, R>,
    message_index: usize,
    with_lat_lons: bool,
) -> std::result::Result<DecodedField, grib::GribError> {
    let centre = submessage.identification().centre_id();
    let discipline = submessage.indicator().discipline;
    let prod_def = submessage.prod_def();
    let pds_template = prod_def.prod_tmpl_num();
    let payload: Vec<u8> = prod_def.iter().copied().collect();
    let definition = product::parse(centre, pds_template, &payload);

    let category = prod_def.parameter_category();
    let number = prod_def.parameter_number();
    let parameter = category.zip(number).and_then(|(c, n)| tables::parameter(centre, discipline, c, n));
    let variable_short_name = parameter
        .and_then(|p| p.abbrev)
        .map(str::to_string)
        .unwrap_or_else(|| format!("var{discipline}_{}_{}", fmt_code(category), fmt_code(number)));
    let variable_name = parameter.and_then(|p| p.name).map_or_else(|| variable_short_name.clone(), str::to_string);
    let units = parameter.map_or("unknown", |p| p.unit).to_string();

    let surfaces = prod_def.fixed_surfaces();
    let (first, second) = match &surfaces {
        Some((first, second)) => (Some(first), Some(second)),
        None => (None, None),
    };
    let level_type = first.map(|s| s.surface_type).map_or_else(
        || "level_unknown".to_string(),
        |t| tables::code_name(centre, CodeTable::LevelType, u32::from(t)).map_or_else(|| format!("level_{t}"), str::to_string),
    );
    let level_value2 = second.filter(|s| s.surface_type != MISSING_SURFACE_TYPE).map(FixedSurface::value).filter(|v| !v.is_nan());

    let temporal = submessage.temporal_info();
    let reference_time_ms = temporal.ref_time.map_or(0, |t| t.timestamp_millis());
    let forecast_time_ms =
        definition.interval_end_ms.or(temporal.forecast_time_target.map(|t| t.timestamp_millis())).unwrap_or(reference_time_ms);

    let (nx, ny) = submessage.grid_shape()?;
    let geometry = geometry::geometry(&submessage);
    let lat_lons = if with_lat_lons { Some(geometry::lat_lons(&submessage, &geometry, nx, ny)?) } else { None };
    let row_length = geometry::alternating_row_length(&submessage, nx, ny);
    let mut values: Vec<f32> = grib::Grib2SubmessageDecoder::from(submessage)?.dispatch()?.collect();
    if let Some(row_length) = row_length {
        geometry::straighten_rows(&mut values, row_length);
    }

    Ok(DecodedField {
        message_index: to_i32(message_index),
        discipline: i32::from(discipline),
        variable_short_name,
        variable_name,
        units,
        level_type,
        level_value: first.map_or(f64::NAN, FixedSurface::value),
        level_value2,
        reference_time_ms,
        forecast_time_ms,
        pds_template: i32::from(pds_template),
        nx: to_i32(nx),
        ny: to_i32(ny),
        geometry,
        lat_lons,
        values,
        product: definition.attributes,
    })
}

fn fmt_code(code: Option<u8>) -> String {
    code.map_or_else(|| "missing".to_string(), |c| c.to_string())
}

fn to_i32(n: usize) -> i32 {
    i32::try_from(n).expect("GRIB counts fit in i32")
}
