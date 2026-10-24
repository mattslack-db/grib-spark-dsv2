//! Grid geometry as the Scala reader reports it (netCDF-Java's
//! `GdsHorizCoordSys`): an origin and signed increments for gridded mode, and
//! per-point latitudes/longitudes (0..360) for flat mode.

use grib::projection::{Ellipsoid, Lcc, LccParams, Project};
use grib::{GridDefinitionTemplateValues, GridPointIndex, LatLons};

/// Lambert conformal and other template-3.x angles are in micro-degrees.
const MICRO_DEGREES: f64 = 1e-6;
/// Template 3.30 grid lengths are in millimetres; netCDF-Java reports km.
const MM_PER_KM: f64 = 1e6;
const METRES_PER_KM: f64 = 1e3;
const FULL_CIRCLE: f64 = 360.0;
const HALF_CIRCLE: f64 = 180.0;

/// Origin and signed increments. For lat/lon grids these are degrees (dy is
/// negative when the grid scans north to south); for Lambert grids they are
/// the first point's projected x/y and the grid lengths, in km. NaN when the
/// grid template isn't supported.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Geometry {
    pub lat_start: f64,
    pub lon_start: f64,
    pub lat_increment: f64,
    pub lon_increment: f64,
}

impl Geometry {
    const UNKNOWN: Self = Self { lat_start: f64::NAN, lon_start: f64::NAN, lat_increment: f64::NAN, lon_increment: f64::NAN };
}

pub fn geometry<R: grib::Grib2Read>(submessage: &grib::SubMessage<'_, R>) -> Geometry {
    match GridDefinitionTemplateValues::try_from(submessage.grid_def()) {
        Ok(GridDefinitionTemplateValues::Template0(def)) => {
            let ll = &def.lat_lon;
            let dx = f64::from(ll.i_direction_inc) * MICRO_DEGREES;
            let dy = f64::from(ll.j_direction_inc) * MICRO_DEGREES;
            Geometry {
                lat_start: f64::from(ll.grid.first_point_lat) * MICRO_DEGREES,
                lon_start: f64::from(ll.grid.first_point_lon) * MICRO_DEGREES,
                lat_increment: if ll.scanning_mode.scans_positively_for_j() { dy } else { -dy },
                lon_increment: if ll.scanning_mode.scans_positively_for_i() { dx } else { -dx },
            }
        }
        Ok(GridDefinitionTemplateValues::Template30(def)) => lambert_geometry(&def).unwrap_or(Geometry::UNKNOWN),
        _ => Geometry::UNKNOWN,
    }
}

fn lambert_geometry(def: &grib::def::grib2::template::Template3_30) -> Option<Geometry> {
    let (a, b) = def.earth_shape.radii()?;
    let degrees = |v: f64| v * MICRO_DEGREES;
    let projection = Lcc::new(&LccParams {
        ellipsoid: Ellipsoid::from_a_and_b(a, b),
        lat_0: degrees(f64::from(def.lad)),
        lon_0: degrees(f64::from(def.lov)),
        lat_1: degrees(f64::from(def.latin1)),
        lat_2: degrees(f64::from(def.latin2)),
    })
    .ok()?;
    let first = (
        degrees(f64::from(def.first_point_lon)).to_radians(),
        degrees(f64::from(def.first_point_lat)).to_radians(),
    );
    let (x, y) = projection.project(&first, false).ok()?;
    let dx = f64::from(def.dx) / MM_PER_KM;
    let dy = f64::from(def.dy) / MM_PER_KM;
    Some(Geometry {
        lat_start: y / METRES_PER_KM,
        lon_start: x / METRES_PER_KM,
        lat_increment: if def.scanning_mode.scans_positively_for_j() { dy } else { -dy },
        lon_increment: if def.scanning_mode.scans_positively_for_i() { dx } else { -dx },
    })
}

/// The row length to reverse every other row by when adjacent rows are stored
/// in opposite directions (scanning mode flag 0x10, e.g. NBM), or None.
/// netCDF-Java returns such grids with every row in the same direction.
pub fn alternating_row_length<R: grib::Grib2Read>(submessage: &grib::SubMessage<'_, R>, nx: usize, ny: usize) -> Option<usize> {
    let grid = GridDefinitionTemplateValues::try_from(submessage.grid_def()).ok()?;
    let mode = grid.scanning_mode();
    mode.scans_alternating_rows().then_some(if mode.is_consecutive_for_i() { nx } else { ny })
}

/// Reverse every second row (from the second), undoing alternating scanning.
pub fn straighten_rows<T>(points: &mut [T], row_length: usize) {
    for row in points.chunks_mut(row_length.max(1)).skip(1).step_by(2) {
        row.reverse();
    }
}

/// Per-point latitudes and longitudes in the grid's (straightened) value
/// order, longitudes in 0..360. Regular lat/lon grids use the origin and
/// increments row by row (as the Scala reader does); other grids use grib-rs's
/// projections, which follow the stored order.
pub fn lat_lons<R: grib::Grib2Read>(
    submessage: &grib::SubMessage<'_, R>,
    geometry: &Geometry,
    nx: usize,
    ny: usize,
) -> Result<(Vec<f64>, Vec<f64>), grib::GribError> {
    if submessage.grid_def().grid_tmpl_num() == 0 {
        let lats = (0..ny).flat_map(|y| std::iter::repeat_n(geometry.lat_start + y as f64 * geometry.lat_increment, nx));
        let lons = (0..ny).flat_map(|_| (0..nx).map(|x| wrap_0_360(geometry.lon_start + x as f64 * geometry.lon_increment)));
        return Ok((lats.collect(), lons.collect()));
    }
    let (mut lats, mut lons): (Vec<f64>, Vec<f64>) = submessage.latlons_unchecked()?.map(|(lat, lon)| (lat, wrap_0_360(lon))).unzip();
    if let Some(row_length) = alternating_row_length(submessage, nx, ny) {
        straighten_rows(&mut lats, row_length);
        straighten_rows(&mut lons, row_length);
    }
    Ok((lats, lons))
}

pub fn wrap_0_360(lon: f64) -> f64 {
    lon.rem_euclid(FULL_CIRCLE)
}

/// The `-180to180` longitude range: values above 180 shifted down a turn.
pub fn to_signed_longitude(lon: f64) -> f64 {
    if lon > HALF_CIRCLE { lon - FULL_CIRCLE } else { lon }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn straightening_reverses_every_second_row() {
        let mut points = [1, 2, 3, 6, 5, 4, 7, 8, 9];
        straighten_rows(&mut points, 3);
        assert_eq!(points, [1, 2, 3, 4, 5, 6, 7, 8, 9]);
    }

    #[test]
    fn longitudes_wrap_into_0_to_360() {
        assert_eq!(wrap_0_360(-1.0), 359.0);
        assert_eq!(wrap_0_360(360.0), 0.0);
        assert_eq!(wrap_0_360(359.5), 359.5);
    }

    #[test]
    fn signed_range_shifts_only_the_eastern_hemisphere_above_180() {
        assert_eq!(to_signed_longitude(270.0), -90.0);
        assert_eq!(to_signed_longitude(180.0), 180.0);
        assert_eq!(to_signed_longitude(10.0), 10.0);
    }
}
