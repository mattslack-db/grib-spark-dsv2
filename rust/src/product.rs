//! Template-specific attributes of a GRIB2 product definition (section 4),
//! mirroring `ProductAttributes.scala`, plus the end of a statistically
//! processed field's time interval (which netCDF-Java reports as its forecast
//! time). Parsed from the section's octets per the WMO template layouts.

use chrono::{NaiveDate, NaiveDateTime};

use crate::tables::{self, CodeTable};

#[derive(Debug, Clone, Default, PartialEq)]
pub struct ProductAttributes {
    pub aerosol: Option<Aerosol>,
    pub ensemble: Option<Ensemble>,
    pub ensemble_derived: Option<EnsembleDerived>,
    pub probability: Option<Probability>,
    pub percentile: Option<i32>,
    pub statistic: Option<Statistic>,
}

/// Aerosol identity (template 4.48). Sizes and wavelengths in metres; the
/// interval types are code table 4.91, and limits a type doesn't use are None.
#[derive(Debug, Clone, PartialEq)]
pub struct Aerosol {
    pub aerosol_type: i32,
    pub type_name: Option<String>,
    pub size_interval_type: Option<i32>,
    pub size_first_m: Option<f64>,
    pub size_second_m: Option<f64>,
    pub wavelength_interval_type: Option<i32>,
    pub wavelength_first_m: Option<f64>,
    pub wavelength_second_m: Option<f64>,
}

/// One ensemble member (templates 4.1, 4.11, 4.60, 4.61).
#[derive(Debug, Clone, PartialEq)]
pub struct Ensemble {
    pub perturbation_type: i32,
    pub perturbation_type_name: Option<String>,
    pub member: i32,
    pub ensemble_size: i32,
}

/// A product derived from all members (templates 4.2, 4.12).
#[derive(Debug, Clone, PartialEq)]
pub struct EnsembleDerived {
    pub derived_type: i32,
    pub type_name: Option<String>,
    pub ensemble_size: i32,
}

/// Probability of being below/above/between limits (templates 4.5, 4.9).
#[derive(Debug, Clone, PartialEq)]
pub struct Probability {
    pub probability_type: i32,
    pub type_name: Option<String>,
    pub lower_limit: Option<f64>,
    pub upper_limit: Option<f64>,
}

/// Statistical processing over a time interval (templates 4.8-4.12, 4.61).
#[derive(Debug, Clone, PartialEq)]
pub struct Statistic {
    pub process: i32,
    pub process_name: Option<String>,
    pub interval_hours: Option<f64>,
}

/// What a product definition section says beyond variable and level.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ProductDefinition {
    pub attributes: ProductAttributes,
    /// End of the overall time interval, for statistically processed templates.
    pub interval_end_ms: Option<i64>,
}

/// Octet positions (1-based, within section 4) of each template's blocks.
#[derive(Debug, Default)]
struct Layout {
    ensemble: Option<usize>,
    derived: Option<usize>,
    probability: Option<usize>,
    percentile: Option<usize>,
    interval: Option<usize>,
    aerosol: Option<usize>,
}

fn layout(template: u16) -> Layout {
    let at = Some;
    match template {
        1 => Layout { ensemble: at(35), ..Layout::default() },
        2 => Layout { derived: at(35), ..Layout::default() },
        5 => Layout { probability: at(35), ..Layout::default() },
        6 => Layout { percentile: at(35), ..Layout::default() },
        8 => Layout { interval: at(35), ..Layout::default() },
        9 => Layout { probability: at(35), interval: at(48), ..Layout::default() },
        10 => Layout { percentile: at(35), interval: at(36), ..Layout::default() },
        11 => Layout { ensemble: at(35), interval: at(38), ..Layout::default() },
        12 => Layout { derived: at(35), interval: at(37), ..Layout::default() },
        48 => Layout { aerosol: at(12), ..Layout::default() },
        // Ensemble reforecasts: the model version date (octets 38-44) precedes the interval.
        60 => Layout { ensemble: at(35), ..Layout::default() },
        61 => Layout { ensemble: at(35), interval: at(45), ..Layout::default() },
        _ => Layout::default(),
    }
}

// Code table 4.91: which limits each interval type uses.
const USES_FIRST_LIMIT: [u8; 8] = [0, 2, 3, 5, 7, 8, 10, 11];
const USES_SECOND_LIMIT: [u8; 7] = [1, 2, 4, 6, 7, 9, 10];
// Code table 4.9: which limits each probability type uses.
const USES_LOWER_LIMIT: [u8; 3] = [0, 2, 3];
const USES_UPPER_LIMIT: [u8; 3] = [1, 2, 4];
const MISSING_INTERVAL_TYPE: u8 = 255;
const INTERVAL_HEADER_OCTETS: usize = 12;
const TIME_RANGE_OCTETS: usize = 12;
/// Code table 4.4: missing unit of time range.
const MISSING_UNIT: u8 = 255;

/// Section 4's octets, addressed by their 1-based WMO octet number. `payload`
/// starts at octet 6 (after the length and section number).
struct Section<'a> {
    payload: &'a [u8],
}

const PAYLOAD_START_OCTET: usize = 6;

impl Section<'_> {
    fn bytes(&self, octet: usize, len: usize) -> Option<&[u8]> {
        let start = octet.checked_sub(PAYLOAD_START_OCTET)?;
        self.payload.get(start..start + len)
    }

    fn u8(&self, octet: usize) -> Option<u8> {
        self.bytes(octet, 1).map(|b| b[0])
    }

    fn u16(&self, octet: usize) -> Option<u16> {
        self.bytes(octet, 2).map(|b| u16::from_be_bytes([b[0], b[1]]))
    }

    fn u32(&self, octet: usize) -> Option<u32> {
        self.bytes(octet, 4).map(|b| u32::from_be_bytes([b[0], b[1], b[2], b[3]]))
    }

    /// `scaled value * 10^-scale factor` (GRIB2 sign-and-magnitude integers),
    /// or None if either is missing (all bits set).
    fn scaled(&self, factor_octet: usize, value_octet: usize) -> Option<f64> {
        let factor = self.u8(factor_octet).filter(|&f| f != u8::MAX)?;
        let value = self.u32(value_octet).filter(|&v| v != u32::MAX)?;
        let factor = sign_magnitude(u32::from(factor), 7);
        let value = sign_magnitude(value, 31);
        Some(value as f64 * 10f64.powi(-factor as i32))
    }
}

fn sign_magnitude(raw: u32, sign_bit: u32) -> i64 {
    let magnitude = i64::from(raw & ((1 << sign_bit) - 1));
    if raw >> sign_bit & 1 == 1 { -magnitude } else { magnitude }
}

/// Parse the template-specific parts of a product definition section, given
/// its payload (from octet 6) and template number.
pub fn parse(centre: u16, template: u16, payload: &[u8]) -> ProductDefinition {
    let section = Section { payload };
    let layout = layout(template);
    let name = |table, code| tables::code_name(centre, table, code).map(str::to_string);
    let interval = layout.interval.and_then(|octet| time_interval(&section, octet, &name));

    ProductDefinition {
        attributes: ProductAttributes {
            aerosol: layout.aerosol.and_then(|o| aerosol(&section, o, &name)),
            ensemble: layout.ensemble.and_then(|o| ensemble(&section, o, &name)),
            ensemble_derived: layout.derived.and_then(|o| ensemble_derived(&section, o, &name)),
            probability: layout.probability.and_then(|o| probability(&section, o, &name)),
            percentile: layout.percentile.and_then(|o| section.u8(o)).map(i32::from),
            statistic: interval.as_ref().map(|(_, statistic)| statistic.clone()),
        },
        interval_end_ms: interval.and_then(|(end, _)| end),
    }
}

type Namer<'a> = dyn Fn(CodeTable, u32) -> Option<String> + 'a;

fn aerosol(s: &Section, o: usize, name: &Namer) -> Option<Aerosol> {
    let aerosol_type = s.u16(o)?;
    let (size_interval_type, size_first_m, size_second_m) = interval_limits(s, o + 2);
    let (wavelength_interval_type, wavelength_first_m, wavelength_second_m) = interval_limits(s, o + 13);
    Some(Aerosol {
        aerosol_type: i32::from(aerosol_type),
        type_name: name(CodeTable::Aerosol, u32::from(aerosol_type)),
        size_interval_type,
        size_first_m,
        size_second_m,
        wavelength_interval_type,
        wavelength_first_m,
        wavelength_second_m,
    })
}

/// An interval type (code table 4.91) followed by two scaled limits.
fn interval_limits(s: &Section, o: usize) -> (Option<i32>, Option<f64>, Option<f64>) {
    match s.u8(o) {
        None | Some(MISSING_INTERVAL_TYPE) => (None, None, None),
        Some(kind) => (
            Some(i32::from(kind)),
            s.scaled(o + 1, o + 2).filter(|_| USES_FIRST_LIMIT.contains(&kind)),
            s.scaled(o + 6, o + 7).filter(|_| USES_SECOND_LIMIT.contains(&kind)),
        ),
    }
}

fn ensemble(s: &Section, o: usize, name: &Namer) -> Option<Ensemble> {
    let perturbation_type = s.u8(o)?;
    Some(Ensemble {
        perturbation_type: i32::from(perturbation_type),
        perturbation_type_name: name(CodeTable::Perturbation, u32::from(perturbation_type)),
        member: i32::from(s.u8(o + 1)?),
        ensemble_size: i32::from(s.u8(o + 2)?),
    })
}

fn ensemble_derived(s: &Section, o: usize, name: &Namer) -> Option<EnsembleDerived> {
    let derived_type = s.u8(o)?;
    Some(EnsembleDerived {
        derived_type: i32::from(derived_type),
        type_name: name(CodeTable::DerivedForecast, u32::from(derived_type)),
        ensemble_size: i32::from(s.u8(o + 1)?),
    })
}

/// Octets: probability number, total probabilities, type, then the two scaled limits.
fn probability(s: &Section, o: usize, name: &Namer) -> Option<Probability> {
    let probability_type = s.u8(o + 2)?;
    Some(Probability {
        probability_type: i32::from(probability_type),
        type_name: name(CodeTable::Probability, u32::from(probability_type)),
        lower_limit: s.scaled(o + 3, o + 4).filter(|_| USES_LOWER_LIMIT.contains(&probability_type)),
        upper_limit: s.scaled(o + 8, o + 9).filter(|_| USES_UPPER_LIMIT.contains(&probability_type)),
    })
}

/// The interval block: its end time, then the first time range's process and
/// the interval's length in hours.
fn time_interval(s: &Section, o: usize, name: &Namer) -> Option<(Option<i64>, Statistic)> {
    let end = NaiveDate::from_ymd_opt(i32::from(s.u16(o)?), u32::from(s.u8(o + 2)?), u32::from(s.u8(o + 3)?))
        .and_then(|d| d.and_hms_opt(u32::from(s.u8(o + 4)?), u32::from(s.u8(o + 5)?), u32::from(s.u8(o + 6)?)))
        .map(|t: NaiveDateTime| t.and_utc().timestamp_millis());
    let ranges: Vec<usize> = (0..usize::from(s.u8(o + 7)?)).map(|i| o + INTERVAL_HEADER_OCTETS + i * TIME_RANGE_OCTETS).collect();
    let process = s.u8(*ranges.first()?)?;
    Some((
        end,
        Statistic {
            process: i32::from(process),
            process_name: name(CodeTable::StatisticalProcess, u32::from(process)),
            interval_hours: interval_hours(s, &ranges),
        },
    ))
}

/// The interval length as netCDF-Java computes it: the sum over time ranges
/// (skipping those with a missing unit) of each range's length plus its time
/// increment, in the first range's unit. Ranges in a different unit make
/// netCDF-Java fail, so the length is unknown then.
fn interval_hours(s: &Section, ranges: &[usize]) -> Option<f64> {
    let mut unit = None;
    let mut total: u64 = 0;
    for &r in ranges {
        let range_unit = s.u8(r + 2)?;
        if range_unit == MISSING_UNIT {
            continue;
        }
        let increment_unit = s.u8(r + 7)?;
        let increment = s.u32(r + 8)?;
        match unit {
            None => unit = Some(range_unit),
            Some(u) if u != range_unit => return None,
            Some(u) if increment_unit != u && increment_unit != MISSING_UNIT && increment != 0 => return None,
            Some(_) => {}
        }
        total += u64::from(s.u32(r + 3)?);
        if increment_unit != MISSING_UNIT {
            total += u64::from(increment);
        }
    }
    hours_per_unit(unit?).map(|h| total as f64 * h).filter(|&h| h > 0.0)
}

/// Code table 4.4 (indicator of unit of time range), as netCDF-Java converts
/// it: a month is 30 days, a year 365.
fn hours_per_unit(unit: u8) -> Option<f64> {
    match unit {
        0 => Some(1.0 / 60.0),
        1 => Some(1.0),
        2 => Some(24.0),
        3 => Some(720.0),
        4 => Some(8760.0),
        10 => Some(3.0),
        11 => Some(6.0),
        12 => Some(12.0),
        13 => Some(1.0 / 3600.0),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const NCEP: u16 = 7;

    /// A section-4 payload (octet 6 onwards) of `len` octets, with `(octet, bytes)` set.
    fn payload(len_octets: usize, fields: &[(usize, &[u8])]) -> Vec<u8> {
        let mut p = vec![0u8; len_octets - PAYLOAD_START_OCTET + 1];
        for (octet, bytes) in fields {
            let start = octet - PAYLOAD_START_OCTET;
            p[start..start + bytes.len()].copy_from_slice(bytes);
        }
        p
    }

    #[test]
    fn decodes_sign_magnitude_scaled_values_and_missing() {
        let p = payload(47, &[(38, &[1]), (39, &3048u32.to_be_bytes()), (43, &[0x81]), (44, &[0x80, 0, 0, 5])]);
        let s = Section { payload: &p };
        assert_eq!(s.scaled(38, 39), Some(304.8));
        assert_eq!(s.scaled(43, 44), Some(-50.0));
        let missing = payload(47, &[(38, &[0xFF])]);
        assert_eq!(Section { payload: &missing }.scaled(38, 39), None);
    }

    #[test]
    fn probability_keeps_only_the_limits_its_type_uses() {
        let p = payload(47, &[(37, &[0]), (38, &[1]), (39, &3048u32.to_be_bytes()), (43, &[0]), (44, &[0, 0, 0, 9])]);
        let def = parse(NCEP, 5, &p);
        let probability = def.attributes.probability.unwrap();
        assert_eq!(probability.probability_type, 0);
        assert_eq!(probability.lower_limit, Some(304.8));
        assert_eq!(probability.upper_limit, None);
        assert!(probability.type_name.is_some());
    }

    #[test]
    fn statistical_template_reports_interval_end_process_and_hours() {
        // Template 4.8: end 2024-01-01 06:00:00, one range: accumulation (1) over 6 hours (unit 1).
        let p = payload(58, &[
            (35, &2024u16.to_be_bytes()), (37, &[1]), (38, &[1]), (39, &[6]), (40, &[0]), (41, &[0]),
            (42, &[1]), (47, &[1]), (48, &[2]), (49, &[1]), (50, &6u32.to_be_bytes()),
        ]);
        let def = parse(NCEP, 8, &p);
        assert_eq!(def.interval_end_ms, Some(1_704_088_800_000));
        let statistic = def.attributes.statistic.unwrap();
        assert_eq!((statistic.process, statistic.interval_hours), (1, Some(6.0)));
        assert_eq!(statistic.process_name.as_deref(), Some("Accumulation"));
    }

    #[test]
    fn interval_length_includes_time_increments_as_netcdf_java_computes_it() {
        // Template 4.8, one range of 1 hour with a 1-hour increment: netCDF-Java reports 2 hours.
        let p = payload(58, &[
            (35, &2024u16.to_be_bytes()), (37, &[1]), (38, &[1]), (39, &[1]),
            (42, &[1]), (47, &[1]), (48, &[2]), (49, &[1]), (50, &1u32.to_be_bytes()), (54, &[1]), (55, &1u32.to_be_bytes()),
        ]);
        assert_eq!(parse(NCEP, 8, &p).attributes.statistic.and_then(|s| s.interval_hours), Some(2.0));
    }

    #[test]
    fn ensemble_statistical_template_has_both_blocks() {
        let p = payload(61, &[
            (35, &[3]), (36, &[1]), (37, &[30]),
            (38, &2024u16.to_be_bytes()), (40, &[1]), (41, &[1]), (42, &[12]), (45, &[1]), (50, &[1]), (52, &[11]),
            (53, &1u32.to_be_bytes()),
        ]);
        let attrs = parse(NCEP, 11, &p).attributes;
        assert_eq!(attrs.ensemble.map(|e| (e.perturbation_type, e.member, e.ensemble_size)), Some((3, 1, 30)));
        assert_eq!(attrs.statistic.and_then(|s| s.interval_hours), Some(6.0));
    }

    #[test]
    fn aerosol_template_reads_type_and_size_interval() {
        // Type 62000, size interval "smaller than second limit" (1): only the second size is used.
        let p = payload(58, &[
            (12, &62000u16.to_be_bytes()), (14, &[1]), (15, &[6]), (16, &[0, 0, 0, 1]), (20, &[6]), (21, &[0, 0, 0, 25]),
            (25, &[MISSING_INTERVAL_TYPE]),
        ]);
        let aerosol = parse(NCEP, 48, &p).attributes.aerosol.unwrap();
        assert_eq!(aerosol.aerosol_type, 62000);
        assert_eq!((aerosol.size_interval_type, aerosol.size_first_m), (Some(1), None));
        assert!((aerosol.size_second_m.unwrap() - 25e-6).abs() < 1e-15);
        assert_eq!(aerosol.wavelength_interval_type, None);
    }

    #[test]
    fn plain_analysis_template_has_no_attributes() {
        assert_eq!(parse(NCEP, 0, &payload(34, &[])), ProductDefinition::default());
    }
}
