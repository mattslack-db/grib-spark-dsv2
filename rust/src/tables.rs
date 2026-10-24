//! GRIB2 parameter names, units and code-table entries, exported from
//! netCDF-Java with this repo's corrections applied (`ExportGribTables.scala`),
//! so names match the Scala reader exactly.

use std::collections::HashMap;
use std::sync::OnceLock;

const PARAMS_TSV: &str = include_str!("../data/params.tsv");
const CODES_TSV: &str = include_str!("../data/codes.tsv");

/// Originating centre whose local tables were exported; every other centre
/// gets the WMO tables (other centres' local tables are not exported).
const NCEP_CENTRE: u16 = 7;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Table {
    Wmo,
    Ncep,
}

impl Table {
    fn for_centre(centre: u16) -> Self {
        if centre == NCEP_CENTRE { Self::Ncep } else { Self::Wmo }
    }

    fn parse(name: &str) -> Self {
        match name {
            "ncep" => Self::Ncep,
            "wmo" => Self::Wmo,
            other => panic!("unknown table {other:?} in embedded TSV"),
        }
    }
}

/// GRIB2 code tables used for names.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CodeTable {
    /// 4.5: type of fixed surface.
    LevelType,
    /// 4.6: type of ensemble forecast.
    Perturbation,
    /// 4.7: derived forecast.
    DerivedForecast,
    /// 4.9: probability type.
    Probability,
    /// 4.10: type of statistical processing.
    StatisticalProcess,
    /// 4.233: aerosol type.
    Aerosol,
}

impl CodeTable {
    fn parse(id: &str) -> Self {
        match id {
            "4.5" => Self::LevelType,
            "4.6" => Self::Perturbation,
            "4.7" => Self::DerivedForecast,
            "4.9" => Self::Probability,
            "4.10" => Self::StatisticalProcess,
            "4.233" => Self::Aerosol,
            other => panic!("unknown code table {other:?} in embedded TSV"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Parameter {
    pub abbrev: Option<&'static str>,
    pub name: Option<&'static str>,
    pub unit: &'static str,
}

struct Tables {
    params: HashMap<(Table, u8, u8, u8), Parameter>,
    codes: HashMap<(Table, CodeTable, u32), &'static str>,
}

fn tables() -> &'static Tables {
    static TABLES: OnceLock<Tables> = OnceLock::new();
    TABLES.get_or_init(|| Tables { params: load_params(), codes: load_codes() })
}

fn rows(tsv: &'static str) -> impl Iterator<Item = Vec<&'static str>> {
    tsv.lines().skip(1).filter(|line| !line.is_empty()).map(|line| line.split('\t').collect())
}

fn non_empty(s: &'static str) -> Option<&'static str> {
    (!s.is_empty()).then_some(s)
}

fn load_params() -> HashMap<(Table, u8, u8, u8), Parameter> {
    rows(PARAMS_TSV)
        .map(|f| {
            let key = (Table::parse(f[0]), parse_code(f[1]), parse_code(f[2]), parse_code(f[3]));
            (key, Parameter { abbrev: non_empty(f[4]), name: non_empty(f[5]), unit: f[6] })
        })
        .collect()
}

fn load_codes() -> HashMap<(Table, CodeTable, u32), &'static str> {
    rows(CODES_TSV)
        .map(|f| ((Table::parse(f[0]), CodeTable::parse(f[1]), f[2].parse().expect("numeric code")), f[3]))
        .collect()
}

fn parse_code(field: &str) -> u8 {
    field.parse().expect("embedded table codes fit in u8")
}

/// The parameter `(discipline, category, number)` as the centre's tables define it.
pub fn parameter(centre: u16, discipline: u8, category: u8, number: u8) -> Option<&'static Parameter> {
    tables().params.get(&(Table::for_centre(centre), discipline, category, number))
}

/// The name of `code` in `table`, as the centre's tables define it.
pub fn code_name(centre: u16, table: CodeTable, code: u32) -> Option<&'static str> {
    tables().codes.get(&(Table::for_centre(centre), table, code)).copied()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ncep_centre_gets_local_parameters_with_corrected_units() {
        let ozone = parameter(NCEP_CENTRE, 0, 14, 192).unwrap();
        assert_eq!(ozone.abbrev, Some("O3MR"));
        let fine_pm = parameter(NCEP_CENTRE, 0, 13, 193).unwrap();
        assert_eq!(fine_pm.unit, "µg.m-3");
    }

    #[test]
    fn other_centres_get_wmo_parameters_only() {
        assert!(parameter(98, 0, 14, 192).is_none());
        assert_eq!(parameter(98, 0, 0, 0).and_then(|p| p.name), Some("Temperature"));
    }

    #[test]
    fn looks_up_code_table_names() {
        assert_eq!(code_name(NCEP_CENTRE, CodeTable::LevelType, 220), Some("Planetary Boundary Layer"));
        assert_eq!(code_name(NCEP_CENTRE, CodeTable::StatisticalProcess, 1), Some("Accumulation"));
        assert_eq!(code_name(98, CodeTable::LevelType, 220), None);
    }
}
