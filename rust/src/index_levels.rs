//! Maps a wgrib2 `.idx` level ("500 mb") to the `level_type`/`level_value`
//! the decoder reports for it ("Isobaric surface", 50000 Pa), so `.idx`
//! entries can be pruned on level filters before decoding. A port of
//! `GribIndexLevels.scala`; unrecognised text returns None, which never prunes.

use std::sync::OnceLock;

use regex::Regex;

#[derive(Debug, Clone, PartialEq)]
pub struct IndexLevel {
    pub level_type: &'static str,
    pub value: f64,
}

const PASCALS_PER_MILLIBAR: f64 = 100.0;
const NUM: &str = r"(-?\d+(?:\.\d+)?(?:e-?\d+)?)";

/// Levels with no number in the `.idx` text; the decoder reports level_value 0 for each.
const NAMED: [(&str, &str); 13] = [
    ("surface", "Ground or water surface"),
    ("mean sea level", "Mean sea level"),
    ("tropopause", "Tropopause"),
    ("max wind", "Maximum wind level"),
    ("entire atmosphere", "Entire atmosphere"),
    ("entire atmosphere (considered as a single layer)", "Entire atmosphere layer"),
    ("planetary boundary layer", "Planetary Boundary Layer"),
    ("cloud ceiling", "Cloud ceiling"),
    ("low cloud layer", "Low cloud layer"),
    ("middle cloud layer", "Middle cloud layer"),
    ("high cloud layer", "High cloud layer"),
    ("highest tropospheric freezing level", "Highest tropospheric freezing level"),
    ("0C isotherm", "Level of 0 °C isotherm"),
];

/// `(pattern, level type, multiplier)`; a layer's value is its first bound.
fn patterns() -> &'static [(Regex, &'static str, f64)] {
    static PATTERNS: OnceLock<Vec<(Regex, &'static str, f64)>> = OnceLock::new();
    PATTERNS.get_or_init(|| {
        let layer = format!("{NUM}-{NUM}");
        [
            (format!("{NUM} mb"), "Isobaric surface", PASCALS_PER_MILLIBAR),
            (format!("{NUM} m above ground"), "Specified height level above ground", 1.0),
            (format!("{layer} m above ground"), "Specified height level above ground", 1.0),
            (format!("{layer} m below ground"), "Depth below land surface", 1.0),
            (
                format!("{layer} mb above ground"),
                "Level at specified pressure difference from ground to level",
                PASCALS_PER_MILLIBAR,
            ),
            (format!("{NUM} m above mean sea level"), "Specific altitude above mean sea level", 1.0),
            (format!("{NUM} hybrid level"), "Hybrid level", 1.0),
            (format!("{NUM} sigma level"), "Sigma level", 1.0),
            (format!("{layer} sigma layer"), "Sigma level", 1.0),
            (format!(r"PV={NUM} \(Km\^2/kg/s\) surface"), "Potential vorticity surface", 1.0),
        ]
        .into_iter()
        .map(|(pattern, level_type, scale)| (Regex::new(&format!("^{pattern}$")).expect("valid regex"), level_type, scale))
        .collect()
    })
}

pub fn parse(level_description: &str) -> Option<IndexLevel> {
    let text = level_description.trim();
    if let Some((_, level_type)) = NAMED.iter().find(|(name, _)| *name == text) {
        return Some(IndexLevel { level_type, value: 0.0 });
    }
    patterns().iter().find_map(|(regex, level_type, scale)| {
        let first: f64 = regex.captures(text)?.get(1)?.as_str().parse().ok()?;
        Some(IndexLevel { level_type, value: first * scale })
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn level(level_type: &'static str, value: f64) -> Option<IndexLevel> {
        Some(IndexLevel { level_type, value })
    }

    #[test]
    fn maps_numbered_levels_to_decoded_type_and_value() {
        assert_eq!(parse("500 mb"), level("Isobaric surface", 50000.0));
        assert_eq!(parse("2 m above ground"), level("Specified height level above ground", 2.0));
        assert_eq!(parse("1 hybrid level"), level("Hybrid level", 1.0));
        assert_eq!(parse("0.995 sigma level"), level("Sigma level", 0.995));
        assert_eq!(parse("1829 m above mean sea level"), level("Specific altitude above mean sea level", 1829.0));
        assert_eq!(parse("PV=2e-06 (Km^2/kg/s) surface"), level("Potential vorticity surface", 2e-6));
        assert_eq!(parse("PV=-2e-06 (Km^2/kg/s) surface"), level("Potential vorticity surface", -2e-6));
    }

    #[test]
    fn layers_report_their_first_bound() {
        assert_eq!(parse("3000-0 m above ground"), level("Specified height level above ground", 3000.0));
        assert_eq!(parse("0-0.1 m below ground"), level("Depth below land surface", 0.0));
        assert_eq!(
            parse("30-0 mb above ground"),
            level("Level at specified pressure difference from ground to level", 3000.0)
        );
        assert_eq!(parse("0.33-1 sigma layer"), level("Sigma level", 0.33));
    }

    #[test]
    fn maps_named_levels_with_value_zero() {
        for (text, level_type) in NAMED {
            assert_eq!(parse(text), level(level_type, 0.0), "{text}");
        }
    }

    #[test]
    fn converts_fractional_millibars_to_pascals() {
        assert!((parse("0.4 mb").unwrap().value - 40.0).abs() < 1e-9);
    }

    #[test]
    fn leaves_unrecognised_text_unknown() {
        for text in ["315 K isentropic level", "-10C isotherm", "1-2 hybrid layer", "top of atmosphere", ""] {
            assert_eq!(parse(text), None, "{text}");
        }
    }
}
