//! Filter semantics, ported from `GribFilters.scala`: the predicate shapes the
//! reader can evaluate, their exact evaluation after decoding, and best-effort
//! superset pruning of `.idx` entries before decoding.
//!
//! The invariant (as in Scala): `index_verdict` may only return `Some(false)`
//! when `passes` would definitely be false, so pruning never drops a row.

use std::cmp::Ordering;

use crate::idx::IndexEntry;
use crate::index_levels::IndexLevel;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Column {
    VariableShortName,
    LevelType,
    Discipline,
    LevelValue,
}

impl Column {
    pub fn parse(name: &str) -> Option<Self> {
        match name {
            "variable_short_name" => Some(Self::VariableShortName),
            "level_type" => Some(Self::LevelType),
            "discipline" => Some(Self::Discipline),
            "level_value" => Some(Self::LevelValue),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Comparison {
    Gt,
    Ge,
    Lt,
    Le,
}

impl Comparison {
    fn holds(self, ordering: Ordering) -> bool {
        match self {
            Self::Gt => ordering == Ordering::Greater,
            Self::Ge => ordering != Ordering::Less,
            Self::Lt => ordering == Ordering::Less,
            Self::Le => ordering != Ordering::Greater,
        }
    }
}

/// A pushed-down predicate. Only shapes `passes` evaluates exactly can be
/// built (see the constructors), mirroring `GribFilters.isSupported`.
#[derive(Debug, Clone, PartialEq)]
pub enum Predicate {
    /// `variable_short_name` or `level_type` equals a string.
    StrEq(Column, String),
    /// `variable_short_name` is one of the strings.
    StrIn(Column, Vec<String>),
    /// `discipline` equals an integer.
    DisciplineEq(i64),
    /// `level_value` equals one of the numbers.
    LevelIn(Vec<f64>),
    LevelCmp(Comparison, f64),
    And(Box<Predicate>, Box<Predicate>),
    Or(Box<Predicate>, Box<Predicate>),
    Not(Box<Predicate>),
}

/// The decoded attributes a predicate is evaluated against.
pub struct FieldView<'a> {
    pub variable_short_name: &'a str,
    pub level_type: &'a str,
    pub discipline: i32,
    pub level_value: f64,
}

/// Compare doubles as Spark SQL does: NaN equals NaN and sorts above every
/// other value, and -0.0 equals 0.0.
fn sql_compare(a: f64, b: f64) -> Ordering {
    match (a.is_nan(), b.is_nan()) {
        (true, true) => Ordering::Equal,
        (true, false) => Ordering::Greater,
        (false, true) => Ordering::Less,
        (false, false) => (a + 0.0).partial_cmp(&(b + 0.0)).expect("not NaN"),
    }
}

impl Predicate {
    /// Exact evaluation after decoding.
    pub fn passes(&self, f: &FieldView) -> bool {
        match self {
            Self::StrEq(Column::VariableShortName, v) => f.variable_short_name == v,
            Self::StrEq(Column::LevelType, v) => f.level_type == v,
            Self::StrIn(Column::VariableShortName, vs) => vs.iter().any(|v| v == f.variable_short_name),
            Self::StrIn(Column::LevelType, vs) => vs.iter().any(|v| v == f.level_type),
            Self::StrEq(..) | Self::StrIn(..) => unreachable!("constructed only for string columns"),
            Self::DisciplineEq(v) => i64::from(f.discipline) == *v,
            Self::LevelIn(vs) => vs.iter().any(|&v| sql_compare(f.level_value, v) == Ordering::Equal),
            Self::LevelCmp(op, v) => op.holds(sql_compare(f.level_value, *v)),
            Self::And(l, r) => l.passes(f) && r.passes(f),
            Self::Or(l, r) => l.passes(f) || r.passes(f),
            Self::Not(p) => !p.passes(f),
        }
    }

    /// Three-valued evaluation against an `.idx` line: `Some(false)` only when
    /// the message definitely fails. `level` is the entry's decoded level when
    /// [`crate::index_levels`] recognises its text.
    pub fn index_verdict(&self, entry: &IndexEntry, level: Option<&IndexLevel>) -> Option<bool> {
        match self {
            Self::StrEq(Column::VariableShortName, v) => false_unless(idx_name_may_be(entry, v)),
            Self::StrIn(Column::VariableShortName, vs) => false_unless(vs.iter().any(|v| idx_name_may_be(entry, v))),
            Self::StrEq(Column::LevelType, v) => level.map(|l| l.level_type == v),
            Self::StrIn(Column::LevelType, vs) => level.map(|l| vs.iter().any(|v| l.level_type == v)),
            Self::StrEq(..) | Self::StrIn(..) => None,
            Self::DisciplineEq(_) => None,
            Self::LevelIn(vs) => level.and_then(|l| any_of(vs.iter().map(|&v| tolerant_compare(l.value, v, |o| o == Ordering::Equal)))),
            Self::LevelCmp(op, v) => level.and_then(|l| tolerant_compare(l.value, *v, |o| op.holds(o))),
            Self::And(l, r) => match (l.index_verdict(entry, level), r.index_verdict(entry, level)) {
                (Some(false), _) | (_, Some(false)) => Some(false),
                (Some(true), Some(true)) => Some(true),
                _ => None,
            },
            Self::Or(l, r) => any_of([l.index_verdict(entry, level), r.index_verdict(entry, level)]),
            Self::Not(p) => p.index_verdict(entry, level).map(|v| !v),
        }
    }
}

/// wgrib2 `.idx` names that differ from netCDF-Java's decoded short names
/// beyond case, as observed on NOAA fixtures (`.idx` name, decoded name).
/// Other pairs are unknown, so an index read can still prune them.
const IDX_NAME_ALIASES: [(&str, &str); 2] = [("CLMR", "CLWMR"), ("LFTX", "LFT X")];

/// Whether the field an `.idx` entry names could decode with short name `v`.
/// Only plain wgrib2 abbreviations are compared: a parameter without one is
/// written as e.g. `var discipline=0 center=7 ... parm=195` in the `.idx` but
/// decodes as `var0_16_195`, so those (and decoded fallback names) never prune.
pub fn idx_name_may_be(entry: &IndexEntry, v: &str) -> bool {
    let idx_name = entry.variable_name.as_str();
    let is_plain = |name: &str| !name.is_empty() && name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_');
    !is_plain(idx_name) || is_decoded_fallback_name(v) || is_same_field(idx_name, v)
}

/// Whether a decoded short name is one of the requested `variables`: ignoring
/// case, and by its `.idx` alias (e.g. `CLMR` for `CLWMR`).
pub fn matches_any_variable(name: &str, variables: &[String]) -> bool {
    variables.iter().any(|v| is_same_field(v, name))
}

/// A short name in upper case, with an `.idx` alias replaced by its decoded name.
fn canonical_name(name: &str) -> String {
    let upper = name.to_ascii_uppercase();
    IDX_NAME_ALIASES.iter().find(|(idx, _)| *idx == upper).map_or(upper, |(_, decoded)| (*decoded).to_string())
}

/// Whether two short names denote the same field: equal ignoring case, or an
/// `.idx` alias and its decoded name, either way round.
fn is_same_field(a: &str, b: &str) -> bool {
    canonical_name(a) == canonical_name(b)
}

/// The decoder's name for a parameter its tables don't know, `var{d}_{c}_{n}`,
/// matched ignoring case like the `variables` option.
fn is_decoded_fallback_name(v: &str) -> bool {
    let has_prefix = v.get(..3).is_some_and(|p| p.eq_ignore_ascii_case("var"));
    has_prefix && v[3..].split('_').all(|p| !p.is_empty() && (p.bytes().all(|b| b.is_ascii_digit()) || p.eq_ignore_ascii_case("missing")))
}

/// Levels derived from `.idx` text (e.g. "0.4 mb" * 100) carry floating-point
/// error, so values this close to the filter's are treated as unknown.
const LEVEL_TOLERANCE: f64 = 1e-6;

fn tolerant_compare(level: f64, target: f64, holds: impl Fn(Ordering) -> bool) -> Option<bool> {
    if level.is_nan() || target.is_nan() {
        return None;
    }
    let diff = level - target;
    if diff.abs() <= LEVEL_TOLERANCE * target.abs().max(1.0) { None } else { Some(holds(diff.partial_cmp(&0.0).expect("not NaN"))) }
}

fn false_unless(maybe_true: bool) -> Option<bool> {
    if maybe_true { None } else { Some(false) }
}

fn any_of(verdicts: impl IntoIterator<Item = Option<bool>>) -> Option<bool> {
    let verdicts: Vec<_> = verdicts.into_iter().collect();
    if verdicts.contains(&Some(true)) {
        Some(true)
    } else if verdicts.iter().all(|v| *v == Some(false)) {
        Some(false)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(variable: &str) -> IndexEntry {
        IndexEntry {
            message_number: 1,
            sub_message: None,
            byte_offset: 0,
            reference_time: String::new(),
            variable_name: variable.into(),
            level_description: String::new(),
            forecast_info: String::new(),
        }
    }

    fn field(name: &str, level_value: f64) -> FieldView<'_> {
        FieldView { variable_short_name: name, level_type: "Isobaric surface", discipline: 0, level_value }
    }

    #[test]
    fn exact_evaluation_is_case_sensitive_and_uses_sql_double_semantics() {
        assert!(Predicate::StrEq(Column::VariableShortName, "TMP".into()).passes(&field("TMP", 0.0)));
        assert!(!Predicate::StrEq(Column::VariableShortName, "tmp".into()).passes(&field("TMP", 0.0)));
        assert!(Predicate::LevelIn(vec![0.0]).passes(&field("TMP", -0.0)));
        assert!(Predicate::LevelCmp(Comparison::Gt, 1e9).passes(&field("TMP", f64::NAN)));
        assert!(Predicate::LevelIn(vec![f64::NAN]).passes(&field("TMP", f64::NAN)));
    }

    #[test]
    fn index_pruning_on_names_is_case_insensitive_and_knows_wgrib2_aliases() {
        let tmp = Predicate::StrEq(Column::VariableShortName, "tmp".into());
        assert_eq!(tmp.index_verdict(&entry("TMP"), None), None);
        assert_eq!(tmp.index_verdict(&entry("RH"), None), Some(false));
        let clwmr = Predicate::StrEq(Column::VariableShortName, "CLWMR".into());
        assert_eq!(clwmr.index_verdict(&entry("CLMR"), None), None);
        let unnamed = Predicate::StrEq(Column::VariableShortName, "var0_16_195".into());
        assert_eq!(unnamed.index_verdict(&entry("var discipline=0 center=7 local_table=1 parmcat=16 parm=195"), None), None);
        assert_eq!(unnamed.index_verdict(&entry("TMP"), None), None);
    }

    #[test]
    fn index_pruning_never_rules_out_fallback_names_whatever_their_case() {
        // The variables option ignores case, so pruning must too.
        assert!(idx_name_may_be(&entry("REFD"), "VAR0_16_195"));
        assert!(idx_name_may_be(&entry("REFD"), "Var0_16_195"));
        assert!(!idx_name_may_be(&entry("REFD"), "VARIANCE"));
    }

    #[test]
    fn aliases_match_whichever_side_carries_the_idx_spelling() {
        assert!(idx_name_may_be(&entry("CLWMR"), "clmr"));
        assert!(idx_name_may_be(&entry("CLMR"), "CLWMR"));
        assert!(!idx_name_may_be(&entry("CLWMR"), "LFTX"));
    }

    #[test]
    fn a_decoded_field_can_be_requested_by_its_idx_alias_without_an_index() {
        assert!(matches_any_variable("CLWMR", &["clmr".into()]));
        assert!(matches_any_variable("LFT X", &["LFTX".into()]));
        assert!(matches_any_variable("CLMR", &["CLWMR".into()]));
        assert!(!matches_any_variable("CLWMR", &["TMP".into()]));
    }

    #[test]
    fn index_pruning_on_levels_needs_a_recognised_level_and_respects_tolerance() {
        let level = IndexLevel { level_type: "Isobaric surface", value: 50000.0 };
        let gt = Predicate::LevelCmp(Comparison::Gt, 60000.0);
        assert_eq!(gt.index_verdict(&entry("TMP"), Some(&level)), Some(false));
        assert_eq!(gt.index_verdict(&entry("TMP"), None), None);
        let near = Predicate::LevelIn(vec![50000.0 + 1e-3]);
        assert_eq!(near.index_verdict(&entry("TMP"), Some(&level)), None);
    }

    #[test]
    fn negation_and_conjunction_combine_three_valued_verdicts() {
        let level = IndexLevel { level_type: "Isobaric surface", value: 50000.0 };
        let is_rh = Predicate::StrEq(Column::VariableShortName, "RH".into());
        let at_500 = Predicate::StrEq(Column::LevelType, "Isobaric surface".into());
        let both = Predicate::And(Box::new(is_rh.clone()), Box::new(at_500.clone()));
        assert_eq!(both.index_verdict(&entry("TMP"), Some(&level)), Some(false));
        let not_isobaric = Predicate::Not(Box::new(at_500));
        assert_eq!(not_isobaric.index_verdict(&entry("TMP"), Some(&level)), Some(false));
        assert_eq!(Predicate::Not(Box::new(is_rh)).index_verdict(&entry("RH"), None), None);
    }

    #[test]
    fn evaluates_every_supported_shape_exactly() {
        let f = field("TMP", 50000.0);
        assert!(Predicate::StrIn(Column::VariableShortName, vec!["RH".into(), "TMP".into()]).passes(&f));
        assert!(Predicate::StrIn(Column::LevelType, vec!["Isobaric surface".into()]).passes(&f));
        assert!(Predicate::StrEq(Column::LevelType, "Isobaric surface".into()).passes(&f));
        assert!(Predicate::DisciplineEq(0).passes(&f) && !Predicate::DisciplineEq(10).passes(&f));
        for (op, v, expected) in [
            (Comparison::Ge, 50000.0, true),
            (Comparison::Gt, 50000.0, false),
            (Comparison::Le, 50000.0, true),
            (Comparison::Lt, 50000.0, false),
        ] {
            assert_eq!(Predicate::LevelCmp(op, v).passes(&f), expected, "{op:?}");
        }
        let either =
            Predicate::Or(Box::new(Predicate::StrEq(Column::VariableShortName, "RH".into())), Box::new(Predicate::LevelIn(vec![50000.0])));
        assert!(either.passes(&f));
        assert!(!Predicate::Not(Box::new(either)).passes(&f));
    }

    #[test]
    fn index_verdicts_for_in_lists_disjunctions_and_unknown_columns() {
        let level = IndexLevel { level_type: "Isobaric surface", value: 50000.0 };
        let names = Predicate::StrIn(Column::VariableShortName, vec!["RH".into(), "UGRD".into()]);
        assert_eq!(names.index_verdict(&entry("TMP"), Some(&level)), Some(false));
        assert_eq!(names.index_verdict(&entry("ugrd"), Some(&level)), None);
        let types = Predicate::StrIn(Column::LevelType, vec!["Mean sea level".into()]);
        assert_eq!(types.index_verdict(&entry("TMP"), Some(&level)), Some(false));
        assert_eq!(Predicate::DisciplineEq(0).index_verdict(&entry("TMP"), Some(&level)), None);
        let levels = Predicate::LevelIn(vec![70000.0, 85000.0]);
        assert_eq!(levels.index_verdict(&entry("TMP"), Some(&level)), Some(false));
        let either = Predicate::Or(Box::new(levels), Box::new(Predicate::LevelCmp(Comparison::Le, 50000.0)));
        assert_eq!(either.index_verdict(&entry("TMP"), Some(&level)), None);
        let both_true = Predicate::And(
            Box::new(Predicate::StrEq(Column::LevelType, "Isobaric surface".into())),
            Box::new(Predicate::LevelCmp(Comparison::Lt, 60000.0)),
        );
        assert_eq!(both_true.index_verdict(&entry("TMP"), Some(&level)), Some(true));
        assert_eq!(Column::parse("units"), None);
    }
}
