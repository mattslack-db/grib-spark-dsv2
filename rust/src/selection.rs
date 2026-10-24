//! Which fields a read keeps: the reader options (`variables`, `levels`,
//! `levelType`) and a pushed-down predicate, applied first to `.idx` entries
//! (best-effort pruning) and then exactly to decoded fields.

use crate::decode::DecodedField;
use crate::error::{Error, Result};
use crate::filters::{FieldView, Predicate, idx_name_may_be, matches_any_variable};
use crate::idx::IndexEntry;
use crate::index_levels;

#[derive(Debug, Clone, Default)]
pub struct Selection {
    /// Variable short names, case-insensitive. A field matches by its decoded
    /// name or, when it has one, its `.idx` name (they differ for a few NCEP
    /// parameters, e.g. `CLMR` in the `.idx` decodes as `CLWMR`).
    pub variables: Option<Vec<String>>,
    /// `.idx` level descriptions, e.g. `"2 m above ground"` (exact). Needs an `.idx`.
    pub levels: Option<Vec<String>>,
    /// Case-insensitive substring of the decoded `level_type` (the Scala reader's `levelType` option).
    pub level_type: Option<String>,
    /// A pushed-down SQL predicate, evaluated exactly.
    pub predicate: Option<Predicate>,
}

impl Selection {
    /// Best-effort pruning of an `.idx` entry: false only if none of its
    /// message's fields can be kept.
    pub fn keeps_entry(&self, entry: &IndexEntry) -> bool {
        let variable_ok = self.variables.as_ref().is_none_or(|vs| vs.iter().any(|v| idx_name_may_be(entry, v)));
        let level_ok = self.levels.as_ref().is_none_or(|ls| ls.contains(&entry.level_description));
        let predicate_ok = self.predicate.as_ref().is_none_or(|p| {
            let level = index_levels::parse(&entry.level_description);
            p.index_verdict(entry, level.as_ref()) != Some(false)
        });
        variable_ok && level_ok && predicate_ok
    }

    /// Exact evaluation of a decoded field, with its `.idx` entry if it has one.
    pub fn keeps_field(&self, field: &DecodedField, entry: Option<&IndexEntry>) -> Result<bool> {
        let variable_ok = self.variables.as_ref().is_none_or(|vs| {
            matches_any_variable(&field.variable_short_name, vs) || entry.is_some_and(|e| vs.iter().any(|v| e.variable_name.eq_ignore_ascii_case(v)))
        });
        let level_ok = match (&self.levels, entry) {
            (None, _) => true,
            (Some(levels), Some(e)) => levels.contains(&e.level_description),
            (Some(_), None) => return Err(Error::LevelsNeedIndex),
        };
        let level_type_ok = self
            .level_type
            .as_ref()
            .is_none_or(|lt| field.level_type.to_lowercase().contains(&lt.to_lowercase()));
        let view = FieldView {
            variable_short_name: &field.variable_short_name,
            level_type: &field.level_type,
            discipline: field.discipline,
            level_value: field.level_value,
        };
        let predicate_ok = self.predicate.as_ref().is_none_or(|p| p.passes(&view));
        Ok(variable_ok && level_ok && level_type_ok && predicate_ok)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::filters::Column;

    fn entry(variable: &str, level: &str) -> IndexEntry {
        IndexEntry {
            message_number: 1,
            sub_message: None,
            byte_offset: 0,
            reference_time: String::new(),
            variable_name: variable.into(),
            level_description: level.into(),
            forecast_info: String::new(),
        }
    }

    #[test]
    fn empty_selection_keeps_everything() {
        assert!(Selection::default().keeps_entry(&entry("TMP", "500 mb")));
    }

    #[test]
    fn variables_match_case_insensitively_including_idx_aliases() {
        let selection = Selection { variables: Some(vec!["clwmr".into()]), ..Selection::default() };
        assert!(selection.keeps_entry(&entry("CLMR", "1 hybrid level")));
        assert!(!selection.keeps_entry(&entry("TMP", "1 hybrid level")));
    }

    #[test]
    fn levels_and_predicate_both_prune() {
        let selection = Selection {
            levels: Some(vec!["2 m above ground".into()]),
            predicate: Some(Predicate::StrEq(Column::VariableShortName, "TMP".into())),
            ..Selection::default()
        };
        assert!(selection.keeps_entry(&entry("TMP", "2 m above ground")));
        assert!(!selection.keeps_entry(&entry("TMP", "500 mb")));
        assert!(!selection.keeps_entry(&entry("RH", "2 m above ground")));
    }
}
