//! The idx-driven read path: fetch the `.idx`, select messages, range-read only
//! their bytes from object storage, and decode them in parallel.

use std::collections::HashMap;
use std::path::Path as FsPath;

use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use rayon::prelude::*;
use url::Url;

use crate::decode::{DecodedField, decode_message};
use crate::error::{Error, Result};
use crate::idx::{self, IndexEntry, IndexedMessage};

/// Which fields to read, matched against `.idx` lines. `None` matches all.
#[derive(Debug, Clone, Default)]
pub struct Selection {
    /// Variable short names, e.g. `["TMP", "UGRD"]` (exact match).
    pub variables: Option<Vec<String>>,
    /// Level descriptions, e.g. `["2 m above ground", "500 mb"]` (exact match).
    pub levels: Option<Vec<String>>,
}

impl Selection {
    fn matches(&self, entry: &IndexEntry) -> bool {
        let allows = |wanted: &Option<Vec<String>>, actual: &str| {
            wanted.as_ref().is_none_or(|w| w.iter().any(|v| v == actual))
        };
        allows(&self.variables, &entry.variable_name) && allows(&self.levels, &entry.level_description)
    }
}

/// What a read did, for benchmarking the access pattern.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReadStats {
    pub file_size: u64,
    pub messages_in_index: usize,
    pub messages_read: usize,
    pub bytes_read: u64,
}

/// Resolve a URL (`s3://`, `az://`, `abfss://`, `gs://`, `https://`, `file://`)
/// or a bare local path (e.g. a `/Volumes/...` FUSE path) to a store and key.
pub fn resolve(location: &str, options: &HashMap<String, String>) -> Result<(Box<dyn ObjectStore>, Path)> {
    let invalid = |reason: String| Error::InvalidLocation { location: location.to_string(), reason };
    let url = match Url::parse(location) {
        Ok(url) => url,
        Err(url::ParseError::RelativeUrlWithoutBase) => {
            let absolute = FsPath::new(location).canonicalize().map_err(|e| invalid(e.to_string()))?;
            Url::from_file_path(&absolute).map_err(|()| invalid("not an absolute path".into()))?
        }
        Err(e) => return Err(invalid(e.to_string())),
    };
    Ok(object_store::parse_url_opts(&url, options)?)
}

/// Read the fields matching `selection` from the GRIB2 object at `location`,
/// which must have a `.idx` sidecar alongside it.
pub async fn read(
    location: &str,
    options: &HashMap<String, String>,
    selection: &Selection,
) -> Result<(Vec<DecodedField>, ReadStats)> {
    let (store, path) = resolve(location, options)?;
    let idx_path = Path::from(format!("{path}.idx"));

    let (idx_bytes, meta) = futures::try_join!(
        async { store.get(&idx_path).await?.bytes().await },
        store.head(&path),
    )?;
    let idx_text = std::str::from_utf8(&idx_bytes).map_err(|_| Error::IndexEncoding(idx_path.to_string()))?;
    let entries = idx::parse_strict(idx_text).ok_or_else(|| Error::MalformedIndex(idx_path.to_string()))?;
    let all_messages = idx::group_messages(&entries, meta.size);

    let selected: Vec<IndexedMessage> = all_messages
        .iter()
        .filter(|m| m.entries.iter().any(|e| selection.matches(e)))
        .cloned()
        .collect();
    let ranges: Vec<_> = selected.iter().map(IndexedMessage::range).collect();
    // get_ranges coalesces nearby ranges and issues the requests concurrently.
    let buffers = store.get_ranges(&path, &ranges).await?;

    let decoded: Vec<Vec<DecodedField>> = selected
        .par_iter()
        .zip(buffers.par_iter())
        .map(|(message, bytes)| decode_message(bytes, message))
        .collect::<Result<_>>()?;

    let fields = decoded
        .into_iter()
        .zip(&selected)
        .flat_map(|(fields, message)| {
            // A sub-message can share its GRIB message with unselected fields.
            fields.into_iter().zip(&message.entries).filter(|(_, e)| selection.matches(e)).map(|(f, _)| f)
        })
        .collect();

    let stats = ReadStats {
        file_size: meta.size,
        messages_in_index: all_messages.len(),
        messages_read: selected.len(),
        bytes_read: ranges.iter().map(|r| r.end - r.start).sum(),
    };
    Ok((fields, stats))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(variable: &str, level: &str) -> IndexEntry {
        IndexEntry {
            message_number: 1,
            sub_message: None,
            byte_offset: 0,
            reference_time: "2024010100".into(),
            variable_name: variable.into(),
            level_description: level.into(),
            forecast_info: "anl".into(),
        }
    }

    #[test]
    fn empty_selection_matches_everything() {
        assert!(Selection::default().matches(&entry("TMP", "500 mb")));
    }

    #[test]
    fn selection_requires_both_variable_and_level_to_match() {
        let selection = Selection {
            variables: Some(vec!["TMP".into(), "RH".into()]),
            levels: Some(vec!["2 m above ground".into()]),
        };
        assert!(selection.matches(&entry("RH", "2 m above ground")));
        assert!(!selection.matches(&entry("TMP", "500 mb")));
        assert!(!selection.matches(&entry("UGRD", "2 m above ground")));
    }

    #[test]
    fn bare_relative_path_that_does_not_exist_is_an_invalid_location() {
        let err = resolve("no/such/file.grib2", &HashMap::new()).err().unwrap();
        assert!(matches!(err, Error::InvalidLocation { .. }));
    }
}
