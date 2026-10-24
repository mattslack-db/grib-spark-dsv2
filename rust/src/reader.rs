//! The read path: list GRIB files, plan each (fetch the `.idx` and keep the
//! messages that can match), then range-read and decode only those, or read a
//! file whole when it has no usable `.idx`.

use std::collections::{HashMap, HashSet};
use std::path::Path as FsPath;

use futures::TryStreamExt;
use globset::GlobBuilder;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use url::Url;

use crate::decode::DecodedField;
use crate::error::{Error, Result};
use crate::idx::{self, IndexEntry, IndexedMessage};
use crate::scan::Scan;
use crate::selection::Selection;

/// UC volume paths come back from Databricks listings as `dbfs:/Volumes/...`;
/// they are read through the `/Volumes` FUSE mount.
const DBFS_VOLUMES_PREFIX: &str = "dbfs:/Volumes/";
const VOLUMES_PREFIX: &str = "/Volumes/";
const GLOB_CHARS: [char; 4] = ['*', '?', '[', '{'];

/// Extensions of files that sit alongside GRIB data but aren't GRIB (as in `GribScan.scala`).
const NON_GRIB_EXTENSIONS: [&str; 42] = [
    "idx", "md5", "sha1", "sha256", "sha512", "crc", // sidecars and checksums
    "gbx9", "ncx", "ncx2", "ncx3", "ncx4", "ncml", // netCDF-Java indexes and catalogs
    "part", "tmp", // partial downloads
    "txt", "md", "json", "xml", "html", "log", "pdf", // documents
    "csv", "tsv", "yaml", "yml", // tables and config
    "nc", "nc4", "h5", "hdf", "parquet", // other data formats
    "gz", "zip", "tar", "bz2", "xz", "zst", // archives
    "png", "jpg", "jpeg", "gif", // images
    "py", "sh", // scripts
];

/// Whether a file found by listing should be read as GRIB: not hidden, not a
/// Spark/Hadoop marker, and not a known non-GRIB extension.
pub fn is_grib_file_candidate(file_name: &str) -> bool {
    let extension = file_name.rsplit_once('.').map(|(_, e)| e.to_lowercase()).unwrap_or_default();
    !file_name.starts_with('.') && !file_name.starts_with('_') && !NON_GRIB_EXTENSIONS.contains(&extension.as_str())
}

/// A store, the key within it, and how to turn keys back into locations.
pub struct Resolved {
    pub store: Box<dyn ObjectStore>,
    pub path: Path,
    location_prefix: String,
}

impl Resolved {
    fn location_of(&self, key: &Path) -> String {
        format!("{}{key}", self.location_prefix)
    }
}

/// Resolve a URL (`s3://`, `az://`, `abfss://`, `gs://`, `https://`,
/// `file://`), a `dbfs:/Volumes/` path, or a local path (e.g. `/Volumes/...`).
pub fn resolve(location: &str, options: &HashMap<String, String>) -> Result<Resolved> {
    let invalid = |reason: String| Error::InvalidLocation { location: location.to_string(), reason };
    let location =
        location.strip_prefix(DBFS_VOLUMES_PREFIX).map_or_else(|| location.to_string(), |rest| format!("{VOLUMES_PREFIX}{rest}"));
    let url = match Url::parse(&location) {
        Ok(url) => url,
        Err(url::ParseError::RelativeUrlWithoutBase) => {
            let absolute = std::path::absolute(FsPath::new(&location)).map_err(|e| invalid(e.to_string()))?;
            Url::from_file_path(normalize(&absolute)).map_err(|()| invalid("not an absolute path".into()))?
        }
        Err(e) => return Err(invalid(e.to_string())),
    };
    let (store, path) = object_store::parse_url_opts(&url, options)?;
    let location_prefix = if url.scheme() == "file" { "/".to_string() } else { format!("{}://{}/", url.scheme(), url.authority()) };
    Ok(Resolved { store, path, location_prefix })
}

/// Collapse `.` and `..` lexically (object_store rejects them), without
/// resolving symlinks or requiring the path to exist.
fn normalize(path: &FsPath) -> std::path::PathBuf {
    path.components().fold(std::path::PathBuf::new(), |mut out, component| {
        match component {
            std::path::Component::CurDir => {}
            std::path::Component::ParentDir => {
                out.pop();
            }
            other => out.push(other),
        }
        out
    })
}

/// A GRIB file found by listing. `has_idx` is None when the listing can't tell.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GribFile {
    pub location: String,
    pub size: u64,
    pub has_idx: Option<bool>,
}

/// Expand a location into GRIB files: a file is returned as is; a directory
/// lists its files (not recursively); a glob (`*`, `?`, `[..]`, `{a,b}`)
/// matches path segments as Hadoop's `globStatus` does. Sidecars and other
/// non-GRIB files are skipped when listing.
pub async fn list_files(location: &str, options: &HashMap<String, String>) -> Result<Vec<GribFile>> {
    if location.contains(GLOB_CHARS) {
        return list_glob(location, options).await;
    }
    let resolved = resolve(location, options)?;
    match resolved.store.head(&resolved.path).await {
        Ok(meta) => return Ok(vec![GribFile { location: location.to_string(), size: meta.size, has_idx: None }]),
        Err(object_store::Error::NotFound { .. }) => {}
        // The local store reports a directory as an error other than NotFound.
        Err(e) if !resolved.location_prefix.starts_with('/') => return Err(e.into()),
        Err(_) => {}
    }
    let listing = resolved.store.list_with_delimiter(Some(&resolved.path)).await?;
    if listing.objects.is_empty() && listing.common_prefixes.is_empty() {
        return Err(Error::InvalidLocation { location: location.to_string(), reason: "no such file or directory".into() });
    }
    let keys: HashSet<String> = listing.objects.iter().map(|o| o.location.to_string()).collect();
    Ok(listing
        .objects
        .iter()
        .filter(|o| o.location.filename().is_some_and(is_grib_file_candidate))
        .map(|o| GribFile {
            location: resolved.location_of(&o.location),
            size: o.size,
            has_idx: Some(keys.contains(&format!("{}.idx", o.location))),
        })
        .collect())
}

async fn list_glob(location: &str, options: &HashMap<String, String>) -> Result<Vec<GribFile>> {
    let glob_start = location.find(GLOB_CHARS).expect("caller checked for a glob");
    let base_end = location[..glob_start].rfind('/').map_or(0, |i| i + 1);
    let (base, pattern) = location.split_at(base_end);
    let resolved = resolve(if base.is_empty() { "." } else { base }, options)?;
    let matcher = GlobBuilder::new(pattern)
        .literal_separator(true)
        .build()
        .map_err(|e| Error::InvalidLocation { location: location.to_string(), reason: e.to_string() })?
        .compile_matcher();

    let prefix = resolved.path.clone();
    let objects: Vec<_> = resolved.store.list(Some(&prefix)).try_collect().await?;
    let relative = |key: &Path| key.prefix_match(&prefix).map(|parts| parts.map(|p| p.as_ref().to_string()).collect::<Vec<_>>().join("/"));
    let keys: HashSet<String> = objects.iter().map(|o| o.location.to_string()).collect();
    let mut files: Vec<GribFile> = objects
        .iter()
        .filter(|o| relative(&o.location).is_some_and(|r| matcher.is_match(&r)))
        .filter(|o| o.location.filename().is_some_and(is_grib_file_candidate))
        .map(|o| GribFile {
            location: resolved.location_of(&o.location),
            size: o.size,
            // A glob may not match the sidecars, so only a listed one is certain.
            has_idx: keys.contains(&format!("{}.idx", o.location)).then_some(true),
        })
        .collect();
    files.sort_by(|a, b| a.location.cmp(&b.location));
    Ok(files)
}

/// A file to read: the `.idx`-located messages that can match, or the whole
/// file (`messages` None) when it has no usable `.idx`.
#[derive(Debug, Clone, PartialEq)]
pub struct FilePlan {
    pub location: String,
    pub file_size: u64,
    pub messages: Option<Vec<IndexedMessage>>,
    pub messages_in_index: Option<usize>,
    pub warnings: Vec<String>,
}

impl FilePlan {
    /// Bytes this plan reads.
    pub fn bytes(&self) -> u64 {
        self.messages.as_ref().map_or(self.file_size, |ms| ms.iter().map(|m| m.length).sum())
    }
}

/// Plan one file: with `use_index`, fetch its `.idx` and keep the messages
/// `selection` can match; otherwise (or without a usable `.idx`) read it whole.
pub async fn plan_file(file: &GribFile, options: &HashMap<String, String>, selection: &Selection, use_index: bool) -> Result<FilePlan> {
    let whole =
        |warnings| FilePlan { location: file.location.clone(), file_size: file.size, messages: None, messages_in_index: None, warnings };
    if !use_index || file.has_idx == Some(false) {
        return Ok(whole(vec![]));
    }
    let resolved = resolve(&file.location, options)?;
    // Path's Display is already percent-encoded, so parse rather than re-encode it.
    let idx_path = Path::parse(format!("{}.idx", resolved.path))
        .map_err(|e| Error::InvalidLocation { location: file.location.clone(), reason: e.to_string() })?;
    let idx_bytes = match resolved.store.get(&idx_path).await {
        Ok(result) => result.bytes().await?,
        Err(object_store::Error::NotFound { .. }) => return Ok(whole(vec![])),
        Err(e) => return Err(e.into()),
    };
    let Some(entries) = std::str::from_utf8(&idx_bytes).ok().and_then(idx::parse_strict) else {
        return Ok(whole(vec![format!("ignoring malformed index {}.idx; reading the file whole", file.location)]));
    };
    let Some(all) = idx::group_messages(&entries, file.size) else {
        return Ok(whole(vec![format!(
            "ignoring index {}.idx: its offsets don't ascend within the file; reading the file whole",
            file.location
        )]));
    };
    let messages_in_index = all.len();
    let kept = all.into_iter().filter(|m| m.entries.iter().any(|e| selection.keeps_entry(e))).collect();
    Ok(FilePlan {
        location: file.location.clone(),
        file_size: file.size,
        messages: Some(kept),
        messages_in_index: Some(messages_in_index),
        warnings: vec![],
    })
}

/// What a read did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReadStats {
    pub messages_read: usize,
    pub bytes_read: u64,
    /// Messages that couldn't be decoded and were skipped.
    pub decode_failures: Vec<String>,
}

/// Read a planned file: the planned messages' byte ranges, or the whole file,
/// keeping the fields `selection` matches.
pub async fn read_plan(
    plan: &FilePlan,
    options: &HashMap<String, String>,
    selection: &Selection,
    with_lat_lons: bool,
) -> Result<(Vec<DecodedField>, ReadStats)> {
    let scan = Scan::open(plan, options, selection.clone(), with_lat_lons).await?;
    scan.collect_all(rayon::current_num_threads())
}

/// Read every file at `location` (a file, directory or glob) end to end.
pub async fn read(
    location: &str,
    options: &HashMap<String, String>,
    selection: &Selection,
    use_index: bool,
    with_lat_lons: bool,
) -> Result<Vec<(FilePlan, Vec<DecodedField>, ReadStats)>> {
    let mut results = Vec::new();
    for file in list_files(location, options).await? {
        let plan = plan_file(&file, options, selection, use_index).await?;
        let (fields, stats) = read_plan(&plan, options, selection, with_lat_lons).await?;
        results.push((plan, fields, stats));
    }
    Ok(results)
}

/// Rebuild an `.idx`-located message from its range and its fields' `.idx`
/// variable and level (all a read needs), e.g. when it comes back from
/// Python. The range must lie within the file.
pub fn indexed_message(
    message_index: usize,
    offset: u64,
    length: u64,
    fields: Vec<(String, String)>,
    file_size: u64,
) -> Result<IndexedMessage> {
    if offset.checked_add(length).is_none_or(|end| end > file_size) || length == 0 {
        return Err(Error::InvalidOption {
            name: "messages".into(),
            reason: format!("range {offset}+{length} is not within the {file_size}-byte file"),
        });
    }
    let entries = fields
        .into_iter()
        .map(|(variable_name, level_description)| IndexEntry {
            message_number: 0,
            sub_message: None,
            byte_offset: offset,
            reference_time: String::new(),
            variable_name,
            level_description,
            forecast_info: String::new(),
        })
        .collect();
    Ok(IndexedMessage { message_index, offset, length, entries })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn skips_sidecars_markers_and_hidden_files() {
        assert!(is_grib_file_candidate("gfs.t00z.pgrb2.1p00.f000"));
        assert!(is_grib_file_candidate("sample.grib2"));
        for name in ["gfs.f000.idx", "_SUCCESS", ".hidden", "notes.MD", "data.tar.gz", "run.sh"] {
            assert!(!is_grib_file_candidate(name), "{name}");
        }
    }

    #[test]
    fn dbfs_volume_paths_are_read_through_the_fuse_mount() {
        let resolved = resolve("dbfs:/Volumes/c/s/v/file.grib2", &HashMap::new()).unwrap();
        assert_eq!(resolved.path.as_ref(), "Volumes/c/s/v/file.grib2");
        assert_eq!(resolved.location_of(&resolved.path), "/Volumes/c/s/v/file.grib2");
    }
}
