//! Reads the repo's GRIB2 fixtures (fetch them with `download_test_fixtures.sh`)
//! through the full list -> plan -> range read -> decode path, via the local store.
//! Value-level parity with the Scala reader is checked by `scripts/parity.py`.

use std::collections::HashMap;
use std::path::PathBuf;

use grib_idx::batch::{self, LongitudeRange, OutputMode};
use grib_idx::filters::{Column, Predicate};
use grib_idx::{DecodedField, ReadStats, Selection, read};

fn resources() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../src/test/resources")
}

fn fixture(name: &str) -> Option<String> {
    let path = resources().join(name);
    if path.exists() {
        Some(path.to_string_lossy().into_owned())
    } else {
        eprintln!("skipping: fixture {name} not downloaded");
        None
    }
}

async fn read_one(location: &str, selection: &Selection, use_index: bool, lat_lons: bool) -> (Vec<DecodedField>, ReadStats) {
    let mut results = read(location, &HashMap::new(), selection, use_index, lat_lons).await.unwrap();
    assert_eq!(results.len(), 1, "expected one file at {location}");
    let (_, fields, stats) = results.remove(0);
    (fields, stats)
}

fn variables(names: &[&str]) -> Selection {
    Selection { variables: Some(names.iter().map(|s| s.to_string()).collect()), ..Selection::default() }
}

#[tokio::test]
async fn reads_every_gfs_field_in_index_order_with_netcdf_java_names() {
    let Some(path) = fixture("gfs_sample.grib2") else { return };

    let (fields, stats) = read_one(&path, &Selection::default(), true, false).await;

    assert_eq!(fields.len(), 696);
    assert_eq!(stats.messages_read, 696);
    let indices: Vec<i32> = fields.iter().map(|f| f.message_index).collect();
    assert_eq!(indices, (0..696).collect::<Vec<_>>());
    let prmsl = &fields[0];
    assert_eq!((prmsl.variable_short_name.as_str(), prmsl.units.as_str()), ("PRMSL", "Pa"));
    assert_eq!(prmsl.level_type, "Mean sea level");
    assert_eq!((prmsl.nx, prmsl.ny), (360, 181));
    assert_eq!((prmsl.geometry.lat_start, prmsl.geometry.lat_increment), (90.0, -1.0));
    assert_eq!(fields[1].variable_short_name, "CLWMR");
}

#[tokio::test]
async fn variable_and_level_selection_reads_only_matching_byte_ranges() {
    let Some(path) = fixture("gfs_sample.grib2") else { return };
    let selection = Selection { levels: Some(vec!["2 m above ground".into()]), ..variables(&["tmp"]) };

    let (fields, stats) = read_one(&path, &selection, true, false).await;

    assert_eq!(fields.len(), 1);
    assert_eq!(stats.messages_read, 1);
    assert!(stats.bytes_read < 100_000);
    assert_eq!((fields[0].level_type.as_str(), fields[0].level_value), ("Specified height level above ground", 2.0));
}

#[tokio::test]
async fn idx_name_and_decoded_name_both_select_the_same_field() {
    let Some(path) = fixture("gfs_sample.grib2") else { return };

    let (by_idx_name, _) = read_one(&path, &variables(&["CLMR"]), true, false).await;
    let (by_decoded_name, _) = read_one(&path, &variables(&["CLWMR"]), true, false).await;

    assert_eq!(by_idx_name.len(), 23);
    assert_eq!(by_idx_name, by_decoded_name);
}

#[tokio::test]
async fn pushed_predicate_prunes_on_idx_levels_and_filters_exactly() {
    let Some(path) = fixture("gfs_sample.grib2") else { return };
    let selection = Selection {
        predicate: Some(Predicate::And(
            Box::new(Predicate::StrEq(Column::VariableShortName, "TMP".into())),
            Box::new(Predicate::LevelIn(vec![50000.0])),
        )),
        ..Selection::default()
    };

    let (fields, stats) = read_one(&path, &selection, true, false).await;

    assert_eq!(fields.len(), 1);
    assert_eq!(fields[0].level_type, "Isobaric surface");
    assert_eq!(stats.messages_read, 1);
}

#[tokio::test]
async fn whole_file_read_matches_indexed_read() {
    let Some(path) = fixture("rap_submessage_sample.grib2") else { return };

    let (indexed, _) = read_one(&path, &Selection::default(), true, false).await;
    let (whole, stats) = read_one(&path, &Selection::default(), false, false).await;

    assert_eq!(whole, indexed);
    assert!(stats.messages_read < indexed.len(), "sub-messages share messages");
}

#[tokio::test]
async fn sub_message_fields_are_filtered_individually() {
    let Some(path) = fixture("rap_submessage_sample.grib2") else { return };
    let (all, _) = read_one(&path, &Selection::default(), true, false).await;

    let (vgrd, _) = read_one(&path, &variables(&["VGRD"]), true, false).await;

    assert!(!vgrd.is_empty());
    for field in &vgrd {
        assert_eq!(field.variable_short_name, "VGRD");
        assert!(all.contains(field));
    }
}

#[tokio::test]
async fn files_without_an_index_are_read_whole() {
    let Some(path) = fixture("templates_sample.grib2") else { return };

    let (fields, _) = read_one(&path, &Selection::default(), true, false).await;

    let templates: Vec<i32> = fields.iter().map(|f| f.pds_template).collect();
    assert_eq!(templates.len(), 7);
    assert!(fields.iter().any(|f| f.product.ensemble.is_some()));
    assert!(fields.iter().any(|f| f.product.statistic.is_some()));
}

#[tokio::test]
async fn levels_option_requires_an_index() {
    let Some(path) = fixture("templates_sample.grib2") else { return };
    let selection = Selection { levels: Some(vec!["2 m above ground".into()]), ..Selection::default() };

    let err = read(&path, &HashMap::new(), &selection, true, false).await.err().unwrap();

    assert!(matches!(err, grib_idx::Error::LevelsNeedIndex));
}

#[tokio::test]
async fn directory_and_glob_listing_skip_sidecars() {
    if fixture("gfs_sample.grib2").is_none() || fixture("templates_sample.grib2").is_none() {
        return;
    }
    let dir = resources().to_string_lossy().into_owned();

    let listed = grib_idx::list_files(&dir, &HashMap::new()).await.unwrap();
    let globbed = grib_idx::list_files(&format!("{dir}/*_sample.grib2"), &HashMap::new()).await.unwrap();

    assert!(listed.iter().all(|f| !f.location.ends_with(".idx")));
    let gfs = listed.iter().find(|f| f.location.ends_with("/gfs_sample.grib2")).unwrap();
    assert_eq!(gfs.has_idx, Some(true));
    let templates = listed.iter().find(|f| f.location.ends_with("/templates_sample.grib2")).unwrap();
    assert_eq!(templates.has_idx, Some(false));
    assert_eq!(globbed.len(), listed.iter().filter(|f| f.location.ends_with("_sample.grib2")).count());
}

#[tokio::test]
async fn record_batches_follow_the_scala_schemas() {
    let Some(path) = fixture("gefs_chem_sample.grib2") else { return };
    let (fields, _) = read_one(&path, &Selection::default(), true, true).await;

    let gridded = batch::gridded_batch(&path, &fields, LongitudeRange::ZeroTo360).unwrap();
    let flat = batch::flat_batch(&path, &fields[0], LongitudeRange::Signed).unwrap();

    assert_eq!(gridded.num_rows(), 15);
    assert_eq!(gridded.schema(), batch::schema(OutputMode::Gridded));
    assert_eq!(flat.num_rows(), fields[0].values.len());
    assert_eq!(flat.schema(), batch::schema(OutputMode::Flat));
    assert!(fields.iter().all(|f| f.product.aerosol.is_some()));
}
