//! Reads the repo's GRIB2 fixtures (fetch them with `download_test_fixtures.sh`)
//! through the full idx -> range read -> decode path, via the local store.

use std::collections::HashMap;
use std::path::PathBuf;

use grib_idx::{Selection, batch, read};

fn fixture(name: &str) -> Option<String> {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../src/test/resources").join(name);
    if path.exists() {
        Some(path.to_string_lossy().into_owned())
    } else {
        eprintln!("skipping: fixture {name} not downloaded");
        None
    }
}

#[tokio::test]
async fn reads_every_gfs_field_in_index_order() {
    let Some(path) = fixture("gfs_sample.grib2") else { return };

    let (fields, stats) = read(&path, &HashMap::new(), &Selection::default()).await.unwrap();

    assert_eq!(stats.messages_read, stats.messages_in_index);
    assert_eq!(stats.bytes_read, stats.file_size);
    assert_eq!(fields.len(), 696);
    let indices: Vec<i32> = fields.iter().map(|f| f.message_index).collect();
    assert_eq!(indices, (0..696).collect::<Vec<_>>());

    let prmsl = &fields[0];
    assert_eq!(prmsl.variable_short_name, "PRMSL");
    assert_eq!((prmsl.nx, prmsl.ny), (360, 181));
    assert_eq!(prmsl.values.len(), 360 * 181);
    assert_eq!(prmsl.lat_start, Some(90.0));
    assert_eq!(prmsl.lat_increment, Some(-1.0));
    // Mean sea level pressure, in Pa.
    assert!(prmsl.values.iter().all(|v| (85_000.0..110_000.0).contains(v)));
}

#[tokio::test]
async fn selection_reads_only_matching_byte_ranges() {
    let Some(path) = fixture("gfs_sample.grib2") else { return };
    let selection = Selection {
        variables: Some(vec!["TMP".into()]),
        levels: Some(vec!["2 m above ground".into()]),
    };

    let (fields, stats) = read(&path, &HashMap::new(), &selection).await.unwrap();

    assert_eq!(fields.len(), 1);
    assert_eq!(stats.messages_read, 1);
    assert!(stats.bytes_read < stats.file_size / 100);
    let t2m = &fields[0];
    assert_eq!(t2m.level_value, Some(2.0));
    // 2 m temperature, in K.
    assert!(t2m.values.iter().all(|v| (180.0..340.0).contains(v)));
}

#[tokio::test]
async fn sub_message_fields_share_their_message_and_are_filtered_individually() {
    let Some(path) = fixture("rap_submessage_sample.grib2") else { return };
    let all = read(&path, &HashMap::new(), &Selection::default()).await.unwrap().0;
    let selection = Selection { variables: Some(vec!["VGRD".into()]), levels: None };

    let (vgrd, _) = read(&path, &HashMap::new(), &selection).await.unwrap();

    assert!(!vgrd.is_empty());
    assert!(vgrd.iter().all(|f| f.variable_short_name == "VGRD"));
    for field in &vgrd {
        let same = all.iter().find(|f| f.message_index == field.message_index).unwrap();
        assert_eq!(same, field);
    }
}

#[tokio::test]
async fn decodes_aerosol_product_template() {
    let Some(path) = fixture("gefs_chem_sample.grib2") else { return };

    let (fields, _) = read(&path, &HashMap::new(), &Selection::default()).await.unwrap();

    assert_eq!(fields.len(), 15);
    assert!(fields.iter().all(|f| f.pds_template == 48));
}

#[tokio::test]
async fn converts_to_record_batch_with_one_row_per_field() {
    let Some(path) = fixture("gefs_chem_sample.grib2") else { return };
    let (fields, _) = read(&path, &HashMap::new(), &Selection::default()).await.unwrap();

    let record_batch = batch::to_record_batch(&path, &fields).unwrap();

    assert_eq!(record_batch.num_rows(), 15);
    assert_eq!(record_batch.schema(), batch::schema());
}
