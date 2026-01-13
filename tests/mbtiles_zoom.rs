use std::path::Path;

use vt_optimizer::mbtiles::{inspect_mbtiles_with_options, InspectOptions, MbtilesStats};

fn create_zoom_sample_mbtiles(path: &Path) {
    let conn = rusqlite::Connection::open(path).expect("open");
    conn.execute_batch(
        "
        CREATE TABLE metadata (name TEXT, value TEXT);
        CREATE TABLE tiles (
            zoom_level INTEGER,
            tile_column INTEGER,
            tile_row INTEGER,
            tile_data BLOB
        );
        ",
    )
    .expect("schema");

    let z0a = vec![0u8; 10];
    let z0b = vec![0u8; 30];
    let z1a = vec![0u8; 50];
    let z1b = vec![0u8; 70];

    conn.execute(
        "INSERT INTO tiles (zoom_level, tile_column, tile_row, tile_data) VALUES (0, 0, 0, ?1)",
        (z0a,),
    )
    .expect("z0a");
    conn.execute(
        "INSERT INTO tiles (zoom_level, tile_column, tile_row, tile_data) VALUES (0, 0, 1, ?1)",
        (z0b,),
    )
    .expect("z0b");
    conn.execute(
        "INSERT INTO tiles (zoom_level, tile_column, tile_row, tile_data) VALUES (1, 0, 0, ?1)",
        (z1a,),
    )
    .expect("z1a");
    conn.execute(
        "INSERT INTO tiles (zoom_level, tile_column, tile_row, tile_data) VALUES (1, 0, 1, ?1)",
        (z1b,),
    )
    .expect("z1b");
}

#[test]
fn inspect_zoom_limits_stats_and_histogram() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("input.mbtiles");
    create_zoom_sample_mbtiles(&path);

    let options = InspectOptions {
        sample: None,
        topn: 0,
        histogram_buckets: 2,
        no_progress: true,
        max_tile_bytes: 100,
        zoom: Some(1),
        bucket: Some(0),
        tile: None,
        summary: false,
        layers: Vec::new(),
        recommend: false,
        include_layer_list: false,
        list_tiles: None,
    };

    let report = inspect_mbtiles_with_options(&path, options).expect("inspect");
    assert_eq!(
        report.overall,
        MbtilesStats {
            tile_count: 2,
            total_bytes: 120,
            max_bytes: 70,
            avg_bytes: 60,
        }
    );
    assert_eq!(report.by_zoom.len(), 1);
    assert_eq!(report.by_zoom[0].zoom, 1);
    assert_eq!(report.bucket_count, Some(1));
    assert_eq!(report.histogram.len(), 2);
    assert_eq!(report.histogram[0].count, 1);
    assert_eq!(report.histogram[1].count, 1);
    assert!((report.histogram[0].pct_tiles - 0.5).abs() < 1e-6);
    assert!((report.histogram[1].pct_tiles - 0.5).abs() < 1e-6);
    assert!((report.histogram[1].accum_pct_tiles - 1.0).abs() < 1e-6);
}

#[test]
fn inspect_zoom_sampling_uses_zoom_tile_count() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("input.mbtiles");
    create_zoom_sample_mbtiles(&path);

    let options = InspectOptions {
        sample: Some(vt_optimizer::mbtiles::SampleSpec::Count(1)),
        topn: 0,
        histogram_buckets: 0,
        no_progress: true,
        max_tile_bytes: 0,
        zoom: Some(1),
        bucket: None,
        tile: None,
        summary: false,
        layers: Vec::new(),
        recommend: false,
        include_layer_list: false,
        list_tiles: None,
    };

    let report = inspect_mbtiles_with_options(&path, options).expect("inspect");
    assert_eq!(report.overall.tile_count, 1);
    assert_eq!(report.by_zoom.len(), 1);
    assert_eq!(report.by_zoom[0].zoom, 1);
}

#[test]
fn inspect_histograms_by_zoom_includes_each_zoom() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("input.mbtiles");
    create_zoom_sample_mbtiles(&path);

    let options = InspectOptions {
        sample: None,
        topn: 0,
        histogram_buckets: 2,
        no_progress: true,
        max_tile_bytes: 0,
        zoom: None,
        bucket: None,
        tile: None,
        summary: false,
        layers: Vec::new(),
        recommend: false,
        include_layer_list: false,
        list_tiles: None,
    };

    let report = inspect_mbtiles_with_options(&path, options).expect("inspect");
    assert_eq!(report.histograms_by_zoom.len(), 2);
    assert_eq!(report.histograms_by_zoom[0].zoom, 0);
    assert_eq!(report.histograms_by_zoom[1].zoom, 1);
    assert_eq!(report.histograms_by_zoom[0].buckets.len(), 2);
    assert_eq!(report.histograms_by_zoom[1].buckets.len(), 2);
}
