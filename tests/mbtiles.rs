use std::path::Path;

use tile_prune::mbtiles::{
    copy_mbtiles, inspect_mbtiles, inspect_mbtiles_with_options, MbtilesStats, MbtilesZoomStats,
    InspectOptions, SampleSpec, parse_sample_spec,
};

fn create_sample_mbtiles(path: &Path) {
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

    conn.execute(
        "INSERT INTO metadata (name, value) VALUES (?1, ?2)",
        ("name", "sample"),
    )
    .expect("metadata");

    let tile1 = vec![0u8; 10];
    let tile2 = vec![0u8; 30];

    conn.execute(
        "INSERT INTO tiles (zoom_level, tile_column, tile_row, tile_data) VALUES (0, 0, 0, ?1)",
        (tile1,),
    )
    .expect("tile1");
    conn.execute(
        "INSERT INTO tiles (zoom_level, tile_column, tile_row, tile_data) VALUES (0, 0, 1, ?1)",
        (tile2,),
    )
    .expect("tile2");
}

#[test]
fn inspect_mbtiles_reports_minimal_stats() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("input.mbtiles");
    create_sample_mbtiles(&path);

    let report = inspect_mbtiles(&path).expect("inspect");
    assert_eq!(
        report.overall,
        MbtilesStats {
            tile_count: 2,
            total_bytes: 40,
            max_bytes: 30,
            avg_bytes: 20,
        }
    );
    assert_eq!(
        report.by_zoom,
        vec![MbtilesZoomStats {
            zoom: 0,
            stats: MbtilesStats {
                tile_count: 2,
                total_bytes: 40,
                max_bytes: 30,
                avg_bytes: 20,
            },
        }]
    );
    assert_eq!(report.empty_tiles, 2);
    assert_eq!(report.empty_ratio, 1.0);
    assert!(!report.sampled);
    assert_eq!(report.sample_total_tiles, 2);
    assert_eq!(report.sample_used_tiles, 2);
    assert!(report.histogram.is_empty());
    assert!(report.top_tiles.is_empty());
}

#[test]
fn copy_mbtiles_copies_tiles_and_metadata() {
    let dir = tempfile::tempdir().expect("tempdir");
    let input = dir.path().join("input.mbtiles");
    let output = dir.path().join("output.mbtiles");
    create_sample_mbtiles(&input);

    copy_mbtiles(&input, &output).expect("copy");

    let report = inspect_mbtiles(&output).expect("inspect output");
    assert_eq!(report.overall.tile_count, 2);
    assert_eq!(report.overall.total_bytes, 40);
    assert_eq!(report.overall.max_bytes, 30);
    assert_eq!(report.overall.avg_bytes, 20);

    let conn = rusqlite::Connection::open(output).expect("open output");
    let value: String = conn
        .query_row(
            "SELECT value FROM metadata WHERE name = 'name'",
            [],
            |row| row.get(0),
        )
        .expect("metadata value");
    assert_eq!(value, "sample");
}

#[test]
fn inspect_mbtiles_rejects_non_mbtiles_path() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("input.pmtiles");
    create_sample_mbtiles(&path);

    let err = inspect_mbtiles(&path).expect_err("should error");
    let msg = err.to_string();
    assert!(msg.contains("mbtiles"));
}

#[test]
fn copy_mbtiles_rejects_non_mbtiles_paths() {
    let dir = tempfile::tempdir().expect("tempdir");
    let input = dir.path().join("input.pmtiles");
    let output = dir.path().join("output.pmtiles");
    create_sample_mbtiles(&input);

    let err = copy_mbtiles(&input, &output).expect_err("should error");
    let msg = err.to_string();
    assert!(msg.contains("mbtiles"));
}

#[test]
fn inspect_mbtiles_topn_and_histogram() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("input.mbtiles");
    create_sample_mbtiles(&path);

    let options = InspectOptions {
        sample: None,
        topn: 1,
        histogram_buckets: 2,
        no_progress: true,
        zoom: None,
        bucket: None,
        list_tiles: None,
    };
    let report = inspect_mbtiles_with_options(&path, options).expect("inspect");
    assert_eq!(report.top_tiles.len(), 1);
    assert_eq!(report.top_tiles[0].bytes, 30);
    assert_eq!(report.histogram.len(), 2);
    assert_eq!(report.histogram.iter().map(|b| b.count).sum::<u64>(), 2);
}

#[test]
fn inspect_mbtiles_sample_count() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("input.mbtiles");
    create_sample_mbtiles(&path);

    let options = InspectOptions {
        sample: Some(SampleSpec::Count(1)),
        topn: 0,
        histogram_buckets: 0,
        no_progress: true,
        zoom: None,
        bucket: None,
        list_tiles: None,
    };
    let report = inspect_mbtiles_with_options(&path, options).expect("inspect");
    assert_eq!(report.sample_used_tiles, 1);
    assert_eq!(report.overall.tile_count, 1);
}

#[test]
fn parse_sample_spec_ratio_and_count() {
    let ratio = parse_sample_spec("0.25").expect("ratio");
    assert_eq!(ratio, SampleSpec::Ratio(0.25));
    let count = parse_sample_spec("10").expect("count");
    assert_eq!(count, SampleSpec::Count(10));
}
