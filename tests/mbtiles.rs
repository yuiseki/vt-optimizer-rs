use std::path::Path;

use tile_prune::mbtiles::{copy_mbtiles, inspect_mbtiles, MbtilesStats};

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

    let stats = inspect_mbtiles(&path).expect("inspect");
    assert_eq!(
        stats,
        MbtilesStats {
            tile_count: 2,
            total_bytes: 40,
            max_bytes: 30,
        }
    );
}

#[test]
fn copy_mbtiles_copies_tiles_and_metadata() {
    let dir = tempfile::tempdir().expect("tempdir");
    let input = dir.path().join("input.mbtiles");
    let output = dir.path().join("output.mbtiles");
    create_sample_mbtiles(&input);

    copy_mbtiles(&input, &output).expect("copy");

    let stats = inspect_mbtiles(&output).expect("inspect output");
    assert_eq!(stats.tile_count, 2);
    assert_eq!(stats.total_bytes, 40);
    assert_eq!(stats.max_bytes, 30);

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
