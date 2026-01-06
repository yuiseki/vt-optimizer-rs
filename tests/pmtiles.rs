use std::path::Path;

use tile_prune::mbtiles::inspect_mbtiles;
use tile_prune::pmtiles::{mbtiles_to_pmtiles, pmtiles_to_mbtiles};

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

    let tile1 = vec![1u8; 10];
    let tile2 = vec![2u8; 20];

    conn.execute(
        "INSERT INTO tiles (zoom_level, tile_column, tile_row, tile_data) VALUES (0, 0, 0, ?1)",
        (tile1,),
    )
    .expect("tile1");
    conn.execute(
        "INSERT INTO tiles (zoom_level, tile_column, tile_row, tile_data) VALUES (1, 1, 1, ?1)",
        (tile2,),
    )
    .expect("tile2");
}

#[test]
fn mbtiles_to_pmtiles_and_back_preserves_counts() {
    let dir = tempfile::tempdir().expect("tempdir");
    let input = dir.path().join("input.mbtiles");
    let pmtiles = dir.path().join("output.pmtiles");
    let output = dir.path().join("roundtrip.mbtiles");
    create_sample_mbtiles(&input);

    mbtiles_to_pmtiles(&input, &pmtiles).expect("mbtiles->pmtiles");
    pmtiles_to_mbtiles(&pmtiles, &output).expect("pmtiles->mbtiles");

    let report = inspect_mbtiles(&output).expect("inspect output");
    assert_eq!(report.overall.tile_count, 2);
    assert_eq!(report.overall.total_bytes, 30);
    assert_eq!(report.overall.max_bytes, 20);
}
