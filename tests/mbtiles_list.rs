use std::path::Path;

use vt_optimizer::mbtiles::{
    inspect_mbtiles_with_options, InspectOptions, TileListOptions, TileSort,
};

fn create_list_mbtiles(path: &Path) {
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

    let small = vec![0u8; 10];
    let medium = vec![0u8; 50];
    let large = vec![0u8; 100];

    conn.execute(
        "INSERT INTO tiles (zoom_level, tile_column, tile_row, tile_data) VALUES (2, 1, 1, ?1)",
        (medium,),
    )
    .expect("medium");
    conn.execute(
        "INSERT INTO tiles (zoom_level, tile_column, tile_row, tile_data) VALUES (2, 0, 0, ?1)",
        (large,),
    )
    .expect("large");
    conn.execute(
        "INSERT INTO tiles (zoom_level, tile_column, tile_row, tile_data) VALUES (2, 2, 2, ?1)",
        (small,),
    )
    .expect("small");
}

#[test]
fn list_tiles_sorted_by_size_with_limit() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("input.mbtiles");
    create_list_mbtiles(&path);

    let options = InspectOptions {
        sample: None,
        topn: 0,
        histogram_buckets: 2,
        no_progress: true,
        max_tile_bytes: 100,
        zoom: Some(2),
        bucket: Some(0),
        tile: None,
        summary: false,
        layers: Vec::new(),
        recommend: false,
        include_layer_list: false,
        list_tiles: Some(TileListOptions {
            limit: 2,
            sort: TileSort::Size,
        }),
    };

    let report = inspect_mbtiles_with_options(&path, options).expect("inspect");
    assert_eq!(report.bucket_tiles.len(), 2);
    assert_eq!(report.bucket_tiles[0].bytes, 50);
    assert_eq!(report.bucket_tiles[1].bytes, 10);
}

#[test]
fn list_tiles_sorted_by_zxy() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("input.mbtiles");
    create_list_mbtiles(&path);

    let options = InspectOptions {
        sample: None,
        topn: 0,
        histogram_buckets: 2,
        no_progress: true,
        max_tile_bytes: 100,
        zoom: Some(2),
        bucket: Some(1),
        tile: None,
        summary: false,
        layers: Vec::new(),
        recommend: false,
        include_layer_list: false,
        list_tiles: Some(TileListOptions {
            limit: 10,
            sort: TileSort::Zxy,
        }),
    };

    let report = inspect_mbtiles_with_options(&path, options).expect("inspect");
    assert_eq!(report.bucket_tiles.len(), 1);
    assert_eq!(report.bucket_tiles[0].x, 0);
    assert_eq!(report.bucket_tiles[0].y, 0);
}
