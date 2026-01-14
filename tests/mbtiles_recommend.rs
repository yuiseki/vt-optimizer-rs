use std::path::Path;

use mvt::{GeomEncoder, GeomType, Tile};

use vt_optimizer::mbtiles::{InspectOptions, inspect_mbtiles_with_options};

fn create_tile_with_points(count: usize) -> Vec<u8> {
    let mut tile = Tile::new(4096);
    let mut layer = tile.create_layer("points");
    for i in 0..count {
        let geom = GeomEncoder::new(GeomType::Point)
            .point(i as f64, i as f64)
            .expect("point")
            .encode()
            .expect("encode");
        layer = layer.into_feature(geom).into_layer();
    }
    tile.add_layer(layer).expect("add layer");
    tile.to_bytes().expect("tile bytes")
}

fn create_recommend_mbtiles(path: &Path) {
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

    let small = create_tile_with_points(1);
    let medium = create_tile_with_points(3);
    let large = create_tile_with_points(6);

    conn.execute(
        "INSERT INTO tiles (zoom_level, tile_column, tile_row, tile_data) VALUES (0, 0, 0, ?1)",
        (small,),
    )
    .expect("small");
    conn.execute(
        "INSERT INTO tiles (zoom_level, tile_column, tile_row, tile_data) VALUES (0, 0, 1, ?1)",
        (medium,),
    )
    .expect("medium");
    conn.execute(
        "INSERT INTO tiles (zoom_level, tile_column, tile_row, tile_data) VALUES (0, 1, 0, ?1)",
        (large,),
    )
    .expect("large");
}

#[test]
fn inspect_recommend_selects_over_limit_bucket_and_summaries() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("input.mbtiles");
    create_recommend_mbtiles(&path);

    let options = InspectOptions {
        sample: None,
        topn: 2,
        histogram_buckets: 2,
        no_progress: true,
        max_tile_bytes: 50,
        zoom: Some(0),
        bucket: None,
        tile: None,
        summary: false,
        layers: Vec::new(),
        recommend: true,
        include_layer_list: false,
        list_tiles: None,
    };

    let report = inspect_mbtiles_with_options(&path, options).expect("inspect");
    assert_eq!(report.recommended_buckets, vec![1]);
    assert_eq!(report.top_tiles.len(), 2);
    assert_eq!(report.top_tile_summaries.len(), 2);
    assert_eq!(report.top_tile_summaries[0].zoom, report.top_tiles[0].zoom);
    assert_eq!(report.top_tile_summaries[0].x, report.top_tiles[0].x);
    assert_eq!(report.top_tile_summaries[0].y, report.top_tiles[0].y);
}
