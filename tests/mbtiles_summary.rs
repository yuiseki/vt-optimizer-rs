use std::io::Write;
use std::path::Path;

use flate2::{write::GzEncoder, Compression};
use mvt::{GeomEncoder, GeomType, Tile};

use vt_optimizer::mbtiles::{inspect_mbtiles_with_options, InspectOptions, TileCoord};

fn create_vector_tile() -> Vec<u8> {
    let mut tile = Tile::new(4096);

    let layer = tile.create_layer("roads");
    let geom = GeomEncoder::new(GeomType::Point)
        .point(1.0, 2.0)
        .expect("point")
        .encode()
        .expect("encode");
    let mut feature = layer.into_feature(geom);
    feature.add_tag_string("class", "primary");
    feature.add_tag_string("name", "Main");
    let layer = feature.into_layer();
    let geom = GeomEncoder::new(GeomType::Point)
        .point(3.0, 4.0)
        .expect("point")
        .encode()
        .expect("encode");
    let mut feature = layer.into_feature(geom);
    feature.add_tag_string("name", "Side");
    let layer = feature.into_layer();
    tile.add_layer(layer).expect("add roads layer");

    let layer = tile.create_layer("buildings");
    let geom = GeomEncoder::new(GeomType::Point)
        .point(5.0, 6.0)
        .expect("point")
        .encode()
        .expect("encode");
    let mut feature = layer.into_feature(geom);
    feature.add_tag_string("height", "10");
    let layer = feature.into_layer();
    tile.add_layer(layer).expect("add buildings layer");

    tile.to_bytes().expect("tile bytes")
}

fn create_summary_mbtiles(path: &Path, tile_data: Vec<u8>) {
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
        "INSERT INTO tiles (zoom_level, tile_column, tile_row, tile_data) VALUES (3, 4, 5, ?1)",
        (tile_data,),
    )
    .expect("tile insert");
}

#[test]
fn inspect_tile_summary_reports_layer_counts() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("input.mbtiles");
    let data = create_vector_tile();
    create_summary_mbtiles(&path, data);

    let options = InspectOptions {
        sample: None,
        topn: 0,
        histogram_buckets: 0,
        no_progress: true,
        max_tile_bytes: 0,
        zoom: None,
        bucket: None,
        tile: Some(TileCoord {
            zoom: 3,
            x: 4,
            y: 5,
        }),
        summary: true,
        layers: Vec::new(),
        recommend: false,
        include_layer_list: false,
        list_tiles: None,
    };

    let report = inspect_mbtiles_with_options(&path, options).expect("inspect");
    let summary = report.tile_summary.expect("summary");
    assert_eq!(summary.zoom, 3);
    assert_eq!(summary.x, 4);
    assert_eq!(summary.y, 5);
    assert_eq!(summary.layer_count, 2);
    assert_eq!(summary.total_features, 3);
    assert_eq!(summary.vertex_count, 3);
    assert_eq!(summary.property_key_count, 3);
    assert_eq!(summary.property_value_count, 4);
    assert_eq!(summary.layers.len(), 2);
    assert_eq!(summary.layers[0].name, "roads");
    assert_eq!(summary.layers[0].feature_count, 2);
    assert_eq!(summary.layers[0].vertex_count, 2);
    assert_eq!(summary.layers[0].property_key_count, 2);
    assert_eq!(summary.layers[0].property_value_count, 3);
    assert_eq!(
        summary.layers[0].property_keys,
        vec!["class".to_string(), "name".to_string()]
    );
    assert_eq!(summary.layers[1].name, "buildings");
    assert_eq!(summary.layers[1].feature_count, 1);
    assert_eq!(summary.layers[1].vertex_count, 1);
    assert_eq!(summary.layers[1].property_key_count, 1);
    assert_eq!(summary.layers[1].property_value_count, 1);
    assert_eq!(summary.layers[1].property_keys, vec!["height".to_string()]);
}

#[test]
fn inspect_tile_summary_decodes_gzip_tiles() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("input.mbtiles");
    let data = create_vector_tile();
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(&data).expect("gzip write");
    let compressed = encoder.finish().expect("gzip finish");
    create_summary_mbtiles(&path, compressed);

    let options = InspectOptions {
        sample: None,
        topn: 0,
        histogram_buckets: 0,
        no_progress: true,
        max_tile_bytes: 0,
        zoom: None,
        bucket: None,
        tile: Some(TileCoord {
            zoom: 3,
            x: 4,
            y: 5,
        }),
        summary: true,
        layers: Vec::new(),
        recommend: false,
        include_layer_list: false,
        list_tiles: None,
    };

    let report = inspect_mbtiles_with_options(&path, options).expect("inspect");
    let summary = report.tile_summary.expect("summary");
    assert_eq!(summary.layers[0].name, "roads");
    assert_eq!(summary.layers[1].name, "buildings");
}

#[test]
fn inspect_tile_summary_filters_layer() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("input.mbtiles");
    let data = create_vector_tile();
    create_summary_mbtiles(&path, data);

    let options = InspectOptions {
        sample: None,
        topn: 0,
        histogram_buckets: 0,
        no_progress: true,
        max_tile_bytes: 0,
        zoom: None,
        bucket: None,
        tile: Some(TileCoord {
            zoom: 3,
            x: 4,
            y: 5,
        }),
        summary: true,
        layers: vec!["roads".to_string()],
        recommend: false,
        include_layer_list: false,
        list_tiles: None,
    };

    let report = inspect_mbtiles_with_options(&path, options).expect("inspect");
    let summary = report.tile_summary.expect("summary");
    assert_eq!(summary.total_features, 2);
    assert_eq!(summary.layer_count, 1);
    assert_eq!(summary.vertex_count, 2);
    assert_eq!(summary.property_key_count, 2);
    assert_eq!(summary.property_value_count, 3);
    assert_eq!(summary.layers.len(), 1);
    assert_eq!(summary.layers[0].name, "roads");
    assert_eq!(summary.layers[0].vertex_count, 2);
    assert_eq!(summary.layers[0].property_key_count, 2);
    assert_eq!(summary.layers[0].property_value_count, 3);
    assert_eq!(
        summary.layers[0].property_keys,
        vec!["class".to_string(), "name".to_string()]
    );
}
