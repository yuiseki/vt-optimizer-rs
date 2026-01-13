use std::path::Path;

use mvt::{GeomEncoder, GeomType, Tile};

use vt_optimizer::mbtiles::{inspect_mbtiles_with_options, FileLayerSummary, InspectOptions};

fn create_layer_tile() -> Vec<u8> {
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
    tile.add_layer(layer).expect("add roads");

    let layer = tile.create_layer("buildings");
    let geom = GeomEncoder::new(GeomType::Point)
        .point(3.0, 4.0)
        .expect("point")
        .encode()
        .expect("encode");
    let mut feature = layer.into_feature(geom);
    feature.add_tag_string("height", "10");
    let layer = feature.into_layer();
    tile.add_layer(layer).expect("add buildings");

    tile.to_bytes().expect("tile bytes")
}

fn create_layer_mbtiles(path: &Path) {
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

    let data = create_layer_tile();
    conn.execute(
        "INSERT INTO tiles (zoom_level, tile_column, tile_row, tile_data) VALUES (0, 0, 0, ?1)",
        (data,),
    )
    .expect("tile insert");
}

#[test]
fn inspect_collects_file_layer_list() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("input.mbtiles");
    create_layer_mbtiles(&path);

    let options = InspectOptions {
        sample: None,
        topn: 0,
        histogram_buckets: 0,
        no_progress: true,
        max_tile_bytes: 0,
        zoom: None,
        bucket: None,
        tile: None,
        summary: false,
        layers: Vec::new(),
        recommend: false,
        include_layer_list: true,
        list_tiles: None,
    };

    let report = inspect_mbtiles_with_options(&path, options).expect("inspect");
    let mut layers = report.file_layers.clone();
    layers.sort_by(|a, b| a.name.cmp(&b.name));
    assert_eq!(
        layers,
        vec![
            FileLayerSummary {
                name: "buildings".to_string(),
                vertex_count: 1,
                feature_count: 1,
                property_key_count: 1,
                property_value_count: 1,
            },
            FileLayerSummary {
                name: "roads".to_string(),
                vertex_count: 1,
                feature_count: 1,
                property_key_count: 2,
                property_value_count: 2,
            },
        ]
    );
}

#[test]
fn inspect_filters_file_layer_list() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("input.mbtiles");
    create_layer_mbtiles(&path);

    let options = InspectOptions {
        sample: None,
        topn: 0,
        histogram_buckets: 0,
        no_progress: true,
        max_tile_bytes: 0,
        zoom: None,
        bucket: None,
        tile: None,
        summary: false,
        layers: vec!["roads".to_string()],
        recommend: false,
        include_layer_list: true,
        list_tiles: None,
    };

    let report = inspect_mbtiles_with_options(&path, options).expect("inspect");
    assert_eq!(
        report.file_layers,
        vec![FileLayerSummary {
            name: "roads".to_string(),
            vertex_count: 1,
            feature_count: 1,
            property_key_count: 2,
            property_value_count: 2,
        }]
    );
}
