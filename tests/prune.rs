use std::fs;
use std::path::Path;

use mvt::{GeomEncoder, GeomType, Tile};
use mvt_reader::Reader;

use vt_optimizer::mbtiles::{PruneOptions, inspect_mbtiles, prune_mbtiles_layer_only};
use vt_optimizer::style::read_style;

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

fn create_layer_mbtiles_multiple(path: &Path) {
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
        (data.clone(),),
    )
    .expect("tile insert 0");
    conn.execute(
        "INSERT INTO tiles (zoom_level, tile_column, tile_row, tile_data) VALUES (0, 0, 1, ?1)",
        (data,),
    )
    .expect("tile insert 1");
}

fn create_layer_mbtiles_map_images(path: &Path) {
    let conn = rusqlite::Connection::open(path).expect("open");
    conn.execute_batch(
        "
        CREATE TABLE metadata (name TEXT, value TEXT);
        CREATE TABLE map (zoom_level INTEGER, tile_column INTEGER, tile_row INTEGER, tile_id TEXT);
        CREATE TABLE images (tile_id TEXT, tile_data BLOB);
        ",
    )
    .expect("schema");

    let data = create_layer_tile();
    conn.execute(
        "INSERT INTO map (zoom_level, tile_column, tile_row, tile_id) VALUES (0, 0, 0, 't1')",
        [],
    )
    .expect("map insert");
    conn.execute(
        "INSERT INTO images (tile_id, tile_data) VALUES ('t1', ?1)",
        (data,),
    )
    .expect("image insert");
}
#[test]
fn prune_mbtiles_removes_unlisted_layers() {
    let dir = tempfile::tempdir().expect("tempdir");
    let input = dir.path().join("input.mbtiles");
    let output = dir.path().join("output.mbtiles");
    let style = dir.path().join("style.json");
    create_layer_mbtiles(&input);

    fs::write(
        &style,
        r#"{"version":8,"sources":{"osm":{"type":"vector"}},"layers":[{"id":"roads","type":"line","source":"osm","source-layer":"roads","paint":{"line-width":1}},{"id":"buildings","type":"fill","source":"osm","source-layer":"buildings","paint":{"fill-opacity":0}}]}"#,
    )
    .expect("write style");
    let style = read_style(&style).expect("read style");

    prune_mbtiles_layer_only(
        &input,
        &output,
        &style,
        false,
        PruneOptions {
            threads: 1,
            io_batch: 10,
            readers: 1,
            read_cache_mb: None,
            write_cache_mb: None,
            drop_empty_tiles: false,
        },
    )
    .expect("prune mbtiles");

    let conn = rusqlite::Connection::open(&output).expect("open output");
    let data: Vec<u8> = conn
        .query_row(
            "SELECT tile_data FROM tiles WHERE zoom_level = 0 AND tile_column = 0 AND tile_row = 0",
            [],
            |row| row.get(0),
        )
        .expect("read tile");
    let reader = Reader::new(data).expect("decode");
    let layers = reader.get_layer_metadata().expect("layers");
    assert_eq!(layers.len(), 1);
    assert_eq!(layers[0].name, "roads");
}

#[test]
fn prune_mbtiles_supports_map_images_schema() {
    let dir = tempfile::tempdir().expect("tempdir");
    let input = dir.path().join("input.mbtiles");
    let output = dir.path().join("output.mbtiles");
    let style = dir.path().join("style.json");
    create_layer_mbtiles_map_images(&input);

    fs::write(
        &style,
        r#"{"version":8,"sources":{"osm":{"type":"vector"}},"layers":[{"id":"roads","type":"line","source":"osm","source-layer":"roads","paint":{"line-width":1}}]}"#,
    )
    .expect("write style");
    let style = read_style(&style).expect("read style");

    prune_mbtiles_layer_only(
        &input,
        &output,
        &style,
        false,
        PruneOptions {
            threads: 1,
            io_batch: 10,
            readers: 1,
            read_cache_mb: None,
            write_cache_mb: None,
            drop_empty_tiles: false,
        },
    )
    .expect("prune mbtiles");

    let report = inspect_mbtiles(&output).expect("inspect output");
    assert_eq!(report.overall.tile_count, 1);
}

#[test]
fn prune_mbtiles_handles_multiple_tiles() {
    let dir = tempfile::tempdir().expect("tempdir");
    let input = dir.path().join("input.mbtiles");
    let output = dir.path().join("output.mbtiles");
    let style = dir.path().join("style.json");

    create_layer_mbtiles_multiple(&input);

    fs::write(
        &style,
        r#"{"version":8,"sources":{"osm":{"type":"vector"}},"layers":[{"id":"roads","type":"line","source":"osm","source-layer":"roads","paint":{"line-width":1}}]}"#,
    )
    .expect("write style");
    let style = read_style(&style).expect("read style");

    prune_mbtiles_layer_only(
        &input,
        &output,
        &style,
        false,
        PruneOptions {
            threads: 2,
            io_batch: 10,
            readers: 2,
            read_cache_mb: None,
            write_cache_mb: None,
            drop_empty_tiles: false,
        },
    )
    .expect("prune mbtiles");

    let report = inspect_mbtiles(&output).expect("inspect output");
    assert_eq!(report.overall.tile_count, 2);
}
#[test]
fn prune_mbtiles_filters_features_by_style() {
    let dir = tempfile::tempdir().expect("tempdir");
    let input = dir.path().join("input.mbtiles");
    let output = dir.path().join("output.mbtiles");
    let style_path = dir.path().join("style.json");

    let conn = rusqlite::Connection::open(&input).expect("open");
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

    let mut tile = Tile::new(4096);
    let layer = tile.create_layer("roads");
    let geom = GeomEncoder::new(GeomType::Point)
        .point(1.0, 2.0)
        .expect("point")
        .encode()
        .expect("encode");
    let mut feature = layer.into_feature(geom);
    feature.add_tag_string("class", "primary");
    let layer = feature.into_layer();
    let geom = GeomEncoder::new(GeomType::Point)
        .point(3.0, 4.0)
        .expect("point")
        .encode()
        .expect("encode");
    let mut feature = layer.into_feature(geom);
    feature.add_tag_string("class", "secondary");
    let layer = feature.into_layer();
    tile.add_layer(layer).expect("add roads");
    let data = tile.to_bytes().expect("tile bytes");

    conn.execute(
        "INSERT INTO tiles (zoom_level, tile_column, tile_row, tile_data) VALUES (0, 0, 0, ?1)",
        (data,),
    )
    .expect("tile insert");

    fs::write(
        &style_path,
        r#"{"version":8,"sources":{"osm":{"type":"vector"}},"layers":[{"id":"roads","type":"line","source":"osm","source-layer":"roads","filter":["==","class","primary"],"paint":{"line-width":1}}]}"#,
    )
    .expect("write style");
    let style = read_style(&style_path).expect("read style");

    prune_mbtiles_layer_only(
        &input,
        &output,
        &style,
        true,
        PruneOptions {
            threads: 2,
            io_batch: 10,
            readers: 2,
            read_cache_mb: None,
            write_cache_mb: None,
            drop_empty_tiles: false,
        },
    )
    .expect("prune mbtiles");

    let conn = rusqlite::Connection::open(&output).expect("open output");
    let data: Vec<u8> = conn
        .query_row(
            "SELECT tile_data FROM tiles WHERE zoom_level = 0 AND tile_column = 0 AND tile_row = 0",
            [],
            |row| row.get(0),
        )
        .expect("read tile");
    let reader = Reader::new(data).expect("decode");
    let layers = reader.get_layer_metadata().expect("layers");
    assert_eq!(layers.len(), 1);
    let features = reader
        .get_features(layers[0].layer_index)
        .expect("features");
    assert_eq!(features.len(), 1);
    let props = features[0].properties.as_ref().expect("props");
    assert_eq!(
        props.get("class").unwrap(),
        &mvt_reader::feature::Value::String("primary".to_string())
    );
}

#[test]
fn prune_mbtiles_keeps_features_on_unknown_filter() {
    let dir = tempfile::tempdir().expect("tempdir");
    let input = dir.path().join("input.mbtiles");
    let output = dir.path().join("output.mbtiles");
    let style_path = dir.path().join("style.json");

    create_layer_mbtiles(&input);

    fs::write(
        &style_path,
        r#"{"version":8,"sources":{"osm":{"type":"vector"}},"layers":[{"id":"roads","type":"line","source":"osm","source-layer":"roads","filter":["mystery",["get","class"],"primary"]}]}"#,
    )
    .expect("write style");
    let style = read_style(&style_path).expect("read style");

    prune_mbtiles_layer_only(
        &input,
        &output,
        &style,
        true,
        PruneOptions {
            threads: 2,
            io_batch: 10,
            readers: 2,
            read_cache_mb: None,
            write_cache_mb: None,
            drop_empty_tiles: false,
        },
    )
    .expect("prune mbtiles");

    let conn = rusqlite::Connection::open(&output).expect("open output");
    let data: Vec<u8> = conn
        .query_row(
            "SELECT tile_data FROM tiles WHERE zoom_level = 0 AND tile_column = 0 AND tile_row = 0",
            [],
            |row| row.get(0),
        )
        .expect("read tile");
    let reader = Reader::new(data).expect("decode");
    let layers = reader.get_layer_metadata().expect("layers");
    assert_eq!(layers.len(), 1);
    assert_eq!(layers[0].name, "roads");
}

#[test]
fn prune_mbtiles_handles_multiple_readers() {
    let dir = tempfile::tempdir().expect("tempdir");
    let input = dir.path().join("input.mbtiles");
    let output = dir.path().join("output.mbtiles");
    let style = dir.path().join("style.json");

    create_layer_mbtiles_multiple(&input);

    fs::write(
        &style,
        r#"{"version":8,"sources":{"osm":{"type":"vector"}},"layers":[{"id":"roads","type":"line","source":"osm","source-layer":"roads","paint":{"line-width":1}}]}"#,
    )
    .expect("write style");
    let style = read_style(&style).expect("read style");

    prune_mbtiles_layer_only(
        &input,
        &output,
        &style,
        false,
        PruneOptions {
            threads: 4,
            io_batch: 10,
            readers: 2,
            read_cache_mb: None,
            write_cache_mb: None,
            drop_empty_tiles: false,
        },
    )
    .expect("prune mbtiles");

    let report = inspect_mbtiles(&output).expect("inspect output");
    assert_eq!(report.overall.tile_count, 2);
}

#[test]
fn prune_mbtiles_drop_empty_tiles() {
    let dir = tempfile::tempdir().expect("tempdir");
    let input = dir.path().join("input.mbtiles");
    let output = dir.path().join("output.mbtiles");
    let style = dir.path().join("style.json");

    create_layer_mbtiles_multiple(&input);

    fs::write(
        &style,
        r#"{"version":8,"sources":{"osm":{"type":"vector"}},"layers":[{"id":"nope","type":"line","source":"osm","source-layer":"nope"}]}"#,
    )
    .expect("write style");
    let style = read_style(&style).expect("read style");

    prune_mbtiles_layer_only(
        &input,
        &output,
        &style,
        false,
        PruneOptions {
            threads: 2,
            io_batch: 10,
            readers: 1,
            read_cache_mb: None,
            write_cache_mb: None,
            drop_empty_tiles: true,
        },
    )
    .expect("prune mbtiles");

    let report = inspect_mbtiles(&output).expect("inspect output");
    assert_eq!(report.overall.tile_count, 0);
}
