use std::path::Path;

use mvt::{GeomEncoder, GeomType, Tile};
use mvt_reader::Reader;
use vt_optimizer::mbtiles::{InspectOptions, TileCoord, simplify_mbtiles_tile};
use vt_optimizer::pmtiles::{
    inspect_pmtiles_with_options, mbtiles_to_pmtiles, simplify_pmtiles_tile,
};

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

fn create_line_tile() -> Vec<u8> {
    let mut tile = Tile::new(4096);
    let layer = tile.create_layer("roads");
    let geom = GeomEncoder::new(GeomType::Linestring)
        .point(0.0, 0.0)
        .expect("point0")
        .point(1.0, 0.1)
        .expect("point1")
        .point(2.0, 0.0)
        .expect("point2")
        .point(3.0, 0.1)
        .expect("point3")
        .point(4.0, 0.0)
        .expect("point4")
        .encode()
        .expect("encode");
    let mut feature = layer.into_feature(geom);
    feature.add_tag_string("class", "primary");
    let layer = feature.into_layer();
    tile.add_layer(layer).expect("add roads");
    tile.to_bytes().expect("tile bytes")
}

fn create_line_mbtiles(path: &Path) {
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

    let data = create_line_tile();
    conn.execute(
        "INSERT INTO tiles (zoom_level, tile_column, tile_row, tile_data) VALUES (0, 0, 0, ?1)",
        (data,),
    )
    .expect("tile insert");
}

#[test]
fn simplify_mbtiles_tile_filters_layers() {
    let dir = tempfile::tempdir().expect("tempdir");
    let input = dir.path().join("input.mbtiles");
    let output = dir.path().join("output.mbtiles");
    create_layer_mbtiles(&input);

    let coord = TileCoord {
        zoom: 0,
        x: 0,
        y: 0,
    };
    simplify_mbtiles_tile(&input, &output, coord, &["roads".to_string()], None).expect("simplify");

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
fn simplify_mbtiles_tile_keeps_all_layers_when_empty() {
    let dir = tempfile::tempdir().expect("tempdir");
    let input = dir.path().join("input.mbtiles");
    let output = dir.path().join("output.mbtiles");
    create_layer_mbtiles(&input);

    let coord = TileCoord {
        zoom: 0,
        x: 0,
        y: 0,
    };
    simplify_mbtiles_tile(&input, &output, coord, &[], None).expect("simplify");

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
    assert_eq!(layers.len(), 2);
    let names: Vec<_> = layers.iter().map(|layer| layer.name.as_str()).collect();
    assert!(names.contains(&"roads"));
    assert!(names.contains(&"buildings"));
}

#[test]
fn simplify_mbtiles_tile_applies_tolerance() {
    let dir = tempfile::tempdir().expect("tempdir");
    let input = dir.path().join("input.mbtiles");
    let output = dir.path().join("output.mbtiles");
    create_line_mbtiles(&input);

    let coord = TileCoord {
        zoom: 0,
        x: 0,
        y: 0,
    };
    simplify_mbtiles_tile(&input, &output, coord, &[], Some(0.5)).expect("simplify");

    let conn = rusqlite::Connection::open(&output).expect("open output");
    let data: Vec<u8> = conn
        .query_row(
            "SELECT tile_data FROM tiles WHERE zoom_level = 0 AND tile_column = 0 AND tile_row = 0",
            [],
            |row| row.get(0),
        )
        .expect("read tile");
    let reader = Reader::new(data).expect("decode");
    let features = reader.get_features(0).expect("features");
    let geom = features[0].get_geometry().clone();
    if let geo_types::Geometry::LineString(line) = geom {
        assert!(line.0.len() <= 3, "line was not simplified");
    } else {
        panic!("expected linestring geometry");
    }
}

#[test]
fn simplify_pmtiles_tile_outputs_single_tile() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mbtiles = dir.path().join("input.mbtiles");
    let pmtiles = dir.path().join("input.pmtiles");
    let output = dir.path().join("output.pmtiles");
    create_layer_mbtiles(&mbtiles);
    mbtiles_to_pmtiles(&mbtiles, &pmtiles).expect("to pmtiles");

    let coord = TileCoord {
        zoom: 0,
        x: 0,
        y: 0,
    };
    simplify_pmtiles_tile(&pmtiles, &output, coord, &[], None).expect("simplify");

    let report =
        inspect_pmtiles_with_options(&output, &InspectOptions::default()).expect("inspect");
    assert_eq!(report.overall.tile_count, 1);
}
