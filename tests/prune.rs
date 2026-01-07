use std::fs;
use std::path::Path;

use mvt::{GeomEncoder, GeomType, Tile};
use mvt_reader::Reader;

use tile_prune::mbtiles::prune_mbtiles_layer_only;
use tile_prune::style::read_style_source_layers;

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

#[test]
fn prune_mbtiles_removes_unlisted_layers() {
    let dir = tempfile::tempdir().expect("tempdir");
    let input = dir.path().join("input.mbtiles");
    let output = dir.path().join("output.mbtiles");
    let style = dir.path().join("style.json");
    create_layer_mbtiles(&input);

    fs::write(
        &style,
        r#"{"version":8,"layers":[{"id":"roads","type":"line","source-layer":"roads"}]}"#,
    )
    .expect("write style");
    let keep_layers = read_style_source_layers(&style).expect("read style");

    prune_mbtiles_layer_only(&input, &output, &keep_layers).expect("prune mbtiles");

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
