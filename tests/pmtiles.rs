use std::fs;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

use brotli::CompressorWriter;
use flate2::read::GzDecoder;
use mvt::{GeomEncoder, GeomType, Tile};
use mvt_reader::Reader;
use vt_optimizer::mbtiles::{InspectOptions, inspect_mbtiles};
use vt_optimizer::pmtiles::{
    inspect_pmtiles_with_options, mbtiles_to_pmtiles, pmtiles_to_mbtiles, prune_pmtiles_layer_only,
};
use vt_optimizer::style::read_style;

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

fn write_pmtiles_with_metadata(path: &Path, metadata_json: &str) {
    write_pmtiles_with_metadata_and_compression(path, metadata_json, 0);
}

fn write_pmtiles_with_metadata_and_compression(
    path: &Path,
    metadata_json: &str,
    internal_compression: u8,
) {
    const HEADER_SIZE: usize = 127;
    const MAGIC: &[u8; 7] = b"PMTiles";
    const VERSION: u8 = 3;

    let metadata_bytes = match internal_compression {
        0 => metadata_json.as_bytes().to_vec(),
        2 => {
            let mut compressed = Vec::new();
            {
                let mut writer = CompressorWriter::new(&mut compressed, 4096, 5, 22);
                writer
                    .write_all(metadata_json.as_bytes())
                    .expect("compress metadata");
            }
            compressed
        }
        other => panic!("unsupported compression for test: {}", other),
    };
    let metadata_offset = HEADER_SIZE as u64;
    let metadata_length = metadata_bytes.len() as u64;
    let root_offset = metadata_offset + metadata_length;
    let root_length = 0u64;

    let mut header = Vec::with_capacity(HEADER_SIZE);
    header.extend_from_slice(MAGIC);
    header.push(VERSION);

    for value in [
        root_offset,
        root_length,
        metadata_offset,
        metadata_length,
        0u64,        // leaf_offset
        0u64,        // leaf_length
        root_offset, // data_offset
        0u64,        // data_length
    ] {
        header.extend_from_slice(&value.to_le_bytes());
    }

    for value in [0u64, 0u64, 0u64] {
        header.extend_from_slice(&value.to_le_bytes());
    }

    header.push(0); // clustered
    header.push(internal_compression); // internal_compression
    header.push(0); // tile_compression
    header.push(0); // tile_type
    header.push(0); // min_zoom
    header.push(0); // max_zoom
    header.extend_from_slice(&0i32.to_le_bytes()); // min_longitude
    header.extend_from_slice(&0i32.to_le_bytes()); // min_latitude
    header.extend_from_slice(&0i32.to_le_bytes()); // max_longitude
    header.extend_from_slice(&0i32.to_le_bytes()); // max_latitude
    header.push(0); // center_zoom
    header.extend_from_slice(&0i32.to_le_bytes()); // center_longitude
    header.extend_from_slice(&0i32.to_le_bytes()); // center_latitude

    assert_eq!(header.len(), HEADER_SIZE);

    let mut file = File::create(path).expect("create pmtiles");
    file.write_all(&header).expect("write header");
    file.seek(SeekFrom::Start(metadata_offset))
        .expect("seek metadata");
    file.write_all(&metadata_bytes).expect("write metadata");
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
    assert_eq!(report.overall.avg_bytes, 15);
}

#[test]
fn inspect_pmtiles_reads_metadata() {
    let dir = tempfile::tempdir().expect("tempdir");
    let pmtiles = dir.path().join("metadata.pmtiles");
    write_pmtiles_with_metadata(
        &pmtiles,
        r#"{"name":"sample","minzoom":0,"maxzoom":2,"format":"pbf"}"#,
    );

    let report = inspect_pmtiles_with_options(&pmtiles, &InspectOptions::default())
        .expect("inspect pmtiles");
    assert_eq!(
        report.metadata.get("name").map(String::as_str),
        Some("sample")
    );
    assert_eq!(
        report.metadata.get("minzoom").map(String::as_str),
        Some("0")
    );
    assert_eq!(
        report.metadata.get("maxzoom").map(String::as_str),
        Some("2")
    );
    assert_eq!(
        report.metadata.get("format").map(String::as_str),
        Some("pbf")
    );
}

#[test]
fn inspect_pmtiles_reads_brotli_metadata() {
    let dir = tempfile::tempdir().expect("tempdir");
    let pmtiles = dir.path().join("metadata-brotli.pmtiles");
    write_pmtiles_with_metadata_and_compression(&pmtiles, r#"{"name":"sample","minzoom":1}"#, 2);

    let report = inspect_pmtiles_with_options(&pmtiles, &InspectOptions::default())
        .expect("inspect pmtiles");
    assert_eq!(
        report.metadata.get("name").map(String::as_str),
        Some("sample")
    );
    assert_eq!(
        report.metadata.get("minzoom").map(String::as_str),
        Some("1")
    );
}

#[test]
fn prune_pmtiles_removes_unlisted_layers() {
    let dir = tempfile::tempdir().expect("tempdir");
    let input_mbtiles = dir.path().join("input.mbtiles");
    let input_pmtiles = dir.path().join("input.pmtiles");
    let output_pmtiles = dir.path().join("output.pmtiles");
    let output_mbtiles = dir.path().join("output.mbtiles");
    let style_path = dir.path().join("style.json");

    create_layer_mbtiles(&input_mbtiles);
    mbtiles_to_pmtiles(&input_mbtiles, &input_pmtiles).expect("mbtiles->pmtiles");

    fs::write(
        &style_path,
        r#"{"version":8,"sources":{"osm":{"type":"vector"}},"layers":[{"id":"roads","type":"line","source":"osm","source-layer":"roads","paint":{"line-width":1}}]}"#,
    )
    .expect("write style");
    let style = read_style(&style_path).expect("read style");

    prune_pmtiles_layer_only(&input_pmtiles, &output_pmtiles, &style, false, true)
        .expect("prune pmtiles");

    pmtiles_to_mbtiles(&output_pmtiles, &output_mbtiles).expect("pmtiles->mbtiles");
    let conn = rusqlite::Connection::open(&output_mbtiles).expect("open output");
    let data: Vec<u8> = conn
        .query_row(
            "SELECT tile_data FROM tiles WHERE zoom_level = 0 AND tile_column = 0 AND tile_row = 0",
            [],
            |row| row.get(0),
        )
        .expect("read tile");
    let payload = if data.starts_with(&[0x1f, 0x8b]) {
        let mut decoder = GzDecoder::new(data.as_slice());
        let mut decoded = Vec::new();
        decoder.read_to_end(&mut decoded).expect("decode gzip");
        decoded
    } else {
        data
    };
    let reader = Reader::new(payload).expect("decode");
    let layers = reader.get_layer_metadata().expect("layers");
    assert_eq!(layers.len(), 1);
    assert_eq!(layers[0].name, "roads");
}

#[test]
fn prune_pmtiles_preserves_tile_compression() {
    let dir = tempfile::tempdir().expect("tempdir");
    let input_mbtiles = dir.path().join("input.mbtiles");
    let input_pmtiles = dir.path().join("input.pmtiles");
    let output_pmtiles = dir.path().join("output.pmtiles");
    let style_path = dir.path().join("style.json");

    create_layer_mbtiles(&input_mbtiles);
    mbtiles_to_pmtiles(&input_mbtiles, &input_pmtiles).expect("mbtiles->pmtiles");

    fs::write(
        &style_path,
        r#"{"version":8,"sources":{"osm":{"type":"vector"}},"layers":[{"id":"roads","type":"line","source":"osm","source-layer":"roads","paint":{"line-width":1}}]}"#,
    )
    .expect("write style");
    let style = read_style(&style_path).expect("read style");

    prune_pmtiles_layer_only(&input_pmtiles, &output_pmtiles, &style, false, true)
        .expect("prune pmtiles");

    let input_tile_compression =
        read_tile_compression(&input_pmtiles).expect("read input compression");
    let output_tile_compression =
        read_tile_compression(&output_pmtiles).expect("read output compression");
    assert_eq!(input_tile_compression, output_tile_compression);
}

fn read_tile_compression(path: &Path) -> std::io::Result<u8> {
    const HEADER_SIZE: usize = 127;
    const MAGIC: &[u8; 7] = b"PMTiles";
    let mut buf = [0u8; HEADER_SIZE];
    let mut file = File::open(path)?;
    file.read_exact(&mut buf)?;
    if &buf[0..MAGIC.len()] != MAGIC {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "invalid PMTiles magic",
        ));
    }
    let mut cursor = &buf[MAGIC.len()..];
    let _version = read_u8(&mut cursor)?;
    for _ in 0..8 {
        read_u64(&mut cursor)?;
    }
    for _ in 0..3 {
        read_u64(&mut cursor)?;
    }
    let _clustered = read_u8(&mut cursor)?;
    let _internal = read_u8(&mut cursor)?;
    read_u8(&mut cursor)
}

fn read_u8(cursor: &mut &[u8]) -> std::io::Result<u8> {
    if cursor.is_empty() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "unexpected eof",
        ));
    }
    let value = cursor[0];
    *cursor = &cursor[1..];
    Ok(value)
}

fn read_u64(cursor: &mut &[u8]) -> std::io::Result<u64> {
    if cursor.len() < 8 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "unexpected eof",
        ));
    }
    let mut buf = [0u8; 8];
    buf.copy_from_slice(&cursor[..8]);
    *cursor = &cursor[8..];
    Ok(u64::from_le_bytes(buf))
}

#[test]
fn inspect_pmtiles_counts_tiles_by_zoom() {
    let dir = tempfile::tempdir().expect("tempdir");
    let input = dir.path().join("input.mbtiles");
    let pmtiles = dir.path().join("output.pmtiles");
    create_sample_mbtiles(&input);

    mbtiles_to_pmtiles(&input, &pmtiles).expect("mbtiles->pmtiles");
    let report = inspect_pmtiles_with_options(&pmtiles, &InspectOptions::default())
        .expect("inspect pmtiles");

    assert_eq!(report.overall.tile_count, 2);
    assert_eq!(report.overall.total_bytes, 30);
    assert_eq!(report.overall.max_bytes, 20);
    assert_eq!(report.overall.avg_bytes, 15);
    assert_eq!(report.by_zoom.len(), 2);
    assert!(report.by_zoom.iter().any(|entry| {
        entry.zoom == 0
            && entry.stats.tile_count == 1
            && entry.stats.total_bytes == 10
            && entry.stats.max_bytes == 10
            && entry.stats.avg_bytes == 10
    }));
    assert!(report.by_zoom.iter().any(|entry| {
        entry.zoom == 1
            && entry.stats.tile_count == 1
            && entry.stats.total_bytes == 20
            && entry.stats.max_bytes == 20
            && entry.stats.avg_bytes == 20
    }));
}

#[test]
fn inspect_pmtiles_builds_histograms_by_zoom() {
    let dir = tempfile::tempdir().expect("tempdir");
    let input = dir.path().join("input.mbtiles");
    let pmtiles = dir.path().join("output.pmtiles");
    create_sample_mbtiles(&input);

    mbtiles_to_pmtiles(&input, &pmtiles).expect("mbtiles->pmtiles");
    let options = InspectOptions {
        histogram_buckets: 3,
        ..Default::default()
    };
    let report = inspect_pmtiles_with_options(&pmtiles, &options).expect("inspect pmtiles");

    assert_eq!(report.histograms_by_zoom.len(), 2);
    let z0 = report
        .histograms_by_zoom
        .iter()
        .find(|entry| entry.zoom == 0)
        .expect("z0 histogram");
    assert_eq!(z0.buckets.len(), 3);
    assert_eq!(z0.buckets[0].count, 1);
    assert_eq!(z0.buckets[0].total_bytes, 10);

    let z1 = report
        .histograms_by_zoom
        .iter()
        .find(|entry| entry.zoom == 1)
        .expect("z1 histogram");
    assert_eq!(z1.buckets.len(), 3);
    assert_eq!(z1.buckets[0].count, 1);
    assert_eq!(z1.buckets[0].total_bytes, 20);
}

#[test]
fn inspect_pmtiles_collects_top_tiles() {
    let dir = tempfile::tempdir().expect("tempdir");
    let input = dir.path().join("input.mbtiles");
    let pmtiles = dir.path().join("output.pmtiles");
    create_sample_mbtiles(&input);

    mbtiles_to_pmtiles(&input, &pmtiles).expect("mbtiles->pmtiles");
    let options = InspectOptions {
        topn: 1,
        ..Default::default()
    };
    let report = inspect_pmtiles_with_options(&pmtiles, &options).expect("inspect pmtiles");

    assert_eq!(report.top_tiles.len(), 1);
    let tile = &report.top_tiles[0];
    assert_eq!(tile.zoom, 1);
    assert_eq!(tile.x, 1);
    assert_eq!(tile.y, 1);
    assert_eq!(tile.bytes, 20);
}

#[test]
fn inspect_pmtiles_collects_layer_list() {
    let dir = tempfile::tempdir().expect("tempdir");
    let input = dir.path().join("input.mbtiles");
    let pmtiles = dir.path().join("output.pmtiles");
    create_layer_mbtiles(&input);

    mbtiles_to_pmtiles(&input, &pmtiles).expect("mbtiles->pmtiles");
    let options = InspectOptions {
        histogram_buckets: 0,
        include_layer_list: true,
        ..Default::default()
    };
    let report = inspect_pmtiles_with_options(&pmtiles, &options).expect("inspect pmtiles");

    let mut layers = report.file_layers.clone();
    layers.sort_by(|a, b| a.name.cmp(&b.name));
    assert_eq!(layers.len(), 2);
    assert_eq!(layers[0].name, "buildings");
    assert_eq!(layers[0].vertex_count, 1);
    assert_eq!(layers[0].feature_count, 1);
    assert_eq!(layers[0].property_key_count, 1);
    assert_eq!(layers[0].property_value_count, 1);
    assert_eq!(layers[1].name, "roads");
    assert_eq!(layers[1].vertex_count, 1);
    assert_eq!(layers[1].feature_count, 1);
    assert_eq!(layers[1].property_key_count, 2);
    assert_eq!(layers[1].property_value_count, 2);
}
