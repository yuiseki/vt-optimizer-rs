use std::collections::BTreeMap;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

use anyhow::{Context, Result};
use brotli::Decompressor;
use flate2::read::GzDecoder;
use hilbert_2d::{h2xy_discrete, xy2h_discrete, Variant};
use rusqlite::Connection;
use serde_json::Value;
use varint_rs::{VarintReader, VarintWriter};

use crate::mbtiles::{MbtilesReport, MbtilesStats};

const HEADER_SIZE: usize = 127;
const MAGIC: &[u8; 7] = b"PMTiles";
const VERSION: u8 = 3;

#[derive(Debug, Clone)]
struct Header {
    root_offset: u64,
    root_length: u64,
    metadata_offset: u64,
    metadata_length: u64,
    leaf_offset: u64,
    leaf_length: u64,
    data_offset: u64,
    data_length: u64,
    n_addressed_tiles: u64,
    n_tile_entries: u64,
    n_tile_contents: u64,
    clustered: u8,
    internal_compression: u8,
    tile_compression: u8,
    tile_type: u8,
    min_zoom: u8,
    max_zoom: u8,
    min_longitude: i32,
    min_latitude: i32,
    max_longitude: i32,
    max_latitude: i32,
    center_zoom: u8,
    center_longitude: i32,
    center_latitude: i32,
}

#[derive(Debug, Clone)]
struct Entry {
    tile_id: u64,
    offset: u64,
    length: u32,
    run_length: u32,
}

fn ensure_pmtiles_path(path: &Path) -> Result<()> {
    let ext = path.extension().and_then(|ext| ext.to_str()).unwrap_or("");
    if ext.eq_ignore_ascii_case("pmtiles") {
        Ok(())
    } else {
        anyhow::bail!("only .pmtiles paths are supported in v0.0.3");
    }
}

fn ensure_mbtiles_path(path: &Path) -> Result<()> {
    let ext = path.extension().and_then(|ext| ext.to_str()).unwrap_or("");
    if ext.eq_ignore_ascii_case("mbtiles") {
        Ok(())
    } else {
        anyhow::bail!("only .mbtiles paths are supported in v0.0.3");
    }
}

fn tile_id_from_xyz(z: u8, x: u32, y: u32) -> u64 {
    if z == 0 {
        return 0;
    }
    let order = z as usize;
    let hilbert = xy2h_discrete(x as usize, y as usize, order, Variant::Hilbert) as u64;
    let base_id = (pow4(z) - 1) / 3;
    base_id + hilbert
}

fn tile_id_to_xyz(tile_id: u64) -> (u8, u32, u32) {
    if tile_id == 0 {
        return (0, 0, 0);
    }
    let mut z = 1u8;
    loop {
        let base_id = (pow4(z) - 1) / 3;
        let next_base = (pow4(z + 1) - 1) / 3;
        if tile_id < next_base {
            let idx = tile_id - base_id;
            let (x, y) = h2xy_discrete(idx as usize, z as usize, Variant::Hilbert);
            return (z, x as u32, y as u32);
        }
        z += 1;
    }
}

fn pow4(z: u8) -> u64 {
    1u64 << (2 * (z as u64))
}

fn encode_directory(entries: &[Entry]) -> Result<Vec<u8>> {
    let mut buf = Vec::new();
    buf.write_usize_varint(entries.len())?;

    let mut last_tile_id = 0u64;
    for entry in entries {
        let delta = entry.tile_id - last_tile_id;
        buf.write_u64_varint(delta)?;
        last_tile_id = entry.tile_id;
    }

    for entry in entries {
        buf.write_u32_varint(entry.run_length)?;
    }

    for entry in entries {
        buf.write_u32_varint(entry.length)?;
    }

    for (idx, entry) in entries.iter().enumerate() {
        if idx == 0 {
            buf.write_u64_varint(entry.offset + 1)?;
        } else {
            let prev = &entries[idx - 1];
            let expected = prev.offset + prev.length as u64;
            if entry.offset == expected {
                buf.write_u64_varint(0)?;
            } else {
                buf.write_u64_varint(entry.offset + 1)?;
            }
        }
    }

    Ok(buf)
}

fn decode_directory(mut data: &[u8]) -> Result<Vec<Entry>> {
    let n_entries = data.read_usize_varint()?;
    let mut entries = vec![Entry {
        tile_id: 0,
        offset: 0,
        length: 0,
        run_length: 0,
    }; n_entries];

    let mut next_tile_id = 0u64;
    for entry in entries.iter_mut() {
        next_tile_id += data.read_u64_varint()?;
        entry.tile_id = next_tile_id;
    }

    for entry in entries.iter_mut() {
        entry.run_length = data.read_u32_varint()?;
    }

    for entry in entries.iter_mut() {
        entry.length = data.read_u32_varint()?;
    }

    let mut last_entry: Option<Entry> = None;
    for entry in entries.iter_mut() {
        let offset = data.read_u64_varint()?;
        entry.offset = if offset == 0 {
            let prev = last_entry.as_ref().context("invalid directory entry")?;
            prev.offset + prev.length as u64
        } else {
            offset - 1
        };
        last_entry = Some(entry.clone());
    }

    Ok(entries)
}

fn build_header(
    root_length: u64,
    data_length: u64,
    tile_count: u64,
    min_zoom: u8,
    max_zoom: u8,
) -> Header {
    Header {
        root_offset: HEADER_SIZE as u64,
        root_length,
        metadata_offset: 0,
        metadata_length: 0,
        leaf_offset: 0,
        leaf_length: 0,
        data_offset: HEADER_SIZE as u64 + root_length,
        data_length,
        n_addressed_tiles: tile_count,
        n_tile_entries: tile_count,
        n_tile_contents: tile_count,
        clustered: 0,
        internal_compression: 1,
        tile_compression: 1,
        tile_type: 0,
        min_zoom,
        max_zoom,
        min_longitude: (-180.0 * 10_000_000.0) as i32,
        min_latitude: (-85.0 * 10_000_000.0) as i32,
        max_longitude: (180.0 * 10_000_000.0) as i32,
        max_latitude: (85.0 * 10_000_000.0) as i32,
        center_zoom: 0,
        center_longitude: 0,
        center_latitude: 0,
    }
}

fn write_header(mut file: &File, header: &Header) -> Result<()> {
    let mut buf = Vec::with_capacity(HEADER_SIZE);
    buf.extend_from_slice(MAGIC);
    buf.push(VERSION);

    for value in [
        header.root_offset,
        header.root_length,
        header.metadata_offset,
        header.metadata_length,
        header.leaf_offset,
        header.leaf_length,
        header.data_offset,
        header.data_length,
    ] {
        buf.extend_from_slice(&value.to_le_bytes());
    }

    for value in [
        header.n_addressed_tiles,
        header.n_tile_entries,
        header.n_tile_contents,
    ] {
        buf.extend_from_slice(&value.to_le_bytes());
    }

    buf.push(header.clustered);
    buf.push(header.internal_compression);
    buf.push(header.tile_compression);
    buf.push(header.tile_type);
    buf.push(header.min_zoom);
    buf.push(header.max_zoom);
    buf.extend_from_slice(&header.min_longitude.to_le_bytes());
    buf.extend_from_slice(&header.min_latitude.to_le_bytes());
    buf.extend_from_slice(&header.max_longitude.to_le_bytes());
    buf.extend_from_slice(&header.max_latitude.to_le_bytes());
    buf.push(header.center_zoom);
    buf.extend_from_slice(&header.center_longitude.to_le_bytes());
    buf.extend_from_slice(&header.center_latitude.to_le_bytes());

    if buf.len() != HEADER_SIZE {
        anyhow::bail!("invalid header size: {}", buf.len());
    }

    file.seek(SeekFrom::Start(0))
        .context("seek header")?;
    file.write_all(&buf).context("write header")?;
    Ok(())
}

fn read_header(mut file: &File) -> Result<Header> {
    let mut buf = vec![0u8; HEADER_SIZE];
    file.seek(SeekFrom::Start(0))
        .context("seek header")?;
    file.read_exact(&mut buf).context("read header")?;

    if &buf[0..MAGIC.len()] != MAGIC {
        anyhow::bail!("invalid PMTiles magic");
    }

    let mut cursor = &buf[MAGIC.len()..];
    let _version = read_u8(&mut cursor)?;
    let read_u64 = |c: &mut &[u8]| -> Result<u64> {
        let mut bytes = [0u8; 8];
        c.read_exact(&mut bytes)?;
        Ok(u64::from_le_bytes(bytes))
    };
    let read_i32 = |c: &mut &[u8]| -> Result<i32> {
        let mut bytes = [0u8; 4];
        c.read_exact(&mut bytes)?;
        Ok(i32::from_le_bytes(bytes))
    };

    let root_offset = read_u64(&mut cursor)?;
    let root_length = read_u64(&mut cursor)?;
    let metadata_offset = read_u64(&mut cursor)?;
    let metadata_length = read_u64(&mut cursor)?;
    let leaf_offset = read_u64(&mut cursor)?;
    let leaf_length = read_u64(&mut cursor)?;
    let data_offset = read_u64(&mut cursor)?;
    let data_length = read_u64(&mut cursor)?;
    let n_addressed_tiles = read_u64(&mut cursor)?;
    let n_tile_entries = read_u64(&mut cursor)?;
    let n_tile_contents = read_u64(&mut cursor)?;

    let mut rest = cursor;
    let clustered = read_u8(&mut rest)?;
    let internal_compression = read_u8(&mut rest)?;
    let tile_compression = read_u8(&mut rest)?;
    let tile_type = read_u8(&mut rest)?;
    let min_zoom = read_u8(&mut rest)?;
    let max_zoom = read_u8(&mut rest)?;
    let min_longitude = read_i32(&mut rest)?;
    let min_latitude = read_i32(&mut rest)?;
    let max_longitude = read_i32(&mut rest)?;
    let max_latitude = read_i32(&mut rest)?;
    let center_zoom = read_u8(&mut rest)?;
    let center_longitude = read_i32(&mut rest)?;
    let center_latitude = read_i32(&mut rest)?;

    Ok(Header {
        root_offset,
        root_length,
        metadata_offset,
        metadata_length,
        leaf_offset,
        leaf_length,
        data_offset,
        data_length,
        n_addressed_tiles,
        n_tile_entries,
        n_tile_contents,
        clustered,
        internal_compression,
        tile_compression,
        tile_type,
        min_zoom,
        max_zoom,
        min_longitude,
        min_latitude,
        max_longitude,
        max_latitude,
        center_zoom,
        center_longitude,
        center_latitude,
    })
}

fn read_u8(input: &mut &[u8]) -> Result<u8> {
    if input.is_empty() {
        anyhow::bail!("unexpected EOF");
    }
    let value = input[0];
    *input = &input[1..];
    Ok(value)
}

fn read_metadata_section(mut file: &File, header: &Header) -> Result<BTreeMap<String, String>> {
    if header.metadata_length == 0 {
        return Ok(BTreeMap::new());
    }
    file.seek(SeekFrom::Start(header.metadata_offset))
        .context("seek metadata")?;
    let mut data = vec![0u8; header.metadata_length as usize];
    file.read_exact(&mut data).context("read metadata")?;

    let decoded = decode_metadata_bytes(data, header.internal_compression)?;

    let value: Value = serde_json::from_slice(&decoded).context("parse metadata json")?;
    let mut metadata = BTreeMap::new();
    if let Value::Object(map) = value {
        for (key, value) in map.into_iter() {
            let text = match value {
                Value::String(text) => text,
                other => other.to_string(),
            };
            metadata.insert(key, text);
        }
    }
    Ok(metadata)
}

fn decode_metadata_bytes(data: Vec<u8>, internal_compression: u8) -> Result<Vec<u8>> {
    if data.starts_with(&[0x1f, 0x8b]) {
        let mut decoder = GzDecoder::new(data.as_slice());
        let mut decoded = Vec::new();
        decoder
            .read_to_end(&mut decoded)
            .context("decode gzip metadata")?;
        return Ok(decoded);
    }

    match internal_compression {
        0 => Ok(data),
        1 => {
            let mut decoder = GzDecoder::new(data.as_slice());
            let mut decoded = Vec::new();
            decoder
                .read_to_end(&mut decoded)
                .context("decode gzip metadata")?;
            Ok(decoded)
        }
        2 => {
            let mut decoder = Decompressor::new(data.as_slice(), 4096);
            let mut decoded = Vec::new();
            decoder
                .read_to_end(&mut decoded)
                .context("decode brotli metadata")?;
            Ok(decoded)
        }
        other => anyhow::bail!("unsupported PMTiles metadata compression: {other}"),
    }
}

pub fn inspect_pmtiles_metadata(path: &Path) -> Result<MbtilesReport> {
    ensure_pmtiles_path(path)?;
    let file = File::open(path)
        .with_context(|| format!("failed to open input pmtiles: {}", path.display()))?;
    let header = read_header(&file).context("read header")?;
    let metadata = read_metadata_section(&file, &header)?;

    Ok(MbtilesReport {
        metadata,
        overall: MbtilesStats {
            tile_count: 0,
            total_bytes: 0,
            max_bytes: 0,
            avg_bytes: 0,
        },
        by_zoom: Vec::new(),
        empty_tiles: 0,
        empty_ratio: 0.0,
        sampled: false,
        sample_total_tiles: 0,
        sample_used_tiles: 0,
        histogram: Vec::new(),
        histograms_by_zoom: Vec::new(),
        file_layers: Vec::new(),
        top_tiles: Vec::new(),
        bucket_count: None,
        bucket_tiles: Vec::new(),
        tile_summary: None,
        recommended_buckets: Vec::new(),
        top_tile_summaries: Vec::new(),
    })
}

pub fn mbtiles_to_pmtiles(input: &Path, output: &Path) -> Result<()> {
    ensure_mbtiles_path(input)?;
    ensure_pmtiles_path(output)?;

    let conn = Connection::open(input)
        .with_context(|| format!("failed to open input mbtiles: {}", input.display()))?;

    let mut stmt = conn
        .prepare(
            "SELECT zoom_level, tile_column, tile_row, tile_data FROM tiles ORDER BY zoom_level, tile_column, tile_row",
        )
        .context("prepare tiles")?;
    let mut rows = stmt.query([]).context("query tiles")?;

    let mut tiles = Vec::new();
    let mut min_zoom = u8::MAX;
    let mut max_zoom = u8::MIN;
    while let Some(row) = rows.next().context("read tile row")? {
        let z: u8 = row.get(0)?;
        let x: u32 = row.get(1)?;
        let y: u32 = row.get(2)?;
        let data: Vec<u8> = row.get(3)?;
        min_zoom = min_zoom.min(z);
        max_zoom = max_zoom.max(z);
        let tile_id = tile_id_from_xyz(z, x, y);
        tiles.push((tile_id, data));
    }

    tiles.sort_by(|a, b| a.0.cmp(&b.0));

    let mut entries = Vec::with_capacity(tiles.len());
    let mut data_section = Vec::new();
    for (tile_id, data) in tiles.iter() {
        let offset = data_section.len() as u64;
        let length = data.len() as u32;
        data_section.extend_from_slice(data);
        entries.push(Entry {
            tile_id: *tile_id,
            offset,
            length,
            run_length: 1,
        });
    }

    let dir_bytes = encode_directory(&entries)?;
    let header = build_header(
        dir_bytes.len() as u64,
        data_section.len() as u64,
        entries.len() as u64,
        if min_zoom == u8::MAX { 0 } else { min_zoom },
        if max_zoom == u8::MIN { 0 } else { max_zoom },
    );

    let file = File::create(output)
        .with_context(|| format!("failed to create output pmtiles: {}", output.display()))?;
    write_header(&file, &header)?;

    let mut file = file;
    file.seek(SeekFrom::Start(header.root_offset))
        .context("seek root directory")?;
    file.write_all(&dir_bytes).context("write root directory")?;

    file.seek(SeekFrom::Start(header.data_offset))
        .context("seek data")?;
    file.write_all(&data_section).context("write data")?;

    Ok(())
}

pub fn pmtiles_to_mbtiles(input: &Path, output: &Path) -> Result<()> {
    ensure_pmtiles_path(input)?;
    ensure_mbtiles_path(output)?;

    let file = File::open(input)
        .with_context(|| format!("failed to open input pmtiles: {}", input.display()))?;
    let header = read_header(&file).context("read header")?;

    let mut file = file;
    file.seek(SeekFrom::Start(header.root_offset))
        .context("seek root directory")?;
    let mut dir_buf = vec![0u8; header.root_length as usize];
    file.read_exact(&mut dir_buf).context("read root directory")?;

    let entries = decode_directory(&dir_buf)?;

    let mut output_conn = Connection::open(output)
        .with_context(|| format!("failed to open output mbtiles: {}", output.display()))?;
    output_conn
        .execute_batch(
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
        .context("create output schema")?;

    let tx = output_conn.transaction().context("begin output transaction")?;

    for entry in entries {
        let mut data = vec![0u8; entry.length as usize];
        let data_offset = header.data_offset + entry.offset;
        file.seek(SeekFrom::Start(data_offset))
            .context("seek tile")?;
        file.read_exact(&mut data).context("read tile data")?;

        for i in 0..entry.run_length.max(1) {
            let tile_id = entry.tile_id + i as u64;
            let (z, x, y) = tile_id_to_xyz(tile_id);
            tx.execute(
                "INSERT INTO tiles (zoom_level, tile_column, tile_row, tile_data) VALUES (?1, ?2, ?3, ?4)",
                (z as i64, x as i64, y as i64, data.clone()),
            )
            .context("insert tile")?;
        }
    }

    tx.commit().context("commit output")?;
    Ok(())
}
