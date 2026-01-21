use crate::pmtiles::{
    types::{Entry, HEADER_SIZE},
    Header,
};
use anyhow::{Context, Result};
use hilbert_2d::{Variant, h2xy_discrete, xy2h_discrete};
use varint_rs::{VarintReader, VarintWriter};

pub fn histogram_bucket_index_pmtiles(
    value: u64,
    min_len: Option<u64>,
    max_len: Option<u64>,
    buckets: usize,
) -> Option<usize> {
    if buckets == 0 {
        return None;
    }
    let min_len = min_len?;
    let max_len = max_len?;
    if min_len > max_len {
        return None;
    }
    let range = (max_len - min_len).max(1);
    let bucket_size = ((range as f64) / buckets as f64).ceil() as u64;
    let mut bucket = ((value.saturating_sub(min_len)) / bucket_size) as usize;
    if bucket >= buckets {
        bucket = buckets - 1;
    }
    Some(bucket)
}

pub fn tile_id_from_xyz(z: u8, x: u32, y: u32) -> u64 {
    if z == 0 {
        return 0;
    }
    let order = z as usize;
    let hilbert = xy2h_discrete(x as usize, y as usize, order, Variant::Hilbert) as u64;
    let base_id = (pow4(z) - 1) / 3;
    base_id + hilbert
}

pub fn tile_id_to_xyz(tile_id: u64) -> (u8, u32, u32) {
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

pub fn pow4(z: u8) -> u64 {
    1u64 << (2 * (z as u64))
}

pub fn splitmix64(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9e3779b97f4a7c15);
    let mut z = x;
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb);
    z ^ (z >> 31)
}

pub fn encode_directory(entries: &[Entry]) -> Result<Vec<u8>> {
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

pub fn decode_directory(mut data: &[u8]) -> Result<Vec<Entry>> {
    let n_entries = data.read_usize_varint()?;
    let mut entries = vec![
        Entry {
            tile_id: 0,
            offset: 0,
            length: 0,
            run_length: 0,
        };
        n_entries
    ];

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

pub fn build_header(
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

#[allow(clippy::too_many_arguments)]
pub fn build_header_with_metadata(
    root_length: u64,
    metadata_length: u64,
    data_length: u64,
    tile_count: u64,
    min_zoom: u8,
    max_zoom: u8,
    internal_compression: u8,
    tile_compression: u8,
    tile_type: u8,
) -> Header {
    let root_offset = HEADER_SIZE as u64;
    let metadata_offset = if metadata_length == 0 {
        0
    } else {
        root_offset + root_length
    };
    let data_offset = if metadata_length == 0 {
        root_offset + root_length
    } else {
        metadata_offset + metadata_length
    };
    Header {
        root_offset,
        root_length,
        metadata_offset,
        metadata_length,
        leaf_offset: 0,
        leaf_length: 0,
        data_offset,
        data_length,
        n_addressed_tiles: tile_count,
        n_tile_entries: tile_count,
        n_tile_contents: tile_count,
        clustered: 0,
        internal_compression,
        tile_compression,
        tile_type,
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
