
use crate::mbtiles::{
    HistogramBucket, InspectOptions, MbtilesReport, MbtilesZoomStats, TileListOptions,
    TileSort, TopTile, ZoomHistogram, count_vertices, encode_tile_payload, format_property_value,
    prune_tile_layers, simplify_tile_payload, PruneStats,
};
use crate::pmtiles::{
    algo::{
        decode_directory, encode_directory, histogram_bucket_index_pmtiles, splitmix64,
        tile_id_from_xyz, tile_id_to_xyz, build_header,
    },
    types::{Entry, Header, ProgressTracker, HEADER_SIZE, MAGIC, VERSION},
    LayerAccum, StatAccum, build_header_with_metadata, progress_for_phase,
};
use anyhow::{Context, Result};
use brotli::{CompressorWriter, Decompressor};
use flate2::Compression;
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use mvt_reader::Reader;
use rusqlite::Connection;
use serde_json::Value;
use std::cmp::Reverse;
use std::collections::{BTreeMap, BinaryHeap, HashSet};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

pub fn include_sample(index: u64, total: u64, sample: Option<&crate::mbtiles::SampleSpec>) -> bool {
    match sample {
        None => true,
        Some(crate::mbtiles::SampleSpec::Count(count)) => index <= *count,
        Some(crate::mbtiles::SampleSpec::Ratio(ratio)) => {
            if *ratio >= 1.0 {
                return true;
            }
            if *ratio <= 0.0 {
                return false;
            }
            let threshold = (ratio * u64::MAX as f64) as u64;
            let hash = splitmix64(index ^ total);
            hash <= threshold
        }
    }
}


pub fn read_u8(input: &mut &[u8]) -> Result<u8> {
    if input.is_empty() {
        anyhow::bail!("unexpected EOF");
    }
    let value = input[0];
    *input = &input[1..];
    Ok(value)
}

pub fn read_header(mut x: &File) -> Result<Header> {
    let mut buf = [0u8; HEADER_SIZE];
    x.read_exact(&mut buf).context("read header")?;
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

pub fn write_header(mut file: &File, header: &Header) -> Result<()> {
    let mut buf = Vec::with_capacity(HEADER_SIZE);
    buf.write_all(MAGIC)?;
    buf.write_all(&[VERSION])?;

    let write_u64 = |v: u64, b: &mut Vec<u8>| -> Result<()> {
        b.write_all(&v.to_le_bytes())?;
        Ok(())
    };
    let write_i32 = |v: i32, b: &mut Vec<u8>| -> Result<()> {
        b.write_all(&v.to_le_bytes())?;
        Ok(())
    };

    write_u64(header.root_offset, &mut buf)?;
    write_u64(header.root_length, &mut buf)?;
    write_u64(header.metadata_offset, &mut buf)?;
    write_u64(header.metadata_length, &mut buf)?;
    write_u64(header.leaf_offset, &mut buf)?;
    write_u64(header.leaf_length, &mut buf)?;
    write_u64(header.data_offset, &mut buf)?;
    write_u64(header.data_length, &mut buf)?;
    write_u64(header.n_addressed_tiles, &mut buf)?;
    write_u64(header.n_tile_entries, &mut buf)?;
    write_u64(header.n_tile_contents, &mut buf)?;

    buf.push(header.clustered);
    buf.push(header.internal_compression);
    buf.push(header.tile_compression);
    buf.push(header.tile_type);
    buf.push(header.min_zoom);
    buf.push(header.max_zoom);
    write_i32(header.min_longitude, &mut buf)?;
    write_i32(header.min_latitude, &mut buf)?;
    write_i32(header.max_longitude, &mut buf)?;
    write_i32(header.max_latitude, &mut buf)?;
    buf.push(header.center_zoom);
    write_i32(header.center_longitude, &mut buf)?;
    write_i32(header.center_latitude, &mut buf)?;

    // Pad to HEADER_SIZE
    while buf.len() < HEADER_SIZE {
        buf.push(0);
    }

    file.seek(SeekFrom::Start(0))?;
    file.write_all(&buf)?;
    Ok(())
}

pub fn read_metadata_section(mut file: &File, header: &Header) -> Result<BTreeMap<String, String>> {
    if header.metadata_length == 0 {
        return Ok(BTreeMap::new());
    }
    file.seek(SeekFrom::Start(header.metadata_offset))
        .context("seek metadata")?;
    let mut data = vec![0u8; header.metadata_length as usize];
    file.read_exact(&mut data).context("read metadata")?;

    let decoded = decode_internal_bytes(data, header.internal_compression)?;

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

pub fn decode_internal_bytes(data: Vec<u8>, internal_compression: u8) -> Result<Vec<u8>> {
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
            if !data.starts_with(&[0x1f, 0x8b]) {
                return Ok(data);
            }
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

pub fn encode_internal_bytes(data: &[u8], internal_compression: u8) -> Result<Vec<u8>> {
    match internal_compression {
        0 => Ok(data.to_vec()),
        1 => {
            let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
            encoder
                .write_all(data)
                .context("encode gzip internal data")?;
            encoder.finish().context("finish gzip internal data")
        }
        2 => {
            let mut compressed = Vec::new();
            {
                let mut writer = CompressorWriter::new(&mut compressed, 4096, 5, 22);
                writer
                    .write_all(data)
                    .context("encode brotli internal data")?;
            }
            Ok(compressed)
        }
        other => anyhow::bail!("unsupported PMTiles internal compression: {other}"),
    }
}

pub fn decode_tile_payload_pmtiles(data: &[u8], tile_compression: u8) -> Result<Vec<u8>> {
    if data.starts_with(&[0x1f, 0x8b]) {
        let mut decoder = GzDecoder::new(data);
        let mut decoded = Vec::new();
        decoder
            .read_to_end(&mut decoded)
            .context("decode gzip tile data")?;
        return Ok(decoded);
    }
    match tile_compression {
        0 => Ok(data.to_vec()),
        1 => Ok(data.to_vec()),
        2 => {
            let mut decoder = Decompressor::new(data, 4096);
            let mut decoded = Vec::new();
            decoder
                .read_to_end(&mut decoded)
                .context("decode brotli tile data")?;
            Ok(decoded)
        }
        other => anyhow::bail!("unsupported PMTiles tile compression: {other}"),
    }
}

pub fn encode_tile_payload_pmtiles(data: &[u8], tile_compression: u8) -> Result<Vec<u8>> {
    match tile_compression {
        0 => Ok(data.to_vec()),
        1 => encode_tile_payload(data, true),
        2 => {
            let mut compressed = Vec::new();
            {
                let mut writer = CompressorWriter::new(&mut compressed, 4096, 5, 22);
                writer.write_all(data).context("encode brotli tile data")?;
            }
            Ok(compressed)
        }
        other => anyhow::bail!("unsupported PMTiles tile compression: {other}"),
    }
}

pub fn read_directory_section(
    mut file: &File,
    header: &Header,
    offset: u64,
    length: u64,
) -> Result<Vec<Entry>> {
    if length == 0 {
        return Ok(Vec::new());
    }
    file.seek(SeekFrom::Start(offset))
        .context("seek directory")?;
    let mut data = vec![0u8; length as usize];
    file.read_exact(&mut data).context("read directory")?;
    let decoded = decode_internal_bytes(data, header.internal_compression)?;
    decode_directory(&decoded)
}

#[allow(clippy::too_many_arguments)]
pub fn accumulate_tile_counts(
    file: &File,
    header: &Header,
    entries: &[Entry],
    zoom_filter: Option<u8>,
    overall: &mut StatAccum,
    by_zoom: &mut BTreeMap<u8, StatAccum>,
    empty_tiles: &mut u64,
    over_limit_tiles: &mut u64,
    min_len: &mut Option<u64>,
    max_len: &mut Option<u64>,
    zoom_minmax: &mut BTreeMap<u8, (u64, u64)>,
    max_tile_bytes: u64,
    mut progress: Option<&mut ProgressTracker>,
) -> Result<()> {
    for entry in entries {
        if entry.run_length == 0 {
            if entry.length == 0 {
                continue;
            }
            let leaf_offset = header.leaf_offset + entry.offset;
            let leaf_entries =
                read_directory_section(file, header, leaf_offset, entry.length as u64)?;
            accumulate_tile_counts(
                file,
                header,
                &leaf_entries,
                zoom_filter,
                overall,
                by_zoom,
                empty_tiles,
                over_limit_tiles,
                min_len,
                max_len,
                zoom_minmax,
                max_tile_bytes,
                progress.as_deref_mut(),
            )?;
            continue;
        }
        let run = entry.run_length.max(1);
        let length = entry.length as u64;
        for idx in 0..run {
            let tile_id = entry.tile_id + idx as u64;
            let (z, _x, _y) = tile_id_to_xyz(tile_id);
            if let Some(target_zoom) = zoom_filter
                && z != target_zoom
            {
                continue;
            }
            overall.add_tile(length);
            by_zoom
                .entry(z)
                .or_insert_with(|| StatAccum {
                    tile_count: 0,
                    total_bytes: 0,
                    max_bytes: 0,
                })
                .add_tile(length);
            if max_tile_bytes > 0 && length > max_tile_bytes {
                *over_limit_tiles += 1;
            }
            if length <= crate::mbtiles::EMPTY_TILE_MAX_BYTES {
                *empty_tiles += 1;
            }
            *min_len = Some(min_len.map_or(length, |min| min.min(length)));
            *max_len = Some(max_len.map_or(length, |max| max.max(length)));
            zoom_minmax
                .entry(z)
                .and_modify(|(min, max)| {
                    *min = (*min).min(length);
                    *max = (*max).max(length);
                })
                .or_insert((length, length));
            if let Some(progress) = progress.as_deref_mut() {
                progress.inc(1);
            }
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub fn build_histogram_from_entries(
    file: &File,
    header: &Header,
    entries: &[Entry],
    zoom_filter: Option<u8>,
    total_tiles_used: u64,
    total_bytes_used: u64,
    buckets: usize,
    min_len: u64,
    max_len: u64,
    max_tile_bytes: u64,
    mut progress: Option<&mut ProgressTracker>,
) -> Result<Vec<HistogramBucket>> {
    if buckets == 0 || min_len > max_len {
        return Ok(Vec::new());
    }
    let range = (max_len - min_len).max(1);
    let bucket_size = ((range as f64) / buckets as f64).ceil() as u64;
    let mut counts = vec![0u64; buckets];
    let mut bytes = vec![0u64; buckets];

    let mut stack = vec![entries.to_vec()];
    while let Some(entries) = stack.pop() {
        for entry in entries.iter() {
            if entry.run_length == 0 {
                if entry.length == 0 {
                    continue;
                }
                let leaf_offset = header.leaf_offset + entry.offset;
                let leaf_entries =
                    read_directory_section(file, header, leaf_offset, entry.length as u64)?;
                stack.push(leaf_entries);
                continue;
            }
            let length = entry.length as u64;
            let run = entry.run_length.max(1);
            for idx in 0..run {
                let tile_id = entry.tile_id + idx as u64;
                let (z, _x, _y) = tile_id_to_xyz(tile_id);
                if let Some(target_zoom) = zoom_filter
                    && z != target_zoom
                {
                    continue;
                }
                let mut bucket = ((length.saturating_sub(min_len)) / bucket_size) as usize;
                if bucket >= buckets {
                    bucket = buckets - 1;
                }
                counts[bucket] += 1;
                bytes[bucket] += length;
                if let Some(progress) = progress.as_deref_mut() {
                    progress.inc(1);
                }
            }
        }
    }

    let mut result = Vec::with_capacity(buckets);
    let mut accum_count = 0u64;
    let mut accum_bytes = 0u64;
    let limit_threshold = (max_tile_bytes as f64) * 0.9;

    for i in 0..buckets {
        let b_min = min_len + bucket_size * i as u64;
        let b_max = if i + 1 == buckets {
            max_len
        } else {
            (min_len + bucket_size * (i as u64 + 1)).saturating_sub(1)
        };
        accum_count += counts[i];
        accum_bytes += bytes[i];
        let running_avg = if accum_count == 0 {
            0
        } else {
            accum_bytes / accum_count
        };
        let pct_tiles = if total_tiles_used == 0 {
            0.0
        } else {
            counts[i] as f64 / total_tiles_used as f64
        };
        let pct_level_bytes = if total_bytes_used == 0 {
            0.0
        } else {
            bytes[i] as f64 / total_bytes_used as f64
        };
        let accum_pct_tiles = if total_tiles_used == 0 {
            0.0
        } else {
            accum_count as f64 / total_tiles_used as f64
        };
        let accum_pct_level_bytes = if total_bytes_used == 0 {
            0.0
        } else {
            accum_bytes as f64 / total_bytes_used as f64
        };
        let avg_over_limit = max_tile_bytes > 0 && (running_avg as f64) > max_tile_bytes as f64;
        let avg_near_limit =
            max_tile_bytes > 0 && !avg_over_limit && (running_avg as f64) >= limit_threshold;
        result.push(HistogramBucket {
            min_bytes: b_min,
            max_bytes: b_max,
            count: counts[i],
            total_bytes: bytes[i],
            running_avg_bytes: running_avg,
            pct_tiles,
            pct_level_bytes,
            accum_pct_tiles,
            accum_pct_level_bytes,
            avg_near_limit,
            avg_over_limit,
        });
    }
    Ok(result)
}

#[allow(clippy::too_many_arguments)]
pub fn build_zoom_histograms_from_entries(
    file: &File,
    header: &Header,
    entries: &[Entry],
    zoom_filter: Option<u8>,
    zoom_minmax: &BTreeMap<u8, (u64, u64)>,
    buckets: usize,
    max_tile_bytes: u64,
    mut progress: Option<&mut ProgressTracker>,
) -> Result<Vec<ZoomHistogram>> {
    if buckets == 0 || zoom_minmax.is_empty() {
        return Ok(Vec::new());
    }

    struct ZoomAccum {
        min_len: u64,
        max_len: u64,
        bucket_size: u64,
        counts: Vec<u64>,
        bytes: Vec<u64>,
        used_tiles: u64,
        used_bytes: u64,
    }

    let mut accums: BTreeMap<u8, ZoomAccum> = BTreeMap::new();
    for (zoom, (min_len, max_len)) in zoom_minmax.iter() {
        if let Some(target_zoom) = zoom_filter
            && *zoom != target_zoom
        {
            continue;
        }
        let range = (max_len - min_len).max(1);
        let bucket_size = ((range as f64) / buckets as f64).ceil() as u64;
        accums.insert(
            *zoom,
            ZoomAccum {
                min_len: *min_len,
                max_len: *max_len,
                bucket_size,
                counts: vec![0u64; buckets],
                bytes: vec![0u64; buckets],
                used_tiles: 0,
                used_bytes: 0,
            },
        );
    }

    let mut stack = vec![entries.to_vec()];
    while let Some(entries) = stack.pop() {
        for entry in entries.iter() {
            if entry.run_length == 0 {
                if entry.length == 0 {
                    continue;
                }
                let leaf_offset = header.leaf_offset + entry.offset;
                let leaf_entries =
                    read_directory_section(file, header, leaf_offset, entry.length as u64)?;
                stack.push(leaf_entries);
                continue;
            }
            let length = entry.length as u64;
            let run = entry.run_length.max(1);
            for idx in 0..run {
                let tile_id = entry.tile_id + idx as u64;
                let (z, _x, _y) = tile_id_to_xyz(tile_id);
                if let Some(target_zoom) = zoom_filter
                    && z != target_zoom
                {
                    continue;
                }
                let Some(accum) = accums.get_mut(&z) else {
                    continue;
                };
                let mut bucket =
                    ((length.saturating_sub(accum.min_len)) / accum.bucket_size) as usize;
                if bucket >= buckets {
                    bucket = buckets - 1;
                }
                accum.counts[bucket] += 1;
                accum.bytes[bucket] += length;
                accum.used_tiles += 1;
                accum.used_bytes += length;
                if let Some(progress) = progress.as_deref_mut() {
                    progress.inc(1);
                }
            }
        }
    }

    let mut result = Vec::new();
    let limit_threshold = (max_tile_bytes as f64) * 0.9;
    for (zoom, accum) in accums.into_iter() {
        let mut buckets_vec = Vec::with_capacity(buckets);
        let mut accum_count = 0u64;
        let mut accum_bytes = 0u64;
        for i in 0..buckets {
            let b_min = accum.min_len + accum.bucket_size * i as u64;
            let b_max = if i + 1 == buckets {
                accum.max_len
            } else {
                (accum.min_len + accum.bucket_size * (i as u64 + 1)).saturating_sub(1)
            };
            accum_count += accum.counts[i];
            accum_bytes += accum.bytes[i];
            let running_avg = if accum_count == 0 {
                0
            } else {
                accum_bytes / accum_count
            };
            let pct_tiles = if accum.used_tiles == 0 {
                0.0
            } else {
                accum.counts[i] as f64 / accum.used_tiles as f64
            };
            let pct_level_bytes = if accum.used_bytes == 0 {
                0.0
            } else {
                accum.bytes[i] as f64 / accum.used_bytes as f64
            };
            let accum_pct_tiles = if accum.used_tiles == 0 {
                0.0
            } else {
                accum_count as f64 / accum.used_tiles as f64
            };
            let accum_pct_level_bytes = if accum.used_bytes == 0 {
                0.0
            } else {
                accum_bytes as f64 / accum.used_bytes as f64
            };
            let avg_over_limit = max_tile_bytes > 0 && (running_avg as f64) > max_tile_bytes as f64;
            let avg_near_limit =
                max_tile_bytes > 0 && !avg_over_limit && (running_avg as f64) >= limit_threshold;
            buckets_vec.push(HistogramBucket {
                min_bytes: b_min,
                max_bytes: b_max,
                count: accum.counts[i],
                total_bytes: accum.bytes[i],
                running_avg_bytes: running_avg,
                pct_tiles,
                pct_level_bytes,
                accum_pct_tiles,
                accum_pct_level_bytes,
                avg_near_limit,
                avg_over_limit,
            });
        }
        result.push(ZoomHistogram {
            zoom,
            buckets: buckets_vec,
        });
    }
    Ok(result)
}

#[allow(clippy::too_many_arguments)]
pub fn collect_top_tiles_from_entries(
    file: &File,
    header: &Header,
    entries: &[Entry],
    zoom_filter: Option<u8>,
    topn: usize,
    bucket: Option<usize>,
    list_options: Option<&TileListOptions>,
    min_len: Option<u64>,
    max_len: Option<u64>,
    histogram_buckets: usize,
    mut progress: Option<&mut ProgressTracker>,
) -> Result<(Vec<TopTile>, Vec<TopTile>)> {
    if topn == 0 && (bucket.is_none() || list_options.is_none()) {
        return Ok((Vec::new(), Vec::new()));
    }

    let mut top_heap: BinaryHeap<Reverse<(u64, u8, u32, u32)>> = BinaryHeap::new();
    let mut bucket_tiles: Vec<TopTile> = Vec::new();
    let bucket_target = bucket.unwrap_or(0);
    let bucketable = bucket.is_some()
        && list_options.is_some()
        && histogram_buckets > 0
        && min_len.is_some()
        && max_len.is_some();

    let mut stack = vec![entries.to_vec()];
    while let Some(entries) = stack.pop() {
        for entry in entries.iter() {
            if entry.run_length == 0 {
                if entry.length == 0 {
                    continue;
                }
                let leaf_offset = header.leaf_offset + entry.offset;
                let leaf_entries =
                    read_directory_section(file, header, leaf_offset, entry.length as u64)?;
                stack.push(leaf_entries);
                continue;
            }
            let length = entry.length as u64;
            let run = entry.run_length.max(1);
            for idx in 0..run {
                let tile_id = entry.tile_id + idx as u64;
                let (z, x, y) = tile_id_to_xyz(tile_id);
                if let Some(target_zoom) = zoom_filter
                    && z != target_zoom
                {
                    continue;
                }
                if let Some(progress) = progress.as_deref_mut() {
                    progress.inc(1);
                }
                if topn > 0 {
                    top_heap.push(Reverse((length, z, x, y)));
                    if top_heap.len() > topn {
                        top_heap.pop();
                    }
                }
                if bucketable
                    && let Some(bucket_idx) =
                        histogram_bucket_index_pmtiles(length, min_len, max_len, histogram_buckets)
                    && bucket_idx == bucket_target
                {
                    bucket_tiles.push(TopTile {
                        zoom: z,
                        x,
                        y,
                        bytes: length,
                    });
                    let list_options = list_options.expect("list options");
                    if bucket_tiles.len() > list_options.limit {
                        if list_options.sort == TileSort::Size {
                            bucket_tiles.sort_by(|a, b| b.bytes.cmp(&a.bytes));
                        } else {
                            bucket_tiles
                                .sort_by(|a, b| (a.zoom, a.x, a.y).cmp(&(b.zoom, b.x, b.y)));
                        }
                        bucket_tiles.truncate(list_options.limit);
                    }
                }
            }
        }
    }

    let mut top_tiles = top_heap
        .into_iter()
        .map(|Reverse((bytes, zoom, x, y))| TopTile { zoom, x, y, bytes })
        .collect::<Vec<_>>();
    top_tiles.sort_by(|a, b| b.bytes.cmp(&a.bytes));

    Ok((top_tiles, bucket_tiles))
}

pub fn build_file_layer_list_pmtiles(
    mut file: &File,
    header: &Header,
    entries: &[Entry],
    options: &InspectOptions,
    total_tiles: u64,
    mut progress: Option<&mut ProgressTracker>,
) -> Result<Vec<crate::mbtiles::FileLayerSummary>> {
    if !options.include_layer_list {
        return Ok(Vec::new());
    }

    let mut map: BTreeMap<String, LayerAccum> = BTreeMap::new();
    let mut index: u64 = 0;
    let mut stack = vec![entries.to_vec()];

    while let Some(entries) = stack.pop() {
        for entry in entries.iter() {
            if entry.run_length == 0 {
                if entry.length == 0 {
                    continue;
                }
                let leaf_offset = header.leaf_offset + entry.offset;
                let leaf_entries =
                    read_directory_section(file, header, leaf_offset, entry.length as u64)?;
                stack.push(leaf_entries);
                continue;
            }
            let run = entry.run_length.max(1);
            let mut selected = 0u64;
            for idx in 0..run {
                let tile_id = entry.tile_id + idx as u64;
                let (z, _x, _y) = tile_id_to_xyz(tile_id);
                if let Some(target_zoom) = options.zoom
                    && z != target_zoom
                {
                    continue;
                }
                index += 1;
                if let Some(progress) = progress.as_deref_mut() {
                    progress.inc(1);
                }
                if include_sample(index, total_tiles, options.sample.as_ref()) {
                    selected += 1;
                }
            }
            if selected == 0 {
                continue;
            }
            let data_offset = header.data_offset + entry.offset;
            let mut data = vec![0u8; entry.length as usize];
            file.seek(SeekFrom::Start(data_offset))
                .context("seek tile data")?;
            file.read_exact(&mut data).context("read tile data")?;
            let payload = decode_tile_payload_pmtiles(&data, header.tile_compression)?;
            let reader =
                Reader::new(payload).map_err(|err| anyhow::anyhow!("decode vector tile: {err}"))?;
            let layers = reader
                .get_layer_metadata()
                .map_err(|err| anyhow::anyhow!("read layer metadata: {err}"))?;
            for layer in layers {
                let entry = map.entry(layer.name.clone()).or_insert_with(|| LayerAccum {
                    feature_count: 0,
                    vertex_count: 0,
                    property_keys: HashSet::new(),
                    property_values: HashSet::new(),
                });
                entry.feature_count += (layer.feature_count as u64) * selected;
                let features = reader
                    .get_features(layer.layer_index)
                    .map_err(|err| anyhow::anyhow!("read layer features: {err}"))?;
                for feature in features {
                    entry.vertex_count += (count_vertices(&feature.geometry) as u64) * selected;
                    if let Some(props) = feature.properties {
                        for (key, value) in props {
                            entry.property_keys.insert(key.clone());
                            entry.property_values.insert(format_property_value(&value));
                        }
                    }
                }
            }
        }
    }

    let mut result = map
        .into_iter()
        .map(|(name, accum)| crate::mbtiles::FileLayerSummary {
            name,
            vertex_count: accum.vertex_count,
            feature_count: accum.feature_count,
            property_key_count: accum.property_keys.len(),
            property_value_count: accum.property_values.len(),
        })
        .collect::<Vec<_>>();
    result.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(result)
}

pub fn ensure_pmtiles_path(path: &Path) -> Result<()> {
    let ext = path.extension().and_then(|ext| ext.to_str()).unwrap_or("");
    if ext.eq_ignore_ascii_case("pmtiles") {
        Ok(())
    } else {
        anyhow::bail!("only .pmtiles paths are supported in v0.0.3");
    }
}

pub fn ensure_mbtiles_path(path: &Path) -> Result<()> {
    let ext = path.extension().and_then(|ext| ext.to_str()).unwrap_or("");
    if ext.eq_ignore_ascii_case("mbtiles") {
        Ok(())
    } else {
        anyhow::bail!("only .mbtiles paths are supported");
    }
}

pub fn inspect_pmtiles_with_options(
    path: &Path,
    options: &InspectOptions,
) -> Result<MbtilesReport> {
    ensure_pmtiles_path(path)?;
    let file = File::open(path)
        .with_context(|| format!("failed to open input pmtiles: {}", path.display()))?;
    let header = read_header(&file).context("read header")?;
    let metadata = read_metadata_section(&file, &header)?;

    let root_entries =
        read_directory_section(&file, &header, header.root_offset, header.root_length)
            .context("read root directory")?;
    let total_estimate = header
        .n_addressed_tiles
        .max(header.n_tile_entries)
        .max(header.n_tile_contents);
    let use_bar = options.zoom.is_none() && total_estimate > 0;
    let mut overall = StatAccum {
        tile_count: 0,
        total_bytes: 0,
        max_bytes: 0,
    };
    let mut by_zoom: BTreeMap<u8, StatAccum> = BTreeMap::new();
    let mut empty_tiles = 0u64;
    let mut over_limit_tiles = 0u64;
    let mut min_len: Option<u64> = None;
    let mut max_len: Option<u64> = None;
    let mut zoom_minmax: BTreeMap<u8, (u64, u64)> = BTreeMap::new();
    let mut counting_progress = progress_for_phase(
        "counting tiles",
        total_estimate,
        use_bar,
        options.no_progress,
    );
    accumulate_tile_counts(
        &file,
        &header,
        &root_entries,
        options.zoom,
        &mut overall,
        &mut by_zoom,
        &mut empty_tiles,
        &mut over_limit_tiles,
        &mut min_len,
        &mut max_len,
        &mut zoom_minmax,
        options.max_tile_bytes,
        counting_progress.as_mut(),
    )?;
    if let Some(progress) = counting_progress {
        progress.finish();
    }

    let histogram = match (min_len, max_len) {
        (Some(min_len), Some(max_len)) => {
            let mut histogram_progress = progress_for_phase(
                "processing histogram",
                total_estimate,
                use_bar,
                options.no_progress,
            );
            let histogram = build_histogram_from_entries(
                &file,
                &header,
                &root_entries,
                options.zoom,
                overall.tile_count,
                overall.total_bytes,
                options.histogram_buckets,
                min_len,
                max_len,
                options.max_tile_bytes,
                histogram_progress.as_mut(),
            )?;
            if let Some(progress) = histogram_progress {
                progress.finish();
            }
            histogram
        }
        _ => Vec::new(),
    };

    let needs_top_tiles =
        options.topn > 0 || (options.bucket.is_some() && options.list_tiles.is_some());
    let mut top_tiles_progress = if needs_top_tiles {
        progress_for_phase(
            "processing top tiles",
            total_estimate,
            use_bar,
            options.no_progress,
        )
    } else {
        None
    };
    let (top_tiles, bucket_tiles) = collect_top_tiles_from_entries(
        &file,
        &header,
        &root_entries,
        options.zoom,
        options.topn,
        options.bucket,
        options.list_tiles.as_ref(),
        min_len,
        max_len,
        options.histogram_buckets,
        top_tiles_progress.as_mut(),
    )?;
    if let Some(progress) = top_tiles_progress {
        progress.finish();
    }

    let mut histograms_by_zoom_progress = progress_for_phase(
        "processing histogram by zoom",
        total_estimate,
        use_bar,
        options.no_progress,
    );
    let histograms_by_zoom = build_zoom_histograms_from_entries(
        &file,
        &header,
        &root_entries,
        options.zoom,
        &zoom_minmax,
        options.histogram_buckets,
        options.max_tile_bytes,
        histograms_by_zoom_progress.as_mut(),
    )?;
    if let Some(progress) = histograms_by_zoom_progress {
        progress.finish();
    }
    let mut layers_progress = if options.include_layer_list {
        progress_for_phase(
            "processing layers",
            total_estimate,
            use_bar,
            options.no_progress,
        )
    } else {
        None
    };
    let mut file_layers = build_file_layer_list_pmtiles(
        &file,
        &header,
        &root_entries,
        options,
        overall.tile_count,
        layers_progress.as_mut(),
    )?;
    if let Some(progress) = layers_progress {
        progress.finish();
    }
    if !options.layers.is_empty() {
        let filter: HashSet<&str> = options.layers.iter().map(|s| s.as_str()).collect();
        file_layers.retain(|layer| filter.contains(layer.name.as_str()));
    }

    let by_zoom = by_zoom
        .into_iter()
        .map(|(zoom, stats)| MbtilesZoomStats {
            zoom,
            stats: stats.into_stats(),
        })
        .collect::<Vec<_>>();

    let overall_stats = overall.into_stats();
    let empty_ratio = if overall_stats.tile_count == 0 {
        0.0
    } else {
        empty_tiles as f64 / overall_stats.tile_count as f64
    };

    let bucket_count = options
        .bucket
        .and_then(|idx| histogram.get(idx).map(|b| b.count));

    let recommended_buckets = if options.recommend {
        let mut indices = histogram
            .iter()
            .enumerate()
            .filter_map(|(idx, bucket)| {
                if bucket.avg_over_limit {
                    Some(idx)
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        if indices.is_empty() {
            indices = histogram
                .iter()
                .enumerate()
                .filter_map(|(idx, bucket)| {
                    if bucket.avg_near_limit {
                        Some(idx)
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>();
        }
        indices
    } else {
        Vec::new()
    };

    Ok(MbtilesReport {
        metadata,
        overall: overall_stats,
        by_zoom,
        empty_tiles,
        empty_ratio,
        over_limit_tiles,
        sampled: false,
        sample_total_tiles: 0,
        sample_used_tiles: 0,
        histogram,
        histograms_by_zoom,
        file_layers,
        top_tiles,
        bucket_count,
        bucket_tiles,
        tile_summary: None,
        recommended_buckets,
        top_tile_summaries: Vec::new(),
    })
}

pub fn prune_pmtiles_layer_only(
    input: &Path,
    output: &Path,
    style: &crate::style::MapboxStyle,
    apply_filters: bool,
    keep_unknown_filters: bool,
) -> Result<PruneStats> {
    ensure_pmtiles_path(input)?;
    ensure_pmtiles_path(output)?;

    let file = File::open(input)
        .with_context(|| format!("failed to open input pmtiles: {}", input.display()))?;
    let header = read_header(&file).context("read header")?;
    let root_entries =
        read_directory_section(&file, &header, header.root_offset, header.root_length)?;

    let metadata = read_metadata_section(&file, &header)?;
    let keep_layers = style.source_layers();
    let mut stats = PruneStats::default();
    let mut tiles: Vec<(u64, Vec<u8>)> = Vec::new();
    let mut min_zoom = u8::MAX;
    let mut max_zoom = u8::MIN;

    let mut stack = vec![root_entries];
    let mut file = file;
    while let Some(entries) = stack.pop() {
        for entry in entries {
            if entry.run_length == 0 {
                if entry.length == 0 {
                    continue;
                }
                let leaf_offset = header.leaf_offset + entry.offset;
                let leaf_entries =
                    read_directory_section(&file, &header, leaf_offset, entry.length as u64)?;
                stack.push(leaf_entries);
                continue;
            }
            let data_offset = header.data_offset + entry.offset;
            let mut data = vec![0u8; entry.length as usize];
            file.seek(SeekFrom::Start(data_offset))
                .context("seek tile")?;
            file.read_exact(&mut data).context("read tile data")?;
            let payload = decode_tile_payload_pmtiles(&data, header.tile_compression)?;
            let run = entry.run_length.max(1);
            for idx in 0..run {
                let tile_id = entry.tile_id + idx as u64;
                let (z, _x, _y) = tile_id_to_xyz(tile_id);
                min_zoom = min_zoom.min(z);
                max_zoom = max_zoom.max(z);
                let encoded = prune_tile_layers(
                    &payload,
                    z,
                    style,
                    &keep_layers,
                    apply_filters,
                    keep_unknown_filters,
                    &mut stats,
                )?;
                let tile_data =
                    encode_tile_payload_pmtiles(&encoded.bytes, header.tile_compression)?;
                tiles.push((tile_id, tile_data));
            }
        }
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
    let dir_section = encode_internal_bytes(&dir_bytes, header.internal_compression)?;
    let metadata_bytes = if metadata.is_empty() {
        Vec::new()
    } else {
        let mut map = serde_json::Map::new();
        for (key, value) in metadata.into_iter() {
            map.insert(key, Value::String(value));
        }
        let json = Value::Object(map).to_string();
        encode_internal_bytes(json.as_bytes(), header.internal_compression)?
    };
    let header = build_header_with_metadata(
        dir_section.len() as u64,
        metadata_bytes.len() as u64,
        data_section.len() as u64,
        entries.len() as u64,
        if min_zoom == u8::MAX { 0 } else { min_zoom },
        if max_zoom == u8::MIN { 0 } else { max_zoom },
        header.internal_compression,
        header.tile_compression,
        header.tile_type,
    );

    let mut file = File::create(output)
        .with_context(|| format!("failed to create output pmtiles: {}", output.display()))?;
    write_header(&file, &header).context("write header")?;
    file.seek(SeekFrom::Start(header.root_offset))
        .context("seek root directory")?;
    file.write_all(&dir_section)
        .context("write root directory")?;

    if header.metadata_length > 0 {
        file.seek(SeekFrom::Start(header.metadata_offset))
            .context("seek metadata")?;
        file.write_all(&metadata_bytes)
            .context("write metadata")?;
    }
    file.seek(SeekFrom::Start(header.data_offset))
        .context("seek data")?;
    file.write_all(&data_section).context("write data")?;

    Ok(stats)
}

pub fn simplify_pmtiles_tile(
    input: &Path,
    output: &Path,
    coord: crate::mbtiles::TileCoord,
    layers: &[String],
    tolerance: Option<f64>,
) -> Result<crate::mbtiles::SimplifyStats> {
    ensure_pmtiles_path(input)?;
    ensure_pmtiles_path(output)?;

    let file = File::open(input)
        .with_context(|| format!("failed to open input pmtiles: {}", input.display()))?;
    let header = read_header(&file).context("read header")?;
    let root_entries =
        read_directory_section(&file, &header, header.root_offset, header.root_length)?;
    let metadata = read_metadata_section(&file, &header)?;

    let target_id = tile_id_from_xyz(coord.zoom, coord.x, coord.y);
    let mut data: Option<Vec<u8>> = None;

    let mut stack = vec![root_entries];
    let mut file = file;
    'search: while let Some(entries) = stack.pop() {
        for entry in entries {
            if entry.run_length == 0 {
                if entry.length == 0 {
                    continue;
                }
                let leaf_offset = header.leaf_offset + entry.offset;
                let leaf_entries =
                    read_directory_section(&file, &header, leaf_offset, entry.length as u64)?;
                stack.push(leaf_entries);
                continue;
            }
            let run = entry.run_length.max(1);
            let end = entry.tile_id + run as u64;
            if target_id < entry.tile_id || target_id >= end {
                continue;
            }
            let data_offset = header.data_offset + entry.offset;
            let mut buf = vec![0u8; entry.length as usize];
            file.seek(SeekFrom::Start(data_offset))
                .context("seek tile")?;
            file.read_exact(&mut buf).context("read tile data")?;
            data = Some(buf);
            break 'search;
        }
    }

    let Some(data) = data else {
        anyhow::bail!(
            "tile not found: z={} x={} y={}",
            coord.zoom,
            coord.x,
            coord.y
        );
    };

    let payload = decode_tile_payload_pmtiles(&data, header.tile_compression)?;
    let keep_layers: HashSet<String> = layers.iter().cloned().collect();
    let (filtered, stats) = simplify_tile_payload(&payload, &keep_layers, tolerance)?;
    let tile_data = encode_tile_payload_pmtiles(&filtered, header.tile_compression)?;

    let entry = Entry {
        tile_id: target_id,
        offset: 0,
        length: tile_data.len() as u32,
        run_length: 1,
    };
    let dir_bytes = encode_directory(&[entry])?;
    let dir_section = encode_internal_bytes(&dir_bytes, header.internal_compression)?;
    let metadata_bytes = if metadata.is_empty() {
        Vec::new()
    } else {
        let mut map = serde_json::Map::new();
        for (key, value) in metadata.into_iter() {
            map.insert(key, Value::String(value));
        }
        let json = Value::Object(map).to_string();
        encode_internal_bytes(json.as_bytes(), header.internal_compression)?
    };
    let header = build_header_with_metadata(
        dir_section.len() as u64,
        metadata_bytes.len() as u64,
        tile_data.len() as u64,
        1,
        coord.zoom,
        coord.zoom,
        header.internal_compression,
        header.tile_compression,
        header.tile_type,
    );

    let file = File::create(output)
        .with_context(|| format!("failed to create output pmtiles: {}", output.display()))?;
    write_header(&file, &header)?;

    let mut file = file;
    file.seek(SeekFrom::Start(header.root_offset))
        .context("seek root directory")?;
    file.write_all(&dir_section)
        .context("write root directory")?;

    if !metadata_bytes.is_empty() {
        file.seek(SeekFrom::Start(header.metadata_offset))
            .context("seek metadata")?;
        file.write_all(&metadata_bytes).context("write metadata")?;
    }

    file.seek(SeekFrom::Start(header.data_offset))
        .context("seek data")?;
    file.write_all(&tile_data).context("write data")?;

    Ok(stats)
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
        let z: u8 = row.get::<_, u8>(0)?;
        let x: u32 = row.get::<_, u32>(1)?;
        let y: u32 = row.get::<_, u32>(2)?;
        let data: Vec<u8> = row.get::<_, Vec<u8>>(3)?;
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
    let mut header = build_header(
        dir_bytes.len() as u64,
        data_section.len() as u64,
        entries.len() as u64,
        if min_zoom == u8::MAX { 0 } else { min_zoom },
        if max_zoom == u8::MIN { 0 } else { max_zoom },
    );

    if let Some((_, first_data)) = tiles.first() {
        if first_data.starts_with(&[0x1f, 0x8b]) {
            header.tile_compression = 1;
        } else {
            header.tile_compression = 0;
        }
    }
    header.internal_compression = 0;

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
    file.read_exact(&mut dir_buf)
        .context("read root directory")?;
    let dir_bytes = decode_internal_bytes(dir_buf, header.internal_compression)?;
    let entries = decode_directory(&dir_bytes)?;

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

    let tx = output_conn
        .transaction()
        .context("begin output transaction")?;

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
