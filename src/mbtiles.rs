use std::cmp::Reverse;
use std::collections::{BTreeMap, BinaryHeap, HashSet};
use std::path::Path;
use std::io::Read;

use anyhow::{Context, Result};
use flate2::read::GzDecoder;
use indicatif::{ProgressBar, ProgressStyle};
use mvt_reader::Reader;
use rusqlite::{params, Connection, OpenFlags};
use serde::Serialize;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct MbtilesStats {
    pub tile_count: u64,
    pub total_bytes: u64,
    pub max_bytes: u64,
    pub avg_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct MbtilesZoomStats {
    pub zoom: u8,
    pub stats: MbtilesStats,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct MbtilesReport {
    pub overall: MbtilesStats,
    pub by_zoom: Vec<MbtilesZoomStats>,
    pub empty_tiles: u64,
    pub empty_ratio: f64,
    pub sampled: bool,
    pub sample_total_tiles: u64,
    pub sample_used_tiles: u64,
    pub histogram: Vec<HistogramBucket>,
    pub top_tiles: Vec<TopTile>,
    pub bucket_count: Option<u64>,
    pub bucket_tiles: Vec<TopTile>,
    pub tile_summary: Option<TileSummary>,
    pub recommended_buckets: Vec<usize>,
    pub top_tile_summaries: Vec<TileSummary>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct HistogramBucket {
    pub min_bytes: u64,
    pub max_bytes: u64,
    pub count: u64,
    pub total_bytes: u64,
    pub running_avg_bytes: u64,
    pub pct_tiles: f64,
    pub pct_level_bytes: f64,
    pub accum_pct_tiles: f64,
    pub accum_pct_level_bytes: f64,
    pub avg_near_limit: bool,
    pub avg_over_limit: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TopTile {
    pub zoom: u8,
    pub x: u32,
    pub y: u32,
    pub bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct LayerSummary {
    pub name: String,
    pub feature_count: usize,
    pub property_key_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TileSummary {
    pub zoom: u8,
    pub x: u32,
    pub y: u32,
    pub total_features: usize,
    pub layers: Vec<LayerSummary>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TileCoord {
    pub zoom: u8,
    pub x: u32,
    pub y: u32,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SampleSpec {
    Ratio(f64),
    Count(u64),
}

#[derive(Debug, Clone)]
pub struct InspectOptions {
    pub sample: Option<SampleSpec>,
    pub topn: usize,
    pub histogram_buckets: usize,
    pub no_progress: bool,
    pub max_tile_bytes: u64,
    pub zoom: Option<u8>,
    pub bucket: Option<usize>,
    pub tile: Option<TileCoord>,
    pub summary: bool,
    pub layer: Option<String>,
    pub recommend: bool,
    pub list_tiles: Option<TileListOptions>,
}

impl Default for InspectOptions {
    fn default() -> Self {
        Self {
            sample: None,
            topn: 0,
            histogram_buckets: 0,
            no_progress: false,
            max_tile_bytes: 0,
            zoom: None,
            bucket: None,
            tile: None,
            summary: false,
            layer: None,
            recommend: false,
            list_tiles: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TileSort {
    Size,
    Zxy,
}

#[derive(Debug, Clone)]
pub struct TileListOptions {
    pub limit: usize,
    pub sort: TileSort,
}

const EMPTY_TILE_MAX_BYTES: u64 = 50;

fn histogram_bucket_index(
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

fn finalize_stats(stats: &mut MbtilesStats) {
    if stats.tile_count == 0 {
        stats.avg_bytes = 0;
    } else {
        stats.avg_bytes = stats.total_bytes / stats.tile_count;
    }
}

pub fn parse_sample_spec(value: &str) -> Result<SampleSpec> {
    let trimmed = value.trim();
    let as_f64: f64 = trimmed.parse().context("invalid sample value")?;
    if as_f64 <= 0.0 {
        anyhow::bail!("sample must be greater than zero");
    }
    if as_f64 <= 1.0 {
        return Ok(SampleSpec::Ratio(as_f64));
    }
    let as_u64: u64 = trimmed.parse().context("invalid sample count")?;
    Ok(SampleSpec::Count(as_u64))
}

pub fn parse_tile_spec(value: &str) -> Result<TileCoord> {
    let trimmed = value.trim();
    let mut parts = trimmed.split('/');
    let zoom_str = parts.next().context("tile must be in z/x/y format")?;
    let x_str = parts.next().context("tile must be in z/x/y format")?;
    let y_str = parts.next().context("tile must be in z/x/y format")?;
    if parts.next().is_some() {
        anyhow::bail!("tile must be in z/x/y format");
    }
    let zoom: u8 = zoom_str.parse().context("invalid tile zoom")?;
    let x: u32 = x_str.parse().context("invalid tile x")?;
    let y: u32 = y_str.parse().context("invalid tile y")?;
    Ok(TileCoord { zoom, x, y })
}

fn decode_tile_payload(data: &[u8]) -> Result<Vec<u8>> {
    if data.starts_with(&[0x1f, 0x8b]) {
        let mut decoder = GzDecoder::new(data);
        let mut decoded = Vec::new();
        decoder
            .read_to_end(&mut decoded)
            .context("decode gzip tile data")?;
        Ok(decoded)
    } else {
        Ok(data.to_vec())
    }
}

fn build_tile_summary(
    conn: &Connection,
    coord: TileCoord,
    layer_filter: Option<&str>,
) -> Result<TileSummary> {
    let data: Vec<u8> = conn
        .query_row(
            "SELECT tile_data FROM tiles WHERE zoom_level = ?1 AND tile_column = ?2 AND tile_row = ?3",
            params![coord.zoom, coord.x, coord.y],
            |row| row.get(0),
        )
        .context("failed to read tile data")?;
    let payload = decode_tile_payload(&data)?;
    let reader = Reader::new(payload)
        .map_err(|err| anyhow::anyhow!("decode vector tile: {err}"))?;
    let layers = reader
        .get_layer_metadata()
        .map_err(|err| anyhow::anyhow!("read layer metadata: {err}"))?;
    let mut total_features = 0usize;
    let mut summaries = Vec::new();
    for layer in layers {
        if let Some(filter) = layer_filter {
            if layer.name != filter {
                continue;
            }
        }
        let features = reader
            .get_features(layer.layer_index)
            .map_err(|err| anyhow::anyhow!("read layer features: {err}"))?;
        let mut keys = HashSet::new();
        for feature in features {
            if let Some(props) = feature.properties {
                for key in props.keys() {
                    keys.insert(key.clone());
                }
            }
        }
        let feature_count = layer.feature_count;
        total_features += feature_count;
        summaries.push(LayerSummary {
            name: layer.name,
            feature_count,
            property_key_count: keys.len(),
        });
    }
    Ok(TileSummary {
        zoom: coord.zoom,
        x: coord.x,
        y: coord.y,
        total_features,
        layers: summaries,
    })
}

fn include_sample(index: u64, total: u64, spec: Option<&SampleSpec>) -> bool {
    match spec {
        None => true,
        Some(SampleSpec::Count(count)) => index <= *count,
        Some(SampleSpec::Ratio(ratio)) => {
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

fn splitmix64(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9e3779b97f4a7c15);
    let mut z = x;
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb);
    z ^ (z >> 31)
}

fn build_histogram(
    path: &Path,
    sample: Option<&SampleSpec>,
    total_tiles_db: u64,
    total_tiles_used: u64,
    total_bytes_used: u64,
    buckets: usize,
    min_len: u64,
    max_len: u64,
    zoom: Option<u8>,
    max_tile_bytes: u64,
) -> Result<Vec<HistogramBucket>> {
    if buckets == 0 || min_len > max_len {
        return Ok(Vec::new());
    }
    let conn = open_readonly_mbtiles(path)?;
    apply_read_pragmas(&conn)?;
    let mut stmt = conn
        .prepare("SELECT zoom_level, LENGTH(tile_data) FROM tiles")
        .context("prepare histogram scan")?;
    let mut rows = stmt.query([]).context("query histogram scan")?;

    let range = (max_len - min_len).max(1);
    let bucket_size = ((range as f64) / buckets as f64).ceil() as u64;
    let mut counts = vec![0u64; buckets];
    let mut bytes = vec![0u64; buckets];

    let mut index: u64 = 0;
    while let Some(row) = rows.next().context("read histogram row")? {
        let row_zoom: u8 = row.get(0)?;
        let length: u64 = row.get(1)?;
        if let Some(target) = zoom {
            if row_zoom != target {
                continue;
            }
        }
        index += 1;
        if !include_sample(index, total_tiles_db, sample) {
            continue;
        }
        let mut bucket = ((length.saturating_sub(min_len)) / bucket_size) as usize;
        if bucket >= buckets {
            bucket = buckets - 1;
        }
        counts[bucket] += 1;
        bytes[bucket] += length;

        if let Some(SampleSpec::Count(limit)) = sample {
            if counts.iter().sum::<u64>() >= *limit {
                break;
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
        let avg_near_limit = max_tile_bytes > 0
            && !avg_over_limit
            && (running_avg as f64) >= limit_threshold;
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

fn ensure_mbtiles_path(path: &Path) -> Result<()> {
    let ext = path.extension().and_then(|ext| ext.to_str()).unwrap_or("");
    if ext.eq_ignore_ascii_case("mbtiles") {
        Ok(())
    } else {
        anyhow::bail!("only .mbtiles paths are supported in v0.0.3");
    }
}

fn open_readonly_mbtiles(path: &Path) -> Result<Connection> {
    Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)
        .with_context(|| format!("failed to open mbtiles: {}", path.display()))
}

fn apply_read_pragmas(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "
        PRAGMA query_only = ON;
        PRAGMA temp_store = MEMORY;
        PRAGMA synchronous = OFF;
        PRAGMA cache_size = -200000;
        ",
    )
    .context("failed to apply read pragmas")?;
    Ok(())
}

fn make_progress_bar(total: u64) -> ProgressBar {
    let bar = ProgressBar::new(total);
    bar.set_style(
        ProgressStyle::with_template("[{elapsed_precise}] {bar:40.cyan/blue} {pos}/{len} {msg}")
            .unwrap()
            .progress_chars("=>-"),
    );
    bar
}

pub fn inspect_mbtiles(path: &Path) -> Result<MbtilesReport> {
    inspect_mbtiles_with_options(path, InspectOptions::default())
}

pub fn inspect_mbtiles_with_options(path: &Path, options: InspectOptions) -> Result<MbtilesReport> {
    ensure_mbtiles_path(path)?;
    let conn = open_readonly_mbtiles(path)?;
    apply_read_pragmas(&conn)?;

    let tile_summary = if options.summary {
        let coord = options
            .tile
            .context("--summary requires --tile z/x/y")?;
        Some(build_tile_summary(&conn, coord, options.layer.as_deref())?)
    } else {
        None
    };

    let total_tiles: u64 = match options.zoom {
        Some(z) => conn
            .query_row("SELECT COUNT(*) FROM tiles WHERE zoom_level = ?1", [z], |row| row.get(0))
            .context("failed to read tile count (zoom)")?,
        None => conn
            .query_row("SELECT COUNT(*) FROM tiles", [], |row| row.get(0))
            .context("failed to read tile count")?,
    };
    let progress = if options.no_progress {
        ProgressBar::hidden()
    } else {
        make_progress_bar(total_tiles)
    };

    let mut overall = MbtilesStats {
        tile_count: 0,
        total_bytes: 0,
        max_bytes: 0,
        avg_bytes: 0,
    };

    let mut stmt = conn
        .prepare("SELECT zoom_level, tile_column, tile_row, LENGTH(tile_data) FROM tiles")
        .context("prepare tiles scan")?;
    let mut rows = stmt.query([]).context("query tiles scan")?;

    let mut by_zoom: BTreeMap<u8, MbtilesStats> = BTreeMap::new();
    let mut empty_tiles: u64 = 0;
    let mut processed: u64 = 0;
    let mut used: u64 = 0;

    let mut min_len: Option<u64> = None;
    let mut max_len: Option<u64> = None;

    let mut top_heap: BinaryHeap<Reverse<(u64, u8, u32, u32)>> = BinaryHeap::new();
    let mut bucket_tiles: Vec<TopTile> = Vec::new();
    let topn = options.topn;

    while let Some(row) = rows.next().context("read tile row")? {
        let zoom: u8 = row.get(0)?;
        let x: u32 = row.get(1)?;
        let y: u32 = row.get(2)?;
        let length: u64 = row.get(3)?;

        if let Some(target) = options.zoom {
            if zoom != target {
                continue;
            }
        }

        processed += 1;

        if include_sample(processed, total_tiles, options.sample.as_ref()) {
            used += 1;

            overall.tile_count += 1;
            overall.total_bytes += length;
            overall.max_bytes = overall.max_bytes.max(length);

            let entry = by_zoom.entry(zoom).or_insert(MbtilesStats {
                tile_count: 0,
                total_bytes: 0,
                max_bytes: 0,
                avg_bytes: 0,
            });
            entry.tile_count += 1;
            entry.total_bytes += length;
            entry.max_bytes = entry.max_bytes.max(length);

            if length <= EMPTY_TILE_MAX_BYTES {
                empty_tiles += 1;
            }

            min_len = Some(min_len.map_or(length, |v| v.min(length)));
            max_len = Some(max_len.map_or(length, |v| v.max(length)));

            if topn > 0 {
                top_heap.push(Reverse((length, zoom, x, y)));
                if top_heap.len() > topn {
                    top_heap.pop();
                }
            }

            if let (Some(bucket_index), Some(list_options)) =
                (options.bucket, options.list_tiles.as_ref())
            {
                if let Some(bucket_idx) = histogram_bucket_index(
                    length,
                    min_len,
                    max_len,
                    options.histogram_buckets,
                ) {
                    if bucket_idx == bucket_index {
                        bucket_tiles.push(TopTile {
                            zoom,
                            x,
                            y,
                            bytes: length,
                        });
                        if bucket_tiles.len() > list_options.limit {
                            if list_options.sort == TileSort::Size {
                                bucket_tiles.sort_by(|a, b| b.bytes.cmp(&a.bytes));
                            } else {
                                bucket_tiles.sort_by(|a, b| {
                                    (a.zoom, a.x, a.y).cmp(&(b.zoom, b.x, b.y))
                                });
                            }
                            bucket_tiles.truncate(list_options.limit);
                        }
                    }
                }
            }
        }

        if let Some(SampleSpec::Count(limit)) = options.sample {
            if used >= limit {
                break;
            }
        }

        if processed % 5000 == 0 {
            progress.set_position(processed);
        }
    }

    progress.set_position(processed);
    progress.finish_and_clear();

    let by_zoom = by_zoom
        .into_iter()
        .map(|(zoom, mut stats)| {
            finalize_stats(&mut stats);
            MbtilesZoomStats { zoom, stats }
        })
        .collect::<Vec<_>>();

    finalize_stats(&mut overall);

    let mut top_tiles = top_heap
        .into_iter()
        .map(|Reverse((bytes, zoom, x, y))| TopTile {
            zoom,
            x,
            y,
            bytes,
        })
        .collect::<Vec<_>>();
    top_tiles.sort_by(|a, b| b.bytes.cmp(&a.bytes));

    let empty_ratio = if used == 0 {
        0.0
    } else {
        empty_tiles as f64 / used as f64
    };

    let histogram = if options.histogram_buckets > 0 && min_len.is_some() {
        let (level_tiles_used, level_bytes_used) = if let Some(target) = options.zoom {
            by_zoom
                .iter()
                .find(|z| z.zoom == target)
                .map(|z| (z.stats.tile_count, z.stats.total_bytes))
                .unwrap_or((0, 0))
        } else {
            (overall.tile_count, overall.total_bytes)
        };
        build_histogram(
            path,
            options.sample.as_ref(),
            total_tiles,
            level_tiles_used,
            level_bytes_used,
            options.histogram_buckets,
            min_len.unwrap(),
            max_len.unwrap(),
            options.zoom,
            options.max_tile_bytes,
        )?
    } else {
        Vec::new()
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

    let top_tile_summaries = if options.recommend && !top_tiles.is_empty() {
        top_tiles
            .iter()
            .map(|tile| {
                build_tile_summary(
                    &conn,
                    TileCoord {
                        zoom: tile.zoom,
                        x: tile.x,
                        y: tile.y,
                    },
                    None,
                )
            })
            .collect::<Result<Vec<_>>>()?
    } else {
        Vec::new()
    };

    Ok(MbtilesReport {
        overall,
        by_zoom,
        empty_tiles,
        empty_ratio,
        sampled: options.sample.is_some(),
        sample_total_tiles: total_tiles,
        sample_used_tiles: used,
        histogram,
        top_tiles,
        bucket_count,
        bucket_tiles,
        tile_summary,
        recommended_buckets,
        top_tile_summaries,
    })
}

pub fn copy_mbtiles(input: &Path, output: &Path) -> Result<()> {
    ensure_mbtiles_path(input)?;
    ensure_mbtiles_path(output)?;
    let input_conn = Connection::open(input)
        .with_context(|| format!("failed to open input mbtiles: {}", input.display()))?;
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
        .context("failed to create output schema")?;

    let tx = output_conn.transaction().context("begin output transaction")?;

    {
        let mut stmt = input_conn
            .prepare("SELECT name, value FROM metadata")
            .context("prepare metadata")?;
        let mut rows = stmt.query([]).context("query metadata")?;
        while let Some(row) = rows.next().context("read metadata row")? {
            let name: String = row.get(0)?;
            let value: String = row.get(1)?;
            tx.execute(
                "INSERT INTO metadata (name, value) VALUES (?1, ?2)",
                params![name, value],
            )
            .context("insert metadata")?;
        }
    }

    {
        let mut stmt = input_conn
            .prepare(
                "SELECT zoom_level, tile_column, tile_row, tile_data FROM tiles ORDER BY zoom_level, tile_column, tile_row",
            )
            .context("prepare tiles")?;
        let mut rows = stmt.query([]).context("query tiles")?;
        while let Some(row) = rows.next().context("read tile row")? {
            let z: i64 = row.get(0)?;
            let x: i64 = row.get(1)?;
            let y: i64 = row.get(2)?;
            let data: Vec<u8> = row.get(3)?;
            tx.execute(
                "INSERT INTO tiles (zoom_level, tile_column, tile_row, tile_data) VALUES (?1, ?2, ?3, ?4)",
                params![z, x, y, data],
            )
            .context("insert tile")?;
        }
    }

    tx.commit().context("commit output")?;
    Ok(())
}
