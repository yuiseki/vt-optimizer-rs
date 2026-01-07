use std::cmp::Reverse;
use std::collections::{BTreeMap, BinaryHeap};
use std::path::Path;

use anyhow::{Context, Result};
use indicatif::{ProgressBar, ProgressStyle};
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
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct HistogramBucket {
    pub min_bytes: u64,
    pub max_bytes: u64,
    pub count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TopTile {
    pub zoom: u8,
    pub x: u32,
    pub y: u32,
    pub bytes: u64,
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
    pub zoom: Option<u8>,
    pub bucket: Option<usize>,
    pub list_tiles: Option<TileListOptions>,
}

impl Default for InspectOptions {
    fn default() -> Self {
        Self {
            sample: None,
            topn: 0,
            histogram_buckets: 0,
            no_progress: false,
            zoom: None,
            bucket: None,
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
    total_tiles: u64,
    buckets: usize,
    min_len: u64,
    max_len: u64,
    zoom: Option<u8>,
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
        if !include_sample(index, total_tiles, sample) {
            continue;
        }
        let mut bucket = ((length.saturating_sub(min_len)) / bucket_size) as usize;
        if bucket >= buckets {
            bucket = buckets - 1;
        }
        counts[bucket] += 1;

        if let Some(SampleSpec::Count(limit)) = sample {
            if counts.iter().sum::<u64>() >= *limit {
                break;
            }
        }
    }

    let mut result = Vec::with_capacity(buckets);
    for i in 0..buckets {
        let b_min = min_len + bucket_size * i as u64;
        let b_max = if i + 1 == buckets {
            max_len
        } else {
            (min_len + bucket_size * (i as u64 + 1)).saturating_sub(1)
        };
        result.push(HistogramBucket {
            min_bytes: b_min,
            max_bytes: b_max,
            count: counts[i],
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
        build_histogram(
            path,
            options.sample.as_ref(),
            total_tiles,
            options.histogram_buckets,
            min_len.unwrap(),
            max_len.unwrap(),
            options.zoom,
        )?
    } else {
        Vec::new()
    };

    let bucket_count = options
        .bucket
        .and_then(|idx| histogram.get(idx).map(|b| b.count));

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
