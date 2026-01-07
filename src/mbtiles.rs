use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap, HashSet};
use std::path::Path;
use std::io::{Read, Write};
use std::time::Duration;

use anyhow::{Context, Result};
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;
use geo_types::{Geometry, Line, LineString, MultiLineString, MultiPoint, MultiPolygon, Polygon};
use indicatif::{ProgressBar, ProgressDrawTarget, ProgressStyle};
use mvt::{GeomData, GeomEncoder, GeomType, Tile};
use mvt_reader::Reader;
use tracing::warn;
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
    pub metadata: BTreeMap<String, String>,
    pub overall: MbtilesStats,
    pub by_zoom: Vec<MbtilesZoomStats>,
    pub empty_tiles: u64,
    pub empty_ratio: f64,
    pub sampled: bool,
    pub sample_total_tiles: u64,
    pub sample_used_tiles: u64,
    pub histogram: Vec<HistogramBucket>,
    pub histograms_by_zoom: Vec<ZoomHistogram>,
    pub file_layers: Vec<FileLayerSummary>,
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

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ZoomHistogram {
    pub zoom: u8,
    pub buckets: Vec<HistogramBucket>,
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
    pub property_keys: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct FileLayerSummary {
    pub name: String,
    pub vertex_count: u64,
    pub feature_count: u64,
    pub property_key_count: usize,
    pub property_value_count: usize,
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
    pub include_layer_list: bool,
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
            include_layer_list: false,
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

fn encode_tile_payload(data: &[u8], gzip: bool) -> Result<Vec<u8>> {
    if !gzip {
        return Ok(data.to_vec());
    }
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder
        .write_all(data)
        .context("encode gzip tile data")?;
    let encoded = encoder.finish().context("finish gzip tile data")?;
    Ok(encoded)
}

pub(crate) fn count_vertices(geometry: &geo_types::Geometry<f32>) -> usize {
    match geometry {
        geo_types::Geometry::Point(_) => 1,
        geo_types::Geometry::MultiPoint(points) => points.len(),
        geo_types::Geometry::LineString(line) => ring_coords(line).len(),
        geo_types::Geometry::MultiLineString(lines) => lines.iter().map(|l| ring_coords(l).len()).sum(),
        geo_types::Geometry::Line(_) => 2,
        geo_types::Geometry::Polygon(polygon) => {
            let mut count = ring_coords(polygon.exterior()).len();
            for ring in polygon.interiors() {
                count += ring_coords(ring).len();
            }
            count
        }
        geo_types::Geometry::MultiPolygon(polygons) => polygons
            .iter()
            .map(|polygon| {
                let mut count = ring_coords(polygon.exterior()).len();
                for ring in polygon.interiors() {
                    count += ring_coords(ring).len();
                }
                count
            })
            .sum(),
        geo_types::Geometry::Rect(_rect) => 4,
        geo_types::Geometry::Triangle(_) => 3,
        geo_types::Geometry::GeometryCollection(collection) => {
            collection
                .iter()
                .map(|geom| count_vertices(geom))
                .sum()
        }
    }
}

pub(crate) fn format_property_value(value: &mvt_reader::feature::Value) -> String {
    match value {
        mvt_reader::feature::Value::String(text) => text.clone(),
        mvt_reader::feature::Value::Float(val) => val.to_string(),
        mvt_reader::feature::Value::Double(val) => val.to_string(),
        mvt_reader::feature::Value::Int(val) => val.to_string(),
        mvt_reader::feature::Value::UInt(val) => val.to_string(),
        mvt_reader::feature::Value::SInt(val) => val.to_string(),
        mvt_reader::feature::Value::Bool(val) => val.to_string(),
        mvt_reader::feature::Value::Null => "null".to_string(),
    }
}

fn encode_linestring(encoder: &mut GeomEncoder<f32>, line: &LineString<f32>) -> Result<()> {
    for coord in ring_coords(line) {
        encoder
            .add_point(coord.x, coord.y)
            .map_err(|err| anyhow::anyhow!("encode geometry: {err}"))?;
    }
    Ok(())
}

fn ring_coords(line: &LineString<f32>) -> &[geo_types::Coord<f32>] {
    let coords = line.0.as_slice();
    if coords.len() > 1 && coords.first() == coords.last() {
        &coords[..coords.len() - 1]
    } else {
        coords
    }
}

fn encode_geometry(geometry: &Geometry<f32>) -> Result<GeomData> {
    match geometry {
        Geometry::Point(point) => {
            let encoder = GeomEncoder::new(GeomType::Point)
                .point(point.x(), point.y())
                .map_err(|err| anyhow::anyhow!("encode geometry: {err}"))?;
            encoder
                .encode()
                .map_err(|err| anyhow::anyhow!("encode geometry: {err}"))
        }
        Geometry::MultiPoint(MultiPoint(points)) => {
            let mut encoder = GeomEncoder::new(GeomType::Point);
            for point in points {
                encoder
                    .add_point(point.x(), point.y())
                    .map_err(|err| anyhow::anyhow!("encode geometry: {err}"))?;
            }
            encoder
                .encode()
                .map_err(|err| anyhow::anyhow!("encode geometry: {err}"))
        }
        Geometry::LineString(line) => {
            let mut encoder = GeomEncoder::new(GeomType::Linestring);
            encode_linestring(&mut encoder, line)?;
            encoder
                .encode()
                .map_err(|err| anyhow::anyhow!("encode geometry: {err}"))
        }
        Geometry::Line(Line { start, end }) => {
            let line = LineString::from(vec![(start.x, start.y), (end.x, end.y)]);
            let mut encoder = GeomEncoder::new(GeomType::Linestring);
            encode_linestring(&mut encoder, &line)?;
            encoder
                .encode()
                .map_err(|err| anyhow::anyhow!("encode geometry: {err}"))
        }
        Geometry::MultiLineString(MultiLineString(lines)) => {
            let mut encoder = GeomEncoder::new(GeomType::Linestring);
            for (idx, line) in lines.iter().enumerate() {
                encode_linestring(&mut encoder, line)?;
                if idx + 1 < lines.len() {
                    encoder
                        .complete_geom()
                        .map_err(|err| anyhow::anyhow!("encode geometry: {err}"))?;
                }
            }
            encoder
                .encode()
                .map_err(|err| anyhow::anyhow!("encode geometry: {err}"))
        }
        Geometry::Polygon(polygon) => {
            let mut encoder = GeomEncoder::new(GeomType::Polygon);
            let mut rings: Vec<&LineString<f32>> =
                Vec::with_capacity(1 + polygon.interiors().len());
            rings.push(polygon.exterior());
            for ring in polygon.interiors() {
                rings.push(ring);
            }
            for (idx, ring) in rings.iter().enumerate() {
                encode_linestring(&mut encoder, ring)?;
                if idx + 1 < rings.len() {
                    encoder
                        .complete_geom()
                        .map_err(|err| anyhow::anyhow!("encode geometry: {err}"))?;
                }
            }
            encoder
                .encode()
                .map_err(|err| anyhow::anyhow!("encode geometry: {err}"))
        }
        Geometry::MultiPolygon(MultiPolygon(polygons)) => {
            let mut encoder = GeomEncoder::new(GeomType::Polygon);
            for (poly_idx, polygon) in polygons.iter().enumerate() {
                let mut rings: Vec<&LineString<f32>> = Vec::with_capacity(1 + polygon.interiors().len());
                rings.push(polygon.exterior());
                for ring in polygon.interiors() {
                    rings.push(ring);
                }
                for (idx, ring) in rings.iter().enumerate() {
                    encode_linestring(&mut encoder, ring)?;
                    if idx + 1 < rings.len() || poly_idx + 1 < polygons.len() {
                        encoder
                            .complete_geom()
                            .map_err(|err| anyhow::anyhow!("encode geometry: {err}"))?;
                    }
                }
            }
            encoder
                .encode()
                .map_err(|err| anyhow::anyhow!("encode geometry: {err}"))
        }
        Geometry::GeometryCollection(_) => {
            anyhow::bail!("geometry collections are not supported for pruning");
        }
        Geometry::Rect(rect) => {
            let exterior = LineString::from(vec![
                (rect.min().x, rect.min().y),
                (rect.max().x, rect.min().y),
                (rect.max().x, rect.max().y),
                (rect.min().x, rect.max().y),
                (rect.min().x, rect.min().y),
            ]);
            let polygon = Polygon::new(exterior, Vec::new());
            encode_geometry(&Geometry::Polygon(polygon))
        }
        Geometry::Triangle(tri) => {
            let exterior = LineString::from(vec![
                (tri.0.x, tri.0.y),
                (tri.1.x, tri.1.y),
                (tri.2.x, tri.2.y),
                (tri.0.x, tri.0.y),
            ]);
            let polygon = Polygon::new(exterior, Vec::new());
            encode_geometry(&Geometry::Polygon(polygon))
        }
    }
}

fn prune_tile_layers(
    payload: &[u8],
    zoom: u8,
    style: &crate::style::MapboxStyle,
    keep_layers: &HashSet<String>,
    apply_filters: bool,
    stats: &mut PruneStats,
) -> Result<Vec<u8>> {
    let reader = Reader::new(payload.to_vec())
        .map_err(|err| anyhow::anyhow!("decode vector tile: {err}"))?;
    let layers = reader
        .get_layer_metadata()
        .map_err(|err| anyhow::anyhow!("read layer metadata: {err}"))?;

    let mut extent = 4096;
    for layer in layers.iter() {
        if keep_layers.contains(&layer.name) && style.is_layer_visible_on_zoom(&layer.name, zoom) {
            extent = layer.extent;
            break;
        }
    }

    let mut tile = Tile::new(extent);
    for layer in layers {
        if !keep_layers.contains(&layer.name) {
            stats.record_removed_layer(&layer.name, zoom);
            stats.record_removed_features(zoom, layer.feature_count as u64);
            continue;
        }
        if !style.is_layer_visible_on_zoom(&layer.name, zoom) {
            stats.record_removed_layer(&layer.name, zoom);
            stats.record_removed_features(zoom, layer.feature_count as u64);
            continue;
        }
        let mut layer_builder = tile.create_layer(&layer.name);
        let features = reader
            .get_features(layer.layer_index)
            .map_err(|err| anyhow::anyhow!("read layer features: {err}"))?;
        let mut kept_features = 0u64;
        for feature in features {
            if apply_filters {
                match style.should_keep_feature(&layer.name, zoom, &feature, &mut stats.unknown_filters) {
                    crate::style::FilterResult::True => {}
                    crate::style::FilterResult::Unknown => {}
                    crate::style::FilterResult::False => {
                        continue;
                    }
                }
            }
            let geom_data = encode_geometry(feature.get_geometry())?;
            let mut feature_builder = layer_builder.into_feature(geom_data);
            if let Some(id) = feature.id {
                feature_builder.set_id(id);
            }
            if let Some(props) = feature.properties {
                for (key, value) in props {
                    match value {
                        mvt_reader::feature::Value::String(text) => {
                            feature_builder.add_tag_string(&key, &text);
                        }
                        mvt_reader::feature::Value::Float(val) => {
                            feature_builder.add_tag_float(&key, val);
                        }
                        mvt_reader::feature::Value::Double(val) => {
                            feature_builder.add_tag_double(&key, val);
                        }
                        mvt_reader::feature::Value::Int(val) => {
                            feature_builder.add_tag_int(&key, val);
                        }
                        mvt_reader::feature::Value::UInt(val) => {
                            feature_builder.add_tag_uint(&key, val);
                        }
                        mvt_reader::feature::Value::SInt(val) => {
                            feature_builder.add_tag_sint(&key, val);
                        }
                        mvt_reader::feature::Value::Bool(val) => {
                            feature_builder.add_tag_bool(&key, val);
                        }
                        mvt_reader::feature::Value::Null => {}
                    }
                }
            }
            layer_builder = feature_builder.into_layer();
            kept_features += 1;
        }
        let removed_features = (layer.feature_count as u64).saturating_sub(kept_features);
        stats.record_removed_features(zoom, removed_features);
        if kept_features == 0 {
            stats.record_removed_layer(&layer.name, zoom);
            continue;
        }
        tile.add_layer(layer_builder)
            .map_err(|err| anyhow::anyhow!("add layer: {err}"))?;
    }

    tile.to_bytes()
        .map_err(|err| anyhow::anyhow!("encode vector tile: {err}"))
}

struct LayerAccum {
    feature_count: u64,
    vertex_count: u64,
    property_keys: HashSet<String>,
    property_values: HashSet<String>,
}

fn build_file_layer_list(
    conn: &Connection,
    sample: Option<&SampleSpec>,
    total_tiles: u64,
    zoom: Option<u8>,
) -> Result<Vec<FileLayerSummary>> {
    let mut stmt = conn
        .prepare("SELECT zoom_level, tile_data FROM tiles")
        .context("prepare layer list scan")?;
    let mut rows = stmt.query([]).context("query layer list scan")?;

    let mut index: u64 = 0;
    let mut map: BTreeMap<String, LayerAccum> = BTreeMap::new();

    while let Some(row) = rows.next().context("read layer list row")? {
        let row_zoom: u8 = row.get(0)?;
        if let Some(target) = zoom {
            if row_zoom != target {
                continue;
            }
        }
        index += 1;
        if !include_sample(index, total_tiles, sample) {
            continue;
        }
        let data: Vec<u8> = row.get(1)?;
        let payload = decode_tile_payload(&data)?;
        let reader = Reader::new(payload)
            .map_err(|err| anyhow::anyhow!("decode vector tile: {err}"))?;
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
            entry.feature_count += layer.feature_count as u64;
            let features = reader
                .get_features(layer.layer_index)
                .map_err(|err| anyhow::anyhow!("read layer features: {err}"))?;
            for feature in features {
                entry.vertex_count += count_vertices(&feature.geometry) as u64;
                if let Some(props) = feature.properties {
                    for (key, value) in props {
                        entry.property_keys.insert(key.clone());
                        entry
                            .property_values
                            .insert(format_property_value(&value));
                    }
                }
            }
        }

        if let Some(SampleSpec::Count(limit)) = sample {
            if index >= *limit {
                break;
            }
        }
    }

    let mut result = map
        .into_iter()
        .map(|(name, accum)| FileLayerSummary {
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
        let mut key_list = keys.into_iter().collect::<Vec<_>>();
        key_list.sort();
        let feature_count = layer.feature_count;
        total_features += feature_count;
        summaries.push(LayerSummary {
            name: layer.name,
            feature_count,
            property_key_count: key_list.len(),
            property_keys: key_list,
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

fn build_histogram_from_sizes(
    tile_sizes: &[u64],
    total_tiles_used: u64,
    total_bytes_used: u64,
    buckets: usize,
    min_len: u64,
    max_len: u64,
    max_tile_bytes: u64,
) -> Vec<HistogramBucket> {
    if buckets == 0 || min_len > max_len {
        return Vec::new();
    }

    let range = (max_len - min_len).max(1);
    let bucket_size = ((range as f64) / buckets as f64).ceil() as u64;
    let mut counts = vec![0u64; buckets];
    let mut bytes = vec![0u64; buckets];

    for &length in tile_sizes {
        let mut bucket = ((length.saturating_sub(min_len)) / bucket_size) as usize;
        if bucket >= buckets {
            bucket = buckets - 1;
        }
        counts[bucket] += 1;
        bytes[bucket] += length;
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
    result
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
    no_progress: bool,
) -> Result<Vec<HistogramBucket>> {
    if buckets == 0 || min_len > max_len {
        return Ok(Vec::new());
    }
    let conn = open_readonly_mbtiles(path)?;
    apply_read_pragmas(&conn)?;
    let progress = if no_progress {
        ProgressBar::hidden()
    } else {
        let bar = make_progress_bar(total_tiles_db);
        bar.set_message("building histogram");
        bar
    };
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

        if index == 1 || index % 1000 == 0 {
            progress.set_position(index);
        }
    }

    progress.set_position(index);
    progress.finish();

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

fn build_zoom_histograms(
    path: &Path,
    sample: Option<&SampleSpec>,
    zoom_counts: &BTreeMap<u8, u64>,
    zoom_minmax: &BTreeMap<u8, (u64, u64)>,
    buckets: usize,
    max_tile_bytes: u64,
    no_progress: bool,
    total_tiles: u64,
) -> Result<Vec<ZoomHistogram>> {
    if buckets == 0 || zoom_minmax.is_empty() {
        return Ok(Vec::new());
    }
    let conn = open_readonly_mbtiles(path)?;
    apply_read_pragmas(&conn)?;
    let progress = if no_progress {
        ProgressBar::hidden()
    } else {
        let bar = make_progress_bar(total_tiles);
        bar.set_message("building zoom histograms");
        bar
    };
    let mut stmt = conn
        .prepare("SELECT zoom_level, LENGTH(tile_data) FROM tiles")
        .context("prepare zoom histogram scan")?;
    let mut rows = stmt.query([]).context("query zoom histogram scan")?;

    struct ZoomAccum {
        min_len: u64,
        max_len: u64,
        bucket_size: u64,
        counts: Vec<u64>,
        bytes: Vec<u64>,
        index: u64,
        used_tiles: u64,
        used_bytes: u64,
    }

    let mut accums: BTreeMap<u8, ZoomAccum> = BTreeMap::new();
    for (zoom, (min_len, max_len)) in zoom_minmax.iter() {
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
                index: 0,
                used_tiles: 0,
                used_bytes: 0,
            },
        );
    }

    let mut total_index: u64 = 0;
    while let Some(row) = rows.next().context("read zoom histogram row")? {
        let zoom: u8 = row.get(0)?;
        let length: u64 = row.get(1)?;
        total_index += 1;
        let Some(accum) = accums.get_mut(&zoom) else {
            continue;
        };
        accum.index += 1;
        let total_tiles_db = *zoom_counts.get(&zoom).unwrap_or(&0);
        if !include_sample(accum.index, total_tiles_db, sample) {
            continue;
        }
        let mut bucket = ((length.saturating_sub(accum.min_len)) / accum.bucket_size) as usize;
        if bucket >= buckets {
            bucket = buckets - 1;
        }
        accum.counts[bucket] += 1;
        accum.bytes[bucket] += length;
        accum.used_tiles += 1;
        accum.used_bytes += length;

            if let Some(SampleSpec::Count(limit)) = sample {
                if accum.used_tiles >= *limit {
                    // keep scanning other zooms; no-op for this zoom
                }
            }

        if total_index == 1 || total_index % 1000 == 0 {
            progress.set_position(total_index);
        }
    }

    progress.set_position(total_index);
    progress.finish();

    let mut result = Vec::new();
    for (zoom, accum) in accums.into_iter() {
        let mut buckets_vec = Vec::with_capacity(buckets);
        let mut accum_count = 0u64;
        let mut accum_bytes = 0u64;
        let limit_threshold = (max_tile_bytes as f64) * 0.9;
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
            let avg_over_limit =
                max_tile_bytes > 0 && (running_avg as f64) > max_tile_bytes as f64;
            let avg_near_limit = max_tile_bytes > 0
                && !avg_over_limit
                && (running_avg as f64) >= limit_threshold;
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

fn fetch_zoom_counts(conn: &Connection) -> Result<BTreeMap<u8, u64>> {
    let mut stmt = conn
        .prepare("SELECT zoom_level, COUNT(*) FROM tiles GROUP BY zoom_level")
        .context("prepare zoom counts")?;
    let mut rows = stmt.query([]).context("query zoom counts")?;
    let mut counts = BTreeMap::new();
    while let Some(row) = rows.next().context("read zoom count row")? {
        let zoom: u8 = row.get(0)?;
        let count: u64 = row.get(1)?;
        counts.insert(zoom, count);
    }
    Ok(counts)
}

fn make_progress_bar(total: u64) -> ProgressBar {
    let bar = ProgressBar::with_draw_target(Some(total), ProgressDrawTarget::stderr_with_hz(10));
    bar.set_style(
        ProgressStyle::with_template("[{elapsed_precise}] {bar:40.cyan/blue} {pos}/{len} {msg}")
            .unwrap()
            .progress_chars("=>-"),
    );
    bar.enable_steady_tick(Duration::from_millis(200));
    bar
}

pub fn inspect_mbtiles(path: &Path) -> Result<MbtilesReport> {
    inspect_mbtiles_with_options(path, InspectOptions::default())
}

pub fn inspect_mbtiles_with_options(path: &Path, options: InspectOptions) -> Result<MbtilesReport> {
    ensure_mbtiles_path(path)?;
    let conn = open_readonly_mbtiles(path)?;
    apply_read_pragmas(&conn)?;
    let metadata = read_metadata(&conn)?;

    // When sampling, skip the expensive COUNT(*) and use an estimate
    let (total_tiles, needs_counting) = if options.sample.is_some() {
        // Use a rough estimate from sqlite_stat1 or just use 0 (will be determined during scan)
        (0u64, false)
    } else {
        (0u64, true)
    };

    let spinner = if options.no_progress || !needs_counting {
        None
    } else {
        let spinner = ProgressBar::new_spinner();
        spinner.set_draw_target(ProgressDrawTarget::stderr_with_hz(20));
        spinner.set_style(
            ProgressStyle::with_template("{spinner:.cyan} {msg}")
                .unwrap()
                .tick_strings(&["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]),
        );
        spinner.set_message("counting tiles...");
        spinner.enable_steady_tick(Duration::from_millis(80));
        Some(spinner)
    };

    let total_tiles: u64 = if needs_counting {
        let count = match options.zoom {
            Some(z) => conn
                .query_row("SELECT COUNT(*) FROM tiles WHERE zoom_level = ?1", [z], |row| row.get(0))
                .context("failed to read tile count (zoom)")?,
            None => conn
                .query_row("SELECT COUNT(*) FROM tiles", [], |row| row.get(0))
                .context("failed to read tile count")?,
        };
        if let Some(spinner) = spinner {
            spinner.finish_and_clear();
        }
        count
    } else {
        total_tiles
    };

    let tile_summary = if options.summary {
        let coord = options
            .tile
            .context("--summary requires --tile z/x/y")?;
        Some(build_tile_summary(&conn, coord, options.layer.as_deref())?)
    } else {
        None
    };

    let progress = if options.no_progress {
        ProgressBar::hidden()
    } else if options.sample.is_some() {
        // Use spinner for sampling (unknown total)
        let spinner = ProgressBar::new_spinner();
        spinner.set_draw_target(ProgressDrawTarget::stderr_with_hz(20));
        spinner.set_style(
            ProgressStyle::with_template("{spinner:.cyan} {msg} ({pos} tiles processed)")
                .unwrap()
                .tick_strings(&["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]),
        );
        spinner.set_message("processing");
        spinner.enable_steady_tick(Duration::from_millis(80));
        spinner
    } else {
        let bar = make_progress_bar(total_tiles);
        bar.set_message("processing");
        bar
    };

    let mut overall = MbtilesStats {
        tile_count: 0,
        total_bytes: 0,
        max_bytes: 0,
        avg_bytes: 0,
    };

    let mut by_zoom: BTreeMap<u8, MbtilesStats> = BTreeMap::new();
    let mut zoom_minmax: BTreeMap<u8, (u64, u64)> = BTreeMap::new();
    let mut empty_tiles: u64 = 0;
    let mut processed: u64 = 0;
    let mut used: u64 = 0;

    let mut min_len: Option<u64> = None;
    let mut max_len: Option<u64> = None;

    let mut top_heap: BinaryHeap<Reverse<(u64, u8, u32, u32)>> = BinaryHeap::new();
    let mut bucket_tiles: Vec<TopTile> = Vec::new();
    let topn = options.topn;

    // Store tile sizes for histogram building (when sampling)
    let should_collect_sizes = options.sample.is_some() && options.histogram_buckets > 0;
    let mut tile_sizes: Vec<u64> = if should_collect_sizes {
        Vec::new()
    } else {
        Vec::with_capacity(0)
    };

    // Collect layer information from sampled tiles
    let collect_layers = options.sample.is_some() && options.include_layer_list;
    let mut layer_accums: BTreeMap<String, LayerAccum> = if collect_layers {
        BTreeMap::new()
    } else {
        BTreeMap::new()  // Will remain empty
    };

    // When sampling and need layer list, fetch tile_data too for layer extraction
    let need_tile_data = collect_layers;
    let query = if need_tile_data {
        "SELECT zoom_level, tile_column, tile_row, LENGTH(tile_data), tile_data FROM tiles"
    } else {
        "SELECT zoom_level, tile_column, tile_row, LENGTH(tile_data) FROM tiles"
    };
    let mut stmt = conn
        .prepare(query)
        .context("prepare tiles scan")?;
    let mut rows = stmt.query([]).context("query tiles scan")?;

    while let Some(row) = rows.next().context("read tile row")? {
        let zoom: u8 = row.get(0)?;
        let x: u32 = row.get(1)?;
        let y: u32 = row.get(2)?;
        let length: u64 = row.get(3)?;
        let tile_data: Option<Vec<u8>> = if need_tile_data {
            Some(row.get(4)?)
        } else {
            None
        };

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
            zoom_minmax
                .entry(zoom)
                .and_modify(|(min, max)| {
                    *min = (*min).min(length);
                    *max = (*max).max(length);
                })
                .or_insert((length, length));

            // Store tile size for histogram (when sampling)
            if should_collect_sizes {
                tile_sizes.push(length);
            }

            // Collect layer information (when sampling)
            if collect_layers && tile_data.is_some() {
                if let Ok(payload) = decode_tile_payload(tile_data.as_ref().unwrap()) {
                    if let Ok(reader) = Reader::new(payload) {
                        if let Ok(layers) = reader.get_layer_metadata() {
                            for layer in layers {
                                let entry = layer_accums.entry(layer.name.clone()).or_insert_with(|| LayerAccum {
                                    feature_count: 0,
                                    vertex_count: 0,
                                    property_keys: HashSet::new(),
                                    property_values: HashSet::new(),
                                });
                                entry.feature_count += layer.feature_count as u64;
                                if let Ok(features) = reader.get_features(layer.layer_index) {
                                    for feature in features {
                                        entry.vertex_count += count_vertices(&feature.geometry) as u64;
                                        if let Some(props) = feature.properties {
                                            for (key, value) in props {
                                                entry.property_keys.insert(key.clone());
                                                entry
                                                    .property_values
                                                    .insert(format_property_value(&value));
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }

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

        if processed == 1 || processed % 100 == 0 {
            progress.set_position(processed);
        }
    }

    progress.set_position(processed);
    progress.finish();

    // Build layer list from collected samples or full scan
    let file_layers = if collect_layers && !layer_accums.is_empty() {
        // Build from sampled tiles
        let mut result = layer_accums
            .into_iter()
            .map(|(name, accum)| FileLayerSummary {
                name,
                vertex_count: accum.vertex_count,
                feature_count: accum.feature_count,
                property_key_count: accum.property_keys.len(),
                property_value_count: accum.property_values.len(),
            })
            .collect::<Vec<_>>();
        result.sort_by(|a, b| a.name.cmp(&b.name));
        result
    } else if options.include_layer_list && options.sample.is_none() {
        build_file_layer_list(&conn, options.sample.as_ref(), total_tiles, options.zoom)?
    } else {
        Vec::new()
    };

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

    let zoom_counts = if options.histogram_buckets > 0 && options.zoom.is_none() {
        Some(fetch_zoom_counts(&conn)?)
    } else {
        None
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

        // If sampling, build histogram from collected tile sizes (faster)
        if !tile_sizes.is_empty() {
            build_histogram_from_sizes(
                &tile_sizes,
                level_tiles_used,
                level_bytes_used,
                options.histogram_buckets,
                min_len.unwrap(),
                max_len.unwrap(),
                options.max_tile_bytes,
            )
        } else {
            // Full scan required
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
                options.no_progress,
            )?
        }
    } else {
        Vec::new()
    };

    let histograms_by_zoom = if options.histogram_buckets > 0 && options.zoom.is_none() && options.sample.is_none() {
        let zoom_counts = zoom_counts.as_ref().expect("zoom counts");
        build_zoom_histograms(
            path,
            options.sample.as_ref(),
            zoom_counts,
            &zoom_minmax,
            options.histogram_buckets,
            options.max_tile_bytes,
            options.no_progress,
            total_tiles,
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
        metadata,
        overall,
        by_zoom,
        empty_tiles,
        empty_ratio,
        sampled: options.sample.is_some(),
        sample_total_tiles: total_tiles,
        sample_used_tiles: used,
        histogram,
        histograms_by_zoom,
        file_layers,
        top_tiles,
        bucket_count,
        bucket_tiles,
        tile_summary,
        recommended_buckets,
        top_tile_summaries,
    })
}

fn read_metadata(conn: &Connection) -> Result<BTreeMap<String, String>> {
    let mut metadata = BTreeMap::new();
    let mut stmt = match conn.prepare("SELECT name, value FROM metadata") {
        Ok(stmt) => stmt,
        Err(err) => {
            if err.to_string().contains("no such table") {
                return Ok(metadata);
            }
            return Err(err).context("prepare metadata");
        }
    };
    let mut rows = stmt.query([]).context("query metadata")?;
    while let Some(row) = rows.next().context("read metadata row")? {
        let name: String = row.get(0)?;
        let value: String = row.get(1)?;
        metadata.insert(name, value);
    }
    Ok(metadata)
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

#[derive(Debug, Default)]
pub struct PruneStats {
    pub removed_features_by_zoom: BTreeMap<u8, u64>,
    pub removed_layers_by_zoom: BTreeMap<String, BTreeSet<u8>>,
    pub unknown_filters: usize,
}

impl PruneStats {
    fn record_removed_features(&mut self, zoom: u8, count: u64) {
        if count == 0 {
            return;
        }
        *self.removed_features_by_zoom.entry(zoom).or_insert(0) += count;
    }

    fn record_removed_layer(&mut self, layer: &str, zoom: u8) {
        self.removed_layers_by_zoom
            .entry(layer.to_string())
            .or_default()
            .insert(zoom);
    }
}

pub fn prune_mbtiles_layer_only(
    input: &Path,
    output: &Path,
    style: &crate::style::MapboxStyle,
    apply_filters: bool,
) -> Result<PruneStats> {
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

    let mut meta_stmt = input_conn
        .prepare("SELECT name, value FROM metadata")
        .context("prepare metadata read")?;
    let mut meta_rows = meta_stmt.query([]).context("query metadata")?;
    while let Some(row) = meta_rows.next().context("read metadata row")? {
        let name: String = row.get(0)?;
        let value: String = row.get(1)?;
        tx.execute(
            "INSERT INTO metadata (name, value) VALUES (?1, ?2)",
            (name, value),
        )
        .context("insert metadata")?;
    }

    let mut stmt = input_conn
        .prepare(
            "SELECT zoom_level, tile_column, tile_row, tile_data FROM tiles ORDER BY zoom_level, tile_column, tile_row",
        )
        .context("prepare tile scan")?;
    let mut rows = stmt.query([]).context("query tiles")?;

    let keep_layers = style.source_layers();
    let mut stats = PruneStats::default();
    while let Some(row) = rows.next().context("read tile row")? {
        let zoom: u8 = row.get(0)?;
        let x: u32 = row.get(1)?;
        let y: u32 = row.get(2)?;
        let data: Vec<u8> = row.get(3)?;

        let is_gzip = data.starts_with(&[0x1f, 0x8b]);
        let payload = decode_tile_payload(&data)?;
        let encoded = prune_tile_layers(
            &payload,
            zoom,
            style,
            &keep_layers,
            apply_filters,
            &mut stats,
        )?;
        let tile_data = encode_tile_payload(&encoded, is_gzip)?;

        tx.execute(
            "INSERT INTO tiles (zoom_level, tile_column, tile_row, tile_data) VALUES (?1, ?2, ?3, ?4)",
            (zoom as i64, x as i64, y as i64, tile_data),
        )
        .context("insert tile")?;
    }

    tx.commit().context("commit output")?;
    if apply_filters && stats.unknown_filters > 0 {
        warn!(count = stats.unknown_filters, "unknown filter expressions encountered");
    }
    Ok(stats)
}
