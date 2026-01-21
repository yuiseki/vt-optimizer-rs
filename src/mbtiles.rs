use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap, HashSet};
use std::io::{Read, Write};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::Duration;

use anyhow::{Context, Result};
use crossbeam_channel::{Receiver, Sender, bounded};
use flate2::Compression;
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use geo_types::{
    Coord, Geometry, Line, LineString, MultiLineString, MultiPoint, MultiPolygon, Polygon,
};
use indicatif::{ProgressBar, ProgressDrawTarget, ProgressStyle};
use mvt::{GeomData, GeomEncoder, GeomType, Tile};
use mvt_reader::Reader;
use rayon::prelude::*;
use rusqlite::{Connection, OpenFlags, params};
use serde::Serialize;
use tracing::warn;

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
    pub over_limit_tiles: u64,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SimplifyStats {
    pub feature_count: u64,
    pub vertices_before: u64,
    pub vertices_after: u64,
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
    pub vertex_count: u64,
    pub property_key_count: usize,
    pub property_value_count: usize,
    #[serde(skip_serializing_if = "Vec::is_empty")]
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
    pub tile_bytes: u64,
    pub layer_count: usize,
    pub total_features: usize,
    pub vertex_count: u64,
    pub property_key_count: usize,
    pub property_value_count: usize,
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
    pub layers: Vec<String>,
    pub recommend: bool,
    pub include_layer_list: bool,
    pub list_tiles: Option<TileListOptions>,
}

#[allow(clippy::derivable_impls)]
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
            layers: Vec::new(),
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

pub(crate) fn encode_tile_payload(data: &[u8], gzip: bool) -> Result<Vec<u8>> {
    if !gzip {
        return Ok(data.to_vec());
    }
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(data).context("encode gzip tile data")?;
    let encoded = encoder.finish().context("finish gzip tile data")?;
    Ok(encoded)
}

pub(crate) fn count_vertices(geometry: &geo_types::Geometry<f32>) -> usize {
    match geometry {
        geo_types::Geometry::Point(_) => 1,
        geo_types::Geometry::MultiPoint(points) => points.len(),
        geo_types::Geometry::LineString(line) => ring_coords(line).len(),
        geo_types::Geometry::MultiLineString(lines) => {
            lines.iter().map(|l| ring_coords(l).len()).sum()
        }
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
            collection.iter().map(count_vertices).sum()
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
                let mut rings: Vec<&LineString<f32>> =
                    Vec::with_capacity(1 + polygon.interiors().len());
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

pub(crate) struct PrunedTile {
    pub bytes: Vec<u8>,
    pub empty: bool,
}

pub(crate) fn prune_tile_layers(
    payload: &[u8],
    zoom: u8,
    style: &crate::style::MapboxStyle,
    keep_layers: &HashSet<String>,
    apply_filters: bool,
    keep_unknown_filters: bool,
    stats: &mut PruneStats,
) -> Result<PrunedTile> {
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
    let mut kept_layers = 0u32;
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
                match style.should_keep_feature(
                    &layer.name,
                    zoom,
                    &feature,
                    &mut stats.unknown_filters,
                ) {
                    crate::style::FilterResult::True => {}
                    crate::style::FilterResult::Unknown => {
                        stats.record_unknown_layer(&layer.name);
                        if !keep_unknown_filters {
                            continue;
                        }
                    }
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
        kept_layers += 1;
    }

    let bytes = tile
        .to_bytes()
        .map_err(|err| anyhow::anyhow!("encode vector tile: {err}"))?;
    Ok(PrunedTile {
        bytes,
        empty: kept_layers == 0,
    })
}

pub(crate) fn simplify_tile_payload(
    payload: &[u8],
    keep_layers: &HashSet<String>,
    tolerance: Option<f64>,
) -> Result<(Vec<u8>, SimplifyStats)> {
    let reader = Reader::new(payload.to_vec())
        .map_err(|err| anyhow::anyhow!("decode vector tile: {err}"))?;
    let layers = reader
        .get_layer_metadata()
        .map_err(|err| anyhow::anyhow!("read layer metadata: {err}"))?;

    let mut extent = 4096;
    for layer in layers.iter() {
        if keep_layers.is_empty() || keep_layers.contains(&layer.name) {
            extent = layer.extent;
            break;
        }
    }

    let mut tile = Tile::new(extent);
    let mut stats = SimplifyStats {
        feature_count: 0,
        vertices_before: 0,
        vertices_after: 0,
    };
    for layer in layers {
        if !keep_layers.is_empty() && !keep_layers.contains(&layer.name) {
            continue;
        }
        let mut layer_builder = tile.create_layer(&layer.name);
        let features = reader
            .get_features(layer.layer_index)
            .map_err(|err| anyhow::anyhow!("read layer features: {err}"))?;
        for feature in features {
            let geometry = feature.get_geometry();
            stats.feature_count += 1;
            stats.vertices_before += count_vertices(geometry) as u64;
            let geometry = match tolerance {
                Some(value) if value > 0.0 => simplify_geometry(geometry, value as f32),
                _ => geometry.clone(),
            };
            stats.vertices_after += count_vertices(&geometry) as u64;
            let geom_data = encode_geometry(&geometry)?;
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
        }
        tile.add_layer(layer_builder)
            .map_err(|err| anyhow::anyhow!("add layer: {err}"))?;
    }

    tile.to_bytes()
        .map_err(|err| anyhow::anyhow!("encode vector tile: {err}"))
        .map(|bytes| (bytes, stats))
}

fn fetch_tile_data(conn: &Connection, coord: TileCoord) -> Result<Option<Vec<u8>>> {
    let query = select_tile_data_query(conn)?;
    let mut stmt = conn.prepare(&query).context("prepare tile data")?;
    let mut rows = stmt
        .query(params![coord.zoom, coord.x, coord.y])
        .context("query tile data")?;
    if let Some(row) = rows.next().context("read tile row")? {
        let data: Vec<u8> = row.get(0)?;
        Ok(Some(data))
    } else {
        Ok(None)
    }
}

fn simplify_geometry(geometry: &Geometry<f32>, tolerance: f32) -> Geometry<f32> {
    if tolerance <= 0.0 {
        return geometry.clone();
    }

    match geometry {
        Geometry::LineString(line) => {
            let simplified = simplify_line(&line.0, tolerance);
            Geometry::LineString(LineString::from(simplified))
        }
        Geometry::MultiLineString(lines) => {
            let simplified = lines
                .0
                .iter()
                .map(|line| LineString::from(simplify_line(&line.0, tolerance)))
                .collect::<Vec<_>>();
            Geometry::MultiLineString(MultiLineString(simplified))
        }
        Geometry::Polygon(polygon) => {
            let exterior = simplify_ring(&polygon.exterior().0, tolerance);
            let interiors = polygon
                .interiors()
                .iter()
                .map(|ring| simplify_ring(&ring.0, tolerance))
                .map(LineString::from)
                .collect::<Vec<_>>();
            Geometry::Polygon(Polygon::new(LineString::from(exterior), interiors))
        }
        Geometry::MultiPolygon(polygons) => {
            let simplified = polygons
                .0
                .iter()
                .map(|polygon| {
                    let exterior = simplify_ring(&polygon.exterior().0, tolerance);
                    let interiors = polygon
                        .interiors()
                        .iter()
                        .map(|ring| simplify_ring(&ring.0, tolerance))
                        .map(LineString::from)
                        .collect::<Vec<_>>();
                    Polygon::new(LineString::from(exterior), interiors)
                })
                .collect::<Vec<_>>();
            Geometry::MultiPolygon(MultiPolygon(simplified))
        }
        _ => geometry.clone(),
    }
}

fn simplify_ring(points: &[Coord<f32>], tolerance: f32) -> Vec<Coord<f32>> {
    if points.len() <= 4 {
        return points.to_vec();
    }

    let closed = points.first() == points.last();
    let core = if closed {
        points[..points.len() - 1].to_vec()
    } else {
        points.to_vec()
    };
    let simplified = simplify_line(&core, tolerance);
    if simplified.len() < 3 {
        return points.to_vec();
    }
    let mut out = simplified;
    if closed {
        out.push(out[0]);
    }
    out
}

fn simplify_line(points: &[Coord<f32>], tolerance: f32) -> Vec<Coord<f32>> {
    if points.len() <= 2 {
        return points.to_vec();
    }
    let sq_tolerance = tolerance * tolerance;
    let mut reduced = simplify_radial_dist(points, sq_tolerance);
    if reduced.len() <= 2 {
        return reduced;
    }
    reduced = simplify_douglas_peucker(&reduced, sq_tolerance);
    reduced
}

fn simplify_radial_dist(points: &[Coord<f32>], sq_tolerance: f32) -> Vec<Coord<f32>> {
    let mut prev = points[0];
    let mut out = vec![prev];
    for point in points.iter().skip(1) {
        if get_sq_dist(*point, prev) > sq_tolerance {
            out.push(*point);
            prev = *point;
        }
    }
    if prev != *points.last().unwrap() {
        out.push(*points.last().unwrap());
    }
    out
}

// Ramer–Douglas–Peucker algorithm
fn simplify_douglas_peucker(points: &[Coord<f32>], sq_tolerance: f32) -> Vec<Coord<f32>> {
    let last = points.len() - 1;
    let mut simplified = vec![points[0]];
    simplify_dp_step(points, 0, last, sq_tolerance, &mut simplified);
    simplified.push(points[last]);
    simplified
}

fn simplify_dp_step(
    points: &[Coord<f32>],
    first: usize,
    last: usize,
    sq_tolerance: f32,
    simplified: &mut Vec<Coord<f32>>,
) {
    let mut max_sq_dist = sq_tolerance;
    let mut index = None;

    for i in (first + 1)..last {
        let sq_dist = get_sq_seg_dist(points[i], points[first], points[last]);
        if sq_dist > max_sq_dist {
            index = Some(i);
            max_sq_dist = sq_dist;
        }
    }

    if let Some(idx) = index {
        if idx - first > 1 {
            simplify_dp_step(points, first, idx, sq_tolerance, simplified);
        }
        simplified.push(points[idx]);
        if last - idx > 1 {
            simplify_dp_step(points, idx, last, sq_tolerance, simplified);
        }
    }
}

fn get_sq_dist(p1: Coord<f32>, p2: Coord<f32>) -> f32 {
    let dx = p1.x - p2.x;
    let dy = p1.y - p2.y;
    dx * dx + dy * dy
}

fn get_sq_seg_dist(p: Coord<f32>, p1: Coord<f32>, p2: Coord<f32>) -> f32 {
    let mut x = p1.x;
    let mut y = p1.y;
    let dx = p2.x - x;
    let dy = p2.y - y;

    if dx != 0.0 || dy != 0.0 {
        let t = ((p.x - x) * dx + (p.y - y) * dy) / (dx * dx + dy * dy);
        if t > 1.0 {
            x = p2.x;
            y = p2.y;
        } else if t > 0.0 {
            x += dx * t;
            y += dy * t;
        }
    }

    let dx = p.x - x;
    let dy = p.y - y;
    dx * dx + dy * dy
}

struct LayerAccum {
    feature_count: u64,
    vertex_count: u64,
    property_keys: HashSet<String>,
    property_values: HashSet<String>,
}

impl LayerAccum {
    fn new() -> Self {
        Self {
            feature_count: 0,
            vertex_count: 0,
            property_keys: HashSet::new(),
            property_values: HashSet::new(),
        }
    }
}

fn build_file_layer_list(
    conn: &Connection,
    sample: Option<&SampleSpec>,
    total_tiles: u64,
    zoom: Option<u8>,
    no_progress: bool,
) -> Result<Vec<FileLayerSummary>> {
    let data_expr = tiles_data_expr(conn)?;
    let source = tiles_source_clause(conn)?;
    let zoom_col = if source == "tiles" {
        "zoom_level"
    } else {
        "map.zoom_level"
    };
    let query = format!("SELECT {zoom_col}, {data_expr} FROM {source}");
    let mut stmt = conn.prepare(&query).context("prepare layer list scan")?;
    let mut rows = stmt.query([]).context("query layer list scan")?;

    let mut index: u64 = 0;
    let mut tiles: Vec<Vec<u8>> = Vec::new();
    let read_progress = if no_progress {
        ProgressBar::hidden()
    } else if total_tiles > 0 {
        let bar = make_progress_bar(total_tiles);
        bar.set_message("reading layers");
        bar
    } else {
        let spinner = ProgressBar::new_spinner();
        spinner.set_draw_target(ProgressDrawTarget::stderr_with_hz(20));
        spinner.set_style(
            ProgressStyle::with_template("[{elapsed_precise}] {spinner:.cyan} {msg}")
                .unwrap()
                .tick_strings(&["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]),
        );
        spinner.set_message("reading layers");
        spinner.enable_steady_tick(Duration::from_millis(80));
        spinner
    };

    while let Some(row) = rows.next().context("read layer list row")? {
        let row_zoom: u8 = row.get(0)?;
        if let Some(target) = zoom
            && row_zoom != target
        {
            continue;
        }
        index += 1;
        if !include_sample(index, total_tiles, sample) {
            continue;
        }
        let data: Vec<u8> = row.get(1)?;
        tiles.push(data);

        if let Some(SampleSpec::Count(limit)) = sample
            && index >= *limit
        {
            break;
        }

        if index == 1 || index.is_multiple_of(100) {
            read_progress.set_position(index);
        }
    }

    read_progress.set_position(index);
    read_progress.finish();

    let processing = if no_progress {
        ProgressBar::hidden()
    } else {
        let bar = make_progress_bar(tiles.len() as u64);
        bar.set_message("processing layers");
        bar
    };

    let map = tiles
        .into_par_iter()
        .map(|data| {
            let result = (|| -> Result<BTreeMap<String, LayerAccum>> {
                let payload = decode_tile_payload(&data)?;
                let reader = Reader::new(payload)
                    .map_err(|err| anyhow::anyhow!("decode vector tile: {err}"))?;
                let layers = reader
                    .get_layer_metadata()
                    .map_err(|err| anyhow::anyhow!("read layer metadata: {err}"))?;
                let mut local = BTreeMap::new();
                for layer in layers {
                    let entry = local
                        .entry(layer.name.clone())
                        .or_insert_with(LayerAccum::new);
                    entry.feature_count += layer.feature_count as u64;
                    let features = reader
                        .get_features(layer.layer_index)
                        .map_err(|err| anyhow::anyhow!("read layer features: {err}"))?;
                    for feature in features {
                        entry.vertex_count += count_vertices(&feature.geometry) as u64;
                        if let Some(props) = feature.properties {
                            for (key, value) in props {
                                entry.property_keys.insert(key);
                                entry.property_values.insert(format_property_value(&value));
                            }
                        }
                    }
                }
                Ok(local)
            })();
            processing.inc(1);
            result
        })
        .reduce(
            || Ok(BTreeMap::new()),
            |left, right| -> Result<BTreeMap<String, LayerAccum>> {
                let mut left = left?;
                let right = right?;
                for (name, accum) in right {
                    let entry = left.entry(name).or_insert_with(LayerAccum::new);
                    entry.feature_count += accum.feature_count;
                    entry.vertex_count += accum.vertex_count;
                    entry.property_keys.extend(accum.property_keys);
                    entry.property_values.extend(accum.property_values);
                }
                Ok(left)
            },
        )?;

    processing.finish();

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
    layers_filter: &[String],
) -> Result<TileSummary> {
    let query = select_tile_data_query(conn)?;
    let data: Vec<u8> = conn
        .query_row(&query, params![coord.zoom, coord.x, coord.y], |row| {
            row.get(0)
        })
        .context("failed to read tile data")?;
    let tile_bytes = u64::try_from(data.len()).context("tile data size overflow")?;
    let payload = decode_tile_payload(&data)?;
    let reader =
        Reader::new(payload).map_err(|err| anyhow::anyhow!("decode vector tile: {err}"))?;
    let layers = reader
        .get_layer_metadata()
        .map_err(|err| anyhow::anyhow!("read layer metadata: {err}"))?;
    let mut total_features = 0usize;
    let mut total_vertices = 0u64;
    let mut tile_keys: HashSet<String> = HashSet::new();
    let mut tile_values: HashSet<String> = HashSet::new();
    let mut summaries = Vec::new();
    let filter_set = if layers_filter.is_empty() {
        None
    } else {
        Some(layers_filter.iter().cloned().collect::<HashSet<_>>())
    };
    for layer in layers {
        if let Some(filter) = filter_set.as_ref()
            && !filter.contains(&layer.name)
        {
            continue;
        }
        let features = reader
            .get_features(layer.layer_index)
            .map_err(|err| anyhow::anyhow!("read layer features: {err}"))?;
        let mut keys = HashSet::new();
        let mut values = HashSet::new();
        let mut vertex_count = 0u64;
        let mut feature_count = 0usize;
        for feature in features {
            feature_count += 1;
            vertex_count += count_vertices(&feature.geometry) as u64;
            if let Some(props) = feature.properties {
                for (key, value) in props {
                    keys.insert(key.clone());
                    tile_keys.insert(key);
                    let value_text = format_property_value(&value);
                    values.insert(value_text.clone());
                    tile_values.insert(value_text);
                }
            }
        }
        let mut key_list = keys.into_iter().collect::<Vec<_>>();
        key_list.sort();
        total_features += feature_count;
        total_vertices += vertex_count;
        summaries.push(LayerSummary {
            name: layer.name,
            feature_count,
            vertex_count,
            property_key_count: key_list.len(),
            property_value_count: values.len(),
            property_keys: key_list,
        });
    }
    Ok(TileSummary {
        zoom: coord.zoom,
        x: coord.x,
        y: coord.y,
        tile_bytes,
        layer_count: summaries.len(),
        total_features,
        vertex_count: total_vertices,
        property_key_count: tile_keys.len(),
        property_value_count: tile_values.len(),
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
    result
}

#[allow(clippy::too_many_arguments)]
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
    let range = (max_len - min_len).max(1);
    let bucket_size = ((range as f64) / buckets as f64).ceil() as u64;
    let tile_source = tiles_source_clause(&conn)?;
    let allow_column_chunk = sample.is_none() && tile_source == "tiles";
    let chunk_count = (rayon::current_num_threads() as u64)
        .saturating_mul(4)
        .max(1);
    let query = select_zoom_length_by_zoom_query(&conn)?;
    let query_with_column_range = select_zoom_length_by_zoom_and_column_range_query(&conn)?;
    let zoom_counts = fetch_zoom_counts(&conn)?;
    let zooms = if let Some(target) = zoom {
        vec![target]
    } else {
        zoom_counts.keys().copied().collect::<Vec<_>>()
    };
    let processed = Arc::new(AtomicU64::new(0));
    let progress = progress.clone();

    let mut tasks = Vec::new();
    for zoom in &zooms {
        if allow_column_chunk && *zoom >= 12 {
            if let Some(ranges) = tile_column_chunks(*zoom, chunk_count) {
                for range in ranges {
                    tasks.push((*zoom, Some(range)));
                }
            } else {
                tasks.push((*zoom, None));
            }
        } else {
            tasks.push((*zoom, None));
        }
    }

    let (counts, bytes) = tasks
        .into_par_iter()
        .map(|(zoom, range)| -> Result<(Vec<u64>, Vec<u64>)> {
            let conn = open_readonly_mbtiles(path)?;
            apply_read_pragmas(&conn)?;
            let mut stmt = if range.is_some() {
                conn.prepare(&query_with_column_range)
                    .context("prepare histogram scan (column range)")?
            } else {
                conn.prepare(&query).context("prepare histogram scan")?
            };
            let mut rows = if let Some((col_min, col_max)) = range {
                stmt.query(params![zoom, col_min, col_max])
                    .context("query histogram scan (column range)")?
            } else {
                stmt.query([zoom]).context("query histogram scan")?
            };

            let total_tiles_db = *zoom_counts.get(&zoom).unwrap_or(&0);
            let mut index: u64 = 0;
            let mut used: u64 = 0;
            let mut local_counts = vec![0u64; buckets];
            let mut local_bytes = vec![0u64; buckets];
            let mut batch: u64 = 0;

            while let Some(row) = rows.next().context("read histogram row")? {
                let length: i64 = row.get(0)?;
                let length = u64::try_from(length).context("tile length must be non-negative")?;
                index += 1;
                batch += 1;
                if batch >= 1000 {
                    let total = processed.fetch_add(batch, Ordering::Relaxed) + batch;
                    progress.set_position(total);
                    batch = 0;
                }

                if !include_sample(index, total_tiles_db, sample) {
                    continue;
                }
                used += 1;
                let mut bucket = ((length.saturating_sub(min_len)) / bucket_size) as usize;
                if bucket >= buckets {
                    bucket = buckets - 1;
                }
                local_counts[bucket] += 1;
                local_bytes[bucket] += length;

                if let Some(SampleSpec::Count(limit)) = sample
                    && used >= *limit
                {
                    break;
                }
            }

            if batch > 0 {
                let total = processed.fetch_add(batch, Ordering::Relaxed) + batch;
                progress.set_position(total);
            }

            Ok((local_counts, local_bytes))
        })
        .reduce(
            || Ok((vec![0u64; buckets], vec![0u64; buckets])),
            |left, right| -> Result<(Vec<u64>, Vec<u64>)> {
                let mut left = left?;
                let right = right?;
                for i in 0..buckets {
                    left.0[i] += right.0[i];
                    left.1[i] += right.1[i];
                }
                Ok(left)
            },
        )?;

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
    let tile_source = tiles_source_clause(&conn)?;
    let allow_column_chunk = sample.is_none() && tile_source == "tiles";
    let chunk_count = (rayon::current_num_threads() as u64)
        .saturating_mul(4)
        .max(1);
    let query = select_zoom_length_by_zoom_query(&conn)?;
    let query_with_column_range = select_zoom_length_by_zoom_and_column_range_query(&conn)?;

    #[derive(Clone, Copy)]
    struct ZoomConfig {
        min_len: u64,
        max_len: u64,
        bucket_size: u64,
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

    let mut configs: BTreeMap<u8, ZoomConfig> = BTreeMap::new();
    for (zoom, (min_len, max_len)) in zoom_minmax.iter() {
        let range = (max_len - min_len).max(1);
        let bucket_size = ((range as f64) / buckets as f64).ceil() as u64;
        configs.insert(
            *zoom,
            ZoomConfig {
                min_len: *min_len,
                max_len: *max_len,
                bucket_size,
            },
        );
    }

    let zooms = configs.keys().copied().collect::<Vec<_>>();
    let mut tasks = Vec::new();
    for zoom in &zooms {
        if allow_column_chunk && *zoom >= 12 {
            if let Some(ranges) = tile_column_chunks(*zoom, chunk_count) {
                for range in ranges {
                    tasks.push((*zoom, Some(range)));
                }
            } else {
                tasks.push((*zoom, None));
            }
        } else {
            tasks.push((*zoom, None));
        }
    }
    let processed = Arc::new(AtomicU64::new(0));
    let progress = progress.clone();

    let accums = tasks
        .into_par_iter()
        .map(|(zoom, range)| -> Result<(u8, ZoomAccum)> {
            let conn = open_readonly_mbtiles(path)?;
            apply_read_pragmas(&conn)?;
            let mut stmt = if range.is_some() {
                conn.prepare(&query_with_column_range)
                    .context("prepare zoom histogram scan (column range)")?
            } else {
                conn.prepare(&query)
                    .context("prepare zoom histogram scan")?
            };
            let mut rows = if let Some((col_min, col_max)) = range {
                stmt.query(params![zoom, col_min, col_max])
                    .context("query zoom histogram scan (column range)")?
            } else {
                stmt.query([zoom]).context("query zoom histogram scan")?
            };

            let config = configs.get(&zoom).expect("zoom histogram config missing");
            let mut accum = ZoomAccum {
                min_len: config.min_len,
                max_len: config.max_len,
                bucket_size: config.bucket_size,
                counts: vec![0u64; buckets],
                bytes: vec![0u64; buckets],
                used_tiles: 0,
                used_bytes: 0,
            };
            let total_tiles_db = *zoom_counts.get(&zoom).unwrap_or(&0);
            let mut index: u64 = 0;
            let mut batch: u64 = 0;

            while let Some(row) = rows.next().context("read zoom histogram row")? {
                let length: i64 = row.get(0)?;
                let length = u64::try_from(length).context("tile length must be non-negative")?;
                index += 1;
                batch += 1;
                if batch >= 1000 {
                    let total = processed.fetch_add(batch, Ordering::Relaxed) + batch;
                    progress.set_position(total);
                    batch = 0;
                }

                if !include_sample(index, total_tiles_db, sample) {
                    continue;
                }
                let mut bucket =
                    ((length.saturating_sub(accum.min_len)) / accum.bucket_size) as usize;
                if bucket >= buckets {
                    bucket = buckets - 1;
                }
                accum.counts[bucket] += 1;
                accum.bytes[bucket] += length;
                accum.used_tiles += 1;
                accum.used_bytes += length;

                if let Some(SampleSpec::Count(limit)) = sample
                    && accum.used_tiles >= *limit
                {
                    break;
                }
            }

            if batch > 0 {
                let total = processed.fetch_add(batch, Ordering::Relaxed) + batch;
                progress.set_position(total);
            }

            Ok((zoom, accum))
        })
        .try_fold(
            BTreeMap::new,
            |mut map, item| -> Result<BTreeMap<u8, ZoomAccum>> {
                let (zoom, accum) = item?;
                let entry = map.entry(zoom).or_insert_with(|| ZoomAccum {
                    min_len: accum.min_len,
                    max_len: accum.max_len,
                    bucket_size: accum.bucket_size,
                    counts: vec![0u64; buckets],
                    bytes: vec![0u64; buckets],
                    used_tiles: 0,
                    used_bytes: 0,
                });
                for i in 0..buckets {
                    entry.counts[i] += accum.counts[i];
                    entry.bytes[i] += accum.bytes[i];
                }
                entry.used_tiles += accum.used_tiles;
                entry.used_bytes += accum.used_bytes;
                Ok(map)
            },
        )
        .try_reduce(
            BTreeMap::new,
            |mut left, right| -> Result<BTreeMap<u8, ZoomAccum>> {
                for (zoom, accum) in right {
                    let entry = left.entry(zoom).or_insert_with(|| ZoomAccum {
                        min_len: accum.min_len,
                        max_len: accum.max_len,
                        bucket_size: accum.bucket_size,
                        counts: vec![0u64; buckets],
                        bytes: vec![0u64; buckets],
                        used_tiles: 0,
                        used_bytes: 0,
                    });
                    for i in 0..buckets {
                        entry.counts[i] += accum.counts[i];
                        entry.bytes[i] += accum.bytes[i];
                    }
                    entry.used_tiles += accum.used_tiles;
                    entry.used_bytes += accum.used_bytes;
                }
                Ok(left)
            },
        )?;

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
    apply_read_pragmas_with_cache(conn, Some(200))
}

fn apply_read_pragmas_with_cache(conn: &Connection, cache_mb: Option<u64>) -> Result<()> {
    let cache_kb = cache_mb.unwrap_or(200).saturating_mul(1024);
    conn.execute_batch(&format!(
        "
        PRAGMA query_only = ON;
        PRAGMA temp_store = MEMORY;
        PRAGMA synchronous = OFF;
        PRAGMA cache_size = -{cache_kb};
        "
    ))
    .context("failed to apply read pragmas")?;
    Ok(())
}

fn apply_write_pragmas_with_cache(conn: &Connection, cache_mb: Option<u64>) -> Result<()> {
    let cache_kb = cache_mb.unwrap_or(200).saturating_mul(1024);
    conn.execute_batch(&format!(
        "
        PRAGMA journal_mode = WAL;
        PRAGMA synchronous = OFF;
        PRAGMA temp_store = MEMORY;
        PRAGMA cache_size = -{cache_kb};
        "
    ))
    .context("failed to apply write pragmas")?;
    Ok(())
}

fn supports_rowid(conn: &Connection, table: &str) -> Result<bool> {
    let query = format!("SELECT rowid FROM {table} LIMIT 1",);
    match conn.query_row(&query, [], |_row| Ok(())) {
        Ok(_) => Ok(true),
        Err(_) => Ok(false),
    }
}

fn fetch_zoom_counts(conn: &Connection) -> Result<BTreeMap<u8, u64>> {
    let source = tiles_count_source_clause(conn)?;
    let zoom_col = if source == "map" {
        "map.zoom_level"
    } else {
        "zoom_level"
    };
    let query = format!("SELECT {zoom_col}, COUNT(*) FROM {source} GROUP BY {zoom_col}",);
    let mut stmt = conn.prepare(&query).context("prepare zoom counts")?;
    let mut rows = stmt.query([]).context("query zoom counts")?;
    let mut counts = BTreeMap::new();
    while let Some(row) = rows.next().context("read zoom count row")? {
        let zoom: u8 = row.get(0)?;
        let count: i64 = row.get(1)?;
        let count = u64::try_from(count).context("tile count must be non-negative")?;
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

#[allow(clippy::unnecessary_unwrap)]
pub fn inspect_mbtiles_with_options(path: &Path, options: InspectOptions) -> Result<MbtilesReport> {
    ensure_mbtiles_path(path)?;
    let conn = open_readonly_mbtiles(path)?;
    apply_read_pragmas(&conn)?;
    let metadata = read_metadata(&conn)?;

    // When sampling, avoid COUNT(*) and use per-zoom counts for sampling decisions.
    let (mut total_tiles, needs_counting) = if options.sample.is_some() {
        (0u64, false)
    } else {
        (0u64, true)
    };
    let mut zoom_counts: Option<BTreeMap<u8, u64>> = None;
    if options.sample.is_some() {
        let counts = fetch_zoom_counts(&conn)?;
        total_tiles = counts.values().sum();
        zoom_counts = Some(counts);
    }

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
        let query = select_tile_count_query(&conn, options.zoom.is_some())?;
        let count = match options.zoom {
            Some(z) => conn
                .query_row(&query, [z], |row| row.get::<_, i64>(0))
                .context("failed to read tile count (zoom)")?,
            None => conn
                .query_row(&query, [], |row| row.get::<_, i64>(0))
                .context("failed to read tile count")?,
        };
        let count = u64::try_from(count).context("tile count must be non-negative")?;
        if let Some(spinner) = spinner {
            spinner.finish_and_clear();
        }
        count
    } else {
        total_tiles
    };

    let tile_summary = if options.summary {
        let coord = options.tile.context("--summary requires --tile z/x/y")?;
        Some(build_tile_summary(&conn, coord, &options.layers)?)
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
    let mut over_limit_tiles: u64 = 0;
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
    let mut layer_accums: BTreeMap<String, LayerAccum> = BTreeMap::new();

    let zoom_counts_for_scan = if let Some(counts) = zoom_counts.as_ref() {
        counts.clone()
    } else {
        fetch_zoom_counts(&conn)?
    };
    let tile_source = tiles_source_clause(&conn)?;
    let allow_column_chunk = options.sample.is_none() && tile_source == "tiles";
    let chunk_count = (rayon::current_num_threads() as u64)
        .saturating_mul(4)
        .max(1);
    let zooms = if let Some(target) = options.zoom {
        if zoom_counts_for_scan.get(&target).copied().unwrap_or(0) > 0 {
            vec![target]
        } else {
            Vec::new()
        }
    } else {
        zoom_counts_for_scan.keys().copied().collect::<Vec<_>>()
    };
    struct Pass1Accum {
        zoom: u8,
        stats: MbtilesStats,
        min_len: Option<u64>,
        max_len: Option<u64>,
        empty_tiles: u64,
        over_limit_tiles: u64,
        top_heap: BinaryHeap<Reverse<(u64, u8, u32, u32)>>,
        tile_sizes: Vec<u64>,
        layer_accums: BTreeMap<String, LayerAccum>,
        used: u64,
    }

    // When sampling and need layer list, fetch tile_data too for layer extraction
    let need_tile_data = collect_layers;
    let query = select_tiles_query_by_zoom(&conn, need_tile_data)?;
    let query_with_column_range =
        select_tiles_query_by_zoom_and_column_range(&conn, need_tile_data)?;
    let processed = Arc::new(AtomicU64::new(0));
    let progress = progress.clone();

    let mut pass1_tasks = Vec::new();
    for zoom in &zooms {
        if allow_column_chunk && *zoom >= 12 {
            if let Some(ranges) = tile_column_chunks(*zoom, chunk_count) {
                for range in ranges {
                    pass1_tasks.push((*zoom, Some(range)));
                }
            } else {
                pass1_tasks.push((*zoom, None));
            }
        } else {
            pass1_tasks.push((*zoom, None));
        }
    }

    let pass1 = pass1_tasks
        .into_par_iter()
        .map(|(zoom, range)| -> Result<Pass1Accum> {
            let conn = open_readonly_mbtiles(path)?;
            apply_read_pragmas(&conn)?;
            let mut stmt = if range.is_some() {
                conn.prepare(&query_with_column_range)
                    .context("prepare tiles scan (column range)")?
            } else {
                conn.prepare(&query).context("prepare tiles scan")?
            };
            let mut rows = if let Some((col_min, col_max)) = range {
                stmt.query(params![zoom, col_min, col_max])
                    .context("query tiles scan (column range)")?
            } else {
                stmt.query([zoom]).context("query tiles scan")?
            };

            let total_tiles_db = *zoom_counts_for_scan.get(&zoom).unwrap_or(&0);
            let mut index: u64 = 0;
            let mut used: u64 = 0;
            let mut stats = MbtilesStats {
                tile_count: 0,
                total_bytes: 0,
                max_bytes: 0,
                avg_bytes: 0,
            };
            let mut local_min_len: Option<u64> = None;
            let mut local_max_len: Option<u64> = None;
            let mut empty_tiles: u64 = 0;
            let mut over_limit_tiles: u64 = 0;
            let mut top_heap: BinaryHeap<Reverse<(u64, u8, u32, u32)>> = BinaryHeap::new();
            let mut tile_sizes: Vec<u64> = if should_collect_sizes {
                Vec::new()
            } else {
                Vec::with_capacity(0)
            };
            let mut layer_accums: BTreeMap<String, LayerAccum> = BTreeMap::new();
            let mut batch: u64 = 0;

            while let Some(row) = rows.next().context("read tile row")? {
                let _zoom: u8 = row.get(0)?;
                let x: u32 = row.get(1)?;
                let y: u32 = row.get(2)?;
                let length: i64 = row.get(3)?;
                let length = u64::try_from(length).context("tile length must be non-negative")?;
                let tile_data: Option<Vec<u8>> = if need_tile_data {
                    Some(row.get(4)?)
                } else {
                    None
                };

                index += 1;
                batch += 1;
                if batch >= 1000 {
                    let total = processed.fetch_add(batch, Ordering::Relaxed) + batch;
                    progress.set_position(total);
                    batch = 0;
                }

                if options.max_tile_bytes > 0 && length > options.max_tile_bytes {
                    over_limit_tiles += 1;
                }

                if let Some(sample) = options.sample.as_ref()
                    && !include_sample(index, total_tiles_db, Some(sample))
                {
                    continue;
                }

                used += 1;
                stats.tile_count += 1;
                stats.total_bytes += length;
                stats.max_bytes = stats.max_bytes.max(length);

                if length <= EMPTY_TILE_MAX_BYTES {
                    empty_tiles += 1;
                }

                local_min_len = Some(local_min_len.map_or(length, |v| v.min(length)));
                local_max_len = Some(local_max_len.map_or(length, |v| v.max(length)));

                if should_collect_sizes {
                    tile_sizes.push(length);
                }

                if collect_layers
                    && tile_data.is_some()
                    && let Ok(payload) = decode_tile_payload(tile_data.as_ref().unwrap())
                    && let Ok(reader) = Reader::new(payload)
                    && let Ok(layers) = reader.get_layer_metadata()
                {
                    for layer in layers {
                        let entry = layer_accums
                            .entry(layer.name.clone())
                            .or_insert_with(LayerAccum::new);
                        entry.feature_count += layer.feature_count as u64;
                        if let Ok(features) = reader.get_features(layer.layer_index) {
                            for feature in features {
                                entry.vertex_count += count_vertices(&feature.geometry) as u64;
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

                if topn > 0 {
                    top_heap.push(Reverse((length, zoom, x, y)));
                    if top_heap.len() > topn {
                        top_heap.pop();
                    }
                }

                if let Some(SampleSpec::Count(limit)) = options.sample
                    && used >= limit
                {
                    break;
                }
            }

            if batch > 0 {
                let total = processed.fetch_add(batch, Ordering::Relaxed) + batch;
                progress.set_position(total);
            }

            Ok(Pass1Accum {
                zoom,
                stats,
                min_len: local_min_len,
                max_len: local_max_len,
                empty_tiles,
                over_limit_tiles,
                top_heap,
                tile_sizes,
                layer_accums,
                used,
            })
        })
        .collect::<Result<Vec<_>>>()?;

    let mut pass1_by_zoom: BTreeMap<u8, Pass1Accum> = BTreeMap::new();
    for accum in pass1 {
        let entry = pass1_by_zoom
            .entry(accum.zoom)
            .or_insert_with(|| Pass1Accum {
                zoom: accum.zoom,
                stats: MbtilesStats {
                    tile_count: 0,
                    total_bytes: 0,
                    max_bytes: 0,
                    avg_bytes: 0,
                },
                min_len: None,
                max_len: None,
                empty_tiles: 0,
                over_limit_tiles: 0,
                top_heap: BinaryHeap::new(),
                tile_sizes: if should_collect_sizes {
                    Vec::new()
                } else {
                    Vec::with_capacity(0)
                },
                layer_accums: BTreeMap::new(),
                used: 0,
            });

        entry.used += accum.used;
        entry.stats.tile_count += accum.stats.tile_count;
        entry.stats.total_bytes += accum.stats.total_bytes;
        entry.stats.max_bytes = entry.stats.max_bytes.max(accum.stats.max_bytes);
        entry.empty_tiles += accum.empty_tiles;
        entry.over_limit_tiles += accum.over_limit_tiles;
        if let Some(min) = accum.min_len {
            entry.min_len = Some(entry.min_len.map_or(min, |v| v.min(min)));
        }
        if let Some(max) = accum.max_len {
            entry.max_len = Some(entry.max_len.map_or(max, |v| v.max(max)));
        }
        if should_collect_sizes {
            entry.tile_sizes.extend(accum.tile_sizes);
        }
        if collect_layers {
            for (name, layer_accum) in accum.layer_accums {
                let target = entry
                    .layer_accums
                    .entry(name)
                    .or_insert_with(LayerAccum::new);
                target.feature_count += layer_accum.feature_count;
                target.vertex_count += layer_accum.vertex_count;
                target.property_keys.extend(layer_accum.property_keys);
                target.property_values.extend(layer_accum.property_values);
            }
        }
        if topn > 0 {
            for Reverse(item) in accum.top_heap {
                entry.top_heap.push(Reverse(item));
                if entry.top_heap.len() > topn {
                    entry.top_heap.pop();
                }
            }
        }
    }

    for accum in pass1_by_zoom.into_values() {
        let zoom = accum.zoom;
        used += accum.used;
        overall.tile_count += accum.stats.tile_count;
        overall.total_bytes += accum.stats.total_bytes;
        overall.max_bytes = overall.max_bytes.max(accum.stats.max_bytes);
        by_zoom.insert(zoom, accum.stats);
        empty_tiles += accum.empty_tiles;
        over_limit_tiles += accum.over_limit_tiles;
        if let (Some(min), Some(max)) = (accum.min_len, accum.max_len) {
            min_len = Some(min_len.map_or(min, |v| v.min(min)));
            max_len = Some(max_len.map_or(max, |v| v.max(max)));
            zoom_minmax.insert(zoom, (min, max));
        }
        if should_collect_sizes {
            tile_sizes.extend(accum.tile_sizes);
        }
        if collect_layers {
            for (name, layer_accum) in accum.layer_accums {
                let entry = layer_accums.entry(name).or_insert_with(LayerAccum::new);
                entry.feature_count += layer_accum.feature_count;
                entry.vertex_count += layer_accum.vertex_count;
                entry.property_keys.extend(layer_accum.property_keys);
                entry.property_values.extend(layer_accum.property_values);
            }
        }
        if topn > 0 {
            for Reverse(item) in accum.top_heap {
                top_heap.push(Reverse(item));
                if top_heap.len() > topn {
                    top_heap.pop();
                }
            }
        }
    }

    progress.finish();
    if !options.no_progress {
        eprintln!();
    }

    let bucketable = options.bucket.is_some()
        && options.list_tiles.is_some()
        && options.histogram_buckets > 0
        && min_len.is_some()
        && max_len.is_some();
    if bucketable {
        let bucket_target = options.bucket.expect("bucket target");
        let list_options = options.list_tiles.expect("list options");
        let query = select_tiles_query_by_zoom(&conn, false)?;
        let query_with_column_range = select_tiles_query_by_zoom_and_column_range(&conn, false)?;
        let zooms = if let Some(target) = options.zoom {
            vec![target]
        } else {
            zoom_counts_for_scan.keys().copied().collect::<Vec<_>>()
        };

        let mut bucket_tasks = Vec::new();
        for zoom in &zooms {
            if allow_column_chunk && *zoom >= 12 {
                if let Some(ranges) = tile_column_chunks(*zoom, chunk_count) {
                    for range in ranges {
                        bucket_tasks.push((*zoom, Some(range)));
                    }
                } else {
                    bucket_tasks.push((*zoom, None));
                }
            } else {
                bucket_tasks.push((*zoom, None));
            }
        }

        let bucket_results = bucket_tasks
            .into_par_iter()
            .map(|(zoom, range)| -> Result<Vec<TopTile>> {
                let conn = open_readonly_mbtiles(path)?;
                apply_read_pragmas(&conn)?;
                let mut stmt = if range.is_some() {
                    conn.prepare(&query_with_column_range)
                        .context("prepare bucket scan (column range)")?
                } else {
                    conn.prepare(&query).context("prepare bucket scan")?
                };
                let mut rows = if let Some((col_min, col_max)) = range {
                    stmt.query(params![zoom, col_min, col_max])
                        .context("query bucket scan (column range)")?
                } else {
                    stmt.query([zoom]).context("query bucket scan")?
                };

                let total_tiles_db = *zoom_counts_for_scan.get(&zoom).unwrap_or(&0);
                let mut index: u64 = 0;
                let mut used: u64 = 0;
                let mut tiles = Vec::new();

                while let Some(row) = rows.next().context("read bucket row")? {
                    let _zoom: u8 = row.get(0)?;
                    let x: u32 = row.get(1)?;
                    let y: u32 = row.get(2)?;
                    let length: i64 = row.get(3)?;
                    let length =
                        u64::try_from(length).context("tile length must be non-negative")?;
                    index += 1;

                    if let Some(sample) = options.sample.as_ref()
                        && !include_sample(index, total_tiles_db, Some(sample))
                    {
                        continue;
                    }

                    used += 1;
                    if let Some(bucket_idx) =
                        histogram_bucket_index(length, min_len, max_len, options.histogram_buckets)
                        && bucket_idx == bucket_target
                    {
                        tiles.push(TopTile {
                            zoom,
                            x,
                            y,
                            bytes: length,
                        });
                        if tiles.len() > list_options.limit {
                            if list_options.sort == TileSort::Size {
                                tiles.sort_by(|a, b| b.bytes.cmp(&a.bytes));
                            } else {
                                tiles.sort_by(|a, b| (a.zoom, a.x, a.y).cmp(&(b.zoom, b.x, b.y)));
                            }
                            tiles.truncate(list_options.limit);
                        }
                    }

                    if let Some(SampleSpec::Count(limit)) = options.sample
                        && used >= limit
                    {
                        break;
                    }
                }

                Ok(tiles)
            })
            .collect::<Result<Vec<_>>>()?;

        bucket_tiles = bucket_results.into_iter().flatten().collect();
        if bucket_tiles.len() > list_options.limit {
            if list_options.sort == TileSort::Size {
                bucket_tiles.sort_by(|a, b| b.bytes.cmp(&a.bytes));
            } else {
                bucket_tiles.sort_by(|a, b| (a.zoom, a.x, a.y).cmp(&(b.zoom, b.x, b.y)));
            }
            bucket_tiles.truncate(list_options.limit);
        }
    }

    // Build layer list from collected samples or full scan
    let mut file_layers = if collect_layers && !layer_accums.is_empty() {
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
        build_file_layer_list(
            &conn,
            options.sample.as_ref(),
            total_tiles,
            options.zoom,
            options.no_progress,
        )?
    } else {
        Vec::new()
    };
    if !options.layers.is_empty() {
        let filter: HashSet<&str> = options.layers.iter().map(|s| s.as_str()).collect();
        file_layers.retain(|layer| filter.contains(layer.name.as_str()));
    }

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
        .map(|Reverse((bytes, zoom, x, y))| TopTile { zoom, x, y, bytes })
        .collect::<Vec<_>>();
    top_tiles.sort_by(|a, b| b.bytes.cmp(&a.bytes));

    let empty_ratio = if used == 0 {
        0.0
    } else {
        empty_tiles as f64 / used as f64
    };

    if zoom_counts.is_none() && options.histogram_buckets > 0 && options.zoom.is_none() {
        zoom_counts = Some(fetch_zoom_counts(&conn)?);
    }

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

    let histograms_by_zoom =
        if options.histogram_buckets > 0 && options.zoom.is_none() && options.sample.is_none() {
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
                    &[],
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
        over_limit_tiles,
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

fn tiles_schema_mode(conn: &Connection) -> Result<TilesSchemaMode> {
    if has_table(conn, "tiles")? || has_view(conn, "tiles")? {
        return Ok(TilesSchemaMode::Tiles);
    }
    if has_table(conn, "map")? && has_table(conn, "images")? {
        return Ok(TilesSchemaMode::MapImages);
    }
    anyhow::bail!("mbtiles missing tiles table or map/images tables");
}

#[derive(Clone, Copy)]
enum TilesSchemaMode {
    Tiles,
    MapImages,
}

fn create_output_schema(conn: &Connection, mode: TilesSchemaMode) -> Result<()> {
    match mode {
        TilesSchemaMode::Tiles => {
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
            .context("failed to create output schema")?;
        }
        TilesSchemaMode::MapImages => {
            conn.execute_batch(
                "
                CREATE TABLE metadata (name TEXT, value TEXT);
                CREATE TABLE map (
                    zoom_level INTEGER,
                    tile_column INTEGER,
                    tile_row INTEGER,
                    tile_id TEXT
                );
                CREATE TABLE images (
                    tile_id TEXT,
                    tile_data BLOB
                );
                ",
            )
            .context("failed to create output schema")?;
        }
    }
    Ok(())
}

fn has_table(conn: &Connection, name: &str) -> Result<bool> {
    let count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?1",
            [name],
            |row| row.get(0),
        )
        .context("check table exists")?;
    Ok(count > 0)
}

fn has_view(conn: &Connection, name: &str) -> Result<bool> {
    let count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='view' AND name=?1",
            [name],
            |row| row.get(0),
        )
        .context("check view exists")?;
    Ok(count > 0)
}

fn tiles_source_clause(conn: &Connection) -> Result<&'static str> {
    if has_table(conn, "tiles")? || has_view(conn, "tiles")? {
        Ok("tiles")
    } else if has_table(conn, "map")? && has_table(conn, "images")? {
        Ok("map JOIN images ON map.tile_id = images.tile_id")
    } else {
        anyhow::bail!("mbtiles missing tiles table or map/images tables")
    }
}

fn tiles_count_source_clause(conn: &Connection) -> Result<&'static str> {
    if has_table(conn, "tiles_shallow")? {
        Ok("tiles_shallow")
    } else if has_table(conn, "tiles")? || has_view(conn, "tiles")? {
        Ok("tiles")
    } else if has_table(conn, "map")? && has_table(conn, "images")? {
        Ok("map")
    } else {
        anyhow::bail!("mbtiles missing tiles table or map/images tables")
    }
}

fn tiles_data_expr(conn: &Connection) -> Result<&'static str> {
    if has_table(conn, "tiles")? || has_view(conn, "tiles")? {
        Ok("tile_data")
    } else {
        Ok("images.tile_data")
    }
}

fn select_tiles_query_by_zoom(conn: &Connection, with_data: bool) -> Result<String> {
    let source = tiles_source_clause(conn)?;
    let data_expr = tiles_data_expr(conn)?;
    let (zoom_col, x_col, y_col) = if source == "tiles" {
        ("zoom_level", "tile_column", "tile_row")
    } else {
        ("map.zoom_level", "map.tile_column", "map.tile_row")
    };
    let select = if with_data {
        format!(
            "SELECT {zoom_col}, {x_col}, {y_col}, LENGTH({data_expr}), {data_expr} \
FROM {source} WHERE {zoom_col} = ?1",
        )
    } else {
        format!(
            "SELECT {zoom_col}, {x_col}, {y_col}, LENGTH({data_expr}) FROM {source} \
WHERE {zoom_col} = ?1",
        )
    };
    Ok(select)
}

fn select_tiles_query_by_zoom_and_column_range(
    conn: &Connection,
    with_data: bool,
) -> Result<String> {
    let source = tiles_source_clause(conn)?;
    let data_expr = tiles_data_expr(conn)?;
    let (zoom_col, x_col, y_col) = if source == "tiles" {
        ("zoom_level", "tile_column", "tile_row")
    } else {
        ("map.zoom_level", "map.tile_column", "map.tile_row")
    };
    let select = if with_data {
        format!(
            "SELECT {zoom_col}, {x_col}, {y_col}, LENGTH({data_expr}), {data_expr} \
FROM {source} WHERE {zoom_col} = ?1 AND {x_col} BETWEEN ?2 AND ?3",
        )
    } else {
        format!(
            "SELECT {zoom_col}, {x_col}, {y_col}, LENGTH({data_expr}) FROM {source} \
WHERE {zoom_col} = ?1 AND {x_col} BETWEEN ?2 AND ?3",
        )
    };
    Ok(select)
}

fn tile_column_chunks(zoom: u8, chunks: u64) -> Option<Vec<(i64, i64)>> {
    let cols = 1u64.checked_shl(u32::from(zoom))?;
    if cols == 0 {
        return None;
    }
    let max_col = cols - 1;
    if max_col > u64::from(u32::MAX) {
        return None;
    }
    let chunk_count = chunks.max(1);
    let chunk_size = cols.div_ceil(chunk_count);
    let mut ranges = Vec::new();
    let mut start = 0u64;
    while start <= max_col {
        let end = (start + chunk_size - 1).min(max_col);
        ranges.push((start as i64, end as i64));
        if end == max_col {
            break;
        }
        start = end + 1;
    }
    Some(ranges)
}

fn select_tile_data_query(conn: &Connection) -> Result<String> {
    let source = tiles_source_clause(conn)?;
    let data_expr = tiles_data_expr(conn)?;
    let (zoom_col, x_col, y_col) = if source == "tiles" {
        ("zoom_level", "tile_column", "tile_row")
    } else {
        ("map.zoom_level", "map.tile_column", "map.tile_row")
    };
    Ok(format!(
        "SELECT {data_expr} FROM {source} WHERE {zoom_col} = ?1 AND {x_col} = ?2 AND {y_col} = ?3",
    ))
}

fn select_zoom_length_by_zoom_query(conn: &Connection) -> Result<String> {
    let source = tiles_source_clause(conn)?;
    let data_expr = tiles_data_expr(conn)?;
    let zoom_col = if source == "tiles" {
        "zoom_level"
    } else {
        "map.zoom_level"
    };
    Ok(format!(
        "SELECT LENGTH({data_expr}) FROM {source} WHERE {zoom_col} = ?1",
    ))
}

fn select_zoom_length_by_zoom_and_column_range_query(conn: &Connection) -> Result<String> {
    let source = tiles_source_clause(conn)?;
    let data_expr = tiles_data_expr(conn)?;
    let (zoom_col, x_col) = if source == "tiles" {
        ("zoom_level", "tile_column")
    } else {
        ("map.zoom_level", "map.tile_column")
    };
    Ok(format!(
        "SELECT LENGTH({data_expr}) FROM {source} WHERE {zoom_col} = ?1 AND {x_col} BETWEEN ?2 AND ?3",
    ))
}

fn select_tile_count_query(conn: &Connection, with_zoom: bool) -> Result<String> {
    let source = tiles_count_source_clause(conn)?;
    let zoom_col = if source == "map" {
        "map.zoom_level"
    } else {
        "zoom_level"
    };
    if with_zoom {
        Ok(format!(
            "SELECT COUNT(*) FROM {source} WHERE {zoom_col} = ?1",
        ))
    } else {
        Ok(format!("SELECT COUNT(*) FROM {source}"))
    }
}

pub fn copy_mbtiles(input: &Path, output: &Path) -> Result<()> {
    ensure_mbtiles_path(input)?;
    ensure_mbtiles_path(output)?;
    let input_conn = Connection::open(input)
        .with_context(|| format!("failed to open input mbtiles: {}", input.display()))?;
    let mut output_conn = Connection::open(output)
        .with_context(|| format!("failed to open output mbtiles: {}", output.display()))?;
    let schema_mode = tiles_schema_mode(&input_conn)?;
    create_output_schema(&output_conn, schema_mode)?;

    let tx = output_conn
        .transaction()
        .context("begin output transaction")?;

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

    match schema_mode {
        TilesSchemaMode::Tiles => {
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
        TilesSchemaMode::MapImages => {
            let mut stmt = input_conn
                .prepare(
                    "SELECT map.zoom_level, map.tile_column, map.tile_row, map.tile_id, images.tile_data FROM map JOIN images ON map.tile_id = images.tile_id ORDER BY map.zoom_level, map.tile_column, map.tile_row",
                )
                .context("prepare map/images")?;
            let mut rows = stmt.query([]).context("query map/images")?;
            while let Some(row) = rows.next().context("read map/images row")? {
                let z: i64 = row.get(0)?;
                let x: i64 = row.get(1)?;
                let y: i64 = row.get(2)?;
                let tile_id: String = row.get(3)?;
                let data: Vec<u8> = row.get(4)?;
                tx.execute(
                    "INSERT INTO map (zoom_level, tile_column, tile_row, tile_id) VALUES (?1, ?2, ?3, ?4)",
                    params![z, x, y, tile_id],
                )
                .context("insert map row")?;
                tx.execute(
                    "INSERT INTO images (tile_id, tile_data) VALUES (?1, ?2)",
                    params![tile_id, data],
                )
                .context("insert image row")?;
            }
        }
    }

    tx.commit().context("commit output")?;
    Ok(())
}

#[derive(Debug, Default, Serialize)]
pub struct PruneStats {
    pub removed_features_by_zoom: BTreeMap<u8, u64>,
    pub removed_layers_by_zoom: BTreeMap<String, BTreeSet<u8>>,
    pub unknown_filters: usize,
    pub unknown_filters_by_layer: BTreeMap<String, u64>,
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

    fn record_unknown_layer(&mut self, layer: &str) {
        *self
            .unknown_filters_by_layer
            .entry(layer.to_string())
            .or_insert(0) += 1;
    }

    fn merge(&mut self, other: PruneStats) {
        for (zoom, count) in other.removed_features_by_zoom.into_iter() {
            *self.removed_features_by_zoom.entry(zoom).or_insert(0) += count;
        }
        for (layer, zooms) in other.removed_layers_by_zoom.into_iter() {
            self.removed_layers_by_zoom
                .entry(layer)
                .or_default()
                .extend(zooms);
        }
        self.unknown_filters += other.unknown_filters;
        for (layer, count) in other.unknown_filters_by_layer.into_iter() {
            *self.unknown_filters_by_layer.entry(layer).or_insert(0) += count;
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct PruneOptions {
    pub threads: usize,
    pub io_batch: u32,
    pub readers: usize,
    pub read_cache_mb: Option<u64>,
    pub write_cache_mb: Option<u64>,
    pub drop_empty_tiles: bool,
    pub keep_unknown_filters: bool,
}

pub fn prune_mbtiles_layer_only(
    input: &Path,
    output: &Path,
    style: &crate::style::MapboxStyle,
    apply_filters: bool,
    options: PruneOptions,
) -> Result<PruneStats> {
    ensure_mbtiles_path(input)?;
    ensure_mbtiles_path(output)?;

    let input_conn = Connection::open(input)
        .with_context(|| format!("failed to open input mbtiles: {}", input.display()))?;
    apply_read_pragmas_with_cache(&input_conn, options.read_cache_mb)?;
    let mut output_conn = Connection::open(output)
        .with_context(|| format!("failed to open output mbtiles: {}", output.display()))?;
    apply_write_pragmas_with_cache(&output_conn, options.write_cache_mb)?;
    let schema_mode = tiles_schema_mode(&input_conn)?;
    create_output_schema(&output_conn, schema_mode)?;

    let tx = output_conn
        .transaction()
        .context("begin output transaction")?;

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

    let keep_layers = style.source_layers();
    let worker_count = options.threads.max(1);
    let reader_count = options.readers.max(1);
    let queue_capacity = options.io_batch.max(1) as usize;

    let (tx_in, rx_in): (Sender<TileInput>, Receiver<TileInput>) = bounded(queue_capacity);
    let (tx_out, rx_out): (Sender<TileOutput>, Receiver<TileOutput>) = bounded(queue_capacity);

    let mut worker_handles = Vec::with_capacity(worker_count);
    for _ in 0..worker_count {
        let rx_in = rx_in.clone();
        let tx_out = tx_out.clone();
        let keep_layers = keep_layers.clone();
        let style = style.clone();
        let drop_empty_tiles = options.drop_empty_tiles;
        let keep_unknown_filters = options.keep_unknown_filters;
        worker_handles.push(thread::spawn(move || -> Result<PruneStats> {
            let mut stats = PruneStats::default();
            while let Ok(tile) = rx_in.recv() {
                let is_gzip = tile.data.starts_with(&[0x1f, 0x8b]);
                let payload = decode_tile_payload(&tile.data)?;
                let encoded = prune_tile_layers(
                    &payload,
                    tile.zoom,
                    &style,
                    &keep_layers,
                    apply_filters,
                    keep_unknown_filters,
                    &mut stats,
                )?;
                if encoded.empty && drop_empty_tiles {
                    continue;
                }
                let tile_data = encode_tile_payload(&encoded.bytes, is_gzip)?;
                let output = if tile.map_images {
                    let tile_id = format!("{}-{}-{}", tile.zoom, tile.x, tile.y);
                    TileOutput::MapImages {
                        zoom: tile.zoom,
                        x: tile.x,
                        y: tile.y,
                        tile_id,
                        data: tile_data,
                    }
                } else {
                    TileOutput::Tiles {
                        zoom: tile.zoom,
                        x: tile.x,
                        y: tile.y,
                        data: tile_data,
                    }
                };
                tx_out.send(output).context("send processed tile")?;
            }
            Ok(stats)
        }));
    }
    drop(tx_out);

    let ranges = match schema_mode {
        TilesSchemaMode::Tiles => rowid_ranges(&input_conn, "tiles", reader_count).ok(),
        TilesSchemaMode::MapImages => rowid_ranges(&input_conn, "map", reader_count).ok(),
    };
    let rowid_available = match schema_mode {
        TilesSchemaMode::Tiles => supports_rowid(&input_conn, "tiles")?,
        TilesSchemaMode::MapImages => supports_rowid(&input_conn, "map")?,
    };

    let reader_handles = if rowid_available {
        let ranges = ranges.unwrap_or_default();
        let mut handles = Vec::with_capacity(ranges.len());
        for (start_rowid, end_rowid) in ranges {
            let tx_in = tx_in.clone();
            let input_path = input.to_path_buf();
            let read_cache_mb = options.read_cache_mb;
            handles.push(thread::spawn(move || -> Result<()> {
                let input_conn = Connection::open(&input_path).with_context(|| {
                    format!("failed to open input mbtiles: {}", input_path.display())
                })?;
                apply_read_pragmas_with_cache(&input_conn, read_cache_mb)?;
                match schema_mode {
                    TilesSchemaMode::Tiles => {
                        let mut stmt = input_conn
                            .prepare(
                                "SELECT zoom_level, tile_column, tile_row, tile_data FROM tiles WHERE rowid BETWEEN ?1 AND ?2 ORDER BY rowid",
                            )
                            .context("prepare tile scan")?;
                        let mut rows = stmt
                            .query(params![start_rowid, end_rowid])
                            .context("query tiles")?;
                        while let Some(row) = rows.next().context("read tile row")? {
                            let zoom: u8 = row.get(0)?;
                            let x: u32 = row.get(1)?;
                            let y: u32 = row.get(2)?;
                            let data: Vec<u8> = row.get(3)?;
                            if tx_in
                                .send(TileInput {
                                    zoom,
                                    x,
                                    y,
                                    data,
                                    map_images: false,
                                })
                                .is_err()
                            {
                                break;
                            }
                        }
                    }
                    TilesSchemaMode::MapImages => {
                        let mut stmt = input_conn
                            .prepare(
                                "SELECT map.zoom_level, map.tile_column, map.tile_row, images.tile_data FROM map JOIN images ON map.tile_id = images.tile_id WHERE map.rowid BETWEEN ?1 AND ?2 ORDER BY map.rowid",
                            )
                            .context("prepare map/images scan")?;
                        let mut rows = stmt
                            .query(params![start_rowid, end_rowid])
                            .context("query map/images")?;
                        while let Some(row) = rows.next().context("read map/images row")? {
                            let zoom: u8 = row.get(0)?;
                            let x: u32 = row.get(1)?;
                            let y: u32 = row.get(2)?;
                            let data: Vec<u8> = row.get(3)?;
                            if tx_in
                                .send(TileInput {
                                    zoom,
                                    x,
                                    y,
                                    data,
                                    map_images: true,
                                })
                                .is_err()
                            {
                                break;
                            }
                        }
                    }
                }
                Ok(())
            }));
        }
        handles
    } else {
        let zoom_groups = zoom_partitions(&input_conn, reader_count)?;
        let mut handles = Vec::with_capacity(zoom_groups.len());
        for zooms in zoom_groups {
            let tx_in = tx_in.clone();
            let input_path = input.to_path_buf();
            let read_cache_mb = options.read_cache_mb;
            handles.push(thread::spawn(move || -> Result<()> {
                let input_conn = Connection::open(&input_path).with_context(|| {
                    format!("failed to open input mbtiles: {}", input_path.display())
                })?;
                apply_read_pragmas_with_cache(&input_conn, read_cache_mb)?;
                match schema_mode {
                    TilesSchemaMode::Tiles => {
                        let mut stmt = input_conn
                            .prepare(
                                "SELECT zoom_level, tile_column, tile_row, tile_data FROM tiles WHERE zoom_level = ?1 ORDER BY zoom_level, tile_column, tile_row",
                            )
                            .context("prepare tile scan by zoom")?;
                        for zoom in zooms {
                            let mut rows = stmt.query(params![zoom]).context("query tiles")?;
                            while let Some(row) = rows.next().context("read tile row")? {
                                let zoom: u8 = row.get(0)?;
                                let x: u32 = row.get(1)?;
                                let y: u32 = row.get(2)?;
                                let data: Vec<u8> = row.get(3)?;
                                if tx_in
                                    .send(TileInput {
                                        zoom,
                                        x,
                                        y,
                                        data,
                                        map_images: false,
                                    })
                                    .is_err()
                                {
                                    break;
                                }
                            }
                        }
                    }
                    TilesSchemaMode::MapImages => {
                        let mut stmt = input_conn
                            .prepare(
                                "SELECT map.zoom_level, map.tile_column, map.tile_row, images.tile_data FROM map JOIN images ON map.tile_id = images.tile_id WHERE map.zoom_level = ?1 ORDER BY map.zoom_level, map.tile_column, map.tile_row",
                            )
                            .context("prepare map/images scan by zoom")?;
                        for zoom in zooms {
                            let mut rows = stmt
                                .query(params![zoom])
                                .context("query map/images")?;
                            while let Some(row) = rows.next().context("read map/images row")? {
                                let zoom: u8 = row.get(0)?;
                                let x: u32 = row.get(1)?;
                                let y: u32 = row.get(2)?;
                                let data: Vec<u8> = row.get(3)?;
                                if tx_in
                                    .send(TileInput {
                                        zoom,
                                        x,
                                        y,
                                        data,
                                        map_images: true,
                                    })
                                    .is_err()
                                {
                                    break;
                                }
                            }
                        }
                    }
                }
                Ok(())
            }));
        }
        handles
    };
    drop(tx_in);

    let mut stats = PruneStats::default();
    for output in rx_out.iter() {
        match output {
            TileOutput::Tiles { zoom, x, y, data } => {
                tx.execute(
                    "INSERT INTO tiles (zoom_level, tile_column, tile_row, tile_data) VALUES (?1, ?2, ?3, ?4)",
                    (zoom as i64, x as i64, y as i64, data),
                )
                .context("insert tile")?;
            }
            TileOutput::MapImages {
                zoom,
                x,
                y,
                tile_id,
                data,
            } => {
                tx.execute(
                    "INSERT INTO map (zoom_level, tile_column, tile_row, tile_id) VALUES (?1, ?2, ?3, ?4)",
                    (zoom as i64, x as i64, y as i64, tile_id.clone()),
                )
                .context("insert map row")?;
                tx.execute(
                    "INSERT INTO images (tile_id, tile_data) VALUES (?1, ?2)",
                    (tile_id, data),
                )
                .context("insert image row")?;
            }
        }
    }

    for handle in reader_handles {
        handle
            .join()
            .map_err(|_| anyhow::anyhow!("reader thread panicked"))??;
    }

    for handle in worker_handles {
        let worker_stats = handle
            .join()
            .map_err(|_| anyhow::anyhow!("worker thread panicked"))??;
        stats.merge(worker_stats);
    }

    tx.commit().context("commit output")?;
    if apply_filters && stats.unknown_filters > 0 {
        warn!(
            count = stats.unknown_filters,
            "unknown filter expressions encountered"
        );
    }
    Ok(stats)
}

#[cfg(test)]
mod tests {
    use super::tile_column_chunks;

    #[test]
    fn tile_column_chunks_small_zoom() {
        let ranges = tile_column_chunks(2, 4).expect("ranges");
        assert_eq!(ranges, vec![(0, 0), (1, 1), (2, 2), (3, 3)]);
    }

    #[test]
    fn tile_column_chunks_even_splits() {
        let ranges = tile_column_chunks(12, 8).expect("ranges");
        assert_eq!(ranges.len(), 8);
        assert_eq!(ranges.first().copied(), Some((0, 511)));
        assert_eq!(ranges.last().copied(), Some((3584, 4095)));
    }

    #[test]
    fn tile_column_chunks_skips_large_zoom() {
        assert!(tile_column_chunks(33, 8).is_none());
    }
}

fn rowid_ranges(conn: &Connection, table: &str, readers: usize) -> Result<Vec<(i64, i64)>> {
    let query = format!("SELECT MIN(rowid), MAX(rowid) FROM {table}",);
    let (min_rowid, max_rowid): (Option<i64>, Option<i64>) =
        conn.query_row(&query, [], |row| Ok((row.get(0)?, row.get(1)?)))?;
    let (Some(min_rowid), Some(max_rowid)) = (min_rowid, max_rowid) else {
        return Ok(Vec::new());
    };
    if max_rowid < min_rowid {
        return Ok(Vec::new());
    }
    let total = max_rowid - min_rowid + 1;
    let reader_count = readers.max(1) as i64;
    let chunk = (total + reader_count - 1) / reader_count;
    let mut ranges = Vec::new();
    for idx in 0..reader_count {
        let start = min_rowid + idx * chunk;
        if start > max_rowid {
            break;
        }
        let end = (start + chunk - 1).min(max_rowid);
        ranges.push((start, end));
    }
    Ok(ranges)
}

fn zoom_partitions(conn: &Connection, readers: usize) -> Result<Vec<Vec<u8>>> {
    let mut counts: Vec<(u8, u64)> = fetch_zoom_counts(conn)?.into_iter().collect();
    if counts.is_empty() {
        return Ok(Vec::new());
    }
    counts.sort_by(|a, b| b.1.cmp(&a.1));
    let reader_count = readers.max(1);
    let mut groups: Vec<(u64, Vec<u8>)> = (0..reader_count).map(|_| (0u64, Vec::new())).collect();
    for (zoom, count) in counts {
        let (idx, _min) = groups
            .iter()
            .enumerate()
            .min_by_key(|(_, (total, _))| *total)
            .unwrap();
        groups[idx].0 += count;
        groups[idx].1.push(zoom);
    }
    Ok(groups.into_iter().map(|(_, zooms)| zooms).collect())
}

#[derive(Debug)]
struct TileInput {
    zoom: u8,
    x: u32,
    y: u32,
    data: Vec<u8>,
    map_images: bool,
}

#[derive(Debug)]
enum TileOutput {
    Tiles {
        zoom: u8,
        x: u32,
        y: u32,
        data: Vec<u8>,
    },
    MapImages {
        zoom: u8,
        x: u32,
        y: u32,
        tile_id: String,
        data: Vec<u8>,
    },
}

pub fn simplify_mbtiles_tile(
    input: &Path,
    output: &Path,
    coord: TileCoord,
    layers: &[String],
    tolerance: Option<f64>,
) -> Result<SimplifyStats> {
    ensure_mbtiles_path(input)?;
    ensure_mbtiles_path(output)?;

    let input_conn = Connection::open(input)
        .with_context(|| format!("failed to open input mbtiles: {}", input.display()))?;
    let output_conn = Connection::open(output)
        .with_context(|| format!("failed to open output mbtiles: {}", output.display()))?;

    let schema_mode = tiles_schema_mode(&input_conn)?;
    create_output_schema(&output_conn, schema_mode)?;

    let mut meta_stmt = input_conn
        .prepare("SELECT name, value FROM metadata")
        .context("prepare metadata read")?;
    let mut meta_rows = meta_stmt.query([]).context("query metadata")?;
    while let Some(row) = meta_rows.next().context("read metadata row")? {
        let name: String = row.get(0)?;
        let value: String = row.get(1)?;
        output_conn
            .execute(
                "INSERT INTO metadata (name, value) VALUES (?1, ?2)",
                params![name, value],
            )
            .context("insert metadata")?;
    }

    let Some(data) = fetch_tile_data(&input_conn, coord)? else {
        anyhow::bail!(
            "tile not found: z={} x={} y={}",
            coord.zoom,
            coord.x,
            coord.y
        );
    };
    let is_gzip = data.starts_with(&[0x1f, 0x8b]);
    let payload = decode_tile_payload(&data)?;

    let keep_layers: HashSet<String> = layers.iter().cloned().collect();
    let (filtered, stats) = simplify_tile_payload(&payload, &keep_layers, tolerance)?;
    let encoded = encode_tile_payload(&filtered, is_gzip)?;

    match schema_mode {
        TilesSchemaMode::Tiles => {
            output_conn
                .execute(
                    "INSERT INTO tiles (zoom_level, tile_column, tile_row, tile_data) VALUES (?1, ?2, ?3, ?4)",
                    (coord.zoom as i64, coord.x as i64, coord.y as i64, encoded),
                )
                .context("insert tile")?;
        }
        TilesSchemaMode::MapImages => {
            let tile_id = format!("{}-{}-{}", coord.zoom, coord.x, coord.y);
            output_conn
                .execute(
                    "INSERT INTO map (zoom_level, tile_column, tile_row, tile_id) VALUES (?1, ?2, ?3, ?4)",
                    (coord.zoom as i64, coord.x as i64, coord.y as i64, tile_id.clone()),
                )
                .context("insert map")?;
            output_conn
                .execute(
                    "INSERT INTO images (tile_id, tile_data) VALUES (?1, ?2)",
                    (tile_id, encoded),
                )
                .context("insert image")?;
        }
    }

    Ok(stats)
}
