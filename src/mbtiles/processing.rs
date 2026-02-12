use anyhow::{Context, Result};
use flate2::Compression;
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use mvt::Tile;
use mvt_reader::Reader;
use std::collections::HashSet;
use std::io::{Read, Write};

use crate::mbtiles::algo::{count_vertices, encode_geometry, simplify_geometry};
use crate::mbtiles::stats::{PruneStats, SimplifyStats};
use crate::mbtiles::types::PrunedTile;

pub fn decode_tile_payload(data: &[u8]) -> Result<Vec<u8>> {
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

pub fn encode_tile_payload(data: &[u8], gzip: bool) -> Result<Vec<u8>> {
    if !gzip {
        return Ok(data.to_vec());
    }
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(data).context("encode gzip tile data")?;
    let encoded = encoder.finish().context("finish gzip tile data")?;
    Ok(encoded)
}

pub fn prune_tile_layers(
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

pub fn simplify_tile_payload(
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
