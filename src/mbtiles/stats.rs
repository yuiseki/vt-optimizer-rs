use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet};

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

pub fn finalize_stats(stats: &mut MbtilesStats) {
    if stats.tile_count == 0 {
        stats.avg_bytes = 0;
    } else {
        stats.avg_bytes = stats.total_bytes / stats.tile_count;
    }
}

#[derive(Debug, Default, Serialize)]
pub struct PruneStats {
    pub removed_features_by_zoom: BTreeMap<u8, u64>,
    pub removed_layers_by_zoom: BTreeMap<String, BTreeSet<u8>>,
    pub unknown_filters: usize,
    pub unknown_filters_by_layer: BTreeMap<String, u64>,
}

impl PruneStats {
    pub fn record_removed_features(&mut self, zoom: u8, count: u64) {
        if count == 0 {
            return;
        }
        *self.removed_features_by_zoom.entry(zoom).or_insert(0) += count;
    }

    pub fn record_removed_layer(&mut self, layer: &str, zoom: u8) {
        self.removed_layers_by_zoom
            .entry(layer.to_string())
            .or_default()
            .insert(zoom);
    }

    pub fn record_unknown_layer(&mut self, layer: &str) {
        *self
            .unknown_filters_by_layer
            .entry(layer.to_string())
            .or_insert(0) += 1;
    }

    pub fn merge(&mut self, other: PruneStats) {
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
