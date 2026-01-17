use anyhow::Result;
use nu_ansi_term::Color;
use serde_json::json;

use crate::cli::{ReportFormat, TileInfoFormat};
use std::collections::BTreeMap;

use crate::mbtiles::{
    FileLayerSummary, HistogramBucket, MbtilesReport, MbtilesZoomStats, TileSummary, TopTile,
    ZoomHistogram,
};

use std::collections::BTreeSet;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum StatsSection {
    Metadata,
    Summary,
    Zoom,
    Histogram,
    HistogramByZoom,
    Layers,
    Recommendations,
    Bucket,
    BucketTiles,
    TopTiles,
    TileSummary,
    TopTileSummaries,
}

#[derive(Debug, Clone)]
pub struct StatsFilter {
    include_all: bool,
    sections: BTreeSet<StatsSection>,
}

impl StatsFilter {
    pub fn all() -> Self {
        Self {
            include_all: true,
            sections: BTreeSet::new(),
        }
    }

    pub fn includes(&self, section: StatsSection) -> bool {
        self.include_all || self.sections.contains(&section)
    }
}

pub fn parse_stats_filter(value: Option<&str>) -> Result<StatsFilter> {
    let Some(value) = value else {
        return Ok(StatsFilter::all());
    };
    let mut sections = BTreeSet::new();
    for raw in value.split(',') {
        let token = raw.trim().to_ascii_lowercase();
        if token.is_empty() {
            continue;
        }
        if token == "all" {
            return Ok(StatsFilter::all());
        }
        let section = match token.as_str() {
            "metadata" => StatsSection::Metadata,
            "summary" => StatsSection::Summary,
            "zoom" => StatsSection::Zoom,
            "histogram" => StatsSection::Histogram,
            "histogram_by_zoom" | "histograms_by_zoom" | "zoom_histogram" | "zoom_histograms" => {
                StatsSection::HistogramByZoom
            }
            "layers" => StatsSection::Layers,
            "recommendations" | "recommended_buckets" => StatsSection::Recommendations,
            "bucket" => StatsSection::Bucket,
            "bucket_tiles" | "bucket_tile" => StatsSection::BucketTiles,
            "top_tiles" | "top_tile" => StatsSection::TopTiles,
            "tile_summary" => StatsSection::TileSummary,
            "top_tile_summaries" | "top_tile_summary" => StatsSection::TopTileSummaries,
            _ => {
                return Err(anyhow::anyhow!(
                    "unknown stats section: {} (possible values: metadata, summary, zoom, histogram, histogram_by_zoom, layers, recommendations, bucket, bucket_tiles, top_tiles, tile_summary, top_tile_summaries, all)",
                    token
                ));
            }
        };
        sections.insert(section);
    }
    if sections.is_empty() {
        return Err(anyhow::anyhow!(
            "stats list must not be empty (possible values: metadata, summary, zoom, histogram, histogram_by_zoom, layers, recommendations, bucket, bucket_tiles, top_tiles, tile_summary, top_tile_summaries, all)"
        ));
    }
    Ok(StatsFilter {
        include_all: false,
        sections,
    })
}

pub fn resolve_output_format(requested: ReportFormat, ndjson_compact: bool) -> ReportFormat {
    if ndjson_compact {
        ReportFormat::Ndjson
    } else {
        requested
    }
}

#[derive(Debug, Clone, Copy)]
pub struct NdjsonOptions {
    pub include_summary: bool,
    pub compact: bool,
}

pub fn apply_tile_info_format(mut report: MbtilesReport, format: TileInfoFormat) -> MbtilesReport {
    if matches!(format, TileInfoFormat::Compact) {
        if let Some(summary) = report.tile_summary.as_mut() {
            for layer in summary.layers.iter_mut() {
                layer.property_keys.clear();
            }
        }
        for summary in report.top_tile_summaries.iter_mut() {
            for layer in summary.layers.iter_mut() {
                layer.property_keys.clear();
            }
        }
    }
    report
}

pub fn apply_stats_filter(mut report: MbtilesReport, filter: &StatsFilter) -> MbtilesReport {
    if !filter.includes(StatsSection::Metadata) {
        report.metadata.clear();
    }
    if !filter.includes(StatsSection::Summary) {
        report.overall.tile_count = 0;
        report.overall.total_bytes = 0;
        report.overall.max_bytes = 0;
        report.overall.avg_bytes = 0;
        report.empty_tiles = 0;
        report.empty_ratio = 0.0;
        report.over_limit_tiles = 0;
        report.sampled = false;
        report.sample_total_tiles = 0;
        report.sample_used_tiles = 0;
    }
    if !filter.includes(StatsSection::Zoom) {
        report.by_zoom.clear();
    }
    if !filter.includes(StatsSection::Histogram) {
        report.histogram.clear();
    }
    if !filter.includes(StatsSection::HistogramByZoom) {
        report.histograms_by_zoom.clear();
    }
    if !filter.includes(StatsSection::Layers) {
        report.file_layers.clear();
    }
    if !filter.includes(StatsSection::Recommendations) {
        report.recommended_buckets.clear();
    }
    if !filter.includes(StatsSection::Bucket) {
        report.bucket_count = None;
    }
    if !filter.includes(StatsSection::BucketTiles) {
        report.bucket_tiles.clear();
    }
    if !filter.includes(StatsSection::TopTiles) {
        report.top_tiles.clear();
    }
    if !filter.includes(StatsSection::TileSummary) {
        report.tile_summary = None;
    }
    if !filter.includes(StatsSection::TopTileSummaries) {
        report.top_tile_summaries.clear();
    }
    report
}

pub fn ndjson_lines(report: &MbtilesReport, mut options: NdjsonOptions) -> Result<Vec<String>> {
    if options.compact {
        options.include_summary = false;
    }
    let mut lines = Vec::new();
    if options.include_summary {
        lines.push(serde_json::to_string(&json!({
            "type": "summary",
            "overall": report.overall,
            "by_zoom": report.by_zoom,
            "empty_tiles": report.empty_tiles,
            "empty_ratio": report.empty_ratio,
            "over_limit_tiles": report.over_limit_tiles,
            "sampled": report.sampled,
            "sample_total_tiles": report.sample_total_tiles,
            "sample_used_tiles": report.sample_used_tiles,
        }))?);
    }

    if !report.histogram.is_empty() {
        if options.compact {
            lines.push(serde_json::to_string(&json!({
                "type": "histogram",
                "bucket_count": report.histogram.len(),
            }))?);
        } else {
            lines.push(serde_json::to_string(&json!({
                "type": "histogram",
                "buckets": report.histogram,
            }))?);
        }
    }

    if !report.histograms_by_zoom.is_empty() {
        let mut histograms = report.histograms_by_zoom.clone();
        histograms.sort_by_key(|item| item.zoom);
        for item in histograms.iter() {
            if options.compact {
                lines.push(serde_json::to_string(&json!({
                    "type": "histogram_by_zoom",
                    "zoom": item.zoom,
                    "bucket_count": item.buckets.len(),
                }))?);
            } else {
                lines.push(serde_json::to_string(&json!({
                    "type": "histogram_by_zoom",
                    "zoom": item.zoom,
                    "buckets": item.buckets,
                }))?);
            }
        }
    }

    if let Some(count) = report.bucket_count {
        lines.push(serde_json::to_string(&json!({
            "type": "bucket_count",
            "count": count,
        }))?);
    }

    if !report.bucket_tiles.is_empty() {
        for tile in report.bucket_tiles.iter() {
            if options.compact {
                lines.push(serde_json::to_string(&json!({
                    "type": "bucket_tile",
                    "z": tile.zoom,
                    "x": tile.x,
                    "y": tile.y,
                    "bytes": tile.bytes,
                }))?);
            } else {
                lines.push(serde_json::to_string(&json!({
                    "type": "bucket_tile",
                    "tile": tile,
                }))?);
            }
        }
    }

    if !report.top_tiles.is_empty() {
        for tile in report.top_tiles.iter() {
            if options.compact {
                lines.push(serde_json::to_string(&json!({
                    "type": "top_tile",
                    "z": tile.zoom,
                    "x": tile.x,
                    "y": tile.y,
                    "bytes": tile.bytes,
                }))?);
            } else {
                lines.push(serde_json::to_string(&json!({
                    "type": "top_tile",
                    "tile": tile,
                }))?);
            }
        }
    }

    if let Some(summary) = report.tile_summary.as_ref() {
        if options.compact {
            lines.push(serde_json::to_string(&json!({
                "type": "tile_summary",
                "z": summary.zoom,
                "x": summary.x,
                "y": summary.y,
                "bytes": summary.tile_bytes,
                "layers": summary.layer_count,
                "total_features": summary.total_features,
                "vertices": summary.vertex_count,
                "keys": summary.property_key_count,
                "values": summary.property_value_count,
            }))?);
        } else {
            lines.push(serde_json::to_string(&json!({
                "type": "tile_summary",
                "summary": summary,
            }))?);
        }
    }

    if !report.recommended_buckets.is_empty() {
        let mut buckets = report.recommended_buckets.clone();
        buckets.sort_unstable();
        lines.push(serde_json::to_string(&json!({
            "type": "recommended_buckets",
            "buckets": buckets,
        }))?);
    }

    if !report.top_tile_summaries.is_empty() {
        for summary in report.top_tile_summaries.iter() {
            if options.compact {
                lines.push(serde_json::to_string(&json!({
                "type": "top_tile_summary",
                "z": summary.zoom,
                "x": summary.x,
                "y": summary.y,
                "bytes": summary.tile_bytes,
                "layers": summary.layer_count,
                "total_features": summary.total_features,
                "vertices": summary.vertex_count,
                "keys": summary.property_key_count,
                    "values": summary.property_value_count,
                }))?);
            } else {
                lines.push(serde_json::to_string(&json!({
                    "type": "top_tile_summary",
                    "summary": summary,
                }))?);
            }
        }
    }

    Ok(lines)
}

pub fn format_histogram_table(buckets: &[HistogramBucket]) -> Vec<String> {
    if buckets.is_empty() {
        return Vec::new();
    }
    let count_width = buckets
        .iter()
        .map(|b| b.count)
        .max()
        .unwrap_or(0)
        .to_string()
        .len()
        .max("count".len());
    let bytes_width = buckets
        .iter()
        .map(|b| format_bytes(b.total_bytes).len())
        .max()
        .unwrap_or(0)
        .max("bytes".len());
    let avg_width = buckets
        .iter()
        .map(|b| format_bytes(b.running_avg_bytes).len())
        .max()
        .unwrap_or(0)
        .max("avg".len());
    let mut lines = Vec::with_capacity(buckets.len() + 1);
    lines.push(format!(
        "  {} {} {} {} {} {} {} {}",
        pad_right("range", 17),
        pad_left("count", count_width),
        pad_left("bytes", bytes_width),
        pad_left("avg", avg_width),
        pad_left("%tiles", 8),
        pad_left("%size", 8),
        pad_left("acc%tiles", 10),
        pad_left("acc%size", 10),
    ));
    for bucket in buckets.iter().filter(|bucket| bucket.count > 0) {
        let warn = if bucket.avg_over_limit {
            Color::Red.paint("!! (over)").to_string()
        } else if bucket.avg_near_limit {
            Color::Yellow.paint("! (near)").to_string()
        } else {
            String::new()
        };
        let range = format!(
            "{}-{}",
            format_bytes(bucket.min_bytes),
            format_bytes(bucket.max_bytes)
        );
        lines.push(format!(
            "  {} {} {} {} {:>7.2}% {:>7.2}% {:>9.2}% {:>9.2}% {}",
            pad_right(&range, 17),
            pad_left(&bucket.count.to_string(), count_width),
            pad_left(&format_bytes(bucket.total_bytes), bytes_width),
            pad_left(&format_bytes(bucket.running_avg_bytes), avg_width),
            bucket.pct_tiles * 100.0,
            bucket.pct_level_bytes * 100.0,
            bucket.accum_pct_tiles * 100.0,
            bucket.accum_pct_level_bytes * 100.0,
            warn
        ));
    }
    lines
}

pub fn format_zoom_table(
    stats: &[MbtilesZoomStats],
    total_tiles: u64,
    total_bytes: u64,
) -> Vec<String> {
    if stats.is_empty() {
        return Vec::new();
    }
    let mut items = stats.to_vec();
    items.sort_by_key(|item| item.zoom);
    let zoom_width = items
        .iter()
        .map(|item| item.zoom.to_string().len())
        .max()
        .unwrap_or(0)
        .max("zoom".len());
    let tiles_width = items
        .iter()
        .map(|item| item.stats.tile_count.to_string().len())
        .max()
        .unwrap_or(0)
        .max("tiles".len());
    let total_width = items
        .iter()
        .map(|item| format_bytes(item.stats.total_bytes).len())
        .max()
        .unwrap_or(0)
        .max("total".len());
    let max_width = items
        .iter()
        .map(|item| format_bytes(item.stats.max_bytes).len())
        .max()
        .unwrap_or(0)
        .max("max".len());
    let avg_width = items
        .iter()
        .map(|item| format_bytes(item.stats.avg_bytes).len())
        .max()
        .unwrap_or(0)
        .max("avg".len());
    let pct_tiles = |count: u64| {
        if total_tiles == 0 {
            0.0
        } else {
            (count as f64 / total_tiles as f64) * 100.0
        }
    };
    let pct_bytes = |bytes: u64| {
        if total_bytes == 0 {
            0.0
        } else {
            (bytes as f64 / total_bytes as f64) * 100.0
        }
    };
    let mut lines = Vec::with_capacity(items.len() + 1);
    lines.push(format!(
        "  {} {} {} {} {} {} {} {} {}",
        pad_right("zoom", zoom_width),
        pad_left("tiles", tiles_width),
        pad_left("total", total_width),
        pad_left("max", max_width),
        pad_left("avg", avg_width),
        pad_left("%tiles", 8),
        pad_left("%size", 8),
        pad_left("acc%tiles", 10),
        pad_left("acc%size", 10),
    ));
    let mut acc_tiles = 0u64;
    let mut acc_bytes = 0u64;
    for item in items {
        acc_tiles = acc_tiles.saturating_add(item.stats.tile_count);
        acc_bytes = acc_bytes.saturating_add(item.stats.total_bytes);
        lines.push(format!(
            "  {} {} {} {} {} {:>7.2}% {:>7.2}% {:>9.2}% {:>9.2}%",
            pad_right(&item.zoom.to_string(), zoom_width),
            pad_left(&item.stats.tile_count.to_string(), tiles_width),
            pad_left(&format_bytes(item.stats.total_bytes), total_width),
            pad_left(&format_bytes(item.stats.max_bytes), max_width),
            pad_left(&format_bytes(item.stats.avg_bytes), avg_width),
            pct_tiles(item.stats.tile_count),
            pct_bytes(item.stats.total_bytes),
            pct_tiles(acc_tiles),
            pct_bytes(acc_bytes),
        ));
    }
    lines
}

pub fn format_top_tiles_lines(tiles: &[TopTile]) -> Vec<String> {
    tiles
        .iter()
        .map(|tile| {
            format!(
                "-z {} -x {} -y {} size={}",
                tile.zoom,
                tile.x,
                tile.y,
                format_bytes(tile.bytes)
            )
        })
        .collect()
}

pub fn format_tile_summary_text(summary: &TileSummary) -> Vec<String> {
    let label = |text: &str| Color::Blue.paint(text).to_string();
    vec![
        format!("- z={} x={} y={}", summary.zoom, summary.x, summary.y),
        format!(
            "- {}: {}",
            label("Size of tile"),
            format_bytes(summary.tile_bytes)
        ),
        format!(
            "- {}: {}",
            label("Layers in this tile"),
            summary.layer_count
        ),
        format!(
            "- {}: {}",
            label("Features in this tile"),
            summary.total_features
        ),
        format!(
            "- {}: {}",
            label("Vertices in this tile"),
            summary.vertex_count
        ),
        format!(
            "- {}: {}",
            label("Keys in this tile"),
            summary.property_key_count
        ),
        format!(
            "- {}: {}",
            label("Values in this tile"),
            summary.property_value_count
        ),
    ]
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LayerTotals {
    pub layer_count: usize,
    pub feature_count: u64,
    pub vertex_count: u64,
    pub property_key_count: usize,
    pub property_value_count: usize,
}

pub fn summarize_file_layers(file_layers: &[FileLayerSummary]) -> Option<LayerTotals> {
    if file_layers.is_empty() {
        return None;
    }
    let mut totals = LayerTotals {
        layer_count: file_layers.len(),
        feature_count: 0,
        vertex_count: 0,
        property_key_count: 0,
        property_value_count: 0,
    };
    for layer in file_layers {
        totals.feature_count = totals.feature_count.saturating_add(layer.feature_count);
        totals.vertex_count = totals.vertex_count.saturating_add(layer.vertex_count);
        totals.property_key_count = totals
            .property_key_count
            .saturating_add(layer.property_key_count);
        totals.property_value_count = totals
            .property_value_count
            .saturating_add(layer.property_value_count);
    }
    Some(totals)
}

pub fn format_histograms_by_zoom_section(histograms: &[ZoomHistogram]) -> Vec<String> {
    if histograms.is_empty() {
        return Vec::new();
    }
    let mut items = histograms.to_vec();
    items.sort_by_key(|item| item.zoom);
    let mut lines = Vec::new();
    lines.push("## Histogram by Zoom".to_string());
    for item in items.iter() {
        let buckets = item
            .buckets
            .iter()
            .filter(|&bucket| bucket.count > 0)
            .cloned()
            .collect::<Vec<_>>();
        if buckets.is_empty() {
            continue;
        }
        lines.push(String::new());
        lines.push(format!("### z={}", item.zoom));
        lines.extend(format_histogram_table(&buckets));
    }
    lines
}

pub fn format_metadata_section(metadata: &BTreeMap<String, String>) -> Vec<String> {
    if metadata.is_empty() {
        return Vec::new();
    }
    let mut lines = Vec::with_capacity(metadata.len() + 1);
    lines.push("## Metadata".to_string());
    for (name, value) in metadata.iter() {
        if name == "json" || name == "vector_layers" {
            continue;
        }
        lines.push(format!("- {}: {}", name, value));
    }
    lines
}

pub fn format_bytes(bytes: u64) -> String {
    const KB: f64 = 1024.0;
    const MB: f64 = 1024.0 * 1024.0;
    let bytes_f = bytes as f64;
    if bytes_f >= MB {
        format!("{:.2}MB", bytes_f / MB)
    } else if bytes_f >= KB {
        format!("{:.2}KB", bytes_f / KB)
    } else {
        format!("{}B", bytes)
    }
}

pub fn pad_right(value: &str, width: usize) -> String {
    format!("{:<width$}", value, width = width)
}

pub fn pad_left(value: &str, width: usize) -> String {
    format!("{:>width$}", value, width = width)
}
