use anyhow::Result;
use serde_json::json;

use crate::cli::ReportFormat;
use std::collections::BTreeMap;

use crate::mbtiles::{HistogramBucket, MbtilesReport, ZoomHistogram};

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
                "total_features": summary.total_features,
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
                    "total_features": summary.total_features,
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
    for bucket in buckets.iter() {
        let warn = if bucket.avg_over_limit {
            "!! (over)"
        } else if bucket.avg_near_limit {
            "! (near)"
        } else {
            ""
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

pub fn format_histograms_by_zoom_section(histograms: &[ZoomHistogram]) -> Vec<String> {
    if histograms.is_empty() {
        return Vec::new();
    }
    let mut items = histograms.to_vec();
    items.sort_by_key(|item| item.zoom);
    let mut lines = Vec::new();
    lines.push("## Histogram by Zoom".to_string());
    for item in items.iter() {
        lines.push(String::new());
        lines.push(format!("### z={}", item.zoom));
        lines.extend(format_histogram_table(&item.buckets));
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
