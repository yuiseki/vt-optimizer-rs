use anyhow::Result;
use serde_json::json;

use crate::mbtiles::MbtilesReport;

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
