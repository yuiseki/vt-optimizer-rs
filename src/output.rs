use anyhow::Result;
use serde_json::json;

use crate::mbtiles::MbtilesReport;

pub fn ndjson_lines(report: &MbtilesReport) -> Result<Vec<String>> {
    let mut lines = Vec::new();
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

    if !report.histogram.is_empty() {
        lines.push(serde_json::to_string(&json!({
            "type": "histogram",
            "buckets": report.histogram,
        }))?);
    }

    if !report.histograms_by_zoom.is_empty() {
        for item in report.histograms_by_zoom.iter() {
            lines.push(serde_json::to_string(&json!({
                "type": "histogram_by_zoom",
                "zoom": item.zoom,
                "buckets": item.buckets,
            }))?);
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
            lines.push(serde_json::to_string(&json!({
                "type": "bucket_tile",
                "tile": tile,
            }))?);
        }
    }

    if !report.top_tiles.is_empty() {
        for tile in report.top_tiles.iter() {
            lines.push(serde_json::to_string(&json!({
                "type": "top_tile",
                "tile": tile,
            }))?);
        }
    }

    if let Some(summary) = report.tile_summary.as_ref() {
        lines.push(serde_json::to_string(&json!({
            "type": "tile_summary",
            "summary": summary,
        }))?);
    }

    if !report.recommended_buckets.is_empty() {
        lines.push(serde_json::to_string(&json!({
            "type": "recommended_buckets",
            "buckets": report.recommended_buckets,
        }))?);
    }

    if !report.top_tile_summaries.is_empty() {
        for summary in report.top_tile_summaries.iter() {
            lines.push(serde_json::to_string(&json!({
                "type": "top_tile_summary",
                "summary": summary,
            }))?);
        }
    }

    Ok(lines)
}
