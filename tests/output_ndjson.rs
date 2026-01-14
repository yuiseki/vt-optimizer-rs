use std::collections::BTreeMap;

use vt_optimizer::cli::TileInfoFormat;
use vt_optimizer::mbtiles::{
    HistogramBucket, MbtilesReport, MbtilesStats, MbtilesZoomStats, TileSummary, TopTile,
    ZoomHistogram,
};
use vt_optimizer::output::{
    NdjsonOptions, apply_tile_info_format, ndjson_lines, resolve_output_format,
};

#[test]
fn ndjson_splits_histograms_and_top_tile_summaries() {
    let report = MbtilesReport {
        metadata: BTreeMap::new(),
        overall: MbtilesStats {
            tile_count: 1,
            total_bytes: 10,
            max_bytes: 10,
            avg_bytes: 10,
        },
        by_zoom: vec![MbtilesZoomStats {
            zoom: 0,
            stats: MbtilesStats {
                tile_count: 1,
                total_bytes: 10,
                max_bytes: 10,
                avg_bytes: 10,
            },
        }],
        empty_tiles: 0,
        empty_ratio: 0.0,
        sampled: false,
        sample_total_tiles: 1,
        sample_used_tiles: 1,
        histogram: vec![HistogramBucket {
            min_bytes: 1,
            max_bytes: 10,
            count: 1,
            total_bytes: 10,
            running_avg_bytes: 10,
            pct_tiles: 1.0,
            pct_level_bytes: 1.0,
            accum_pct_tiles: 1.0,
            accum_pct_level_bytes: 1.0,
            avg_near_limit: false,
            avg_over_limit: false,
        }],
        histograms_by_zoom: vec![
            ZoomHistogram {
                zoom: 0,
                buckets: vec![],
            },
            ZoomHistogram {
                zoom: 1,
                buckets: vec![],
            },
        ],
        file_layers: vec![],
        top_tiles: vec![TopTile {
            zoom: 0,
            x: 0,
            y: 0,
            bytes: 10,
        }],
        bucket_count: None,
        bucket_tiles: vec![TopTile {
            zoom: 0,
            x: 1,
            y: 1,
            bytes: 5,
        }],
        tile_summary: None,
        recommended_buckets: vec![0],
        top_tile_summaries: vec![
            TileSummary {
                zoom: 0,
                x: 0,
                y: 0,
                layer_count: 0,
                total_features: 1,
                vertex_count: 0,
                property_key_count: 0,
                property_value_count: 0,
                layers: vec![],
            },
            TileSummary {
                zoom: 1,
                x: 1,
                y: 1,
                layer_count: 0,
                total_features: 2,
                vertex_count: 0,
                property_key_count: 0,
                property_value_count: 0,
                layers: vec![],
            },
        ],
    };

    let lines = ndjson_lines(
        &report,
        NdjsonOptions {
            include_summary: true,
            compact: false,
        },
    )
    .expect("ndjson");
    let types = lines
        .iter()
        .map(|line| {
            let value: serde_json::Value = serde_json::from_str(line).expect("json");
            value
                .get("type")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string()
        })
        .collect::<Vec<_>>();

    let histogram_by_zoom = types
        .iter()
        .filter(|t| t == &&"histogram_by_zoom".to_string())
        .count();
    let top_tile_summary = types
        .iter()
        .filter(|t| t == &&"top_tile_summary".to_string())
        .count();
    let top_tile = types
        .iter()
        .filter(|t| t == &&"top_tile".to_string())
        .count();
    let bucket_tile = types
        .iter()
        .filter(|t| t == &&"bucket_tile".to_string())
        .count();
    assert_eq!(histogram_by_zoom, 2);
    assert_eq!(top_tile_summary, 2);
    assert_eq!(top_tile, 1);
    assert_eq!(bucket_tile, 1);
}

#[test]
fn ndjson_lite_omits_summary() {
    let report = MbtilesReport {
        metadata: BTreeMap::new(),
        overall: MbtilesStats {
            tile_count: 1,
            total_bytes: 10,
            max_bytes: 10,
            avg_bytes: 10,
        },
        by_zoom: vec![],
        empty_tiles: 0,
        empty_ratio: 0.0,
        sampled: false,
        sample_total_tiles: 1,
        sample_used_tiles: 1,
        histogram: vec![],
        histograms_by_zoom: vec![],
        file_layers: vec![],
        top_tiles: vec![],
        bucket_count: None,
        bucket_tiles: vec![],
        tile_summary: None,
        recommended_buckets: vec![],
        top_tile_summaries: vec![],
    };

    let lines = ndjson_lines(
        &report,
        NdjsonOptions {
            include_summary: false,
            compact: false,
        },
    )
    .expect("ndjson");
    assert!(
        !lines
            .iter()
            .any(|line| line.contains("\"type\":\"summary\"")),
        "summary line should be omitted"
    );
}

#[test]
fn ndjson_sorts_zoom_histograms_and_recommendations() {
    let report = MbtilesReport {
        metadata: BTreeMap::new(),
        overall: MbtilesStats {
            tile_count: 1,
            total_bytes: 10,
            max_bytes: 10,
            avg_bytes: 10,
        },
        by_zoom: vec![],
        empty_tiles: 0,
        empty_ratio: 0.0,
        sampled: false,
        sample_total_tiles: 1,
        sample_used_tiles: 1,
        histogram: vec![],
        histograms_by_zoom: vec![
            ZoomHistogram {
                zoom: 3,
                buckets: vec![],
            },
            ZoomHistogram {
                zoom: 1,
                buckets: vec![],
            },
        ],
        file_layers: vec![],
        top_tiles: vec![],
        bucket_count: None,
        bucket_tiles: vec![],
        tile_summary: None,
        recommended_buckets: vec![2, 0, 1],
        top_tile_summaries: vec![],
    };

    let lines = ndjson_lines(
        &report,
        NdjsonOptions {
            include_summary: true,
            compact: false,
        },
    )
    .expect("ndjson");
    let zooms = lines
        .iter()
        .filter_map(|line| {
            let value: serde_json::Value = serde_json::from_str(line).ok()?;
            if value.get("type")?.as_str()? == "histogram_by_zoom" {
                return value.get("zoom")?.as_u64();
            }
            None
        })
        .collect::<Vec<_>>();
    assert_eq!(zooms, vec![1, 3]);

    let buckets = lines
        .iter()
        .find_map(|line| {
            let value: serde_json::Value = serde_json::from_str(line).ok()?;
            if value.get("type")?.as_str()? == "recommended_buckets" {
                return value.get("buckets")?.as_array().cloned();
            }
            None
        })
        .expect("recommended buckets");
    let buckets = buckets
        .into_iter()
        .filter_map(|value| value.as_u64())
        .collect::<Vec<_>>();
    assert_eq!(buckets, vec![0, 1, 2]);
}

#[test]
fn ndjson_compact_minimizes_payloads() {
    let report = MbtilesReport {
        metadata: BTreeMap::new(),
        overall: MbtilesStats {
            tile_count: 1,
            total_bytes: 10,
            max_bytes: 10,
            avg_bytes: 10,
        },
        by_zoom: vec![],
        empty_tiles: 0,
        empty_ratio: 0.0,
        sampled: false,
        sample_total_tiles: 1,
        sample_used_tiles: 1,
        histogram: vec![HistogramBucket {
            min_bytes: 1,
            max_bytes: 10,
            count: 1,
            total_bytes: 10,
            running_avg_bytes: 10,
            pct_tiles: 1.0,
            pct_level_bytes: 1.0,
            accum_pct_tiles: 1.0,
            accum_pct_level_bytes: 1.0,
            avg_near_limit: false,
            avg_over_limit: false,
        }],
        histograms_by_zoom: vec![ZoomHistogram {
            zoom: 0,
            buckets: vec![],
        }],
        file_layers: vec![],
        top_tiles: vec![TopTile {
            zoom: 0,
            x: 0,
            y: 0,
            bytes: 10,
        }],
        bucket_count: None,
        bucket_tiles: vec![TopTile {
            zoom: 1,
            x: 1,
            y: 1,
            bytes: 5,
        }],
        tile_summary: Some(TileSummary {
            zoom: 2,
            x: 2,
            y: 2,
            layer_count: 0,
            total_features: 3,
            vertex_count: 0,
            property_key_count: 0,
            property_value_count: 0,
            layers: vec![],
        }),
        recommended_buckets: vec![],
        top_tile_summaries: vec![TileSummary {
            zoom: 3,
            x: 3,
            y: 3,
            layer_count: 0,
            total_features: 4,
            vertex_count: 0,
            property_key_count: 0,
            property_value_count: 0,
            layers: vec![],
        }],
    };

    let lines = ndjson_lines(
        &report,
        NdjsonOptions {
            include_summary: false,
            compact: true,
        },
    )
    .expect("ndjson");

    let has_verbose_histogram = lines.iter().any(|line| line.contains("\"buckets\""));
    let has_verbose_tile = lines.iter().any(|line| line.contains("\"tile\""));
    let has_verbose_summary = lines.iter().any(|line| line.contains("\"summary\""));
    assert!(!has_verbose_histogram);
    assert!(!has_verbose_tile);
    assert!(!has_verbose_summary);
}

#[test]
fn ndjson_compact_omits_summary_even_when_requested() {
    let report = MbtilesReport {
        metadata: BTreeMap::new(),
        overall: MbtilesStats {
            tile_count: 1,
            total_bytes: 10,
            max_bytes: 10,
            avg_bytes: 10,
        },
        by_zoom: vec![],
        empty_tiles: 0,
        empty_ratio: 0.0,
        sampled: false,
        sample_total_tiles: 1,
        sample_used_tiles: 1,
        histogram: vec![],
        histograms_by_zoom: vec![],
        file_layers: vec![],
        top_tiles: vec![],
        bucket_count: None,
        bucket_tiles: vec![],
        tile_summary: None,
        recommended_buckets: vec![],
        top_tile_summaries: vec![],
    };

    let lines = ndjson_lines(
        &report,
        NdjsonOptions {
            include_summary: true,
            compact: true,
        },
    )
    .expect("ndjson");
    assert!(
        !lines
            .iter()
            .any(|line| line.contains("\"type\":\"summary\"")),
        "compact mode should omit summary"
    );
}

#[test]
fn ndjson_tile_info_format_compact_omits_property_keys() {
    let report = MbtilesReport {
        metadata: BTreeMap::new(),
        overall: MbtilesStats {
            tile_count: 1,
            total_bytes: 10,
            max_bytes: 10,
            avg_bytes: 10,
        },
        by_zoom: vec![],
        empty_tiles: 0,
        empty_ratio: 0.0,
        sampled: false,
        sample_total_tiles: 1,
        sample_used_tiles: 1,
        histogram: vec![],
        histograms_by_zoom: vec![],
        file_layers: vec![],
        top_tiles: vec![],
        bucket_count: None,
        bucket_tiles: vec![],
        tile_summary: Some(TileSummary {
            zoom: 2,
            x: 2,
            y: 2,
            layer_count: 1,
            total_features: 1,
            vertex_count: 1,
            property_key_count: 1,
            property_value_count: 1,
            layers: vec![vt_optimizer::mbtiles::LayerSummary {
                name: "roads".to_string(),
                feature_count: 1,
                vertex_count: 1,
                property_key_count: 1,
                property_value_count: 1,
                property_keys: vec!["name".to_string()],
            }],
        }),
        recommended_buckets: vec![],
        top_tile_summaries: vec![],
    };

    let report = apply_tile_info_format(report, TileInfoFormat::Compact);
    let lines = ndjson_lines(
        &report,
        NdjsonOptions {
            include_summary: true,
            compact: false,
        },
    )
    .expect("ndjson");
    let has_property_keys = lines.iter().any(|line| line.contains("\"property_keys\""));
    assert!(!has_property_keys);
}

#[test]
fn ndjson_compact_forces_output_format() {
    let output = resolve_output_format(vt_optimizer::cli::ReportFormat::Text, true);
    assert_eq!(output, vt_optimizer::cli::ReportFormat::Ndjson);
    let output = resolve_output_format(vt_optimizer::cli::ReportFormat::Json, false);
    assert_eq!(output, vt_optimizer::cli::ReportFormat::Json);
}
