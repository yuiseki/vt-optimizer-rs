use tile_prune::mbtiles::{
    HistogramBucket, MbtilesReport, MbtilesStats, MbtilesZoomStats, TileSummary, TopTile,
    ZoomHistogram,
};
use tile_prune::output::ndjson_lines;

#[test]
fn ndjson_splits_histograms_and_top_tile_summaries() {
    let report = MbtilesReport {
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
                total_features: 1,
                layers: vec![],
            },
            TileSummary {
                zoom: 1,
                x: 1,
                y: 1,
                total_features: 2,
                layers: vec![],
            },
        ],
    };

    let lines = ndjson_lines(&report, true).expect("ndjson");
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
    let top_tile = types.iter().filter(|t| t == &&"top_tile".to_string()).count();
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
        top_tiles: vec![],
        bucket_count: None,
        bucket_tiles: vec![],
        tile_summary: None,
        recommended_buckets: vec![],
        top_tile_summaries: vec![],
    };

    let lines = ndjson_lines(&report, false).expect("ndjson");
    assert!(
        !lines.iter().any(|line| line.contains("\"type\":\"summary\"")),
        "summary line should be omitted"
    );
}
