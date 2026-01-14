use std::collections::BTreeMap;

use vt_optimizer::mbtiles::{HistogramBucket, MbtilesReport, MbtilesStats};
use vt_optimizer::output::{StatsSection, apply_stats_filter, parse_stats_filter};

#[test]
fn parse_stats_filter_selects_sections() {
    let filter = parse_stats_filter(Some("summary,zoom")).expect("filter");
    assert!(filter.includes(StatsSection::Summary));
    assert!(filter.includes(StatsSection::Zoom));
    assert!(!filter.includes(StatsSection::Histogram));
}

#[test]
fn apply_stats_filter_removes_unlisted_sections() {
    let report = MbtilesReport {
        metadata: BTreeMap::from([("name".to_string(), "tiles".to_string())]),
        overall: MbtilesStats {
            tile_count: 1,
            total_bytes: 10,
            max_bytes: 10,
            avg_bytes: 10,
        },
        by_zoom: vec![],
        empty_tiles: 1,
        empty_ratio: 1.0,
        sampled: true,
        sample_total_tiles: 10,
        sample_used_tiles: 1,
        histogram: vec![HistogramBucket {
            min_bytes: 1,
            max_bytes: 2,
            count: 1,
            total_bytes: 1,
            running_avg_bytes: 1,
            pct_tiles: 1.0,
            pct_level_bytes: 1.0,
            accum_pct_tiles: 1.0,
            accum_pct_level_bytes: 1.0,
            avg_near_limit: false,
            avg_over_limit: false,
        }],
        histograms_by_zoom: vec![],
        file_layers: vec![],
        top_tiles: vec![],
        bucket_count: Some(1),
        bucket_tiles: vec![],
        tile_summary: None,
        recommended_buckets: vec![1],
        top_tile_summaries: vec![],
    };

    let filter = parse_stats_filter(Some("summary")).expect("filter");
    let report = apply_stats_filter(report, &filter);
    assert!(report.metadata.is_empty());
    assert!(report.histogram.is_empty());
    assert!(report.recommended_buckets.is_empty());
    assert!(report.bucket_count.is_none());
    assert_eq!(report.overall.tile_count, 1);
}

#[test]
fn parse_stats_filter_rejects_empty_list() {
    let err = parse_stats_filter(Some("")).expect_err("empty list should error");
    assert!(err.to_string().contains("possible values"));
}

#[test]
fn parse_stats_filter_rejects_unknown_section() {
    let err = parse_stats_filter(Some("unknown")).expect_err("unknown should error");
    assert!(err.to_string().contains("possible values"));
}
