use std::collections::BTreeMap;

use vt_optimizer::cli::TileInfoFormat;
use vt_optimizer::mbtiles::{LayerSummary, MbtilesReport, MbtilesStats, TileSummary};
use vt_optimizer::output::apply_tile_info_format;

#[test]
fn tile_info_format_compact_clears_property_keys() {
    let layer = LayerSummary {
        name: "roads".to_string(),
        feature_count: 1,
        vertex_count: 2,
        property_key_count: 2,
        property_value_count: 2,
        property_keys: vec!["class".to_string(), "name".to_string()],
    };
    let summary = TileSummary {
        zoom: 1,
        x: 2,
        y: 3,
        layer_count: 1,
        total_features: 1,
        vertex_count: 2,
        property_key_count: 2,
        property_value_count: 2,
        layers: vec![layer.clone()],
    };
    let report = MbtilesReport {
        metadata: BTreeMap::new(),
        overall: MbtilesStats {
            tile_count: 0,
            total_bytes: 0,
            max_bytes: 0,
            avg_bytes: 0,
        },
        by_zoom: vec![],
        empty_tiles: 0,
        empty_ratio: 0.0,
        sampled: false,
        sample_total_tiles: 0,
        sample_used_tiles: 0,
        histogram: vec![],
        histograms_by_zoom: vec![],
        file_layers: vec![],
        top_tiles: vec![],
        bucket_count: None,
        bucket_tiles: vec![],
        tile_summary: Some(summary),
        recommended_buckets: vec![],
        top_tile_summaries: vec![TileSummary {
            zoom: 4,
            x: 5,
            y: 6,
            layer_count: 1,
            total_features: 1,
            vertex_count: 2,
            property_key_count: 2,
            property_value_count: 2,
            layers: vec![layer],
        }],
    };

    let report = apply_tile_info_format(report, TileInfoFormat::Compact);
    let summary = report.tile_summary.expect("tile summary");
    assert!(summary.layers[0].property_keys.is_empty());
    assert!(
        report.top_tile_summaries[0].layers[0]
            .property_keys
            .is_empty()
    );
}
