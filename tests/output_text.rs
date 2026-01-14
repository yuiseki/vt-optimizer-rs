use std::collections::BTreeMap;

use vt_optimizer::mbtiles::{HistogramBucket, TileSummary, ZoomHistogram};
use vt_optimizer::output::{
    format_histogram_table, format_histograms_by_zoom_section, format_metadata_section,
    format_tile_summary_text, summarize_file_layers, LayerTotals,
};

#[allow(clippy::too_many_arguments)]
fn bucket(
    min_bytes: u64,
    max_bytes: u64,
    count: u64,
    total_bytes: u64,
    running_avg_bytes: u64,
    pct_tiles: f64,
    pct_level_bytes: f64,
    accum_pct_tiles: f64,
    accum_pct_level_bytes: f64,
    avg_near_limit: bool,
    avg_over_limit: bool,
) -> HistogramBucket {
    HistogramBucket {
        min_bytes,
        max_bytes,
        count,
        total_bytes,
        running_avg_bytes,
        pct_tiles,
        pct_level_bytes,
        accum_pct_tiles,
        accum_pct_level_bytes,
        avg_near_limit,
        avg_over_limit,
    }
}

#[test]
fn format_histogram_table_includes_warning_markers() {
    let buckets = vec![
        bucket(0, 10, 1, 5, 5, 0.5, 0.5, 0.5, 0.5, false, true),
        bucket(10, 20, 1, 5, 5, 0.5, 0.5, 1.0, 1.0, true, false),
    ];
    let lines = format_histogram_table(&buckets);
    let header = lines.first().expect("missing header");
    assert!(header.contains("acc%size"));
    assert!(lines.iter().any(|line| line.contains("!! (over)")));
    assert!(lines.iter().any(|line| line.contains("! (near)")));
}

#[test]
fn format_histograms_by_zoom_section_sorts_and_labels() {
    let histograms = vec![
        ZoomHistogram {
            zoom: 5,
            buckets: vec![bucket(0, 10, 1, 5, 5, 1.0, 1.0, 1.0, 1.0, false, false)],
        },
        ZoomHistogram {
            zoom: 2,
            buckets: vec![bucket(0, 10, 1, 5, 5, 1.0, 1.0, 1.0, 1.0, false, false)],
        },
    ];

    let lines = format_histograms_by_zoom_section(&histograms);
    let header_index = lines
        .iter()
        .position(|line| line == "## Histogram by Zoom")
        .expect("missing section header");
    let z2_index = lines
        .iter()
        .position(|line| line == "### z=2")
        .expect("missing z=2 heading");
    let z5_index = lines
        .iter()
        .position(|line| line == "### z=5")
        .expect("missing z=5 heading");

    assert!(header_index < z2_index);
    assert!(z2_index < z5_index);
    assert!(lines.iter().any(|line| line.contains("range")));
}

#[test]
fn format_histograms_by_zoom_omits_empty_buckets() {
    let histograms = vec![ZoomHistogram {
        zoom: 3,
        buckets: vec![
            bucket(0, 10, 0, 0, 0, 0.0, 0.0, 0.0, 0.0, false, false),
            bucket(10, 20, 2, 20, 10, 1.0, 1.0, 1.0, 1.0, false, false),
        ],
    }];

    let lines = format_histograms_by_zoom_section(&histograms);
    let zero_bucket_lines = lines
        .iter()
        .filter(|line| line.contains("0B") && line.contains("  0"))
        .count();
    assert_eq!(zero_bucket_lines, 0);
}

#[test]
fn format_histogram_table_omits_empty_buckets() {
    let buckets = vec![
        bucket(0, 10, 0, 0, 0, 0.0, 0.0, 0.0, 0.0, false, false),
        bucket(10, 20, 1, 10, 10, 1.0, 1.0, 1.0, 1.0, false, false),
    ];
    let lines = format_histogram_table(&buckets);
    let zero_bucket_lines = lines
        .iter()
        .filter(|line| line.contains("0B") && line.contains("  0"))
        .count();
    assert_eq!(zero_bucket_lines, 0);
}

#[test]
fn format_metadata_section_lists_entries() {
    let mut metadata = BTreeMap::new();
    metadata.insert("name".to_string(), "sample".to_string());
    metadata.insert("format".to_string(), "pbf".to_string());
    metadata.insert("json".to_string(), "{\"hello\":true}".to_string());
    metadata.insert(
        "vector_layers".to_string(),
        "[{\"id\":\"roads\"}]".to_string(),
    );
    let lines = format_metadata_section(&metadata);
    assert_eq!(lines.first(), Some(&"## Metadata".to_string()));
    assert!(lines.iter().any(|line| line.contains("- name: sample")));
    assert!(lines.iter().any(|line| line.contains("- format: pbf")));
    assert!(!lines.iter().any(|line| line.contains("- json:")));
    assert!(!lines.iter().any(|line| line.contains("- vector_layers:")));
}

#[test]
fn format_tile_summary_text_includes_tile_counts() {
    let summary = TileSummary {
        zoom: 12,
        x: 345,
        y: 678,
        layer_count: 3,
        total_features: 42,
        vertex_count: 9001,
        property_key_count: 7,
        property_value_count: 9,
        layers: Vec::new(),
    };

    let lines = format_tile_summary_text(&summary);

    assert_eq!(
        lines,
        vec![
            "- z=12 x=345 y=678".to_string(),
            "- Layers in this tile: 3".to_string(),
            "- Features in this tile: 42".to_string(),
            "- Vertices in this tile: 9001".to_string(),
            "- Keys in this tile: 7".to_string(),
            "- Values in this tile: 9".to_string(),
        ]
    );
}

#[test]
fn summarize_file_layers_accumulates_counts() {
    let layers = vec![
        vt_optimizer::mbtiles::FileLayerSummary {
            name: "a".to_string(),
            vertex_count: 10,
            feature_count: 2,
            property_key_count: 3,
            property_value_count: 4,
        },
        vt_optimizer::mbtiles::FileLayerSummary {
            name: "b".to_string(),
            vertex_count: 20,
            feature_count: 5,
            property_key_count: 7,
            property_value_count: 11,
        },
    ];

    let totals = summarize_file_layers(&layers);

    assert_eq!(
        totals,
        Some(LayerTotals {
            layer_count: 2,
            feature_count: 7,
            vertex_count: 30,
            property_key_count: 10,
            property_value_count: 15,
        })
    );
}
