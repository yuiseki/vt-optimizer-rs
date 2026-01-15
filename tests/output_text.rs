use std::collections::BTreeMap;

use nu_ansi_term::Color;
use vt_optimizer::mbtiles::{
    HistogramBucket, MbtilesStats, MbtilesZoomStats, TileSummary, TopTile, ZoomHistogram,
};
use vt_optimizer::output::{
    LayerTotals, format_histogram_table, format_histograms_by_zoom_section,
    format_metadata_section, format_tile_summary_text, format_top_tiles_lines, format_zoom_table,
    summarize_file_layers,
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

fn zoom_stats(zoom: u8, tile_count: u64, total: u64, max: u64, avg: u64) -> MbtilesZoomStats {
    MbtilesZoomStats {
        zoom,
        stats: MbtilesStats {
            tile_count,
            total_bytes: total,
            max_bytes: max,
            avg_bytes: avg,
        },
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
fn format_zoom_table_sorts_and_labels() {
    let stats = vec![
        zoom_stats(5, 10, 50_000, 10_000, 5_000),
        zoom_stats(2, 3, 3_000, 1_200, 1_000),
    ];
    let lines = format_zoom_table(&stats, 20, 100_000);
    let header = lines.first().expect("missing header");
    assert!(header.contains("zoom"));
    assert!(header.contains("%tiles"));
    assert!(header.contains("%size"));
    let z2_index = lines
        .iter()
        .position(|line| line.trim_start().starts_with('2'))
        .expect("missing z=2 row");
    let z5_index = lines
        .iter()
        .position(|line| line.trim_start().starts_with('5'))
        .expect("missing z=5 row");
    assert!(z2_index < z5_index);
}

#[test]
fn format_top_tiles_lines_includes_size() {
    let tiles = vec![TopTile {
        zoom: 1,
        x: 2,
        y: 3,
        bytes: 2048,
    }];
    let lines = format_top_tiles_lines(&tiles);
    assert_eq!(lines.len(), 1);
    assert!(lines[0].contains("-z 1"));
    assert!(lines[0].contains("-x 2"));
    assert!(lines[0].contains("-y 3"));
    assert!(lines[0].contains("size=2.00KB"));
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
        tile_bytes: 2048,
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
            format!("- {}: 2.00KB", Color::Blue.paint("Size of tile")),
            format!("- {}: 3", Color::Blue.paint("Layers in this tile")),
            format!("- {}: 42", Color::Blue.paint("Features in this tile")),
            format!("- {}: 9001", Color::Blue.paint("Vertices in this tile")),
            format!("- {}: 7", Color::Blue.paint("Keys in this tile")),
            format!("- {}: 9", Color::Blue.paint("Values in this tile")),
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
