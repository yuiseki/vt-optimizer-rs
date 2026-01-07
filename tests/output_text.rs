use tile_prune::mbtiles::{HistogramBucket, ZoomHistogram};
use tile_prune::output::{format_histogram_table, format_histograms_by_zoom_section};

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
