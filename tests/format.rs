use std::path::Path;

use tile_prune::format::{decide_formats, TileFormat};

#[test]
fn decide_formats_infer_from_extensions() {
    let decision = decide_formats(
        Path::new("input.mbtiles"),
        Some(Path::new("out.pmtiles")),
        None,
        None,
    )
    .expect("decision");

    assert_eq!(decision.input, TileFormat::Mbtiles);
    assert_eq!(decision.output, TileFormat::Pmtiles);
}

#[test]
fn decide_formats_defaults_output_to_input() {
    let decision = decide_formats(Path::new("input.pmtiles"), None, None, None)
        .expect("decision");

    assert_eq!(decision.input, TileFormat::Pmtiles);
    assert_eq!(decision.output, TileFormat::Pmtiles);
}

#[test]
fn decide_formats_output_without_extension_falls_back_to_input() {
    let decision = decide_formats(
        Path::new("input.mbtiles"),
        Some(Path::new("out")),
        None,
        None,
    )
    .expect("decision");

    assert_eq!(decision.input, TileFormat::Mbtiles);
    assert_eq!(decision.output, TileFormat::Mbtiles);
}

#[test]
fn decide_formats_input_override_takes_precedence() {
    let decision = decide_formats(
        Path::new("input.unknown"),
        None,
        Some("pmtiles"),
        None,
    )
    .expect("decision");

    assert_eq!(decision.input, TileFormat::Pmtiles);
    assert_eq!(decision.output, TileFormat::Pmtiles);
}

#[test]
fn decide_formats_output_override_takes_precedence() {
    let decision = decide_formats(
        Path::new("input.mbtiles"),
        Some(Path::new("out.unknown")),
        None,
        Some("pmtiles"),
    )
    .expect("decision");

    assert_eq!(decision.input, TileFormat::Mbtiles);
    assert_eq!(decision.output, TileFormat::Pmtiles);
}

#[test]
fn decide_formats_errors_when_input_unknown_and_no_override() {
    let err = decide_formats(Path::new("input.unknown"), None, None, None)
        .expect_err("should error");

    let msg = err.to_string();
    assert!(msg.contains("cannot infer input format"));
}

#[test]
fn decide_formats_errors_on_unknown_override() {
    let err = decide_formats(
        Path::new("input.mbtiles"),
        None,
        Some("tilejson"),
        None,
    )
    .expect_err("should error");

    let msg = err.to_string();
    assert!(msg.contains("unknown input format"));
}
