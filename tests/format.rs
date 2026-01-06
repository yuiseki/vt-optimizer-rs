use std::path::Path;

use tile_prune::format::{
    decide_formats, default_output_path_pruned, plan_copy, plan_optimize, TileFormat,
};
use tile_prune::format::validate_output_format_matches_path;

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

#[test]
fn validate_output_format_conflict_errors() {
    let err = validate_output_format_matches_path(
        Some(Path::new("out.pmtiles")),
        Some("mbtiles"),
    )
    .expect_err("should error");

    let msg = err.to_string();
    assert!(msg.contains("conflicts"));
}

#[test]
fn validate_output_format_matches_extension_ok() {
    validate_output_format_matches_path(
        Some(Path::new("out.mbtiles")),
        Some("mbtiles"),
    )
    .expect("should pass");
}

#[test]
fn validate_output_format_with_no_extension_ok() {
    validate_output_format_matches_path(
        Some(Path::new("out")),
        Some("pmtiles"),
    )
    .expect("should pass");
}

#[test]
fn validate_output_format_unknown_errors() {
    let err = validate_output_format_matches_path(
        Some(Path::new("out.mbtiles")),
        Some("tilejson"),
    )
    .expect_err("should error");

    let msg = err.to_string();
    assert!(msg.contains("unknown output format"));
}

#[test]
fn plan_copy_uses_decide_formats() {
    let decision = plan_copy(
        Path::new("input.pmtiles"),
        Some(Path::new("out.mbtiles")),
        None,
        None,
    )
    .expect("decision");

    assert_eq!(decision.input, TileFormat::Pmtiles);
    assert_eq!(decision.output, TileFormat::Mbtiles);
}

#[test]
fn plan_copy_rejects_output_conflict() {
    let err = plan_copy(
        Path::new("input.mbtiles"),
        Some(Path::new("out.pmtiles")),
        None,
        Some("mbtiles"),
    )
    .expect_err("should error");

    let msg = err.to_string();
    assert!(msg.contains("conflicts"));
}

#[test]
fn plan_optimize_uses_decide_formats() {
    let decision = plan_optimize(
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
fn plan_optimize_rejects_output_conflict() {
    let err = plan_optimize(
        Path::new("input.pmtiles"),
        Some(Path::new("out.mbtiles")),
        None,
        Some("pmtiles"),
    )
    .expect_err("should error");

    let msg = err.to_string();
    assert!(msg.contains("conflicts"));
}

#[test]
fn default_output_path_pruned_changes_extension() {
    let path = default_output_path_pruned(Path::new("planet.mbtiles"), TileFormat::Pmtiles);
    assert_eq!(path.as_os_str(), "planet.pruned.pmtiles");
}

#[test]
fn default_output_path_pruned_same_extension() {
    let path = default_output_path_pruned(Path::new("planet.pmtiles"), TileFormat::Pmtiles);
    assert_eq!(path.as_os_str(), "planet.pruned.pmtiles");
}

#[test]
fn default_output_path_pruned_without_extension() {
    let path = default_output_path_pruned(Path::new("planet"), TileFormat::Mbtiles);
    assert_eq!(path.as_os_str(), "planet.pruned.mbtiles");
}

#[test]
fn default_output_path_pruned_preserves_directory() {
    let path = default_output_path_pruned(Path::new("data/planet.mbtiles"), TileFormat::Mbtiles);
    assert_eq!(path.as_os_str(), "data/planet.pruned.mbtiles");
}
