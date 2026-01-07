use clap::Parser;

use tile_prune::cli::{Cli, Command, StyleMode};
use tile_prune::cli::ReportFormat;

#[test]
fn parse_optimize_minimal() {
    let cli = Cli::parse_from(["tile-prune", "optimize", "hoge.mbtiles"]);
    match cli.command {
        Command::Optimize(args) => {
            assert_eq!(args.input.as_os_str(), "hoge.mbtiles");
            assert_eq!(args.output, None);
            assert_eq!(args.input_format, None);
            assert_eq!(args.output_format, None);
            assert_eq!(args.style, None);
            assert_eq!(args.style_mode, StyleMode::LayerFilter);
            assert_eq!(args.max_tile_bytes, 1_280_000);
            assert_eq!(args.threads, None);
            assert_eq!(args.io_batch, 1_000);
            assert_eq!(args.checkpoint, None);
            assert!(!args.resume);
        }
        _ => panic!("expected optimize command"),
    }
}

#[test]
fn parse_optimize_options() {
    let cli = Cli::parse_from([
        "tile-prune",
        "optimize",
        "planet.mbtiles",
        "--output",
        "out.pmtiles",
        "--input-format",
        "mbtiles",
        "--output-format",
        "pmtiles",
        "--style",
        "style.json",
        "--style-mode",
        "layer+filter",
        "--max-tile-bytes",
        "2048",
        "--threads",
        "8",
        "--io-batch",
        "200",
        "--checkpoint",
        "state.json",
        "--resume",
    ]);

    match cli.command {
        Command::Optimize(args) => {
            assert_eq!(args.input.as_os_str(), "planet.mbtiles");
            assert_eq!(args.output.unwrap().as_os_str(), "out.pmtiles");
            assert_eq!(args.input_format.unwrap(), "mbtiles");
            assert_eq!(args.output_format.unwrap(), "pmtiles");
            assert_eq!(args.style.unwrap().as_os_str(), "style.json");
            assert_eq!(args.style_mode, StyleMode::LayerFilter);
            assert_eq!(args.max_tile_bytes, 2048);
            assert_eq!(args.threads, Some(8));
            assert_eq!(args.io_batch, 200);
            assert_eq!(args.checkpoint.unwrap().as_os_str(), "state.json");
            assert!(args.resume);
        }
        _ => panic!("expected optimize command"),
    }
}

#[test]
fn parse_optimize_style_modes() {
    let cli = Cli::parse_from(["tile-prune", "optimize", "in.mbtiles", "--style-mode", "none"]);
    match cli.command {
        Command::Optimize(args) => {
            assert_eq!(args.style_mode, StyleMode::None);
        }
        _ => panic!("expected optimize command"),
    }

    let cli = Cli::parse_from(["tile-prune", "optimize", "in.mbtiles", "--style-mode", "layer"]);
    match cli.command {
        Command::Optimize(args) => {
            assert_eq!(args.style_mode, StyleMode::Layer);
        }
        _ => panic!("expected optimize command"),
    }
}

#[test]
fn parse_inspect_options() {
    let cli = Cli::parse_from([
        "tile-prune",
        "inspect",
        "input.mbtiles",
        "--sample",
        "0.1",
        "--topn",
        "5",
        "--histogram-buckets",
        "12",
        "--output",
        "json",
        "--no-progress",
        "--zoom",
        "3",
        "--bucket",
        "2",
        "--tile",
        "3/4/5",
        "--summary",
        "--layer",
        "roads",
        "--recommend",
        "--fast",
        "--list-tiles",
        "--limit",
        "20",
        "--sort",
        "zxy",
        "--ndjson-lite",
        "--ndjson-compact",
    ]);

    match cli.command {
        Command::Inspect(args) => {
            assert_eq!(args.input.as_os_str(), "input.mbtiles");
            assert_eq!(args.sample.as_deref(), Some("0.1"));
            assert_eq!(args.topn, Some(5));
            assert_eq!(args.histogram_buckets, 12);
            assert_eq!(args.output, ReportFormat::Json);
            assert!(args.no_progress);
            assert_eq!(args.zoom, Some(3));
            assert_eq!(args.bucket, Some(2));
            assert_eq!(args.tile.as_deref(), Some("3/4/5"));
            assert!(args.summary);
            assert_eq!(args.layer.as_deref(), Some("roads"));
            assert!(args.recommend);
            assert!(args.fast);
            assert!(args.list_tiles);
            assert_eq!(args.limit, 20);
            assert_eq!(args.sort, tile_prune::cli::TileSortArg::Zxy);
            assert!(args.ndjson_lite);
            assert!(args.ndjson_compact);
        }
        _ => panic!("expected inspect command"),
    }
}

#[test]
fn parse_inspect_output_ndjson() {
    let cli = Cli::parse_from(["tile-prune", "inspect", "input.mbtiles", "--output", "ndjson"]);
    match cli.command {
        Command::Inspect(args) => {
            assert_eq!(args.output, ReportFormat::Ndjson);
        }
        _ => panic!("expected inspect command"),
    }
}
