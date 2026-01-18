use clap::{CommandFactory, Parser};

use vt_optimizer::cli::ReportFormat;
use vt_optimizer::cli::{Cli, Command, StyleMode, TileInfoFormat, UnknownFilterMode};

#[test]
fn parse_optimize_minimal() {
    let cli = Cli::parse_from(["vt-optimizer", "optimize", "hoge.mbtiles"]);
    match cli.command {
        Some(Command::Optimize(args)) => {
            assert_eq!(args.input.as_os_str(), "hoge.mbtiles");
            assert_eq!(args.output, None);
            assert_eq!(args.input_format, None);
            assert_eq!(args.output_format, None);
            assert_eq!(args.style, None);
            assert_eq!(args.style_mode, StyleMode::LayerFilter);
            assert_eq!(args.unknown_filter, UnknownFilterMode::Keep);
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
        "vt-optimizer",
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
        "--unknown-filter",
        "drop",
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
        Some(Command::Optimize(args)) => {
            assert_eq!(args.input.as_os_str(), "planet.mbtiles");
            assert_eq!(args.output.unwrap().as_os_str(), "out.pmtiles");
            assert_eq!(args.input_format.unwrap(), "mbtiles");
            assert_eq!(args.output_format.unwrap(), "pmtiles");
            assert_eq!(args.style.unwrap().as_os_str(), "style.json");
            assert_eq!(args.style_mode, StyleMode::LayerFilter);
            assert_eq!(args.unknown_filter, UnknownFilterMode::Drop);
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
    let cli = Cli::parse_from([
        "vt-optimizer",
        "optimize",
        "in.mbtiles",
        "--style-mode",
        "layer",
    ]);
    match cli.command {
        Some(Command::Optimize(args)) => {
            assert_eq!(args.style_mode, StyleMode::Layer);
        }
        _ => panic!("expected optimize command"),
    }

    let cli = Cli::parse_from([
        "vt-optimizer",
        "optimize",
        "in.mbtiles",
        "--style-mode",
        "vt-compat",
    ]);
    match cli.command {
        Some(Command::Optimize(args)) => {
            assert_eq!(args.style_mode, StyleMode::VtCompat);
        }
        _ => panic!("expected optimize command"),
    }
}

#[test]
fn parse_inspect_options() {
    let cli = Cli::parse_from([
        "vt-optimizer",
        "inspect",
        "input.mbtiles",
        "--sample",
        "0.1",
        "--topn",
        "5",
        "--histogram-buckets",
        "12",
        "--report-format",
        "json",
        "--stats",
        "summary,zoom",
        "--no-progress",
        "--zoom",
        "3",
        "--bucket",
        "2",
        "--tile",
        "3/4/5",
        "--summary",
        "--layers",
        "roads,buildings",
        "--recommend",
        "--fast",
        "--list-tiles",
        "--limit",
        "20",
        "--sort",
        "zxy",
        "--ndjson-lite",
        "--ndjson-compact",
        "--tile-info-format",
        "compact",
    ]);

    match cli.command {
        Some(Command::Inspect(args)) => {
            assert_eq!(args.input.as_os_str(), "input.mbtiles");
            assert_eq!(args.sample.as_deref(), Some("0.1"));
            assert_eq!(args.topn, Some(5));
            assert_eq!(args.histogram_buckets, 12);
            assert_eq!(args.output, ReportFormat::Json);
            assert_eq!(args.stats.as_deref(), Some("summary,zoom"));
            assert!(args.no_progress);
            assert_eq!(args.zoom, Some(3));
            assert_eq!(args.x, None);
            assert_eq!(args.y, None);
            assert_eq!(args.bucket, Some(2));
            assert_eq!(args.tile.as_deref(), Some("3/4/5"));
            assert!(args.summary);
            assert_eq!(
                args.layers,
                vec!["roads".to_string(), "buildings".to_string()]
            );
            assert!(args.recommend);
            assert!(args.fast);
            assert!(args.list_tiles);
            assert_eq!(args.limit, 20);
            assert_eq!(args.sort, vt_optimizer::cli::TileSortArg::Zxy);
            assert!(args.ndjson_lite);
            assert!(args.ndjson_compact);
            assert_eq!(args.tile_info_format, TileInfoFormat::Compact);
        }
        _ => panic!("expected inspect command"),
    }
}

#[test]
fn parse_inspect_tile_coords_short_flags() {
    let cli = Cli::parse_from([
        "vt-optimizer",
        "inspect",
        "input.mbtiles",
        "-z",
        "5",
        "-x",
        "16",
        "-y",
        "20",
    ]);
    match cli.command {
        Some(Command::Inspect(args)) => {
            assert_eq!(args.zoom, Some(5));
            assert_eq!(args.x, Some(16));
            assert_eq!(args.y, Some(20));
        }
        _ => panic!("expected inspect command"),
    }
}

#[test]
fn parse_inspect_output_ndjson() {
    let cli = Cli::parse_from([
        "vt-optimizer",
        "inspect",
        "input.mbtiles",
        "--report-format",
        "ndjson",
    ]);
    match cli.command {
        Some(Command::Inspect(args)) => {
            assert_eq!(args.output, ReportFormat::Ndjson);
        }
        _ => panic!("expected inspect command"),
    }
}

#[test]
fn parse_inspect_layers_deprecated_alias() {
    let cli = Cli::parse_from([
        "vt-optimizer",
        "inspect",
        "input.mbtiles",
        "--layer",
        "roads",
    ]);
    match cli.command {
        Some(Command::Inspect(args)) => {
            assert_eq!(args.layer, vec!["roads".to_string()]);
        }
        _ => panic!("expected inspect command"),
    }
}

#[test]
fn inspect_help_describes_fields() {
    let mut cmd = Cli::command();
    let inspect = cmd.find_subcommand_mut("inspect").expect("inspect command");
    let mut buffer = Vec::new();
    inspect.write_long_help(&mut buffer).expect("help");
    let help = String::from_utf8(buffer).expect("utf8");

    assert!(help.contains("Sampling strategy"));
    assert!(help.contains("Output format"));
    assert!(help.contains("Limit output sections"));
    assert!(help.contains("Fast defaults"));
    assert!(help.contains("Histogram bucket index"));
    assert!(help.contains("NDJSON"));
    assert!(help.contains("Tile summary detail level"));
}
