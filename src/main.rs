use anyhow::Result;
use clap::Parser;

use tile_prune::cli::{Cli, Command, ReportFormat};
use tile_prune::format::{plan_copy, plan_optimize, resolve_output_path};
use tile_prune::mbtiles::{copy_mbtiles, inspect_mbtiles_with_options, parse_sample_spec, InspectOptions};
use tile_prune::pmtiles::{mbtiles_to_pmtiles, pmtiles_to_mbtiles};

fn main() -> Result<()> {
    let cli = Cli::parse();
    init_tracing(&cli.log);

    match cli.command {
        Command::Inspect(args) => {
            let sample = match args.sample.as_deref() {
                Some(value) => Some(parse_sample_spec(value)?),
                None => None,
            };
            let options = InspectOptions {
                sample,
                topn: args.topn.unwrap_or(0) as usize,
                histogram_buckets: args.histogram_buckets as usize,
                no_progress: args.no_progress,
                zoom: args.zoom,
                bucket: args.bucket,
                list_tiles: None,
            };
            let report = inspect_mbtiles_with_options(&args.input, options)?;
            match args.output {
                ReportFormat::Json => {
                    let json = serde_json::to_string_pretty(&report)?;
                    println!("{}", json);
                }
                ReportFormat::Text => {
                    println!(
                        "tiles: {} total_bytes: {} max_bytes: {} avg_bytes: {}",
                        report.overall.tile_count,
                        report.overall.total_bytes,
                        report.overall.max_bytes,
                        report.overall.avg_bytes
                    );
                    println!(
                        "empty_tiles: {} empty_ratio: {:.4}",
                        report.empty_tiles, report.empty_ratio
                    );
                    if report.sampled {
                        println!(
                            "sample: used={} total={}",
                            report.sample_used_tiles, report.sample_total_tiles
                        );
                    }
                    for zoom in report.by_zoom.iter() {
                        println!(
                            "z={}: tiles={} total_bytes={} max_bytes={} avg_bytes={}",
                            zoom.zoom,
                            zoom.stats.tile_count,
                            zoom.stats.total_bytes,
                            zoom.stats.max_bytes,
                            zoom.stats.avg_bytes
                        );
                    }
                    if !report.histogram.is_empty() {
                        println!("histogram:");
                        for bucket in report.histogram.iter() {
                            println!(
                                "{}-{}: {}",
                                bucket.min_bytes, bucket.max_bytes, bucket.count
                            );
                        }
                    }
                    if let Some(count) = report.bucket_count {
                        println!("bucket_count: {}", count);
                    }
                    if !report.top_tiles.is_empty() {
                        println!("top_tiles:");
                        for tile in report.top_tiles.iter() {
                            println!(
                                "z={}: x={} y={} bytes={}",
                                tile.zoom, tile.x, tile.y, tile.bytes
                            );
                        }
                    }
                }
            }
        }
        Command::Optimize(args) => {
            let decision = plan_optimize(
                &args.input,
                args.output.as_deref(),
                args.input_format.as_deref(),
                args.output_format.as_deref(),
            )?;
            let _output_path =
                resolve_output_path(&args.input, args.output.as_deref(), decision.output);
            println!("optimize: input={}", args.input.display());
        }
        Command::Simplify(args) => {
            println!("simplify: input={} z={} x={} y={}", args.input.display(), args.z, args.x, args.y);
        }
        Command::Copy(args) => {
            let decision = plan_copy(
                &args.input,
                args.output.as_deref(),
                args.input_format.as_deref(),
                args.output_format.as_deref(),
            )?;
            let _output_path =
                resolve_output_path(&args.input, args.output.as_deref(), decision.output);
            match (decision.input, decision.output) {
                (tile_prune::format::TileFormat::Mbtiles, tile_prune::format::TileFormat::Mbtiles) => {
                    copy_mbtiles(&args.input, &_output_path)?;
                }
                (tile_prune::format::TileFormat::Mbtiles, tile_prune::format::TileFormat::Pmtiles) => {
                    mbtiles_to_pmtiles(&args.input, &_output_path)?;
                }
                (tile_prune::format::TileFormat::Pmtiles, tile_prune::format::TileFormat::Mbtiles) => {
                    pmtiles_to_mbtiles(&args.input, &_output_path)?;
                }
                (tile_prune::format::TileFormat::Pmtiles, tile_prune::format::TileFormat::Pmtiles) => {
                    anyhow::bail!("v0.0.3 does not support PMTiles to PMTiles copy");
                }
            }
            println!("copy: input={}", args.input.display());
        }
        Command::Verify(args) => {
            println!("verify: input={}", args.input.display());
        }
    }

    Ok(())
}

fn init_tracing(level: &str) {
    let filter = tracing_subscriber::EnvFilter::try_new(level).unwrap_or_else(|_| {
        tracing_subscriber::EnvFilter::new("info")
    });
    tracing_subscriber::fmt().with_env_filter(filter).init();
}
