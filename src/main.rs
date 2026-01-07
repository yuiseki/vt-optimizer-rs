use anyhow::Result;
use clap::Parser;

use tile_prune::cli::{Cli, Command, ReportFormat, TileSortArg};
use tile_prune::format::{plan_copy, plan_optimize, resolve_output_path};
use tile_prune::mbtiles::{
    copy_mbtiles, inspect_mbtiles_with_options, parse_sample_spec, parse_tile_spec, InspectOptions,
    TileListOptions, TileSort,
};
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
            let tile = match args.tile.as_deref() {
                Some(value) => Some(parse_tile_spec(value)?),
                None => None,
            };
            if args.summary && tile.is_none() {
                anyhow::bail!("--summary requires --tile z/x/y");
            }
            if tile.is_some() && !args.summary {
                anyhow::bail!("--tile requires --summary");
            }
            if args.layer.is_some() && !args.summary {
                anyhow::bail!("--layer requires --summary");
            }
            if args.recommend && args.zoom.is_none() {
                anyhow::bail!("--recommend requires --zoom");
            }
            if args.recommend && args.histogram_buckets == 0 {
                anyhow::bail!("--recommend requires --histogram-buckets");
            }
            let topn = if args.recommend && args.topn.is_none() {
                Some(5)
            } else {
                args.topn
            };
            let options = InspectOptions {
                sample,
                topn: topn.unwrap_or(0) as usize,
                histogram_buckets: args.histogram_buckets as usize,
                no_progress: args.no_progress,
                max_tile_bytes: args.max_tile_bytes,
                zoom: args.zoom,
                bucket: args.bucket,
                tile,
                summary: args.summary,
                layer: args.layer.clone(),
                recommend: args.recommend,
                list_tiles: if args.list_tiles {
                    Some(TileListOptions {
                        limit: args.limit,
                        sort: match args.sort {
                            TileSortArg::Size => TileSort::Size,
                            TileSortArg::Zxy => TileSort::Zxy,
                        },
                    })
                } else {
                    None
                },
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
                            let warn = if bucket.avg_over_limit {
                                "over"
                            } else if bucket.avg_near_limit {
                                "near"
                            } else {
                                ""
                            };
                            println!(
                                "{}-{}: count={} bytes={} running_avg={} pct_tiles={:.4} pct_size={:.4} accum_pct_tiles={:.4} accum_pct_size={:.4} {}",
                                bucket.min_bytes,
                                bucket.max_bytes,
                                bucket.count,
                                bucket.total_bytes,
                                bucket.running_avg_bytes,
                                bucket.pct_tiles,
                                bucket.pct_level_bytes,
                                bucket.accum_pct_tiles,
                                bucket.accum_pct_level_bytes,
                                warn
                            );
                        }
                    }
                    if !report.recommended_buckets.is_empty() {
                        println!(
                            "recommended_buckets: {}",
                            report
                                .recommended_buckets
                                .iter()
                                .map(|idx| idx.to_string())
                                .collect::<Vec<_>>()
                                .join(",")
                        );
                    }
                    if let Some(count) = report.bucket_count {
                        println!("bucket_count: {}", count);
                    }
                    if !report.bucket_tiles.is_empty() {
                        println!("bucket_tiles:");
                        for tile in report.bucket_tiles.iter() {
                            println!(
                                "z={}: x={} y={} bytes={}",
                                tile.zoom, tile.x, tile.y, tile.bytes
                            );
                        }
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
                    if !report.top_tile_summaries.is_empty() {
                        println!("top_tile_summaries:");
                        for summary in report.top_tile_summaries.iter() {
                            println!(
                                "tile_summary: z={} x={} y={} total_features={}",
                                summary.zoom, summary.x, summary.y, summary.total_features
                            );
                            for layer in summary.layers.iter() {
                                println!(
                                    "layer: {} features={} property_keys={}",
                                    layer.name, layer.feature_count, layer.property_key_count
                                );
                            }
                        }
                    }
                    if let Some(summary) = report.tile_summary.as_ref() {
                        println!(
                            "tile_summary: z={} x={} y={} total_features={}",
                            summary.zoom, summary.x, summary.y, summary.total_features
                        );
                        for layer in summary.layers.iter() {
                            println!(
                                "layer: {} features={} property_keys={}",
                                layer.name, layer.feature_count, layer.property_key_count
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
