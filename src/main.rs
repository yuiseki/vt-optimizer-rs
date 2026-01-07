use anyhow::Result;
use clap::Parser;

use tile_prune::cli::{Cli, Command, ReportFormat, TileSortArg};
use tile_prune::format::{plan_copy, plan_optimize, resolve_output_path};
use tile_prune::mbtiles::{
    copy_mbtiles, inspect_mbtiles_with_options, parse_sample_spec, parse_tile_spec, InspectOptions,
    TileListOptions, TileSort,
};
use tile_prune::output::{ndjson_lines, resolve_output_format};
use tile_prune::pmtiles::{mbtiles_to_pmtiles, pmtiles_to_mbtiles};

fn main() -> Result<()> {
    let cli = Cli::parse();
    init_tracing(&cli.log);

    match cli.command {
        Command::Inspect(args) => {
            let output = resolve_output_format(args.output, args.ndjson_compact);
            if args.ndjson_lite && output != ReportFormat::Ndjson {
                anyhow::bail!("--ndjson-lite requires --output ndjson");
            }
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
            let (sample, topn, histogram_buckets) = if args.fast {
                (Some(tile_prune::mbtiles::SampleSpec::Ratio(0.1)), Some(5), 10)
            } else {
                (sample, topn, args.histogram_buckets as usize)
            };
            let options = InspectOptions {
                sample,
                topn: topn.unwrap_or(0) as usize,
                histogram_buckets,
                no_progress: args.no_progress,
                max_tile_bytes: args.max_tile_bytes,
                zoom: args.zoom,
                bucket: args.bucket,
                tile,
                summary: args.summary,
                layer: args.layer.clone(),
                recommend: args.recommend,
                include_layer_list: output == ReportFormat::Text,
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
            match output {
                ReportFormat::Json => {
                    let json = serde_json::to_string_pretty(&report)?;
                    println!("{}", json);
                }
                ReportFormat::Ndjson => {
                    let options = tile_prune::output::NdjsonOptions {
                        include_summary: !args.ndjson_lite && !args.ndjson_compact,
                        compact: args.ndjson_compact,
                    };
                    for line in ndjson_lines(&report, options)? {
                        println!("{}", line);
                    }
                }
                ReportFormat::Text => {
                    println!("summary:");
                    println!(
                        "  tiles: {} total: {} max: {} avg: {}",
                        report.overall.tile_count,
                        format_bytes(report.overall.total_bytes),
                        format_bytes(report.overall.max_bytes),
                        format_bytes(report.overall.avg_bytes)
                    );
                    println!(
                        "  empty_tiles: {} empty_ratio: {:.4}",
                        report.empty_tiles, report.empty_ratio
                    );
                    if report.sampled {
                        println!(
                            "  sample: used={} total={}",
                            report.sample_used_tiles, report.sample_total_tiles
                        );
                    }
                    if !report.by_zoom.is_empty() {
                        println!("zoom:");
                        for zoom in report.by_zoom.iter() {
                            println!(
                                "  z={}: tiles={} total={} max={} avg={}",
                                zoom.zoom,
                                zoom.stats.tile_count,
                                format_bytes(zoom.stats.total_bytes),
                                format_bytes(zoom.stats.max_bytes),
                                format_bytes(zoom.stats.avg_bytes)
                            );
                        }
                    }
                    if !report.histogram.is_empty() {
                        println!("histogram:");
                        let count_width = report
                            .histogram
                            .iter()
                            .map(|b| b.count)
                            .max()
                            .unwrap_or(0)
                            .to_string()
                            .len()
                            .max("count".len());
                        let bytes_width = report
                            .histogram
                            .iter()
                            .map(|b| format_bytes(b.total_bytes).len())
                            .max()
                            .unwrap_or(0)
                            .max("bytes".len());
                        let avg_width = report
                            .histogram
                            .iter()
                            .map(|b| format_bytes(b.running_avg_bytes).len())
                            .max()
                            .unwrap_or(0)
                            .max("avg".len());
                        println!(
                            "{} {} {} {} {} {} {}",
                            pad_right("range", 17),
                            pad_left("count", count_width),
                            pad_left("bytes", bytes_width),
                            pad_left("avg", avg_width),
                            pad_left("%tiles", 7),
                            pad_left("%size", 7),
                            pad_left("acc%tiles", 9),
                        );
                        for bucket in report.histogram.iter() {
                            let warn = if bucket.avg_over_limit {
                                "over"
                            } else if bucket.avg_near_limit {
                                "near"
                            } else {
                                ""
                            };
                            let range = format!(
                                "{}-{}",
                                format_bytes(bucket.min_bytes),
                                format_bytes(bucket.max_bytes)
                            );
                            println!(
                                "{} {} {} {} {:>7.2}% {:>7.2}% {:>9.2}% {}",
                                pad_right(&range, 17),
                                pad_left(&bucket.count.to_string(), count_width),
                                pad_left(&format_bytes(bucket.total_bytes), bytes_width),
                                pad_left(&format_bytes(bucket.running_avg_bytes), avg_width),
                                bucket.pct_tiles * 100.0,
                                bucket.pct_level_bytes * 100.0,
                                bucket.accum_pct_tiles * 100.0,
                                warn
                            );
                        }
                    }
                    if !report.file_layers.is_empty() {
                        println!("layers:");
                        let name_width = report
                            .file_layers
                            .iter()
                            .map(|l| l.name.len())
                            .max()
                            .unwrap_or(4)
                            .max("name".len());
                        let features_width = report
                            .file_layers
                            .iter()
                            .map(|l| l.feature_count)
                            .max()
                            .unwrap_or(0)
                            .to_string()
                            .len()
                            .max("features".len());
                        let keys_width = report
                            .file_layers
                            .iter()
                            .map(|l| l.property_key_count)
                            .max()
                            .unwrap_or(0)
                            .to_string()
                            .len()
                            .max("keys".len());
                        println!(
                            "{} {} {} {} {}",
                            pad_right("name", name_width),
                            pad_left("features", features_width),
                            pad_left("keys", keys_width),
                            pad_left("extent", 6),
                            pad_left("ver", 3),
                        );
                        for layer in report.file_layers.iter() {
                            println!(
                                "{} {} {} {} {}",
                                pad_right(&layer.name, name_width),
                                pad_left(&layer.feature_count.to_string(), features_width),
                                pad_left(&layer.property_key_count.to_string(), keys_width),
                                pad_left(&layer.extent.to_string(), 6),
                                pad_left(&layer.version.to_string(), 3),
                            );
                        }
                    }
                    if !report.recommended_buckets.is_empty() {
                        println!("recommendations:");
                        println!(
                            "  buckets: {}",
                            report
                                .recommended_buckets
                                .iter()
                                .map(|idx| idx.to_string())
                                .collect::<Vec<_>>()
                                .join(",")
                        );
                    }
                    if let Some(count) = report.bucket_count {
                        println!("bucket:");
                        println!("  count: {}", count);
                    }
                    if !report.bucket_tiles.is_empty() {
                        println!("bucket_tiles:");
                        for tile in report.bucket_tiles.iter() {
                            println!(
                                "  z={}: x={} y={} bytes={}",
                                tile.zoom, tile.x, tile.y, tile.bytes
                            );
                        }
                    }
                    if !report.top_tiles.is_empty() {
                        println!("top_tiles:");
                        for tile in report.top_tiles.iter() {
                            println!(
                                "  z={}: x={} y={} bytes={}",
                                tile.zoom, tile.x, tile.y, tile.bytes
                            );
                        }
                    }
                    if !report.top_tile_summaries.is_empty() {
                        println!("top_tile_summaries:");
                        for summary in report.top_tile_summaries.iter() {
                            println!(
                                "  tile_summary: z={} x={} y={} total_features={}",
                                summary.zoom, summary.x, summary.y, summary.total_features
                            );
                            for layer in summary.layers.iter() {
                                println!(
                                    "  layer: {} features={} property_keys={}",
                                    layer.name, layer.feature_count, layer.property_key_count
                                );
                            }
                        }
                    }
                    if let Some(summary) = report.tile_summary.as_ref() {
                        println!("tile_summary:");
                        println!(
                            "  z={} x={} y={} total_features={}",
                            summary.zoom, summary.x, summary.y, summary.total_features
                        );
                        for layer in summary.layers.iter() {
                            println!(
                                "  layer: {} features={} property_keys={}",
                                layer.name, layer.feature_count, layer.property_key_count
                            );
                            if !layer.property_keys.is_empty() {
                                println!("    keys: {}", layer.property_keys.join(","));
                            }
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

fn format_bytes(bytes: u64) -> String {
    const KB: f64 = 1024.0;
    const MB: f64 = 1024.0 * 1024.0;
    let value = bytes as f64;
    if value >= MB {
        format!("{:.2}MB", value / MB)
    } else if value >= KB {
        format!("{:.2}KB", value / KB)
    } else {
        format!("{}B", bytes)
    }
}

fn pad_right(value: &str, width: usize) -> String {
    format!("{value:width$}")
}

fn pad_left(value: &str, width: usize) -> String {
    format!("{value:>width$}")
}
