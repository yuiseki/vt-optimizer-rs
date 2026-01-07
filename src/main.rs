use anyhow::{Context, Result};
use clap::Parser;

use tile_prune::cli::{Cli, Command, ReportFormat, TileSortArg};
use tile_prune::format::{plan_copy, plan_optimize, resolve_output_path};
use tile_prune::mbtiles::{
    copy_mbtiles, inspect_mbtiles_with_options, parse_sample_spec, parse_tile_spec,
    prune_mbtiles_layer_only, InspectOptions, PruneStats, TileListOptions, TileSort,
};
use tile_prune::output::{
    format_bytes, format_histogram_table, format_histograms_by_zoom_section,
    format_metadata_section, ndjson_lines, pad_left, pad_right, resolve_output_format,
};
use tile_prune::pmtiles::{inspect_pmtiles_with_options, mbtiles_to_pmtiles, pmtiles_to_mbtiles};
use tile_prune::style::read_style;

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
            let input_format = tile_prune::format::TileFormat::from_extension(&args.input)
                .ok_or_else(|| anyhow::anyhow!("cannot infer input format from path"))?;
            let report = match input_format {
                tile_prune::format::TileFormat::Mbtiles => {
                    inspect_mbtiles_with_options(&args.input, options)?
                }
                tile_prune::format::TileFormat::Pmtiles => {
                    inspect_pmtiles_with_options(&args.input, &options)?
                }
            };
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
                    println!(
                        "# Vector tile inspection of [{}] by tile-prune",
                        args.input.display()
                    );
                    println!();
                    if !report.metadata.is_empty() {
                        for line in format_metadata_section(&report.metadata) {
                            println!("{}", line);
                        }
                        println!();
                    }
                    println!("## Summary");
                    println!(
                        "- tiles: {} total: {} max: {} avg: {}",
                        report.overall.tile_count,
                        format_bytes(report.overall.total_bytes),
                        format_bytes(report.overall.max_bytes),
                        format_bytes(report.overall.avg_bytes)
                    );
                    println!(
                        "- empty_tiles: {} empty_ratio: {:.4}",
                        report.empty_tiles, report.empty_ratio
                    );
                    if report.sampled {
                        println!(
                            "- sample: used={} total={}",
                            report.sample_used_tiles, report.sample_total_tiles
                        );
                    }
                    if !report.by_zoom.is_empty() {
                        println!();
                        println!("## Zoom");
                        for zoom in report.by_zoom.iter() {
                            println!(
                                "- z={}: tiles={} total={} max={} avg={}",
                                zoom.zoom,
                                zoom.stats.tile_count,
                                format_bytes(zoom.stats.total_bytes),
                                format_bytes(zoom.stats.max_bytes),
                                format_bytes(zoom.stats.avg_bytes)
                            );
                        }
                    }
                    if !report.histogram.is_empty() {
                        println!();
                        println!("## Histogram");
                        for line in format_histogram_table(&report.histogram) {
                            println!("{}", line);
                        }
                    }
                    if !report.histograms_by_zoom.is_empty() {
                        println!();
                        for line in format_histograms_by_zoom_section(&report.histograms_by_zoom) {
                            println!("{}", line);
                        }
                    }
                    if !report.file_layers.is_empty() {
                        println!();
                        println!("## Layers");
                        let name_width = report
                            .file_layers
                            .iter()
                            .map(|l| l.name.len())
                            .max()
                            .unwrap_or(4)
                            .max("name".len());
                        let vertices_width = report
                            .file_layers
                            .iter()
                            .map(|l| l.vertex_count)
                            .max()
                            .unwrap_or(0)
                            .to_string()
                            .len()
                            .max("# of vertices".len());
                        let features_width = report
                            .file_layers
                            .iter()
                            .map(|l| l.feature_count)
                            .max()
                            .unwrap_or(0)
                            .to_string()
                            .len()
                            .max("# of features".len());
                        let keys_width = report
                            .file_layers
                            .iter()
                            .map(|l| l.property_key_count)
                            .max()
                            .unwrap_or(0)
                            .to_string()
                            .len()
                            .max("# of keys".len());
                        let values_width = report
                            .file_layers
                            .iter()
                            .map(|l| l.property_value_count)
                            .max()
                            .unwrap_or(0)
                            .to_string()
                            .len()
                            .max("# of values".len());
                        println!(
                            "  {} {} {} {} {}",
                            pad_right("name", name_width),
                            pad_left("# of vertices", vertices_width),
                            pad_left("# of features", features_width),
                            pad_left("# of keys", keys_width),
                            pad_left("# of values", values_width),
                        );
                        for layer in report.file_layers.iter() {
                            println!(
                                "  {} {} {} {} {}",
                                pad_right(&layer.name, name_width),
                                pad_left(&layer.vertex_count.to_string(), vertices_width),
                                pad_left(&layer.feature_count.to_string(), features_width),
                                pad_left(&layer.property_key_count.to_string(), keys_width),
                                pad_left(&layer.property_value_count.to_string(), values_width),
                            );
                        }
                    }
                    if !report.recommended_buckets.is_empty() {
                        println!();
                        println!("## Recommendations");
                        println!(
                            "- buckets: {}",
                            report
                                .recommended_buckets
                                .iter()
                                .map(|idx| idx.to_string())
                                .collect::<Vec<_>>()
                                .join(",")
                        );
                    }
                    if let Some(count) = report.bucket_count {
                        println!();
                        println!("## Bucket");
                        println!("- count: {}", count);
                    }
                    if !report.bucket_tiles.is_empty() {
                        println!();
                        println!("## Bucket Tiles");
                        for tile in report.bucket_tiles.iter() {
                            println!(
                                "- z={}: x={} y={} bytes={}",
                                tile.zoom, tile.x, tile.y, tile.bytes
                            );
                        }
                    }
                    if !report.top_tiles.is_empty() {
                        println!();
                        println!("## Top Tiles");
                        for tile in report.top_tiles.iter() {
                            println!(
                                "- z={}: x={} y={} bytes={}",
                                tile.zoom, tile.x, tile.y, tile.bytes
                            );
                        }
                    }
                    if !report.top_tile_summaries.is_empty() {
                        println!();
                        println!("## Top Tile Summaries");
                        for summary in report.top_tile_summaries.iter() {
                            println!(
                                "- tile_summary: z={} x={} y={} total_features={}",
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
                        println!();
                        println!("## Tile Summary");
                        println!(
                            "- z={} x={} y={} total_features={}",
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
            let style_path = args
                .style
                .as_ref()
                .context("--style is required for optimize")?;
            if args.style_mode != tile_prune::cli::StyleMode::Layer
                && args.style_mode != tile_prune::cli::StyleMode::LayerFilter
            {
                anyhow::bail!("v0.0.38 only supports --style-mode layer or layer+filter");
            }
            println!("Prune steps");
            println!("- Parsing style file");
            let style = read_style(style_path)?;
            match (decision.input, decision.output) {
                (tile_prune::format::TileFormat::Mbtiles, tile_prune::format::TileFormat::Mbtiles) => {
                    let apply_filters = args.style_mode == tile_prune::cli::StyleMode::LayerFilter;
                    println!("- Processing tiles");
                    let stats = prune_mbtiles_layer_only(&args.input, &_output_path, &style, apply_filters)?;
                    println!("- Writing output file to {}", _output_path.display());
                    print_prune_summary(&stats);
                }
                _ => {
                    anyhow::bail!("v0.0.38 only supports MBTiles input/output for optimize");
                }
            }
            println!("optimize: input={} output={}", args.input.display(), _output_path.display());
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

fn print_prune_summary(stats: &PruneStats) {
    println!("Prune results");
    if stats.removed_features_by_zoom.is_empty() {
        println!("- Removed features: none");
    } else {
        for (zoom, count) in stats.removed_features_by_zoom.iter() {
            println!("- Removed {} features in zoom {}", count, zoom);
        }
    }
    if stats.removed_layers_by_zoom.is_empty() {
        println!("- Removed layers: none");
    } else {
        for (layer, zooms) in stats.removed_layers_by_zoom.iter() {
            let zoom_list = zooms
                .iter()
                .map(|z| z.to_string())
                .collect::<Vec<_>>()
                .join(", ");
            println!("- Removed layer {} from zoom levels {}", layer, zoom_list);
        }
    }
}
