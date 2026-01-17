use std::thread;

use anyhow::{Context, Result};
use clap::Parser;

use nu_ansi_term::{Color, Style};
use vt_optimizer::cli::{Cli, Command, ReportFormat, TileSortArg};
use vt_optimizer::format::{plan_copy, plan_optimize, resolve_output_path};
use vt_optimizer::mbtiles::{
    InspectOptions, PruneOptions, PruneStats, TileListOptions, TileSort, copy_mbtiles,
    inspect_mbtiles_with_options, parse_sample_spec, parse_tile_spec, prune_mbtiles_layer_only,
    simplify_mbtiles_tile,
};
use vt_optimizer::output::{
    format_bytes, format_histogram_table, format_histograms_by_zoom_section,
    format_metadata_section, format_top_tiles_lines, format_zoom_table, ndjson_lines, pad_left,
    pad_right, resolve_output_format,
};
use vt_optimizer::pmtiles::{
    inspect_pmtiles_with_options, mbtiles_to_pmtiles, pmtiles_to_mbtiles, prune_pmtiles_layer_only,
    simplify_pmtiles_tile,
};
use vt_optimizer::style::read_style;

fn main() -> Result<()> {
    let cli = Cli::parse();
    init_tracing(&cli.log);

    match cli.command {
        Some(Command::Inspect(args)) => {
            run_inspect(args)?;
        }
        Some(Command::Optimize(args)) => {
            run_optimize(args)?;
        }
        Some(Command::Simplify(args)) => {
            let input_format = vt_optimizer::format::TileFormat::from_extension(&args.input)
                .ok_or_else(|| anyhow::anyhow!("cannot infer input format from path"))?;
            let coord = vt_optimizer::mbtiles::TileCoord {
                zoom: args.z,
                x: args.x,
                y: args.y,
            };
            let (output, stats) = match input_format {
                vt_optimizer::format::TileFormat::Mbtiles => {
                    let output = args
                        .output
                        .clone()
                        .unwrap_or_else(|| args.input.with_extension("simplified.mbtiles"));
                    let stats = simplify_mbtiles_tile(
                        &args.input,
                        &output,
                        coord,
                        &args.layer,
                        args.tolerance,
                    )?;
                    (output, stats)
                }
                vt_optimizer::format::TileFormat::Pmtiles => {
                    let output = args
                        .output
                        .clone()
                        .unwrap_or_else(|| args.input.with_extension("simplified.pmtiles"));
                    let stats = simplify_pmtiles_tile(
                        &args.input,
                        &output,
                        coord,
                        &args.layer,
                        args.tolerance,
                    )?;
                    (output, stats)
                }
            };
            println!(
                "simplify: input={} output={} z={} x={} y={} features={} vertices={}=>{}",
                args.input.display(),
                output.display(),
                args.z,
                args.x,
                args.y,
                stats.feature_count,
                stats.vertices_before,
                stats.vertices_after
            );
        }
        Some(Command::Copy(args)) => {
            let decision = plan_copy(
                &args.input,
                args.output.as_deref(),
                args.input_format.as_deref(),
                args.output_format.as_deref(),
            )?;
            let _output_path =
                resolve_output_path(&args.input, args.output.as_deref(), decision.output);
            match (decision.input, decision.output) {
                (
                    vt_optimizer::format::TileFormat::Mbtiles,
                    vt_optimizer::format::TileFormat::Mbtiles,
                ) => {
                    copy_mbtiles(&args.input, &_output_path)?;
                }
                (
                    vt_optimizer::format::TileFormat::Mbtiles,
                    vt_optimizer::format::TileFormat::Pmtiles,
                ) => {
                    mbtiles_to_pmtiles(&args.input, &_output_path)?;
                }
                (
                    vt_optimizer::format::TileFormat::Pmtiles,
                    vt_optimizer::format::TileFormat::Mbtiles,
                ) => {
                    pmtiles_to_mbtiles(&args.input, &_output_path)?;
                }
                (
                    vt_optimizer::format::TileFormat::Pmtiles,
                    vt_optimizer::format::TileFormat::Pmtiles,
                ) => {
                    anyhow::bail!("v0.0.3 does not support PMTiles to PMTiles copy");
                }
            }
            println!("copy: input={}", args.input.display());
        }
        Some(Command::Verify(args)) => {
            println!("verify: input={}", args.input.display());
        }
        None => {
            let Some(input) = cli.mbtiles.as_ref() else {
                anyhow::bail!("no subcommand or --mbtiles provided");
            };
            if cli.style.is_some() {
                let args = vt_optimizer::cli::OptimizeArgs {
                    input: input.clone(),
                    output: cli.output.clone(),
                    input_format: None,
                    output_format: None,
                    style: cli.style.clone(),
                    style_mode: vt_optimizer::cli::StyleMode::VtCompat,
                    unknown_filter: vt_optimizer::cli::UnknownFilterMode::Keep,
                    max_tile_bytes: 1_280_000,
                    threads: None,
                    readers: None,
                    io_batch: 1_000,
                    read_cache_mb: None,
                    write_cache_mb: None,
                    drop_empty_tiles: false,
                    checkpoint: None,
                    resume: false,
                };
                run_optimize(args)?;
                return Ok(());
            }
            if let (Some(x), Some(y), Some(z)) = (cli.x, cli.y, cli.z) {
                if !cli.layer.is_empty() || cli.tolerance.is_some() {
                    let args = vt_optimizer::cli::SimplifyArgs {
                        input: input.clone(),
                        output: cli.output.clone(),
                        z,
                        x,
                        y,
                        layer: cli.layer.clone(),
                        tolerance: cli.tolerance,
                    };
                    let input_format = vt_optimizer::format::TileFormat::from_extension(
                        &args.input,
                    )
                    .ok_or_else(|| anyhow::anyhow!("cannot infer input format from path"))?;
                    let coord = vt_optimizer::mbtiles::TileCoord {
                        zoom: args.z,
                        x: args.x,
                        y: args.y,
                    };
                    let (output, stats) =
                        match input_format {
                            vt_optimizer::format::TileFormat::Mbtiles => {
                                let output = args.output.clone().unwrap_or_else(|| {
                                    args.input.with_extension("simplified.mbtiles")
                                });
                                let stats = simplify_mbtiles_tile(
                                    &args.input,
                                    &output,
                                    coord,
                                    &args.layer,
                                    args.tolerance,
                                )?;
                                (output, stats)
                            }
                            vt_optimizer::format::TileFormat::Pmtiles => {
                                let output = args.output.clone().unwrap_or_else(|| {
                                    args.input.with_extension("simplified.pmtiles")
                                });
                                let stats = simplify_pmtiles_tile(
                                    &args.input,
                                    &output,
                                    coord,
                                    &args.layer,
                                    args.tolerance,
                                )?;
                                (output, stats)
                            }
                        };
                    println!(
                        "simplify: input={} output={} z={} x={} y={} features={} vertices={}=>{}",
                        args.input.display(),
                        output.display(),
                        args.z,
                        args.x,
                        args.y,
                        stats.feature_count,
                        stats.vertices_before,
                        stats.vertices_after
                    );
                    return Ok(());
                }
                let args = vt_optimizer::cli::InspectArgs {
                    input: input.clone(),
                    max_tile_bytes: 1_280_000,
                    histogram_buckets: 0,
                    topn: None,
                    sample: None,
                    output: vt_optimizer::cli::ReportFormat::Text,
                    stats: Some("tile_summary".to_string()),
                    no_progress: false,
                    zoom: None,
                    x: None,
                    y: None,
                    bucket: None,
                    tile: Some(format!("{}/{}/{}", z, x, y)),
                    summary: true,
                    layers: Vec::new(),
                    layer: Vec::new(),
                    recommend: false,
                    fast: false,
                    list_tiles: false,
                    limit: 100,
                    sort: vt_optimizer::cli::TileSortArg::Size,
                    ndjson_lite: false,
                    ndjson_compact: false,
                    tile_info_format: vt_optimizer::cli::TileInfoFormat::Full,
                };
                run_inspect(args)?;
                return Ok(());
            }
            let args = vt_optimizer::cli::InspectArgs {
                input: input.clone(),
                max_tile_bytes: 1_280_000,
                histogram_buckets: 10,
                topn: None,
                sample: None,
                output: vt_optimizer::cli::ReportFormat::Text,
                stats: None,
                no_progress: false,
                zoom: None,
                x: None,
                y: None,
                bucket: None,
                tile: None,
                summary: false,
                layers: Vec::new(),
                layer: Vec::new(),
                recommend: false,
                fast: false,
                list_tiles: false,
                limit: 100,
                sort: vt_optimizer::cli::TileSortArg::Size,
                ndjson_lite: false,
                ndjson_compact: false,
                tile_info_format: vt_optimizer::cli::TileInfoFormat::Full,
            };
            run_inspect(args)?;
        }
    }

    Ok(())
}

fn init_tracing(level: &str) {
    let filter = tracing_subscriber::EnvFilter::try_new(level)
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();
}

fn run_inspect(args: vt_optimizer::cli::InspectArgs) -> Result<()> {
    let output = resolve_output_format(args.output, args.ndjson_compact);
    let stats_filter = vt_optimizer::output::parse_stats_filter(args.stats.as_deref())?;
    if args.ndjson_lite && output != ReportFormat::Ndjson {
        anyhow::bail!("--ndjson-lite requires --output ndjson");
    }
    let sample = match args.sample.as_deref() {
        Some(value) => Some(parse_sample_spec(value)?),
        None => None,
    };
    let mut tile = match args.tile.as_deref() {
        Some(value) => Some(parse_tile_spec(value)?),
        None => None,
    };
    if tile.is_some() && (args.x.is_some() || args.y.is_some()) {
        anyhow::bail!("--tile cannot be combined with -x/-y");
    }
    if tile.is_none() {
        if let (Some(z), Some(x), Some(y)) = (args.zoom, args.x, args.y) {
            tile = Some(vt_optimizer::mbtiles::TileCoord { zoom: z, x, y });
        } else if args.x.is_some() || args.y.is_some() {
            anyhow::bail!("-x/-y require -z/--zoom");
        }
    }
    let summary = args.summary || (tile.is_some() && args.tile.is_none() && args.x.is_some());
    if summary && tile.is_none() {
        anyhow::bail!("--summary requires --tile z/x/y");
    }
    if tile.is_some() && !summary {
        anyhow::bail!("--tile requires --summary");
    }
    let mut layers = args.layers.clone();
    layers.extend(args.layer.clone());
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
    let (sample, mut topn, histogram_buckets) = if args.fast {
        (
            Some(vt_optimizer::mbtiles::SampleSpec::Ratio(0.1)),
            Some(5),
            10,
        )
    } else {
        (sample, topn, args.histogram_buckets as usize)
    };
    if output == ReportFormat::Text && topn.unwrap_or(0) == 0 {
        topn = Some(10);
    }
    let topn_value = topn.unwrap_or(0) as usize;
    let options = InspectOptions {
        sample,
        topn: topn_value,
        histogram_buckets,
        no_progress: args.no_progress,
        max_tile_bytes: args.max_tile_bytes,
        zoom: args.zoom,
        bucket: args.bucket,
        tile,
        summary,
        layers,
        recommend: args.recommend,
        include_layer_list: output == ReportFormat::Text
            && (stats_filter.includes(vt_optimizer::output::StatsSection::Layers)
                || stats_filter.includes(vt_optimizer::output::StatsSection::Summary)),
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
    let input_format = vt_optimizer::format::TileFormat::from_extension(&args.input)
        .ok_or_else(|| anyhow::anyhow!("cannot infer input format from path"))?;
    let report = match input_format {
        vt_optimizer::format::TileFormat::Mbtiles => {
            inspect_mbtiles_with_options(&args.input, options)?
        }
        vt_optimizer::format::TileFormat::Pmtiles => {
            inspect_pmtiles_with_options(&args.input, &options)?
        }
    };
    let report = vt_optimizer::output::apply_tile_info_format(report, args.tile_info_format);
    let summary_totals = if stats_filter.includes(vt_optimizer::output::StatsSection::Summary) {
        vt_optimizer::output::summarize_file_layers(&report.file_layers)
    } else {
        None
    };
    let report = vt_optimizer::output::apply_stats_filter(report, &stats_filter);
    match output {
        ReportFormat::Json => {
            let json = serde_json::to_string_pretty(&report)?;
            println!("{}", json);
        }
        ReportFormat::Ndjson => {
            let options = vt_optimizer::output::NdjsonOptions {
                include_summary: !args.ndjson_lite
                    && !args.ndjson_compact
                    && stats_filter.includes(vt_optimizer::output::StatsSection::Summary),
                compact: args.ndjson_compact,
            };
            for line in ndjson_lines(&report, options)? {
                println!("{}", line);
            }
        }
        ReportFormat::Text => {
            println!();
            eprintln!();
            let include_metadata =
                stats_filter.includes(vt_optimizer::output::StatsSection::Metadata);
            let include_summary =
                stats_filter.includes(vt_optimizer::output::StatsSection::Summary);
            let include_zoom = stats_filter.includes(vt_optimizer::output::StatsSection::Zoom)
                && args.zoom.is_none();
            let include_histogram =
                stats_filter.includes(vt_optimizer::output::StatsSection::Histogram);
            let include_histogram_by_zoom = args.stats.is_some()
                && stats_filter.includes(vt_optimizer::output::StatsSection::HistogramByZoom);
            let include_layers = stats_filter.includes(vt_optimizer::output::StatsSection::Layers);
            let hide_tile_summary_sections = args.x.is_some() && args.y.is_some();
            let include_recommendations =
                stats_filter.includes(vt_optimizer::output::StatsSection::Recommendations);
            let include_bucket = stats_filter.includes(vt_optimizer::output::StatsSection::Bucket);
            let include_bucket_tiles =
                stats_filter.includes(vt_optimizer::output::StatsSection::BucketTiles);
            let include_top_tiles = stats_filter
                .includes(vt_optimizer::output::StatsSection::TopTiles)
                && !hide_tile_summary_sections;
            let include_top_tile_summaries =
                stats_filter.includes(vt_optimizer::output::StatsSection::TopTileSummaries);
            let include_tile_summary =
                stats_filter.includes(vt_optimizer::output::StatsSection::TileSummary);
            println!("{}", format_inspect_title(&args.input));
            println!();
            if include_metadata && !hide_tile_summary_sections && !report.metadata.is_empty() {
                for line in format_metadata_section(&report.metadata) {
                    println!("{}", emphasize_section_heading(&line));
                }
                println!();
            }
            if include_summary && !hide_tile_summary_sections {
                println!("{}", emphasize_section_heading("## Summary"));
                println!(
                    "{}",
                    format_summary_label("Number of tiles", report.overall.tile_count)
                );
                println!(
                    "{}",
                    format_summary_label("Total size", format_bytes(report.overall.total_bytes))
                );
                println!(
                    "{}",
                    format_summary_label("Max tile size", format_bytes(report.overall.max_bytes))
                );
                if args.max_tile_bytes > 0 {
                    println!(
                        "{}",
                        format_summary_label("Tiles over limit", report.over_limit_tiles)
                    );
                }
                println!(
                    "{}",
                    format_summary_label(
                        "Average tile size",
                        format_bytes(report.overall.avg_bytes)
                    )
                );
                println!(
                    "{}",
                    format_summary_label("Empty tiles", report.empty_tiles)
                );
                println!(
                    "{}",
                    format_summary_label("Empty tile ratio", format!("{:.4}", report.empty_ratio))
                );
                if report.sampled {
                    println!(
                        "{}",
                        format_summary_label(
                            "sample",
                            format!(
                                "used={} total={}",
                                report.sample_used_tiles, report.sample_total_tiles
                            )
                        )
                    );
                }
                if let Some(totals) = summary_totals {
                    println!(
                        "{}",
                        format_summary_label("Layers in this tile", totals.layer_count)
                    );
                    println!(
                        "{}",
                        format_summary_label("Features in this tile", totals.feature_count)
                    );
                    println!(
                        "{}",
                        format_summary_label("Vertices in this tile", totals.vertex_count)
                    );
                    println!(
                        "{}",
                        format_summary_label("Keys in this tile", totals.property_key_count)
                    );
                    println!(
                        "{}",
                        format_summary_label("Values in this tile", totals.property_value_count)
                    );
                }
            }
            if include_zoom && !report.by_zoom.is_empty() {
                println!();
                println!("{}", emphasize_section_heading("## Zoom"));
                for line in format_zoom_table(
                    &report.by_zoom,
                    report.overall.tile_count,
                    report.overall.total_bytes,
                ) {
                    println!("{}", emphasize_table_header(&line));
                }
                if args.zoom.is_none() {
                    println!();
                    println!(
                        "Tip: use --zoom option to inspect histogram and layers by each zoom level."
                    );
                }
            }
            if include_histogram && !hide_tile_summary_sections && !report.histogram.is_empty() {
                println!();
                println!("{}", emphasize_section_heading("## Histogram"));
                for line in format_histogram_table(&report.histogram) {
                    println!("{}", emphasize_table_header(&line));
                }
            }
            if include_histogram_by_zoom
                && !hide_tile_summary_sections
                && !report.histograms_by_zoom.is_empty()
            {
                println!();
                for line in format_histograms_by_zoom_section(&report.histograms_by_zoom) {
                    let line = emphasize_section_heading(&line);
                    println!("{}", emphasize_table_header(&line));
                }
            }
            if include_layers && !hide_tile_summary_sections && !report.file_layers.is_empty() {
                println!();
                println!("{}", emphasize_section_heading("## Layers"));
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
                let layers_header = format!(
                    "  {} {} {} {} {}",
                    pad_right("name", name_width),
                    pad_left("# of vertices", vertices_width),
                    pad_left("# of features", features_width),
                    pad_left("# of keys", keys_width),
                    pad_left("# of values", values_width),
                );
                println!("{}", emphasize_table_header(&layers_header));
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
            if include_recommendations && !report.recommended_buckets.is_empty() {
                println!();
                println!("{}", emphasize_section_heading("## Recommendations"));
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
            if include_bucket && let Some(count) = report.bucket_count {
                println!();
                println!("{}", emphasize_section_heading("## Bucket"));
                println!("- count: {}", count);
            }
            if include_bucket_tiles && !report.bucket_tiles.is_empty() {
                println!();
                println!("{}", emphasize_section_heading("## Bucket Tiles"));
                for tile in report.bucket_tiles.iter() {
                    println!(
                        "- z={}: x={} y={} bytes={}",
                        tile.zoom, tile.x, tile.y, tile.bytes
                    );
                }
            }
            if include_top_tiles && !report.top_tiles.is_empty() {
                println!();
                println!(
                    "{}",
                    emphasize_section_heading(&format!("## Top {} big tiles", topn_value))
                );
                for line in format_top_tiles_lines(&report.top_tiles) {
                    println!("{}", line);
                }
            }
            if include_top_tile_summaries && !report.top_tile_summaries.is_empty() {
                println!();
                println!("{}", emphasize_section_heading("## Top Tile Summaries"));
                for summary in report.top_tile_summaries.iter() {
                    println!(
                        "- tile_summary: z={} x={} y={} layers={} total_features={} vertices={} keys={} values={}",
                        summary.zoom,
                        summary.x,
                        summary.y,
                        summary.layer_count,
                        summary.total_features,
                        summary.vertex_count,
                        summary.property_key_count,
                        summary.property_value_count
                    );
                    for layer in summary.layers.iter() {
                        println!(
                            "  {}: {} features={} vertices={} property_keys={} values={}",
                            Style::new().fg(Color::Blue).paint("layer"),
                            layer.name,
                            layer.feature_count,
                            layer.vertex_count,
                            layer.property_key_count,
                            layer.property_value_count
                        );
                    }
                }
            }
            if include_tile_summary && let Some(summary) = report.tile_summary.as_ref() {
                println!();
                println!("{}", emphasize_section_heading("## Tile Summary"));
                for line in vt_optimizer::output::format_tile_summary_text(summary) {
                    println!("{}", line);
                }
                for layer in summary.layers.iter() {
                    println!(
                        "  {}: {} features={} vertices={} property_keys={} values={}",
                        Style::new().fg(Color::Blue).paint("layer"),
                        layer.name,
                        layer.feature_count,
                        layer.vertex_count,
                        layer.property_key_count,
                        layer.property_value_count
                    );
                    if !layer.property_keys.is_empty() {
                        println!(
                            "    {}: {}",
                            Style::new().fg(Color::Blue).paint("keys"),
                            layer.property_keys.join(",")
                        );
                    }
                }
            }
        }
    }
    Ok(())
}

fn run_optimize(args: vt_optimizer::cli::OptimizeArgs) -> Result<()> {
    let decision = plan_optimize(
        &args.input,
        args.output.as_deref(),
        args.input_format.as_deref(),
        args.output_format.as_deref(),
    )?;
    let output_path = resolve_output_path(&args.input, args.output.as_deref(), decision.output);
    let style_path = args
        .style
        .as_ref()
        .context("--style is required for optimize")?;
    if args.style_mode != vt_optimizer::cli::StyleMode::Layer
        && args.style_mode != vt_optimizer::cli::StyleMode::LayerFilter
        && args.style_mode != vt_optimizer::cli::StyleMode::VtCompat
    {
        anyhow::bail!("v0.0.55 only supports --style-mode layer, layer+filter, or vt-compat");
    }
    println!("Prune steps");
    println!("- Parsing style file");
    let style = read_style(style_path)?;
    match (decision.input, decision.output) {
        (vt_optimizer::format::TileFormat::Mbtiles, vt_optimizer::format::TileFormat::Mbtiles) => {
            let apply_filters = args.style_mode == vt_optimizer::cli::StyleMode::LayerFilter;
            let threads = args.threads.unwrap_or_else(|| {
                thread::available_parallelism()
                    .map(|n| n.get())
                    .unwrap_or(1)
            });
            let readers = args.readers.unwrap_or(threads);
            println!(
                "- Processing tiles (threads={threads}, readers={readers}, io_batch={})",
                args.io_batch,
            );
            let stats = prune_mbtiles_layer_only(
                &args.input,
                &output_path,
                &style,
                apply_filters,
                PruneOptions {
                    threads,
                    io_batch: args.io_batch,
                    readers,
                    read_cache_mb: args.read_cache_mb,
                    write_cache_mb: args.write_cache_mb,
                    drop_empty_tiles: args.drop_empty_tiles,
                    keep_unknown_filters: args.unknown_filter
                        == vt_optimizer::cli::UnknownFilterMode::Keep,
                },
            )?;
            println!("- Writing output file to {}", output_path.display());
            print_prune_summary(&stats);
        }
        (vt_optimizer::format::TileFormat::Pmtiles, vt_optimizer::format::TileFormat::Pmtiles) => {
            let apply_filters = args.style_mode == vt_optimizer::cli::StyleMode::LayerFilter;
            println!("- Processing tiles");
            let stats = prune_pmtiles_layer_only(
                &args.input,
                &output_path,
                &style,
                apply_filters,
                args.unknown_filter == vt_optimizer::cli::UnknownFilterMode::Keep,
            )?;
            println!("- Writing output file to {}", output_path.display());
            print_prune_summary(&stats);
        }
        _ => {
            anyhow::bail!("v0.0.47 only supports matching input/output formats for optimize");
        }
    }
    println!(
        "optimize: input={} output={}",
        args.input.display(),
        output_path.display()
    );
    Ok(())
}

fn emphasize_section_heading(line: &str) -> String {
    if line.starts_with("# ") || line.starts_with("## ") || line.starts_with("### ") {
        Color::Green.bold().paint(line).to_string()
    } else {
        line.to_string()
    }
}

fn format_inspect_title(path: &std::path::Path) -> String {
    let prefix = "# Vector tile inspection of [";
    let suffix = "] by vt-optimizer";
    let path_text = path.display().to_string();
    let base = Style::new().fg(Color::Green).bold();
    let underline = base.underline();
    format!(
        "{}{}{}",
        base.paint(prefix),
        underline.paint(path_text),
        base.paint(suffix)
    )
}

fn format_summary_label<T: std::fmt::Display>(label: &str, value: T) -> String {
    format!("- {}: {}", Style::new().fg(Color::Blue).paint(label), value)
}

fn emphasize_table_header(line: &str) -> String {
    if line.trim_start().starts_with("range")
        || line.trim_start().starts_with("zoom")
        || line.trim_start().starts_with("name")
        || line.trim_start().starts_with("# of")
    {
        Color::Cyan.bold().paint(line).to_string()
    } else {
        line.to_string()
    }
}
fn print_prune_summary(stats: &PruneStats) {
    println!("Summary");
    if stats.removed_features_by_zoom.is_empty() {
        println!("- Removed features: none");
    } else {
        let total_removed: u64 = stats.removed_features_by_zoom.values().sum();
        println!("- Removed features total: {}", total_removed);
        println!("- Removed features by zoom:");
        for (zoom, count) in stats.removed_features_by_zoom.iter() {
            println!("  z{:02}: {}", zoom, count);
        }
    }
    if stats.removed_layers_by_zoom.is_empty() {
        println!("- Removed layers: none");
    } else {
        println!("- Removed layers:");
        for (layer, zooms) in stats.removed_layers_by_zoom.iter() {
            let zoom_list = zooms
                .iter()
                .map(|z| z.to_string())
                .collect::<Vec<_>>()
                .join(", ");
            println!("  {} @ z{}", layer, zoom_list);
        }
    }
    if stats.unknown_filters > 0 {
        println!(
            "- Unknown filter expressions kept: {}",
            stats.unknown_filters
        );
        println!("- Unknown filter expressions by layer:");
        for (layer, count) in stats.unknown_filters_by_layer.iter() {
            println!("  {}: {}", layer, count);
        }
    }
}
