use std::path::PathBuf;

use clap::{Args, Parser, Subcommand, ValueEnum};

#[derive(Debug, Parser)]
#[command(name = "tile-prune", version, about = "MBTiles/PMTiles inspection and pruning CLI")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,

    /// Log level (error|warn|info|debug|trace)
    #[arg(long, default_value = "info")]
    pub log: String,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    Inspect(InspectArgs),
    Optimize(OptimizeArgs),
    Simplify(SimplifyArgs),
    Copy(CopyArgs),
    Verify(VerifyArgs),
}

#[derive(Debug, Args)]
pub struct InspectArgs {
    pub input: PathBuf,

    #[arg(long, default_value_t = 1_280_000)]
    pub max_tile_bytes: u64,

    #[arg(long, default_value_t = 10)]
    pub histogram_buckets: u32,

    #[arg(long)]
    pub topn: Option<u32>,

    #[arg(long)]
    pub sample: Option<String>,

    #[arg(long, value_enum, default_value_t = ReportFormat::Text)]
    pub output: ReportFormat,

    #[arg(long, default_value_t = false)]
    pub no_progress: bool,

    #[arg(long)]
    pub zoom: Option<u8>,

    #[arg(long)]
    pub bucket: Option<usize>,

    #[arg(long)]
    pub tile: Option<String>,

    #[arg(long, default_value_t = false)]
    pub summary: bool,

    #[arg(long)]
    pub layer: Option<String>,

    #[arg(long, default_value_t = false)]
    pub recommend: bool,

    #[arg(long, default_value_t = false)]
    pub list_tiles: bool,

    #[arg(long, default_value_t = 100)]
    pub limit: usize,

    #[arg(long, value_enum, default_value_t = TileSortArg::Size)]
    pub sort: TileSortArg,
}

#[derive(Debug, Args)]
pub struct OptimizeArgs {
    pub input: PathBuf,

    #[arg(long)]
    pub output: Option<PathBuf>,

    #[arg(long)]
    pub input_format: Option<String>,

    #[arg(long)]
    pub output_format: Option<String>,

    #[arg(long)]
    pub style: Option<PathBuf>,

    #[arg(long, value_enum, default_value_t = StyleMode::LayerFilter)]
    pub style_mode: StyleMode,

    #[arg(long, default_value_t = 1_280_000)]
    pub max_tile_bytes: u64,

    #[arg(long)]
    pub threads: Option<usize>,

    #[arg(long, default_value_t = 1_000)]
    pub io_batch: u32,

    #[arg(long)]
    pub checkpoint: Option<PathBuf>,

    #[arg(long, default_value_t = false)]
    pub resume: bool,
}

#[derive(Debug, Args)]
pub struct SimplifyArgs {
    pub input: PathBuf,

    #[arg(long)]
    pub output: Option<PathBuf>,

    #[arg(long)]
    pub z: u8,

    #[arg(long)]
    pub x: u32,

    #[arg(long)]
    pub y: u32,

    #[arg(long)]
    pub layer: Vec<String>,

    #[arg(long)]
    pub tolerance: Option<f64>,
}

#[derive(Debug, Args)]
pub struct CopyArgs {
    pub input: PathBuf,

    #[arg(long)]
    pub output: Option<PathBuf>,

    #[arg(long)]
    pub input_format: Option<String>,

    #[arg(long)]
    pub output_format: Option<String>,
}

#[derive(Debug, Args)]
pub struct VerifyArgs {
    pub input: PathBuf,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum StyleMode {
    None,
    Layer,
    #[value(name = "layer+filter")]
    LayerFilter,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum ReportFormat {
    Text,
    Json,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum TileSortArg {
    Size,
    Zxy,
}
