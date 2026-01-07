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
    /// Path to an MBTiles or PMTiles file to inspect.
    pub input: PathBuf,

    /// Threshold in bytes for size warnings in histogram averages.
    #[arg(long, default_value_t = 1_280_000)]
    pub max_tile_bytes: u64,

    /// Number of buckets for the size histogram (0 disables histogram output).
    #[arg(long, default_value_t = 10)]
    pub histogram_buckets: u32,

    /// Report the N largest tiles from the sampled set.
    #[arg(long)]
    pub topn: Option<u32>,

    /// Sampling strategy: ratio (e.g. 0.1) or count (e.g. 1000).
    #[arg(long)]
    pub sample: Option<String>,

    /// Output format (text/json/ndjson).
    #[arg(long, value_enum, default_value_t = ReportFormat::Text)]
    pub output: ReportFormat,

    /// Disable the progress bar.
    #[arg(long, default_value_t = false)]
    pub no_progress: bool,

    /// Limit inspection to a specific zoom level.
    #[arg(long)]
    pub zoom: Option<u8>,

    /// Histogram bucket index (0-based) used with --list-tiles.
    #[arg(long)]
    pub bucket: Option<usize>,

    /// Target tile in z/x/y form (requires --summary).
    #[arg(long)]
    pub tile: Option<String>,

    /// Emit a tile summary (requires --tile).
    #[arg(long, default_value_t = false)]
    pub summary: bool,

    /// Filter tile summary to a specific layer (requires --summary).
    #[arg(long)]
    pub layer: Option<String>,

    /// Recommend histogram buckets over/near the size threshold (requires --zoom).
    #[arg(long, default_value_t = false)]
    pub recommend: bool,

    /// Fast defaults: sample=0.1, topn=5, histogram-buckets=10.
    #[arg(long, default_value_t = false)]
    pub fast: bool,

    /// List tiles in the selected bucket (requires --bucket).
    #[arg(long, default_value_t = false)]
    pub list_tiles: bool,

    /// Limit the number of tiles listed per bucket.
    #[arg(long, default_value_t = 100)]
    pub limit: usize,

    /// Sort order for listed tiles.
    #[arg(long, value_enum, default_value_t = TileSortArg::Size)]
    pub sort: TileSortArg,

    /// NDJSON: omit the summary line (requires --output ndjson).
    #[arg(long, default_value_t = false)]
    pub ndjson_lite: bool,

    /// NDJSON: compact payloads and force --output ndjson.
    #[arg(long, default_value_t = false)]
    pub ndjson_compact: bool,
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
    #[value(name = "vt-compat")]
    VtCompat,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum ReportFormat {
    Text,
    Json,
    Ndjson,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum TileSortArg {
    Size,
    Zxy,
}
