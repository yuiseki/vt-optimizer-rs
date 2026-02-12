use anyhow::{Context, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TileCoord {
    pub zoom: u8,
    pub x: u32,
    pub y: u32,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SampleSpec {
    Ratio(f64),
    Count(u64),
}

#[derive(Debug, Clone)]
pub struct InspectOptions {
    pub sample: Option<SampleSpec>,
    pub topn: usize,
    pub histogram_buckets: usize,
    pub no_progress: bool,
    pub max_tile_bytes: u64,
    pub zoom: Option<u8>,
    pub bucket: Option<usize>,
    pub tile: Option<TileCoord>,
    pub summary: bool,
    pub layers: Vec<String>,
    pub recommend: bool,
    pub include_layer_list: bool,
    pub list_tiles: Option<TileListOptions>,
}

#[allow(clippy::derivable_impls)]
impl Default for InspectOptions {
    fn default() -> Self {
        Self {
            sample: None,
            topn: 0,
            histogram_buckets: 0,
            no_progress: false,
            max_tile_bytes: 0,
            zoom: None,
            bucket: None,
            tile: None,
            summary: false,
            layers: Vec::new(),
            recommend: false,
            include_layer_list: false,
            list_tiles: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TileSort {
    Size,
    Zxy,
}

#[derive(Debug, Clone)]
pub struct TileListOptions {
    pub limit: usize,
    pub sort: TileSort,
}

#[derive(Debug, Clone, Copy)]
pub struct PruneOptions {
    pub threads: usize,
    pub io_batch: u32,
    pub readers: usize,
    pub read_cache_mb: Option<u64>,
    pub write_cache_mb: Option<u64>,
    pub drop_empty_tiles: bool,
    pub keep_unknown_filters: bool,
}

pub const EMPTY_TILE_MAX_BYTES: u64 = 50;

pub struct PrunedTile {
    pub bytes: Vec<u8>,
    pub empty: bool,
}

pub fn parse_sample_spec(value: &str) -> Result<SampleSpec> {
    let trimmed = value.trim();
    let as_f64: f64 = trimmed.parse().context("invalid sample value")?;
    if as_f64 <= 0.0 {
        anyhow::bail!("sample must be greater than zero");
    }
    if as_f64 <= 1.0 {
        return Ok(SampleSpec::Ratio(as_f64));
    }
    let as_u64: u64 = trimmed.parse().context("invalid sample count")?;
    Ok(SampleSpec::Count(as_u64))
}

pub fn parse_tile_spec(value: &str) -> Result<TileCoord> {
    let trimmed = value.trim();
    let mut parts = trimmed.split('/');
    let zoom_str = parts.next().context("tile must be in z/x/y format")?;
    let x_str = parts.next().context("tile must be in z/x/y format")?;
    let y_str = parts.next().context("tile must be in z/x/y format")?;
    if parts.next().is_some() {
        anyhow::bail!("tile must be in z/x/y format");
    }
    let zoom: u8 = zoom_str.parse().context("invalid tile zoom")?;
    let x: u32 = x_str.parse().context("invalid tile x")?;
    let y: u32 = y_str.parse().context("invalid tile y")?;
    Ok(TileCoord { zoom, x, y })
}
