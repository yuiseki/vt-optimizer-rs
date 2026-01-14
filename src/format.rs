use std::path::{Path, PathBuf};

use anyhow::{Result, bail};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TileFormat {
    Mbtiles,
    Pmtiles,
}

impl TileFormat {
    pub fn from_extension(path: &Path) -> Option<Self> {
        let ext = path.extension()?.to_str()?.to_ascii_lowercase();
        match ext.as_str() {
            "mbtiles" => Some(TileFormat::Mbtiles),
            "pmtiles" => Some(TileFormat::Pmtiles),
            _ => None,
        }
    }

    #[allow(clippy::should_implement_trait)]
    pub fn from_str(name: &str) -> Option<Self> {
        match name.to_ascii_lowercase().as_str() {
            "mbtiles" => Some(TileFormat::Mbtiles),
            "pmtiles" => Some(TileFormat::Pmtiles),
            _ => None,
        }
    }

    pub fn extension_str(self) -> &'static str {
        match self {
            TileFormat::Mbtiles => "mbtiles",
            TileFormat::Pmtiles => "pmtiles",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FormatDecision {
    pub input: TileFormat,
    pub output: TileFormat,
}

pub fn decide_formats(
    input_path: &Path,
    output_path: Option<&Path>,
    input_format: Option<&str>,
    output_format: Option<&str>,
) -> Result<FormatDecision> {
    let input = if let Some(name) = input_format {
        TileFormat::from_str(name).ok_or_else(|| anyhow::anyhow!("unknown input format: {name}"))?
    } else {
        TileFormat::from_extension(input_path)
            .ok_or_else(|| anyhow::anyhow!("cannot infer input format from path"))?
    };

    let output = if let Some(name) = output_format {
        TileFormat::from_str(name)
            .ok_or_else(|| anyhow::anyhow!("unknown output format: {name}"))?
    } else if let Some(path) = output_path {
        TileFormat::from_extension(path).unwrap_or(input)
    } else {
        input
    };

    Ok(FormatDecision { input, output })
}

pub fn require_known_output_extension(path: &Path) -> Result<TileFormat> {
    TileFormat::from_extension(path).ok_or_else(|| {
        anyhow::anyhow!(
            "output path must have .mbtiles or .pmtiles extension when output format is not provided"
        )
    })
}

pub fn validate_output_format_matches_path(
    output_path: Option<&Path>,
    output_format: Option<&str>,
) -> Result<()> {
    let (path, fmt_name) = match (output_path, output_format) {
        (Some(path), Some(name)) => (path, name),
        _ => return Ok(()),
    };

    let declared = TileFormat::from_str(fmt_name)
        .ok_or_else(|| anyhow::anyhow!("unknown output format: {fmt_name}"))?;

    if let Some(path_fmt) = TileFormat::from_extension(path) && path_fmt != declared {
        bail!("output format ({fmt_name}) conflicts with output file extension",);
    }

    Ok(())
}

pub fn plan_copy(
    input_path: &Path,
    output_path: Option<&Path>,
    input_format: Option<&str>,
    output_format: Option<&str>,
) -> Result<FormatDecision> {
    validate_output_format_matches_path(output_path, output_format)?;
    decide_formats(input_path, output_path, input_format, output_format)
}

pub fn plan_optimize(
    input_path: &Path,
    output_path: Option<&Path>,
    input_format: Option<&str>,
    output_format: Option<&str>,
) -> Result<FormatDecision> {
    validate_output_format_matches_path(output_path, output_format)?;
    decide_formats(input_path, output_path, input_format, output_format)
}

pub fn default_output_path_pruned(input_path: &Path, output_format: TileFormat) -> PathBuf {
    let file_name = input_path.file_name().and_then(|name| name.to_str());
    let stem = input_path
        .file_stem()
        .and_then(|name| name.to_str())
        .or(file_name)
        .unwrap_or("output");

    let file_name = format!("{stem}.pruned.{}", output_format.extension_str());
    let mut out = input_path.parent().map(PathBuf::from).unwrap_or_default();
    out.push(file_name);
    out
}

pub fn resolve_output_path(
    input_path: &Path,
    output_path: Option<&Path>,
    output_format: TileFormat,
) -> PathBuf {
    output_path
        .map(PathBuf::from)
        .unwrap_or_else(|| default_output_path_pruned(input_path, output_format))
}
