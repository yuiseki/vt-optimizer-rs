use std::path::Path;

use anyhow::Result;

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

    pub fn from_str(name: &str) -> Option<Self> {
        match name.to_ascii_lowercase().as_str() {
            "mbtiles" => Some(TileFormat::Mbtiles),
            "pmtiles" => Some(TileFormat::Pmtiles),
            _ => None,
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
        TileFormat::from_str(name).ok_or_else(|| anyhow::anyhow!("unknown output format: {name}"))?
    } else if let Some(path) = output_path {
        if let Some(fmt) = TileFormat::from_extension(path) {
            fmt
        } else {
            input
        }
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
