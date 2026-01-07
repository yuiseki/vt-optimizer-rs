use std::collections::HashSet;
use std::fs;
use std::path::Path;

use anyhow::{Context, Result};
use serde_json::Value;

pub fn read_style_source_layers(path: &Path) -> Result<HashSet<String>> {
    let contents = fs::read_to_string(path)
        .with_context(|| format!("failed to read style file: {}", path.display()))?;
    let value: Value = serde_json::from_str(&contents).context("parse style json")?;
    let layers = value
        .get("layers")
        .and_then(|layers| layers.as_array())
        .ok_or_else(|| anyhow::anyhow!("style json missing layers array"))?;

    let mut source_layers = HashSet::new();
    for layer in layers {
        if let Some(source_layer) = layer.get("source-layer").and_then(|v| v.as_str()) {
            source_layers.insert(source_layer.to_string());
        }
    }

    if source_layers.is_empty() {
        anyhow::bail!("style json contains no source-layer entries");
    }
    Ok(source_layers)
}
