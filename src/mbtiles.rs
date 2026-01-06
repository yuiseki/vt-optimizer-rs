use std::path::Path;

use anyhow::{Context, Result};
use rusqlite::{params, Connection};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MbtilesStats {
    pub tile_count: u64,
    pub total_bytes: u64,
    pub max_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MbtilesZoomStats {
    pub zoom: u8,
    pub stats: MbtilesStats,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MbtilesReport {
    pub overall: MbtilesStats,
    pub by_zoom: Vec<MbtilesZoomStats>,
}

fn ensure_mbtiles_path(path: &Path) -> Result<()> {
    let ext = path.extension().and_then(|ext| ext.to_str()).unwrap_or("");
    if ext.eq_ignore_ascii_case("mbtiles") {
        Ok(())
    } else {
        anyhow::bail!("only .mbtiles paths are supported in v0.0.2");
    }
}

pub fn inspect_mbtiles(path: &Path) -> Result<MbtilesReport> {
    ensure_mbtiles_path(path)?;
    let conn = Connection::open(path).with_context(|| {
        format!("failed to open mbtiles: {}", path.display())
    })?;

    let (count, total, max): (u64, Option<u64>, Option<u64>) = conn
        .query_row(
            "SELECT COUNT(*), SUM(LENGTH(tile_data)), MAX(LENGTH(tile_data)) FROM tiles",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .context("failed to read tiles stats")?;

    let overall = MbtilesStats {
        tile_count: count,
        total_bytes: total.unwrap_or(0),
        max_bytes: max.unwrap_or(0),
    };

    let mut by_zoom = Vec::new();
    let mut stmt = conn
        .prepare(
            "SELECT zoom_level, COUNT(*), SUM(LENGTH(tile_data)), MAX(LENGTH(tile_data)) FROM tiles GROUP BY zoom_level ORDER BY zoom_level",
        )
        .context("prepare zoom stats")?;
    let mut rows = stmt.query([]).context("query zoom stats")?;
    while let Some(row) = rows.next().context("read zoom stats")? {
        let zoom: u8 = row.get(0)?;
        let count: u64 = row.get(1)?;
        let total: Option<u64> = row.get(2)?;
        let max: Option<u64> = row.get(3)?;
        by_zoom.push(MbtilesZoomStats {
            zoom,
            stats: MbtilesStats {
                tile_count: count,
                total_bytes: total.unwrap_or(0),
                max_bytes: max.unwrap_or(0),
            },
        });
    }

    Ok(MbtilesReport { overall, by_zoom })
}

pub fn copy_mbtiles(input: &Path, output: &Path) -> Result<()> {
    ensure_mbtiles_path(input)?;
    ensure_mbtiles_path(output)?;
    let input_conn = Connection::open(input)
        .with_context(|| format!("failed to open input mbtiles: {}", input.display()))?;
    let mut output_conn = Connection::open(output)
        .with_context(|| format!("failed to open output mbtiles: {}", output.display()))?;

    output_conn
        .execute_batch(
            "
            CREATE TABLE metadata (name TEXT, value TEXT);
            CREATE TABLE tiles (
                zoom_level INTEGER,
                tile_column INTEGER,
                tile_row INTEGER,
                tile_data BLOB
            );
            ",
        )
        .context("failed to create output schema")?;

    let tx = output_conn.transaction().context("begin output transaction")?;

    {
        let mut stmt = input_conn
            .prepare("SELECT name, value FROM metadata")
            .context("prepare metadata")?;
        let mut rows = stmt.query([]).context("query metadata")?;
        while let Some(row) = rows.next().context("read metadata row")? {
            let name: String = row.get(0)?;
            let value: String = row.get(1)?;
            tx.execute(
                "INSERT INTO metadata (name, value) VALUES (?1, ?2)",
                params![name, value],
            )
            .context("insert metadata")?;
        }
    }

    {
        let mut stmt = input_conn
            .prepare(
                "SELECT zoom_level, tile_column, tile_row, tile_data FROM tiles ORDER BY zoom_level, tile_column, tile_row",
            )
            .context("prepare tiles")?;
        let mut rows = stmt.query([]).context("query tiles")?;
        while let Some(row) = rows.next().context("read tile row")? {
            let z: i64 = row.get(0)?;
            let x: i64 = row.get(1)?;
            let y: i64 = row.get(2)?;
            let data: Vec<u8> = row.get(3)?;
            tx.execute(
                "INSERT INTO tiles (zoom_level, tile_column, tile_row, tile_data) VALUES (?1, ?2, ?3, ?4)",
                params![z, x, y, data],
            )
            .context("insert tile")?;
        }
    }

    tx.commit().context("commit output")?;
    Ok(())
}
