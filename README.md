# vt-optimizer-rs

A fast CLI to inspect and prune MBTiles/PMTiles vector tiles. It supports a vt-optimizer compatible legacy interface, modern Mapbox/MapLibre style filters, PMTiles output, and a `vt-compat` mode that mirrors vt-optimizer layer visibility behavior.

## Features

- Inspect MBTiles/PMTiles with histograms, layer stats, and summaries
- Optimize (prune) tiles using style visibility and filters
- Simplify a target tile by z/x/y with optional tolerance
- PMTiles input/output for optimize
- MBTiles `map/images` schema support
- `vt-compat` mode for vt-optimizer parity (filter ignored)

## Install

Build from source:

```bash
cargo build --release
```

## Usage

This project ships a `vt-optimizer` CLI. It supports both the modern subcommands and a vt-optimizer compatible legacy interface (no subcommand).

If you want vt-optimizer-style usage, see the Legacy section first.

### Inspect

```bash
# basic summary
vt-optimizer inspect /path/to/tiles.mbtiles

# PMTiles
vt-optimizer inspect /path/to/tiles.pmtiles

# NDJSON output
vt-optimizer inspect /path/to/tiles.mbtiles --output ndjson
```

### Optimize (prune)

```bash
# style-based pruning (layer+filter)
vt-optimizer optimize /path/to/tiles.mbtiles \
  --output /path/to/tiles.pruned.mbtiles \
  --style /path/to/style.json

# vt-optimizer compatible mode (visibility only)
vt-optimizer optimize /path/to/tiles.mbtiles \
  --output /path/to/tiles.pruned.mbtiles \
  --style /path/to/style.json \
  --style-mode vt-compat

# PMTiles optimize
vt-optimizer optimize /path/to/tiles.pmtiles \
  --output /path/to/tiles.pruned.pmtiles \
  --style /path/to/style.json
```

### Copy

```bash
vt-optimizer copy /path/to/tiles.mbtiles --output /path/to/tiles.copy.mbtiles
```

### Simplify

```bash
# MBTiles: simplify a single tile (z/x/y) with tolerance
vt-optimizer simplify /path/to/tiles.mbtiles --z 10 --x 908 --y 396 --tolerance 0.5

# PMTiles: simplify a single tile (z/x/y)
vt-optimizer simplify /path/to/tiles.pmtiles --z 10 --x 908 --y 396 --tolerance 0.5
```

### Legacy (vt-optimizer compatible)

```bash
# optimize (no subcommand)
vt-optimizer -m /path/to/tiles.mbtiles \
  -s /path/to/style.json \
  -o /path/to/tiles.pruned.mbtiles

# inspect a tile summary
vt-optimizer -m /path/to/tiles.mbtiles -z 10 -x 908 -y 396
```

## Style modes

- `layer+filter` (default): keeps features matching supported filter expressions
- `layer`: keeps entire layers that are visible (no filter evaluation)
- `vt-compat`: same as vt-optimizer visibility behavior (min/max zoom, layout visibility, paint non-zero), filter ignored

## Notes

- Unknown filter expressions are treated as **keep** and are reported in the optimize summary.
- MBTiles with `map/images` schema are supported for inspect/copy/optimize.
- PMTiles optimize currently rewrites the archive with preserved metadata and compression.
- simplify outputs a single-tile MBTiles/PMTiles and reports feature/vertex counts in stdout.

## Development

```bash
cargo test
```
