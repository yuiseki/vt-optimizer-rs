# Data Export

Purpose: Explain how data can be extracted or migrated using vt-optimizer-rs.

## What “data export” means for this project

vt-optimizer-rs is a CLI that reads vector-tile archives and writes optimized or simplified archives. Data export in this context means that users can transform and move data between open formats without vendor lock-in.

## Supported input/output formats

- MBTiles (input/output)
- PMTiles (input/output)

## Examples

```bash
# Copy MBTiles to a new file
vt-optimizer copy /path/to/tiles.mbtiles --output /path/to/tiles.copy.mbtiles

# Optimize MBTiles using a style
vt-optimizer optimize /path/to/tiles.mbtiles \
  --output /path/to/tiles.optimized.mbtiles \
  --style /path/to/style.json

# Simplify a single tile
vt-optimizer simplify /path/to/tiles.mbtiles --z 10 --x 908 --y 396 --tolerance 0.5
```

## Non-PII statement

vt-optimizer-rs does not generate or collect personal data. Input datasets may contain sensitive information; users are responsible for lawful handling and publication.

## Migration notes

- Metadata is preserved where possible (see README Notes).
- Output tiles remain in open formats suitable for downstream tooling.
- Reproducibility depends on the input dataset and CLI options used.
