# How To: Inspect and Optimize Monaco MBTiles

Purpose: Provide a reproducible, low-context workflow for inspecting and optimizing a sample tileset.

## Prerequisites

- `monaco.mbtiles` and `rivers.json` are available (see `.env` defaults).
- Build the CLI: `cargo build --release` (or use `cargo run` in dev).

## Inspect (context-saving)

Use a minimal summary + layer list with sampling for quick overview:

```bash
cargo run -- inspect /everything/src/github.com/yuiseki/planetiler-ai/data/monaco.mbtiles \
  --stats summary,layers \
  --fast
```

Notes:
- `--stats summary,layers` keeps the output concise.
- `--fast` uses sampling to reduce runtime and output volume.
- For machine-readable output, add `--output ndjson`.

## Optimize

If the output file already exists, remove it first:

```bash
rm -f ./tmp/monaco.optimized.mbtiles
```

Run optimize with a style filter:

```bash
cargo run -- optimize /everything/src/github.com/yuiseki/planetiler-ai/data/monaco.mbtiles \
  --style /everything/src/github.com/yuiseki/planetiler-ai/data/rivers.json \
  --output ./tmp/monaco.optimized.mbtiles
```

The command prints a summary of removed features and layers.

## Optional: Verify optimized output

```bash
cargo run -- inspect ./tmp/monaco.optimized.mbtiles --stats summary,layers --fast
```
