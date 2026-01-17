# Changelog

Purpose: Track user-visible changes in a concise, release-oriented format.

All notable changes to this project will be documented in this file.

The format is based on Keep a Changelog and adheres to Semantic Versioning.

## [Unreleased]

### Added
- TBD_UNVT_CHANGELOG_ADDED

### Changed
- TBD_UNVT_CHANGELOG_CHANGED

### Fixed
- TBD_UNVT_CHANGELOG_FIXED

### Security
- TBD_UNVT_CHANGELOG_SECURITY

## [0.4.6] - 2026-01-17
### Added
- Count tiles over `--max-tile-bytes` in inspect outputs for MBTiles/PMTiles.

## [0.4.5] - 2026-01-17
### Added
- Add `--unknown-filter` mode to control handling of unsupported style expressions during optimize.

### Changed
- Remove unsupported `--style-mode none` option.

## [0.4.4] - 2026-01-15
### Added
- Add tile size to tile summary output ("Size of tile").
- Add progress indicators for PMTiles inspect (spinner/bar depending on estimate availability).

### Changed
- Align PMTiles inspect output sections with MBTiles.
- Enforce Cargo.toml version bump for release PRs via CI.

## [0.4.3] - 2026-01-14
### Added
- Add a "Top 10 big tiles" section to inspect text output (respects --zoom).
- Allow `inspect -z/-x/-y` to show tile summaries in compat-style arguments.
- Document an inspect workflow and tips in HOW_TO.

### Changed
- Hide Metadata/Summary/Histogram/Layers output when `-x/-y` are provided.
- Skip "Top 10 big tiles" output when `-x/-y` are provided.

## [0.4.2] - 2026-01-14
### Changed
- Improve inspect text output readability (summary list items, zoom table with percentages).
- Hide histogram-by-zoom section by default and add zoom-level tip to the Zoom section.

## [0.4.1] - 2026-01-14
### Changed
- Bump rusqlite to 0.38 (bundled) and adjust SQLite row reads for u64 conversions.

## [0.4.0] - 2026-01-14
### Changed
- Change edition to 2024 in Cargo.toml

## [0.3.2] - 2026-01-14
### Changed
- Limit compat z/x/y inspection output to tile summary only.
- Add label coloring for tile summary entries (including layer/keys lines).

## [0.3.1] - 2026-01-14
### Changed
- Improve inspect text output readability (colored labels, title styling, path underline).
- Show layer/feature/key/value totals in summary output.
- Add a visible gap between progress output and report text.

## [0.3.0] - 2026-01-13

### Added
- parallel MBTiles prune with multi-reader pipeline.
- prune options for SQLite read/write cache sizing and dropping empty tiles.

### Changed
- fallback to zoom-based reader partitioning when rowid is unavailable.

## [0.1.5] - 2026-01-13

### Added
- inspect `--layers` filter for file layers and tile summaries.

## [0.1.4] - 2026-01-13

### Changed
- Allow `inspect --stats` without value to show possible values.

## [0.1.3] - 2026-01-13

### Changed
- Require `make fmt` and `make clippy` before every commit (AGENTS update).

## [0.1.2] - 2026-01-13

### Added
- Tile summary per-layer vertex/value counts.

## [0.1.1] - 2026-01-13

### Added
- Tile summary totals.

## [0.1.0] - 2026-01-08

### Changed
- Removed aarch64-unknown-linux-gnu target from release workflow.
