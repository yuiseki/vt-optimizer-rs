# Release Flow

Purpose: Describe how releases are promoted from main to release and how tags/releases are created.

## Branch model

- `main`: development branch; PRs land here first.
- `release`: stable branch used for tagging and GitHub Releases.

## Main to release PR automation

When `main` is updated, GitHub Actions opens a PR from `main` to `release`.
Maintainers review and merge this PR when ready to ship.

## Tagging and GitHub Releases

When changes are merged into `release`:

1. A tag is created from the `Cargo.toml` version (e.g., `v0.2.0`).
2. The release workflow is dispatched with that tag to build artifacts and publish the GitHub Release.

## Version bump workflow

Use the Makefile target to bump versions locally and open a PR to `main`:

```bash
VERSION=0.2.0 make bump-version
```

Optional: create a local release branch name to mirror the target version:

```bash
VERSION=0.2.0 BUMP_BRANCH=1 make bump-version
```

## Changelog updates

- Always update `CHANGELOG.md` with user-visible changes before release.

## Notes

- Only maintainers should merge into `release`.
- Tags are derived from `Cargo.toml` to avoid mismatch.
