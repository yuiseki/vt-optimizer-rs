# Governance

Purpose: Define project ownership, decision-making, and transfer expectations for vt-optimizer-rs.

## Project scope

vt-optimizer-rs is a Rust CLI for inspecting and optimizing vector tiles (MBTiles/PMTiles). Within UNVT, this project focuses on tooling for safe, reproducible vector-tile processing and interoperability.

## Roles

- Maintainers: Own roadmap, review/merge PRs, manage releases.
- Reviewers: Provide code review and domain review.
- Contributors: Propose changes via issues and PRs.

## Decision making

- Changes require at least one maintainer approval.
- Breaking changes require maintainer consensus and a migration note in CHANGELOG.
- Security-sensitive changes follow SECURITY.md.

## Release process

- Maintainers tag releases and publish release notes (see CHANGELOG.md).
- Release cadence is best-effort and driven by readiness.

## Deprecation policy

- Deprecations are announced in CHANGELOG and documented in CLI help.
- Deprecated options remain for at least one minor release unless security requires removal.

## Communication channels

- GitHub Issues and Pull Requests are primary.
- Contact: TBD_UNVT_CONTACT

## Transfer note

- The project is intended to be transferred from yuiseki to UNVT.
- Post-transfer ownership and permissions will be managed by UNVT maintainers.
