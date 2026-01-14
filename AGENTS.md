# AGENTS

- When tests pass, always make a commit.
- Work in small milestones.
- Use TDD in small steps.
- For milestones, work on a dedicated branch and request a human maintainer to tag after review.
- Run `make fmt` and `make clippy` before every commit.
- Update `docs/SPEC.md` (not root `SPEC.md`) for milestones/spec changes.
- Always create a new branch from `main` and do work on that branch.
- Use PRs for `main` (direct push is blocked); `release` is the promotion branch.
- Direct pushes to `main` are blocked.
- Always use `.github/PULL_REQUEST_TEMPLATE.md` when creating pull requests.
- When asked to bump versions, follow `docs/RELEASE.md`.
- Release flow: `main` → `release` PRs are auto-created/updated; tags are created by workflow on `release` merges and releases are dispatched automatically.
