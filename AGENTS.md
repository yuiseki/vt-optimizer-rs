# AGENTS

- When tests pass, always make a commit.
- Work in small milestones.
- Use TDD in small steps.
- For milestones, work on a dedicated branch and request a human maintainer to tag after review.
- Run `make fmt` and `make clippy` before every commit.
- Update `docs/SPEC.md` (not root `SPEC.md`) for milestones/spec changes.
- Use PRs for `main` (direct push is blocked); `release` is the promotion branch.
- Release flow: `main` → `release` PRs are auto-created/updated; tags are created by workflow on `release` merges and releases are dispatched automatically.
