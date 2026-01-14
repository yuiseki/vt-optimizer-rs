# AGENTS

- When tests pass, always make a commit.
- Work in small milestones.
- Use TDD in small steps.
- For milestones, work on a dedicated branch and request a human maintainer to tag after review.
- Run `make fmt`, `make clippy` and `make test` before every commit.
- Update `docs/SPEC.md` (not root `SPEC.md`) for milestones/spec changes.
- Always create a new branch from `main` and do work on that branch.
- Use PRs for `main` (direct push is blocked); `release` is the promotion branch.
- Direct pushes to `main` are blocked.
- Always use `.github/PULL_REQUEST_TEMPLATE.md` when creating pull requests.
- When editing PR bodies, do not paste raw `cargo test --verbose` output. If you ran `make test` and it passed, just check the Testing checkbox.
- Do not use `gh pr edit` to update PR bodies. Use `gh api -X PATCH` instead.
- Avoid `gh pr create`. Create PRs with `gh api -X POST /repos/<owner>/<repo>/pulls` and pass `title`, `head`, `base`, and `body`.
- When asked to bump versions, follow `docs/RELEASE.md`.
- Release flow: `main` → `release` PRs are auto-created/updated; tags are created by workflow on `release` merges and releases are dispatched automatically.
