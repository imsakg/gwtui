# Changelog

## Unreleased

### Worktree lifecycle

- Lock/unlock linked checkouts with literal reasons; move and repair registrations.
- Preview prune metadata with an explicit Git expiry expression.
- Create branches from explicit commits or detached review checkouts.
- Print a created path for shell workflows; inspect bare-repository worktrees.
- Resolve exact branches and canonical paths before unambiguous substring matching.
- Preserve main-checkout identity regardless of the invoking checkout.
- Refuse main removal/movement and mismatched branch deletion; propagate failures.
- Delete branches from a surviving primary repository after linked checkout removal.
- Preserve NUL-delimited metadata paths and reject non-UTF-8 mutation targets.

### CLI and output

- Target a repository with top-level `-C` / `--repo`.
- Filter/sort listings and emit paths with optional NUL termination.
- Inspect branches, diagnostics and resolved configuration in JSON.
- Add `config path` and non-writing `config check` commands.
- Exhaustive status summary states and CSV change-category counters.
- Unicode-aligned terminal tables with escaped control characters.
- Literal exec argument boundaries, explicit separators and child exit statuses.
- Reject incompatible status formats, zero watch intervals and surplus exec selectors.

### Reliability

- Recognize all unmerged status pairs; prioritize dirty state over staleness.
- Stable status ordering, timestamp-aware activity and overflow-safe counts/durations.
- Parse nested remote namespaces without storing embedded credentials in repo identity.
- Normalize safe, idempotent Windows-compatible branch directory names.
- Discover main/no-origin/unborn repositories; bound symlinks and scan depth.
- Validate configuration updates before atomic replacement; honor explicit/XDG paths.
- Extend real-Git integration, CLI, output, configuration and compiled example tests.
- Add strict local gates and Ubuntu/macOS CI configuration.

These are implemented behaviors, not a claim that every platform, interactive
workflow or remote CI run has been verified. See [CONTRIBUTING](CONTRIBUTING.md)
for the reproducible local gate and testing boundaries.
