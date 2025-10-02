# Scriptable CLI reference

Run `gwtui --help` or `gwtui COMMAND --help` for the exact current argument
contract. `gwtui -C PATH COMMAND` changes the command directory before repository
lookup. Specify `-C` before the subcommand; it is not passed to an executed child.

## Listing and selection

```sh
gwtui list --json --sort branch
gwtui list --linked --filter LOGIN
gwtui list --detached --paths
gwtui list --paths --null
gwtui get feature/login
gwtui get --null feature/login
```

List defaults to Git's main-first order; explicit sort accepts `branch` or `path`.
Filtering is case-insensitive across branch and path. `--linked` excludes the main
registration. `--detached` selects existing checkouts whose branch is `HEAD`.
`--paths` and `--json` are mutually exclusive; `--null` requires `--paths` on list.
Paths containing newline must be consumed with NUL-aware tooling, not line loops.
JSON preserves embedded characters through normal JSON escaping. List's JSON
array contains `path`, `branch`, `commit_hash`, `is_main`, and `created_at`.

`get` emits exactly one path and fails on ambiguous/missing selectors. With no
selector, interactive multi-worktree selection requires a TTY. Non-TTY commands
must specify an unambiguous selector. Global operations use `-g` and the configured
base directory; ordinary main repositories and no-origin repositories are included.

## Status

```sh
gwtui status --json --no-fetch
gwtui status --csv --no-fetch
gwtui status --filter staged --sort branch --no-fetch
gwtui status --watch --interval 5 --no-fetch
```

JSON's `summary` has exhaustive `Total`, `Modified`, `Clean`, `Stale`, `Staged`,
`Conflict`, and `Unknown` state buckets; `worktrees` carries per-checkout data.
CSV includes modified, added, deleted, untracked, staged, conflicts, ahead, and
behind counters. Human tables escape control characters and align Unicode by
terminal display width. Change counters are categories, not a unique file count:
a path with staged and unstaged edits may contribute to both categories.

JSON, CSV, and watch modes cannot be combined. Intervals must be positive.
`--no-fetch` disables status refresh fetching. Branch inspection never fetches.
See [status semantics](status-and-config.md) for filtering and staleness rules.

## Inspect and execute

```sh
gwtui doctor --json
gwtui branches --all --json --filter feature
gwtui exec feature/login -- cargo test
gwtui config path
gwtui config check
gwtui config list --json
```

Doctor reads Git version, effective configuration path, repository path, and
worktree/missing/locked counts. It does not start agents, tasks, or Zellij.
`branches --all` includes local remote-tracking refs, not a network fetch.
`exec` requires `--` and a program; the child receives literal argument boundaries,
runs in the chosen checkout, and retains its exit code. Shell evaluation only
occurs when you explicitly execute a shell. `--stay` opens a shell after the child.

[Lifecycle operations](worktree-lifecycle.md) cover add, lock/unlock, move, repair,
remove, and prune. Task and Zellij commands remain available under their own
`--help`; `tmux` is a compatibility name for the Zellij-backed layer, not tmux.
