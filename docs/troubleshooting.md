# Troubleshooting

Start with read-only diagnostics:

```sh
gwtui doctor --json
gwtui config path
gwtui config check
gwtui list --json
gwtui status --json --no-fetch
```

## Missing Git or invalid repository

Git must be on `PATH`. Verify `git --version`. Use `gwtui -C /actual/repository
list` instead of relying on an unrelated current directory. gwtui asks Git to
validate repository roots; a directory containing a forged or empty `.git`
marker is not accepted. Doctor works outside a repository and reports a null
repository with zero registered worktrees. Bare repositories are supported for
worktree management, but have no checked-out files of their own.

## Ambiguous worktree selection

Choose an exact branch name or an absolute registered path. Local get, exec,
lock/unlock, move and removal reject ambiguous substring selectors instead of
choosing arbitrarily. Interactive multi-selection still requires a real TTY.
An exact branch wins over a longer branch containing the same prefix.

## A move happened outside Git

Use `gwtui repair /new/checkout/path`; then inspect `list --json`. For future
moves prefer `gwtui move branch /new/path`, which updates registration metadata
alongside Git's filesystem move. Do not edit `.git/worktrees` by hand.

## Offline drives, stale registrations and locks

Before disconnecting storage, lock its worktree with a reason. Missing paths
remain registered and doctor reports them. Unlock only when the registration is
no longer intentionally protected. Preview cleanup with `prune --dry-run`; use
`--expire now` only after checking that the missing drive is not merely offline.
Pruning never deletes a branch or an existing checkout.

## Removal failed

Dirty content is protected unless `--force` is supplied. A lock still protects
the checkout with one force flag. The main checkout cannot be removed through
gwtui. A failed batch exits nonzero even if some earlier operations succeeded.
If `--delete-branch` fails because a branch is unmerged, the clean checkout may
already have been removed while its branch remains; this is not a transaction.
Inspect `git branch` before deciding whether to delete that branch manually.

## Configuration errors

`GWTUI_CONFIG` selects an explicit path; `XDG_CONFIG_HOME` selects a config root
on Unix-like systems. `config path` shows the winner. Invalid updates fail before
atomic replacement. `config set` can repair an invalid setting if the resulting
whole document validates. Preserve a malformed original file while investigating;
do not delete personal settings just to make a command start. See
[configuration limits](status-and-config.md) for expansion and persistence details.

## Unusual filenames and noninteractive output

Use JSON or `list --paths --null` for names with control characters. Human tables
escape controls so filenames cannot inject terminal commands. gwtui's worktree
metadata API is UTF-8; if Git reports non-UTF-8 paths, use Git directly rather
than allowing a lossy replacement path to become a mutation target.

No-argument invocation opens a TUI only on a TTY. Use explicit commands in CI.
JSON and CSV cannot be combined with status watch mode; watch intervals must
be positive. Use `--no-fetch` to avoid remote fetching while diagnosing local
status. Tasks and Zellij are separate optional workflows, not required for the
core lifecycle commands above.
