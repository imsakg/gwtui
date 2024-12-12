# Status, naming and global discovery contracts

This guide describes the observable behavior of `core::status`, `core::naming`
and `core::discovery`. These modules retain their existing public struct fields
and serialized status names; callers that construct struct literals remain
compatible. Configuration selection and persistence are documented below.

## Status collection

Git remains the source of truth. The collector reads porcelain v1 for index and
working-tree changes, then uses `git ls-files -z --others --exclude-standard`
for untracked paths. The NUL delimiter counts whitespace-only, newline-bearing
and Unicode filenames correctly; ignored files are not included.

All seven unmerged XY pairs are conflicts: `DD`, `AU`, `UD`, `UA`, `DU`, `AA`
and `UU`. Each unresolved entry contributes one conflict, not a staged change
or a second working-tree change. Ordinary staged paths still contribute to the
`staged` bucket, and unstaged edits contribute to their working-tree buckets.

The state of a successfully inspected worktree is selected in this order:

1. `conflict` when any unresolved entry exists.
2. `staged` when any ordinary index change exists.
3. `modified` when other local changes exist, including untracked files.
4. `clean` otherwise.

Only a **clean** worktree may become `stale` when its last activity exceeds the
configured collector threshold. Old activity never hides dirty/conflicted
state, and a Git failure remains `unknown`. Activity is based on file metadata,
not exclusively on commit age. An unborn repository can still report local
changes. Missing and non-repository directories produce unknown rows rather
than aborting the entire collection.

`collect_all` retains input order even when tasks complete out of order. Direct
API callers that pass concurrency zero get an effective minimum of one.
Configuration loading applies stricter validation; see the configuration
section. Current-directory detection resolves existing filesystem aliases and
uses path-component boundaries, so `repo-other` is not a child of `repo`.

### Change helpers and divergence

`GitStatus::is_dirty()` checks local changes, including conflicts. Ahead/behind
counts do not make a clean checkout dirty.

`GitStatus::total_changes()` sums `modified`, `added`, `deleted`, `untracked`,
`staged` and `conflicts`, saturating at `u32::MAX`. This is a sum of **buckets**,
not a unique-path count: one staged file with additional unstaged edits counts
twice. The changes sort uses the same overflow-safe total.

Despite its existing `fetch_remote` option name, collection only compares local
HEAD/upstream refs. It does not perform a network fetch. Missing upstreams leave
counts zero. Process detection remains unimplemented; `include_process` does
not currently add process information.

### Filtering and sorting

Filters trim surrounding whitespace and are case-insensitive:

- `all` or an empty filter: retain every row.
- `modified` / `changed`, `clean` / `up to date`, `stale` / `inactive`, `staged`,
  `conflict` / `conflicted`, and `unknown`: match the exact state.
- Other text: substring search across branch, repository and path.

`unknown` is now a state keyword rather than a substring query. A clean branch
named `unknown-fix` is not an unknown worktree.

Supported sorts are `branch` / `name`, `status`, `modified` / `changes`,
`activity` / `time`, `ahead`, and `behind`. Sort keys trim surrounding whitespace
and ignore case. Branch names sort ascending; counts and activity sort
descending. State sort preserves its existing display priority: conflict,
modified, staged, stale, clean, unknown. Ties use branch, repository, then path
in ascending order, independent of discovery order. Unknown sort keys leave
input order unchanged.

Activity sorting parses RFC3339 timestamps as instants, so offsets and fractional
seconds compare correctly. Invalid or unknown timestamps sort after valid ones,
then use the same deterministic tie-breakers.

## Repository identity and branch names

Supported origin forms include HTTPS/HTTP, SSH URLs, `git://` transport URLs,
and SCP-style remotes with an arbitrary username or SSH host alias:

```text
https://git.example/team/subgroup/service.git
ssh://deploy@git.example:2222/team/subgroup/service.git
deploy@git.example:team/subgroup/service.git
git.example:team/subgroup/service.git
```

For these examples, the identity is host `git.example`, owner `team/subgroup`,
and repository `service`. The final path component is the repository; all
preceding owner components remain in the namespace. Credentials and transport
ports are excluded from `RepoId`. A single trailing `.git` suffix is removed.

The identity parser is deliberately not a general URL or local-path parser.
Local filesystem/file URLs and IPv6 authorities are not supported. Unsafe
components are rejected: empty interior segments, dot/traversal segments,
backslashes, whitespace/control characters, Windows-reserved punctuation,
query/fragment syntax, and encoded dot/separator/NUL sequences. Invalid ports
and empty hosts are rejected. Discovery can still retain such repositories
using a local directory-name fallback; rejection does not mean Git itself
cannot use the remote.

Templates support both `{{host}}`, `{{owner}}`, `{{repo}}`, `{{branch}}`,
`{{hash}}` and their gwq aliases `{{.Host}}`, `{{.Owner}}`, `{{.Repository}}`,
`{{.Branch}}`, `{{.Hash}}`. Tokens are replaced in a single pass: token-like
text inside a value remains literal. Unknown tokens remain unchanged. The
hash is the first eight lowercase SHA-256 hex characters of
`host/owner/repository/branch`.

Filesystem branch sanitization:

- Applies configured mappings in deterministic map-key order; empty source
  keys are ignored by the helper and rejected by configuration validation.
- Replaces separators, controls and Windows-reserved filename characters with
  hyphens and collapses repeated hyphens.
- Removes leading hyphens and trailing hyphens, dots and ASCII spaces, while
  retaining ordinary hidden names such as `.feature`.
- Uses `branch` when no meaningful name remains.
- Prefixes reserved Windows device names with `_`, including case variants,
  extensions, `COM1`–`COM9`, `LPT1`–`LPT9` and the reserved superscript variants.

Normalization is idempotent. It is not a collision-proof encoding; distinct
branch names can sanitize to the same name. Use a hash-bearing template when
that distinction matters. These naming changes do not rename existing paths.

## Global repository discovery

Discovery includes ordinary main checkouts as well as linked worktrees. A
`.git` marker alone is insufficient: Git must resolve the candidate's exact
canonical top-level directory. Main-checkout identity compares the Git directory
with the common Git directory, including `--separate-git-dir` checkouts.

An origin remote is optional. If no supported origin identity is available,
discovery uses the local main repository directory name when the common Git
directory identifies it, otherwise the candidate directory name. The original
`repository_url` field remains the original Git remote string, or empty without
an origin; unlike `RepoId`, it may contain credentials and must not be treated
as a redacted logging surface.

Symbolic branches are retained for unborn repositories, with an empty commit
hash. Detached checkouts use branch `HEAD`. Results sort by path, then branch.
Filtering remains a case-insensitive substring search over repository, branch,
path and the `repository:branch` combination; an empty pattern matches all.

Traversal boundaries:

- The selected root is depth zero. Direct API depth zero inspects only that
  root; configuration requires a scan depth of at least one.
- Descendant directory symlinks are not followed, preventing loops and escapes.
  An explicitly selected symlink root is supported.
- Hidden directories, `.git`, `node_modules`, `target`, `.idea` and `.vscode`
  are pruned below the selected root.
- A valid linked worktree is reported but its contents are not scanned.
- Ordinary main checkouts remain traversal containers, so legitimate nested
  repositories can be discovered.
- Missing/non-directory roots produce an empty result. Unreadable directory
  entries are skipped; discovery is best-effort, not an exhaustive audit.

## Focused regression checks

Use an isolated Cargo target and scratch directory when running sibling lanes:

```sh
export CARGO_TARGET_DIR="$PWD/../target-status"
export TMPDIR="$HOME/.hermes/cache/scratch"
cargo test --lib core::status
cargo test --test status_reliability --test status_sorting
cargo test --test naming_reliability --test discovery_reliability
cargo build
cargo test
```

The integration tests use temporary real Git repositories, local upstream refs
and isolated subprocess environments. They do not depend on network remotes or
modify the test process's environment with unsafe Rust 2024 APIs.
