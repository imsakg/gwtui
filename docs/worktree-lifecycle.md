# Worktree lifecycle

All mutations delegate to Git. gwtui does not edit `.git/worktrees` directly.
Selectors prefer an exact branch/path, then a canonical filesystem path, then
an unambiguous case-insensitive substring. Empty selectors fail. Branch names
and revision expressions are validated before Git performs checkout mutations.

## Create and review

```sh
gwtui add -b feature/login ../login --base main
gwtui add --detach ../review --revision HEAD~1
gwtui add -b scratch ../scratch --print-path
```

`--base` requires `-b`; `--revision` requires `--detach`. Detached checkout does
not create a branch. Explicit CLI paths expand tilde/environment placeholders;
library `Path` arguments remain literal. Automatically generated destinations
use `worktree.base_dir`, the naming template, and branch sanitization settings.
`--force` skips the CLI empty-directory preflight only; Git still protects
existing contents. It does not recursively delete a destination.

## Preserve, move, repair

```sh
gwtui lock feature/login --reason 'offline drive; do not prune'
gwtui unlock feature/login
gwtui move feature/login ../login-renamed
gwtui repair ../login-renamed
```

Locks protect registrations for temporarily disconnected drives. They do not
make files read-only. Unlock before a deliberate move or removal. Move delegates
to `git worktree move`; Git's limitations for submodules and occupied destinations
still apply. Main checkout movement/removal is refused. Repair is for checkouts
moved externally; without paths, Git attempts repair of known registrations.

## Remove and prune are different

```sh
gwtui remove feature/login --dry-run --delete-branch
gwtui remove feature/login --delete-branch
gwtui prune --dry-run --expire now
gwtui prune --expire now
```

Remove deletes a checkout. Branch deletion is opt-in; `--force-delete-branch`
requires `--delete-branch`. Dirty checkouts need `--force`, but a single force
flag does not override a lock. Errors produce a nonzero exit status. Deleting a
branch after removal is not transactional: Git may preserve an unmerged branch
while the already-clean checkout has been removed. Inspect the reported error.

Prune only removes stale registration metadata, never an existing checkout or
branch. Preview with `--dry-run` first. Git decides expiry and lock eligibility;
`--expire now` is more aggressive than the default grace period. Registrations
for missing paths remain visible in list and doctor until Git prunes them.

Bare repositories can host linked checkouts too. `gwtui -C /path/repo.git list`
includes the bare primary registration; create detached reviews with explicit
paths. Current worktree objects expose directory mtime as `created_at`; this is
not a reliable historical creation timestamp. Paths must be UTF-8 for gwtui's
string-based public metadata model; invalid metadata is rejected, never replaced
with a different lossy path. Use Git directly for non-UTF-8 checkout names.
