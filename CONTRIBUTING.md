# Development and verification

The project is a Rust 2024 package. Install a current stable Rust toolchain,
Git, rustfmt and Clippy. Zellij and external task runners are not required for
ordinary builds or lifecycle tests. Source code forbids unsafe Rust.

## Required local gates

```sh
cargo fmt --all -- --check
cargo clippy --locked --all-targets -- -D warnings
cargo build --locked
cargo test --locked
cargo build --locked --release
./target/release/gwtui --help
python3 scripts/smoke.py ./target/release/gwtui
cargo doc --no-deps
```

The committed workflow runs builds, formatting, strict Clippy, tests and release
help on Ubuntu and macOS. Its existence is not evidence of a successful remote
CI run; check the actual workflow run when submitting a change. Windows runtime
support is not covered by that matrix.

## Test organization

- Unit tests cover parsing, validation, naming, sorting and duration boundaries.
- `tests/common` creates isolated repositories with signing disabled locally.
- Lifecycle integration tests invoke real Git in temporary directories.
- CLI tests invoke Cargo's built binary, isolate HOME/config, and inspect process
  exit codes and structured output rather than matching incidental spacing.
- Output tests cover Unicode width, control escaping, CSV round trips and I/O
  error propagation.
- Rustdoc compiles the public library example.

Tests must not change the developer's real Git config, worktrees, task queues or
home-directory config. Use the shared fixture or equally isolated paths. Preserve
literal argument boundaries; shell snippets should only be used when testing an
explicit shell execution contract. Never require a network fetch or a live agent
subscription for a core worktree regression.

When running multiple worktrees concurrently, set a distinct `CARGO_TARGET_DIR`
for each verification lane. Reusing one shared target directory causes avoidable
build locks and makes binary provenance harder to audit. Run the final gates
against the exact revision being delivered, not an older branch's test summary.

## Changes

Prefer one coherent behavior or regression per commit. Test a bug before fixing
it. Keep Git as the authority for branch safety, dirty checks and registration
changes; do not implement a parallel `.git` database. Destructive commands need
explicit selection, meaningful nonzero failures and regression coverage for
main, dirty and locked checkouts. Preserve the CLI's JSON/NUL contracts when
changing human output. Document compatibility changes rather than hiding them.

If an interaction changes, exercise the actual TUI in a terminal as well as
running automated tests. CLI help in a pipe does not verify TUI behavior. Do not
leave a test UI or worker running after verification. Release smoke checks must
use disposable repositories and perform no pushes.
