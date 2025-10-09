# Shell integration

Build/install gwtui first and put its binary on `PATH`. All examples quote the
selected path; never run `eval` on gwtui output.

## Change the current shell directory

A child process cannot change its parent shell's directory. Use a small function:

```sh
gcd() {
    local destination
    destination="$(gwtui get "$@")" || return
    cd -- "$destination"
}
```

Use `gcd feature/login` from a repository, or `gcd -g feature/login` for configured
global discovery. Command substitution strips trailing newlines; unusual directory
names ending with newline need a NUL-aware consumer instead. This helper is for
normal interactive names and POSIX-style shells supporting `local`.

Create-and-enter explicitly:

```sh
destination="$(gwtui add -b feature/login ../login --print-path)" && cd -- "$destination"
```

The branch must not already exist. Use `add feature/login ../login` to check out
an existing branch. Use `gwtui -C /path/to/repo list --paths` to inspect another
repository without changing the shell's own directory.

## NUL-aware scripting (Bash)

```bash
while IFS= read -r -d '' checkout; do
    git -C "$checkout" status --short
done < <(gwtui list --paths --null)
```

This handles spaces and embedded newlines. Do not use `for p in $(gwtui list)`.
For downstream tools supporting `-0`, `list --paths --null` can also feed `xargs -0`.
JSON is preferable when consumers also need branch and main-checkout metadata.

## Completions

Generate a completion file for your shell:

```sh
gwtui completion bash > gwtui.bash
gwtui completion zsh > _gwtui
gwtui completion fish > gwtui.fish
```

Load the generated file according to your shell's completion configuration.
PowerShell and Elvish generation are supported too. Completions come from the
same Clap definitions as command help, including lifecycle and diagnostics flags.

## Executed commands are not shell strings

```sh
gwtui exec feature/login -- cargo test --locked
gwtui exec feature/login -- sh -c 'printf "%s\n" "$PWD"'
```

The first command preserves argument boundaries directly. The second deliberately
starts a shell. The caller's shell expands unquoted `$VAR` before gwtui receives
it; use quoting when literal values are intended. `exec` preserves a normal
child exit code, so CI failures remain failures. `--stay` is interactive and
should not be used in unattended scripts.
