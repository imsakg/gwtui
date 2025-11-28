#!/usr/bin/env python3
"""Exercise a built gwtui binary against disposable real Git repositories."""

import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile


def main():
    if len(sys.argv) != 2:
        raise SystemExit("usage: python3 scripts/smoke.py /path/to/gwtui")
    binary = Path(sys.argv[1]).resolve(strict=True)
    commands = 0
    with tempfile.TemporaryDirectory(prefix="gwtui-smoke-") as directory:
        temp = Path(directory)
        repo = temp / "repo"
        repo.mkdir()
        home = temp / "home"
        home.mkdir()
        env = os.environ.copy()
        for key in list(env):
            if key.startswith("GIT_"):
                del env[key]
        env.update(
            HOME=str(home),
            XDG_CONFIG_HOME=str(home / ".config"),
            GWTUI_CONFIG=str(home / ".config" / "gwtui" / "config.toml"),
            GIT_CONFIG_NOSYSTEM="1",
            GIT_CONFIG_GLOBAL=os.devnull,
        )

        def execute(args, expected=0):
            nonlocal commands
            commands += 1
            result = subprocess.run(
                [str(arg) for arg in args],
                cwd=repo,
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=30,
                check=False,
            )
            if result.returncode != expected:
                raise AssertionError(
                    f"{args!r}: exit {result.returncode}, expected {expected}\n"
                    + result.stderr.decode(errors="replace")
                )
            return result.stdout

        def git(*args):
            return execute(["git", *args]).decode().strip()

        def cli(*args, expected=0):
            return execute([binary, *args], expected=expected)

        git("init", "-b", "main")
        git("config", "user.name", "Smoke Fixture")
        git("config", "user.email", "smoke@example.invalid")
        git("config", "commit.gpgsign", "false")
        git("config", "core.hooksPath", os.devnull)
        (repo / "README.md").write_text("initial\n")
        git("add", "README.md")
        git("commit", "-m", "initial")

        cli("--help")
        cli("--version")
        cli("config", "check")
        config_file = Path(cli("config", "path").decode().strip())
        assert not config_file.exists()
        settings = json.loads(cli("config", "list", "--json"))
        assert settings["worktree"]["auto_mkdir"] is True
        cli("config", "set", "worktree.base_dir", temp)
        cli("config", "set", "ui.icons", "false")
        assert config_file.is_file()

        checkout = temp / "feature with spaces"
        created = cli(
            "add", "-b", "feature/login", checkout,
            "--base", "main", "--print-path",
        ).decode().rstrip("\n")
        assert Path(created) == checkout.resolve()
        assert cli("get", "feature/login", "--null") == os.fsencode(checkout.resolve()) + b"\0"
        paths = cli("list", "--paths", "--null").split(b"\0")
        assert os.fsencode(checkout.resolve()) in paths
        assert len([path for path in paths if path]) == 2
        rows = json.loads(cli("list", "--json", "--sort", "branch"))
        assert [row["branch"] for row in rows] == ["feature/login", "main"]
        status = json.loads(cli("status", "--json", "--no-fetch"))
        summary = status["summary"]
        assert summary["Total"] == 2
        assert sum(summary[key] for key in ["Modified", "Clean", "Stale", "Staged", "Conflict", "Unknown"]) == 2

        literal = "space ; $(not-a-command) & quote'\""
        output = cli(
            "exec", "feature/login", "--", sys.executable, "-c",
            "import json,sys; print(json.dumps(sys.argv[1:]))", "", literal,
        )
        assert json.loads(output) == ["", literal]
        cli("exec", "feature/login", "--", sys.executable, "-c", "raise SystemExit(37)", expected=37)

        cli("lock", "feature/login", "--reason", "offline | keep")
        report = json.loads(cli("doctor", "--json"))
        assert report["locked_worktrees"] == 1
        cli("prune", "--dry-run", "--expire", "now")
        cli("unlock", "feature/login")
        moved = temp / "moved"
        cli("move", "feature/login", moved)
        assert not checkout.exists() and moved.joinpath(".git").is_file()
        repaired = temp / "repaired"
        moved.rename(repaired)
        cli("repair", repaired)
        assert Path(cli("get", "feature/login").decode().rstrip("\n")) == repaired.resolve()

        review = temp / "review\ncheckout"
        cli("add", "--detach", review, "--revision", "HEAD")
        detached = json.loads(cli("list", "--json", "--detached"))
        assert len(detached) == 1 and detached[0]["branch"] == "HEAD"
        global_rows = json.loads(cli("list", "-g", "--json"))
        assert len(global_rows) == 3
        assert sum(row["is_main"] for row in global_rows) == 1
        branches = json.loads(cli("branches", "--all", "--json"))
        assert {branch["name"] for branch in branches} == {"main", "feature/login"}
        cli("remove", "feature/login", "--dry-run", "--delete-branch")
        assert repaired.exists()
        cli("remove", "feature/login", "--delete-branch")
        assert not repaired.exists()
        assert git("branch", "--list", "feature/login") == ""
        cli("remove", review)
        assert not review.exists()
        assert len(json.loads(cli("list", "--json"))) == 1
        report = json.loads(cli("doctor", "--json"))
        assert report["worktrees"] == 1 and report["missing_worktrees"] == 0

    assert not temp.exists(), "disposable smoke repository was not cleaned up"
    print(json.dumps({"binary": str(binary), "commands": commands, "result": "passed", "temporary_files_removed": True}))


if __name__ == "__main__":
    main()
