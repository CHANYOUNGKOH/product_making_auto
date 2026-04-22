from __future__ import annotations

import os
import subprocess
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT_PATH = REPO_ROOT / "scripts" / "close_ticket.ps1"


def run_command(
    command: list[str],
    cwd: Path,
    *,
    env: dict[str, str] | None = None,
    input_text: str | None = None,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        command,
        cwd=str(cwd),
        env=env,
        input=input_text,
        text=True,
        encoding="utf-8",
        errors="replace",
        capture_output=True,
        check=False,
    )
    if check and result.returncode != 0:
        raise AssertionError(
            f"Command failed ({result.returncode}): {' '.join(command)}\n"
            f"STDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
        )
    return result


def install_fake_gh(tmp_path: Path) -> tuple[Path, Path]:
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    log_path = tmp_path / "gh.log"
    gh_cmd = bin_dir / "gh.cmd"
    gh_cmd.write_text(
        "@echo off\r\n"
        "echo %*>>\"%GH_LOG%\"\r\n"
        "if \"%1 %2\"==\"pr list\" (\r\n"
        "  echo []\r\n"
        "  exit /b 0\r\n"
        ")\r\n"
        "if \"%1 %2\"==\"pr create\" exit /b 0\r\n"
        "exit /b 0\r\n",
        encoding="utf-8",
    )
    return bin_dir, log_path


def make_repo(tmp_path: Path) -> Path:
    origin = tmp_path / "origin.git"
    repo = tmp_path / "repo"
    run_command(["git", "init", "--bare", str(origin)], cwd=tmp_path)
    run_command(["git", "clone", str(origin), str(repo)], cwd=tmp_path)
    run_command(["git", "config", "user.name", "Codex Test"], cwd=repo)
    run_command(["git", "config", "user.email", "codex@example.com"], cwd=repo)
    (repo / ".gitignore").write_text(".worktrees/\n", encoding="utf-8")
    (repo / "README.md").write_text("base\n", encoding="utf-8")
    run_command(["git", "add", ".gitignore", "README.md"], cwd=repo)
    run_command(["git", "commit", "-m", "initial"], cwd=repo)
    run_command(["git", "branch", "-M", "main"], cwd=repo)
    run_command(["git", "push", "-u", "origin", "main"], cwd=repo)
    (repo / ".worktrees").mkdir()
    return repo


def create_ticket_file(repo: Path, ticket_id: str = "PH-META-001") -> Path:
    ticket_path = repo / "docs" / "tickets" / "phase-meta-automation" / f"{ticket_id}.md"
    ticket_path.parent.mkdir(parents=True, exist_ok=True)
    ticket_path.write_text(f"# {ticket_id}\n", encoding="utf-8")
    return ticket_path


def create_implementer_branch(repo: Path) -> tuple[str, Path]:
    branch = "ticket/ph-meta-001-hubcoder"
    run_command(["git", "checkout", "-b", branch, "main"], cwd=repo)
    (repo / "feature.txt").write_text("ticket change\n", encoding="utf-8")
    run_command(["git", "add", "feature.txt"], cwd=repo)
    run_command(["git", "commit", "-m", "implement close ticket flow"], cwd=repo)
    run_command(["git", "checkout", "main"], cwd=repo)
    worktree_path = repo / ".worktrees" / "ph-meta-001-hubcoder"
    run_command(["git", "worktree", "add", str(worktree_path), branch], cwd=repo)
    return branch, worktree_path


def create_review_branch(repo: Path, implementer_branch: str, *, pass_verdict: bool) -> tuple[str, Path]:
    review_branch = "review/ph-meta-001"
    reviewer_path = repo / ".worktrees" / "ph-meta-001-reviewer"
    run_command(["git", "branch", review_branch, implementer_branch], cwd=repo)
    run_command(["git", "worktree", "add", str(reviewer_path), review_branch], cwd=repo)
    (reviewer_path / "review.txt").write_text("review output\n", encoding="utf-8")
    run_command(["git", "add", "review.txt"], cwd=reviewer_path)
    message = "review result VERDICT=PASS" if pass_verdict else "review result needs-fix"
    run_command(["git", "commit", "-m", message], cwd=reviewer_path)
    return review_branch, reviewer_path


def run_close_ticket(
    repo: Path,
    args: str,
    *,
    env: dict[str, str],
    input_text: str | None = None,
) -> subprocess.CompletedProcess[str]:
    return run_command(
        [
            "powershell",
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-Command",
            (
                "[Console]::InputEncoding=[System.Text.UTF8Encoding]::new($false); "
                "[Console]::OutputEncoding=[System.Text.UTF8Encoding]::new($false); "
                f"& '{SCRIPT_PATH}' {args}"
            ),
        ],
        cwd=repo,
        env=env,
        input_text=input_text,
        check=False,
    )


def test_close_ticket_merges_and_cleans_up_from_ticket_file(tmp_path: Path):
    repo = make_repo(tmp_path)
    ticket_path = create_ticket_file(repo)
    implementer_branch, implementer_path = create_implementer_branch(repo)
    review_branch, reviewer_path = create_review_branch(repo, implementer_branch, pass_verdict=True)
    fake_gh_dir, gh_log = install_fake_gh(tmp_path)
    env = os.environ.copy()
    env["PATH"] = f"{fake_gh_dir}{os.pathsep}{env['PATH']}"
    env["GH_LOG"] = str(gh_log)

    result = run_close_ticket(
        repo,
        f"-TicketFile '{ticket_path.relative_to(repo)}' -Confirm:$false",
        env=env,
    )

    assert result.returncode == 0, result.stdout + result.stderr
    assert (repo / "feature.txt").read_text(encoding="utf-8") == "ticket change\n"
    assert not implementer_path.exists()
    assert not reviewer_path.exists()
    assert run_command(["git", "branch", "--list", implementer_branch], cwd=repo).stdout.strip() == ""
    assert run_command(["git", "branch", "--show-current"], cwd=repo).stdout.strip() == "main"
    assert review_branch in run_command(["git", "branch", "--list", review_branch], cwd=repo).stdout
    assert "pr create --base main --head ticket/ph-meta-001-hubcoder --fill" in gh_log.read_text(encoding="utf-8")


def test_close_ticket_blocks_without_pass_verdict(tmp_path: Path):
    repo = make_repo(tmp_path)
    implementer_branch, implementer_path = create_implementer_branch(repo)
    _, reviewer_path = create_review_branch(repo, implementer_branch, pass_verdict=False)
    fake_gh_dir, gh_log = install_fake_gh(tmp_path)
    env = os.environ.copy()
    env["PATH"] = f"{fake_gh_dir}{os.pathsep}{env['PATH']}"
    env["GH_LOG"] = str(gh_log)

    result = run_close_ticket(
        repo,
        "-ImplementerBranch 'ticket/ph-meta-001-hubcoder' -Confirm:$false",
        env=env,
    )

    assert result.returncode != 0
    assert "VERDICT=PASS" in (result.stdout + result.stderr)
    assert implementer_path.exists()
    assert reviewer_path.exists()
    assert not gh_log.exists() or gh_log.read_text(encoding="utf-8").strip() == ""


def test_close_ticket_prompts_for_confirmation_by_default(tmp_path: Path):
    repo = make_repo(tmp_path)
    implementer_branch, implementer_path = create_implementer_branch(repo)
    _, reviewer_path = create_review_branch(repo, implementer_branch, pass_verdict=True)
    fake_gh_dir, gh_log = install_fake_gh(tmp_path)
    env = os.environ.copy()
    env["PATH"] = f"{fake_gh_dir}{os.pathsep}{env['PATH']}"
    env["GH_LOG"] = str(gh_log)

    result = run_close_ticket(
        repo,
        "-ImplementerBranch 'ticket/ph-meta-001-hubcoder'",
        env=env,
        input_text="n\n",
    )

    assert result.returncode != 0
    assert implementer_path.exists()
    assert reviewer_path.exists()
    assert not (repo / "feature.txt").exists()
    assert "Read-Host" in SCRIPT_PATH.read_text(encoding="utf-8")
    assert "pr create --base main --head ticket/ph-meta-001-hubcoder --fill" in gh_log.read_text(encoding="utf-8")


def test_close_ticket_exits_nonzero_when_fast_forward_merge_fails(tmp_path: Path):
    repo = make_repo(tmp_path)
    implementer_branch, implementer_path = create_implementer_branch(repo)
    run_command(["git", "checkout", "main"], cwd=repo)
    (repo / "main-only.txt").write_text("main changed\n", encoding="utf-8")
    run_command(["git", "add", "main-only.txt"], cwd=repo)
    run_command(["git", "commit", "-m", "advance main"], cwd=repo)
    _, reviewer_path = create_review_branch(repo, implementer_branch, pass_verdict=True)
    fake_gh_dir, gh_log = install_fake_gh(tmp_path)
    env = os.environ.copy()
    env["PATH"] = f"{fake_gh_dir}{os.pathsep}{env['PATH']}"
    env["GH_LOG"] = str(gh_log)

    result = run_close_ticket(
        repo,
        "-ImplementerBranch 'ticket/ph-meta-001-hubcoder' -Confirm:$false",
        env=env,
    )

    assert result.returncode != 0
    combined = result.stdout + result.stderr
    assert "--ff-only" in combined or "fast-forward" in combined.lower()
    assert implementer_path.exists()
    assert reviewer_path.exists()
    assert (repo / "main-only.txt").read_text(encoding="utf-8") == "main changed\n"
    assert not (repo / "feature.txt").exists()
    assert "pr create --base main --head ticket/ph-meta-001-hubcoder --fill" in gh_log.read_text(encoding="utf-8")
