"""Conventions validation helpers for Lumos.

Goals:
- Keep conventions consistent across local pre-commit hooks, CI checks, and docs.
- Be dependency-free (stdlib only) so it can run anywhere.

This module intentionally focuses on *format* and *linkage* conventions:
- Branch names include an issue number (e.g., feature/19-issue-linking)
  so branches are trivially traceable to issues.
- PR titles follow Conventional Commits style (common in OSS).
- PR descriptions reference the issue number implied by the branch name.
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from dataclasses import dataclass
from typing import Optional


DEFAULT_BRANCHES = {"main", "master", "develop"}
EXEMPT_BRANCH_PREFIXES = ("dependabot/", "renovate/")

BRANCH_TYPES = ("feature", "fix", "docs", "chore", "refactor", "test")
COMMIT_TYPES = ("feat", "fix", "docs", "style", "refactor", "test", "chore")

# Branch format: <type>/<issue>-<kebab-slug>
BRANCH_RE = re.compile(
    r"^(?P<type>feature|fix|docs|chore|refactor|test)/"
    r"(?P<issue>[0-9]+)-"
    r"(?P<slug>[a-z0-9][a-z0-9-]*)$"
)

# PR title / commit subject format: <type>(<scope>): <subject>
CONVENTIONAL_SUBJECT_RE = re.compile(
    r"^(?P<type>feat|fix|docs|style|refactor|test|chore)"
    r"(\([^)]+\))?:\s"
    r"(?P<subject>.+)$"
)

# Same-repo issue references like "#19" (allow "(#19)", "Fixes #19", etc.)
SAME_REPO_ISSUE_RE = re.compile(r"(?<!\w)#(?P<num>[0-9]+)")
# Cross-repo issue references like "owner/repo#19"
CROSS_REPO_ISSUE_RE = re.compile(r"\b[\w.-]+/[\w.-]+#(?P<num>[0-9]+)")


@dataclass(frozen=True)
class ValidationResult:
    ok: bool
    message: str = ""


def _run_git(args: list[str]) -> str:
    """Run a git command and return stdout, or empty string on failure."""
    try:
        out = subprocess.check_output(["git", *args], stderr=subprocess.DEVNULL)
    except Exception:
        return ""
    return out.decode("utf-8", errors="replace").strip()


def detect_current_branch() -> str:
    """Best-effort branch detection; returns empty string if unknown."""
    # Prefer symbolic-ref; fallback to rev-parse.
    branch = _run_git(["symbolic-ref", "--short", "HEAD"])
    if branch:
        return branch
    branch = _run_git(["rev-parse", "--abbrev-ref", "HEAD"])
    if branch in {"", "HEAD"}:
        return ""
    return branch


def extract_issue_refs(text: str) -> set[int]:
    """Extract issue references from text.

    Supports:
    - #19
    - owner/repo#19
    """
    refs: set[int] = set()
    for m in SAME_REPO_ISSUE_RE.finditer(text):
        refs.add(int(m.group("num")))
    for m in CROSS_REPO_ISSUE_RE.finditer(text):
        refs.add(int(m.group("num")))
    return refs


def branch_issue_number(branch_name: str) -> Optional[int]:
    m = BRANCH_RE.match(branch_name)
    if not m:
        return None
    return int(m.group("issue"))


def validate_branch_name(branch_name: str) -> ValidationResult:
    if not branch_name:
        # Detached HEAD or unable to determine branch; don't block commits.
        return ValidationResult(ok=True)

    if branch_name in DEFAULT_BRANCHES:
        return ValidationResult(ok=True)

    if branch_name.startswith(EXEMPT_BRANCH_PREFIXES):
        return ValidationResult(ok=True)

    if BRANCH_RE.match(branch_name):
        return ValidationResult(ok=True)

    expected = f"<type>/<issue>-<slug> where <type> in {', '.join(BRANCH_TYPES)}"
    examples = (
        "feature/19-issue-linking\n"
        "fix/15-ingestion-status\n"
        "docs/8-add-demo-screenshots\n"
        "chore/16-bump-upload-artifact\n"
    )
    return ValidationResult(
        ok=False,
        message=(
            "❌ Invalid branch name.\n"
            f"Expected: {expected}\n"
            f"Got: {branch_name}\n\n"
            "Examples:\n"
            f"{examples}"
        ),
    )


def validate_conventional_subject(subject_line: str, *, min_subject_len: int = 10) -> ValidationResult:
    if not subject_line:
        return ValidationResult(ok=False, message="❌ Empty message is not allowed.")

    m = CONVENTIONAL_SUBJECT_RE.match(subject_line)
    if not m:
        return ValidationResult(
            ok=False,
            message=(
                "❌ Message does not follow Conventional Commits format.\n"
                "Expected: <type>(<scope>): <subject>\n"
                f"Types: {', '.join(COMMIT_TYPES)}\n"
                f"Got: {subject_line}\n"
            ),
        )

    subj = m.group("subject").strip()
    if len(subj) < min_subject_len:
        return ValidationResult(
            ok=False,
            message=(
                f"❌ Subject too short (min {min_subject_len} chars).\n"
                f"Got: {subject_line}\n"
            ),
        )

    return ValidationResult(ok=True)


def validate_pr_body_links_issue(
    pr_body: str,
    *,
    branch_name: str,
    actor: str = "",
) -> ValidationResult:
    # Exempt bot/automation flows.
    if actor in {"dependabot[bot]", "renovate[bot]"}:
        return ValidationResult(ok=True)

    if branch_name.startswith(EXEMPT_BRANCH_PREFIXES):
        return ValidationResult(ok=True)

    issue_num = branch_issue_number(branch_name)
    if issue_num is None:
        return ValidationResult(
            ok=False,
            message=(
                "❌ Cannot infer issue number from branch name.\n"
                "Branch must be: <type>/<issue>-<slug> (e.g., feature/19-issue-linking)\n"
                f"Got: {branch_name}\n"
            ),
        )

    refs = extract_issue_refs(pr_body or "")
    if not refs:
        return ValidationResult(
            ok=False,
            message=(
                "❌ PR description must reference the issue.\n"
                "Add one of these lines to the PR description:\n"
                f"- Closes #{issue_num}\n"
                f"- Fixes #{issue_num}\n"
                f"- Resolves #{issue_num}\n"
                f"- Refs #{issue_num}\n"
            ),
        )

    if issue_num not in refs:
        return ValidationResult(
            ok=False,
            message=(
                "❌ PR description must reference the issue number implied by the branch name.\n"
                f"Branch: {branch_name} (issue #{issue_num})\n"
                f"Found references: {', '.join(f'#{n}' for n in sorted(refs))}\n"
                f"Add: Closes #{issue_num} (or Fixes/Resolves/Refs)\n"
            ),
        )

    return ValidationResult(ok=True)


def _print_and_exit(result: ValidationResult) -> None:
    if result.ok:
        print("✅ OK")
        raise SystemExit(0)
    print(result.message.rstrip(), file=sys.stderr)
    raise SystemExit(1)

def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(prog="conventions", add_help=True)
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_branch = sub.add_parser("check-branch", help="Validate branch name")
    p_branch.add_argument("--branch", default="", help="Branch name (defaults to current git branch)")

    p_pr_title = sub.add_parser("check-pr-title", help="Validate PR title (Conventional Commits style)")
    p_pr_title.add_argument("--title", required=True, help="PR title")

    p_commit = sub.add_parser("check-commit-subject", help="Validate a commit subject line")
    p_commit.add_argument("--subject", required=True, help="Commit subject line")

    p_pr_body = sub.add_parser("check-pr-body", help="Validate PR body links the issue implied by branch name")
    p_pr_body.add_argument("--branch", required=True, help="PR branch name (head ref)")
    p_pr_body.add_argument("--actor", default="", help="GitHub actor (for bot exemptions)")

    args = parser.parse_args(argv)

    if args.cmd == "check-branch":
        branch = args.branch or detect_current_branch()
        res = validate_branch_name(branch)
        _print_and_exit(res)

    if args.cmd == "check-pr-title":
        res = validate_conventional_subject(args.title)
        _print_and_exit(res)

    if args.cmd == "check-commit-subject":
        res = validate_conventional_subject(args.subject)
        _print_and_exit(res)

    if args.cmd == "check-pr-body":
        body = sys.stdin.read()
        res = validate_pr_body_links_issue(body, branch_name=args.branch, actor=args.actor)
        _print_and_exit(res)

    raise AssertionError(f"Unhandled cmd: {args.cmd}")


if __name__ == "__main__":
    raise SystemExit(main())


