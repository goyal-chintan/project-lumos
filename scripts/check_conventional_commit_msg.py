"""Pre-commit commit-msg hook: validate Conventional Commits subject line.

Rules (matching project docs):
- type(scope): subject
- type ∈ {feat, fix, docs, style, refactor, test, chore}
- scope is optional
- subject length: 10..72 chars

We allow merge commits (e.g. "Merge branch ...") so git pulls/merges don't get blocked.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path


ALLOWED_TYPES = ("feat", "fix", "docs", "style", "refactor", "test", "chore")
SUBJECT_MIN_LEN = 10
SUBJECT_MAX_LEN = 72

CONVENTIONAL_RE = re.compile(
    rf"^({'|'.join(ALLOWED_TYPES)})(\([^)]+\))?: (?P<subject>.+)$"
)


def _first_non_comment_line(text: str) -> str:
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        if stripped.startswith("#"):
            continue
        return stripped
    return ""


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        print("Usage: check_conventional_commit_msg.py <commit_msg_file>", file=sys.stderr)
        return 2

    msg_file = Path(argv[1])
    try:
        raw = msg_file.read_text(encoding="utf-8", errors="replace")
    except FileNotFoundError:
        print(f"Commit message file not found: {msg_file}", file=sys.stderr)
        return 2

    subject_line = _first_non_comment_line(raw)
    if not subject_line:
        print("Empty commit message.", file=sys.stderr)
        return 1

    # Allow merge commits (e.g. from `git pull` / `git merge`).
    if subject_line.startswith("Merge "):
        return 0

    m = CONVENTIONAL_RE.match(subject_line)
    if not m:
        allowed = ", ".join(ALLOWED_TYPES)
        print("Commit message does not follow Conventional Commits.", file=sys.stderr)
        print("Expected: <type>(<scope>): <subject>", file=sys.stderr)
        print(f"Allowed types: {allowed}", file=sys.stderr)
        print(f"Got: {subject_line}", file=sys.stderr)
        return 1

    subject = m.group("subject").strip()
    if not (SUBJECT_MIN_LEN <= len(subject) <= SUBJECT_MAX_LEN):
        print(
            f"Commit subject must be {SUBJECT_MIN_LEN}-{SUBJECT_MAX_LEN} characters "
            f"(got {len(subject)}).",
            file=sys.stderr,
        )
        print(f"Subject: {subject}", file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))


