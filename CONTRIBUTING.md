Contributing Guide

Thank you for your interest in contributing! We welcome issues, bug fixes, improvements, and new features.

Getting Started

1. Fork the repository and create your branch from main:
   - git checkout -b feat/your-feature
2. Set up your environment:
   - python -m venv .venv && source .venv/bin/activate
   - pip install -r requirements.txt
   - pip install -r requirements-dev.txt  # for linting, typing, tests
3. Run the checks:
   - make precommit  (or) pre-commit run -a
   - pytest

Development Standards

- Code style: black, isort, ruff
- Typing: mypy strict on public interfaces
- Tests: add unit tests for new logic, maintain coverage
- Commits: Conventional Commits (feat:, fix:, chore:, docs:, refactor:, test:)

Pull Requests

- Keep PRs focused and small
- Include a clear description of the change and rationale
- Update docs and examples where relevant
- Ensure CI passes

Security

If you discover a security issue, please email security@yourdomain.com instead of filing a public issue.


