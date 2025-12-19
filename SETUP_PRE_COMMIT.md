# Pre-commit Hooks Setup Guide

This guide explains how to set up and use pre-commit hooks for the Lumos Framework project.

## What are Pre-commit Hooks?

Pre-commit hooks automatically run code quality checks before you commit changes. This ensures:
- ✅ Consistent code formatting
- ✅ Proper commit message format
- ✅ Branch naming conventions
- ✅ Code quality standards
- ✅ Security best practices

## Quick Setup

1. **Install development dependencies:**
   ```bash
   pip install -r requirements-dev.txt
   ```

2. **Install pre-commit hooks:**
   ```bash
   pre-commit install
   ```

That's it! Pre-commit hooks will now run automatically before each commit.

## What Gets Checked?

### 1. Branch Naming Convention
- ✅ Format: `<type>/<issue-number>-<short-kebab-case>`
- ✅ Types: `feature`, `fix`, `docs`, `chore`, `refactor`, `test`
- ✅ Example: `feature/19-link-issues-to-prs`
- ❌ Invalid: `feature/add-thing`, `fix/bug-123`, `patch-1`

### 2. Commit Message Format
- ✅ Must follow Conventional Commits: `type(scope): subject`
- ✅ Types: `feat`, `fix`, `docs`, `style`, `refactor`, `test`, `chore`
- ✅ Recommended: add an issue reference like `(#19)` in the subject or `Refs #19` in the footer
- ❌ Invalid: `Update code`, `Fixed bug`, `Changes`

### 3. Code Formatting
- ✅ Black formatting (line length: 100)
- ✅ Ruff formatting (modern, fast)

### 4. Code Quality
- ✅ Ruff linting (replaces Flake8 - 10-100x faster!)
- ✅ MyPy type checking
- ✅ Pydocstyle docstring format
- ✅ Bandit security scanning

### 5. File Checks
- ✅ No trailing whitespace
- ✅ Files end with newline
- ✅ Valid YAML/JSON syntax
- ✅ No merge conflicts
- ✅ No large files (>1MB)

## Manual Usage

### Run checks on all files:
```bash
pre-commit run --all-files
```

### Run checks on staged files only:
```bash
pre-commit run
```

### Run a specific hook:
```bash
pre-commit run black
pre-commit run ruff
pre-commit run mypy
```

### Skip hooks (not recommended):
```bash
git commit --no-verify
```

## Troubleshooting

### Hooks are not running
1. Check if hooks are installed:
   ```bash
   pre-commit --version
   ```

2. Reinstall hooks:
   ```bash
   pre-commit uninstall
   pre-commit install
   ```

### Formatting issues
If Black or Ruff make changes, they will auto-format your code. Just stage the changes and commit again:
```bash
git add .
git commit -m "style: auto-format code"
```

### Type checking errors
MyPy errors are informative but not blocking. Fix them when possible, but you can commit if needed.

### Commit message rejected
Make sure your commit message follows the format:
```
feat(ingestion): add new handler
fix(datahub): resolve connection issue
docs(readme): update installation guide
```

### Branch name rejected
Rename your branch to follow the convention:
```bash
git branch -m old-name feature/19-new-name
```

## Updating Hooks

To update pre-commit hooks to the latest versions:
```bash
pre-commit autoupdate
```

## CI/CD Integration

Pre-commit checks also run automatically in GitHub Actions on:
- Pull requests to `main`, `master`, or `develop`
- Pushes to `main`, `master`, or `develop`

All checks must pass before merging PRs.

## Need Help?

- Check [CONTRIBUTING.md](CONTRIBUTING.md) for contribution guidelines
- Review the `.pre-commit-config.yaml` file for hook configurations
- Open an issue if you encounter problems

