# CI/CD Troubleshooting Guide

## Common CI Failures and Solutions

### 1. Code Quality Checks Failing

#### Ruff Linting Errors
**Symptom**: `ruff check .` fails with linting errors

**Solution**:
```bash
# Install dependencies
pip install -r requirements-dev.txt

# Auto-fix what can be fixed
ruff check . --fix

# Check remaining issues
ruff check .
```

#### Ruff Format Errors
**Symptom**: `ruff format --check .` fails

**Solution**:
```bash
# Auto-format code
ruff format .

# Or use Black
black .
```

#### Black Format Errors
**Symptom**: `black --check .` fails

**Solution**:
```bash
# Auto-format with Black
black .
```

### 2. Pre-commit Checks Failing

#### Branch Naming
**Symptom**: Branch name doesn't follow convention

**Solution**:
- Ensure branch starts with: `feature/`, `fix/`, `docs/`, `chore/`, `refactor/`, or `test/`
- Rename branch: `git branch -m old-name feature/new-name`

#### Commit Message Format
**Symptom**: Commit message doesn't follow Conventional Commits

**Solution**:
- Use format: `type(scope): subject`
- Types: `feat`, `fix`, `docs`, `style`, `refactor`, `test`, `chore`
- Example: `feat(ingestion): add PostgreSQL handler`

#### Pre-commit Hook Failures
**Symptom**: Pre-commit hooks fail on code files

**Solution**:
```bash
# Run pre-commit manually to see errors
pre-commit run --all-files

# Auto-fix what can be fixed
pre-commit run --all-files --hook-stage manual
```

### 3. Test Failures

#### No Tests Found
**Symptom**: `pytest tests/unit/` finds no tests

**Solution**:
- Create test files in `tests/unit/` directory
- Test files should start with `test_` or end with `_test.py`
- For now, this is non-blocking (workflow will skip if no tests)

#### Test Failures
**Symptom**: Tests fail or coverage below threshold

**Solution**:
```bash
# Run tests locally
pytest tests/unit/ -v

# Check coverage
pytest tests/unit/ --cov=core --cov=feature --cov-report=term-missing

# Fix failing tests or improve coverage
```

### 4. Dependency Installation Issues

#### Requirements Installation Fails
**Symptom**: `pip install -r requirements-dev.txt` fails

**Solution**:
```bash
# Upgrade pip first
python -m pip install --upgrade pip

# Install in order
pip install -r requirements.txt
pip install -r requirements-dev.txt

# Or use virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements-dev.txt
```

### 5. Workflow-Specific Issues

#### Code Quality Workflow
- **Issue**: Multiple Python versions failing
- **Solution**: Workflow now uses `continue-on-error: true` for non-critical checks
- **Note**: Only critical checks (like syntax errors) will fail the workflow

#### Pre-commit Checks Workflow
- **Issue**: Pre-commit hooks failing
- **Solution**: Workflow now allows warnings without failing
- **Note**: Fix linting/formatting issues locally before pushing

#### CI Workflow
- **Issue**: Tests or coverage failing
- **Solution**: Workflow handles missing tests gracefully
- **Note**: Add tests gradually to improve coverage

## Quick Fix Commands

### Fix All Formatting Issues
```bash
# Install dependencies
pip install -r requirements-dev.txt

# Fix formatting
ruff format .
black .

# Fix linting
ruff check . --fix

# Run pre-commit
pre-commit run --all-files
```

### Verify Locally Before Pushing
```bash
# Run all checks locally
./test_standards.sh

# Run pre-commit
pre-commit run --all-files

# Run tests
pytest tests/unit/ -v
```

## Current Workflow Status

The workflows have been configured to be **lenient during initial setup**:

- ✅ **Non-blocking checks**: Formatting, linting warnings won't block PRs initially
- ✅ **Graceful handling**: Missing tests won't cause failures
- ✅ **Informative**: All checks still run and report issues

## Making Checks Stricter (Future)

Once the codebase is cleaned up, you can make checks stricter by:

1. Remove `continue-on-error: true` from workflows
2. Remove `|| true` from command chains
3. Set `fail-fast: true` in matrix strategies

## Getting Help

1. Check workflow logs in GitHub Actions
2. Run checks locally: `./test_standards.sh`
3. Review error messages in CI logs
4. See [CONTRIBUTING.md](CONTRIBUTING.md) for setup instructions

---

**Note**: The workflows are currently configured to be informative rather than blocking, allowing the project to establish code quality standards gradually.

