# Code Quality Standards

This document outlines the code quality standards and tooling used in the Lumos Framework project, aligned with top open source projects.

## 🎯 Standards Overview

Our code quality standards are designed to ensure:
- ✅ Consistent code formatting and style
- ✅ High code quality and maintainability
- ✅ Security best practices
- ✅ Comprehensive test coverage
- ✅ Automated dependency management
- ✅ Professional development workflow

## 🛠️ Tools & Configuration

### 1. **Ruff** (Modern Linter)
- **Replaces**: Flake8, isort, and multiple other tools
- **Why**: 10-100x faster than traditional linters, actively maintained
- **Used by**: FastAPI, Pydantic, and other top Python projects
- **Configuration**: `pyproject.toml`

### 2. **Black** (Code Formatter)
- **Standard**: PEP 8 compliant formatting
- **Line length**: 100 characters
- **Configuration**: `pyproject.toml`

### 3. **MyPy** (Type Checking)
- **Purpose**: Static type checking
- **Configuration**: `pyproject.toml`
- **Note**: Informative, not blocking (allows gradual adoption)

### 4. **Pytest** (Testing Framework)
- **Coverage threshold**: 70% minimum
- **Reports**: Terminal, XML, HTML
- **Configuration**: `pyproject.toml`

### 5. **Bandit** (Security Scanner)
- **Purpose**: Security vulnerability detection
- **Level**: Low and medium severity checks

### 6. **Pre-commit Hooks**
- **Purpose**: Run checks before commits
- **Configuration**: `.pre-commit-config.yaml`
- **Enforces**: Branch naming, commit messages, formatting, linting

## 📋 Quality Checks

### Pre-commit Checks
All checks run automatically before each commit:

1. **Branch Naming**: Must follow `feature/`, `fix/`, `docs/`, etc.
2. **Commit Messages**: Conventional Commits format
3. **Code Formatting**: Black + Ruff format
4. **Linting**: Ruff (replaces Flake8)
5. **Type Checking**: MyPy
6. **Docstrings**: Pydocstyle (Google convention)
7. **Security**: Bandit
8. **File Quality**: Trailing whitespace, EOF, YAML/JSON/TOML validation

### CI/CD Checks
GitHub Actions runs comprehensive checks on every PR:

1. **Linting & Formatting**: Ruff, Black
2. **Type Checking**: MyPy
3. **Testing**: Pytest with coverage
4. **Security**: Bandit, Ruff security checks
5. **Multi-version Testing**: Python 3.8, 3.9, 3.10, 3.11

## 📊 Coverage Requirements

- **Minimum Coverage**: 70%
- **Coverage Reports**: 
  - Terminal output
  - XML (for CI integration)
  - HTML (for detailed review)
- **Upload**: Codecov integration

## 🔄 Automated Dependency Management

### Dependabot
- **Schedule**: Weekly (Mondays at 9:00 AM)
- **Updates**: Production and development dependencies
- **Grouping**: Smart grouping of related updates
- **Labels**: Automatic labeling for easy tracking

## 📝 Commit Standards

### Conventional Commits
Format: `<type>(<scope>): <subject>`

**Types:**
- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation
- `style`: Code style (formatting)
- `refactor`: Code refactoring
- `test`: Tests
- `chore`: Maintenance

**Examples:**
```
feat(ingestion): add PostgreSQL handler
fix(datahub): handle connection timeout
docs(readme): update installation guide
```

## 🌿 Branch Standards

**Required prefixes:**
- `feature/` - New features
- `fix/` - Bug fixes
- `docs/` - Documentation
- `chore/` - Maintenance
- `refactor/` - Refactoring
- `test/` - Test-related

**Examples:**
- ✅ `feature/postgres-handler`
- ✅ `fix/connection-timeout`
- ❌ `my-branch`
- ❌ `update-code`

## 📁 Project Structure

### Configuration Files
- `pyproject.toml` - Centralized tool configuration
- `.pre-commit-config.yaml` - Pre-commit hooks
- `.github/workflows/` - CI/CD workflows
- `.github/dependabot.yml` - Dependency updates

### Documentation
- `CHANGELOG.md` - Version history
- `CONTRIBUTING.md` - Contribution guidelines
- `CODE_QUALITY_STANDARDS.md` - This file
- `SETUP_PRE_COMMIT.md` - Pre-commit setup guide

## 🚀 Getting Started

1. **Install dependencies:**
   ```bash
   pip install -r requirements-dev.txt
   ```

2. **Set up pre-commit:**
   ```bash
   pre-commit install
   ```

3. **Run checks manually:**
   ```bash
   pre-commit run --all-files
   ```

## 🔍 Comparison with Top Projects

Our standards match or exceed those used by:

- ✅ **FastAPI** - Ruff, Black, MyPy, Pytest
- ✅ **Pydantic** - Modern tooling, comprehensive checks
- ✅ **Django** - Pre-commit hooks, CI/CD
- ✅ **Requests** - Code quality, security scanning
- ✅ **Pandas** - Coverage thresholds, multi-version testing

## 📈 Continuous Improvement

We continuously update our standards to match industry best practices:
- Regular dependency updates via Dependabot
- Tool version updates in pre-commit
- CI/CD workflow improvements
- Coverage threshold adjustments

## 🤝 Contributing

All contributors must:
1. ✅ Follow branch naming conventions
2. ✅ Use Conventional Commits
3. ✅ Ensure all pre-commit checks pass
4. ✅ Maintain test coverage above 70%
5. ✅ Update documentation as needed

See [CONTRIBUTING.md](CONTRIBUTING.md) for detailed guidelines.

