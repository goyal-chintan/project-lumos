# Contributing to Lumos Framework

Thank you for your interest in contributing to Lumos Framework! This document provides guidelines and instructions for contributing.

## Code of Conduct

This project adheres to a Code of Conduct that all contributors are expected to follow. Please be respectful and constructive in all interactions.

## How to Contribute

### Reporting Bugs

1. Check if the issue already exists in the GitHub Issues
2. If not, create a new issue with:
   - Clear title and description
   - Steps to reproduce
   - Expected vs actual behavior
   - Environment details (Python version, OS, etc.)
   - Relevant logs or error messages

### Suggesting Features

1. Check if the feature request already exists
2. Create a new issue with:
   - Clear description of the feature
   - Use case and motivation
   - Proposed implementation approach (if you have one)

### Contributing Code

#### Development Setup

1. **Fork the repository**
   ```bash
   git clone https://github.com/your-username/project-lumos.git
   cd project-lumos
   ```

2. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Set up development environment**
   ```bash
   # Install development dependencies
   pip install pytest black mypy flake8
   
   # Set up DataHub (optional, for integration tests)
   # See README.md for DataHub setup instructions
   ```

4. **Create a feature branch**
   ```bash
   git checkout -b feature/your-feature-name
   # or
   git checkout -b fix/your-bug-fix
   # or
   git checkout -b docs/your-documentation-update
   ```

#### Development Guidelines

1. **Follow the project structure**
   - Core platform code: `core/`
   - Feature implementations: `feature/`
   - Controllers: `core/controllers/`
   - Platform implementations: `core/platform/impl/`

2. **Follow SOLID principles**
   - Single Responsibility Principle
   - Open/Closed Principle
   - Liskov Substitution Principle
   - Interface Segregation Principle
   - Dependency Inversion Principle

3. **Code style**
   - Use `black` for code formatting
   - Follow PEP 8 style guide
   - Use type hints where appropriate
   - Write docstrings for all public functions and classes

4. **Testing**
   - Write unit tests for new features
   - Write integration tests for platform interactions
   - Ensure all tests pass before submitting PR
   ```bash
   pytest
   ```

5. **Documentation**
   - Update relevant documentation files
   - Add docstrings to new functions/classes
   - Update README.md if adding new features
   - Update architecture docs if changing structure

#### Commit Messages

Follow [Conventional Commits](https://www.conventionalcommits.org/) format:

```
<type>(<scope>): <subject>

[optional body]

[optional footer]
```

**Types:**
- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation changes
- `style`: Code style (formatting, etc.)
- `refactor`: Code refactoring
- `test`: Adding/updating tests
- `chore`: Maintenance tasks

**Examples:**
```
feat(ingestion): add PostgreSQL handler
fix(datahub): handle connection timeout errors
docs(architecture): update extraction services documentation
```

#### Pull Request Process

1. **Update your branch**
   ```bash
   git checkout main
   git pull origin main
   git checkout your-branch
   git rebase main
   ```

2. **Ensure code quality**
   ```bash
   # Format code
   black .
   
   # Type checking
   mypy .
   
   # Run tests
   pytest
   ```

3. **Create Pull Request**
   - Provide clear title and description
   - Reference related issues
   - Describe changes and testing done
   - Add screenshots/examples if applicable

4. **PR Review**
   - Address review comments
   - Keep PR focused and reasonably sized
   - Update documentation as needed

## Adding New Features

### Adding a New Data Source Handler

1. Create handler in `feature/ingestion/handlers/`
2. Extend `BaseIngestionHandler`
3. Implement `ingest()` method
4. Register in `feature/ingestion/handlers/factory.py`
5. Add constants in `feature/ingestion/handlers/constants.py`
6. Create sample config template
7. Add tests

### Adding a New Platform

1. Create handler in `core/platform/impl/`
2. Implement `MetadataPlatformInterface`
3. Register in `core/platform/factory.py`
4. Add config template
5. Add tests

### Adding a New Extraction Service

1. Create extractor in `feature/extraction/`
2. Extend `BaseExtractionService`
3. Implement extraction logic
4. Register in `feature/extraction/extraction_factory.py`
5. Add tests

## Testing

### Running Tests

```bash
# All tests
pytest

# Unit tests only
pytest tests/unit/

# Integration tests (requires DataHub)
export DATAHUB_GMS=http://localhost:8080
pytest tests/integration/

# Specific test file
pytest tests/unit/test_handlers_csv.py
```

### Writing Tests

- Place unit tests in `tests/unit/`
- Place integration tests in `tests/integration/`
- Use descriptive test names
- Test both success and failure cases
- Mock external dependencies for unit tests

## Code Review Guidelines

- Be constructive and respectful
- Focus on code quality and maintainability
- Ask questions if something is unclear
- Suggest improvements, don't just point out issues
- Approve when satisfied with changes

## Questions?

- Check the [README.md](README.md) for general information
- Review [Architecture Documentation](docs/ARCHITECTURE.md) for design details
- Open an issue for questions or discussions

Thank you for contributing to Lumos Framework! 🚀

