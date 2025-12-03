# Contributing to Lumos Framework

First off, thank you for considering contributing to Lumos Framework! It's people like you that make this framework a great tool for the data engineering community.

## Code of Conduct

This project and everyone participating in it is governed by our [Code of Conduct](CODE_OF_CONDUCT.md). By participating, you are expected to uphold this code.

## How Can I Contribute?

### Reporting Bugs

Before creating bug reports, please check the existing issues to avoid duplicates. When you create a bug report, include as many details as possible:

* **Use a clear and descriptive title**
* **Describe the exact steps to reproduce the problem**
* **Provide specific examples to demonstrate the steps**
* **Describe the behavior you observed and what you expected**
* **Include logs and error messages**
* **Specify your environment** (OS, Python version, package versions)

### Suggesting Enhancements

Enhancement suggestions are tracked as GitHub issues. When creating an enhancement suggestion, include:

* **Use a clear and descriptive title**
* **Provide a detailed description of the suggested enhancement**
* **Explain why this enhancement would be useful**
* **List examples of where this enhancement would apply**

### Pull Requests

* Fill in the required template
* Follow the Python style guide (PEP 8)
* Include appropriate test cases
* Update documentation as needed
* End all files with a newline
* Ensure all tests pass before submitting

## Development Setup

1. **Fork and clone the repository**
   ```bash
   git clone https://github.com/yourusername/lumos-framework.git
   cd lumos-framework
   ```

2. **Create a virtual environment**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install development dependencies**
   ```bash
   pip install -r requirements.txt
   pip install -r requirements-dev.txt
   ```

4. **Create a branch for your changes**
   ```bash
   git checkout -b feature/your-feature-name
   ```

## Coding Standards

### Python Style Guide

* Follow [PEP 8](https://www.python.org/dev/peps/pep-0008/)
* Use meaningful variable and function names
* Write docstrings for all public modules, functions, classes, and methods
* Keep functions focused and single-purpose
* Maximum line length: 100 characters

### Docstring Format

Use Google-style docstrings:

```python
def example_function(param1: str, param2: int) -> bool:
    """Brief description of the function.
    
    More detailed description if needed.
    
    Args:
        param1: Description of param1
        param2: Description of param2
        
    Returns:
        Description of return value
        
    Raises:
        ValueError: Description of when this is raised
    """
    pass
```

### Type Hints

* Use type hints for function parameters and return values
* Import from `typing` module when needed

### Testing

* Write unit tests for all new functionality
* Maintain or improve code coverage
* Run tests before submitting PR:
  ```bash
  pytest tests/
  ```

### Code Formatting

* Use `black` for code formatting:
  ```bash
  black .
  ```

* Use `isort` for import sorting:
  ```bash
  isort .
  ```

* Use `mypy` for type checking:
  ```bash
  mypy core_library/ platform_services/
  ```

## Adding New Features

### Adding a New Data Source Handler

1. Create a new handler in `core_library/ingestion_handlers/`
2. Inherit from `BaseIngestionHandler`
3. Implement required methods
4. Register in `IngestionService._handler_registry`
5. Add configuration template in `sample_configs_and_templates/ingestion/`
6. Write tests
7. Update documentation

Example:
```python
from .base_ingestion_handler import BaseIngestionHandler

class MyNewHandler(BaseIngestionHandler):
    """Handler for MyNew data source."""
    
    def __init__(self, config: Dict[str, Any], platform_handler):
        super().__init__(config, platform_handler)
        self.required_fields.extend(["my_field"])
        
    def ingest(self) -> None:
        """Ingests metadata from MyNew source."""
        # Implementation here
        pass
```

### Adding a New Platform

1. Create handler in `platform_services/`
2. Inherit from `MetadataPlatformInterface`
3. Implement all abstract methods
4. Register in `PlatformFactory._handler_registry`
5. Update global_settings.yaml template
6. Write tests
7. Update documentation

## Commit Message Guidelines

Follow conventional commits:

* `feat:` New feature
* `fix:` Bug fix
* `docs:` Documentation changes
* `style:` Code style changes (formatting, etc.)
* `refactor:` Code refactoring
* `test:` Adding or updating tests
* `chore:` Maintenance tasks

Examples:
```
feat: add PostgreSQL ingestion handler
fix: resolve connection timeout in MongoDB handler
docs: update installation instructions
```

## Project Structure

```
lumos-framework/
├── core_library/           # Core framework components
│   ├── common/            # Shared utilities
│   ├── ingestion_handlers/  # Data source handlers
│   ├── enrichment_services/ # Metadata enrichment
│   ├── lineage_services/   # Lineage tracking
│   ├── dq_services/       # Data quality
│   ├── profiling_services/ # Data profiling
│   └── rbac_services/     # Access control
├── platform_services/     # Platform implementations
├── configs/              # Configuration files
├── sample_configs_and_templates/  # Example configs
├── orchestration_examples/  # Orchestration examples
├── tests/               # Test suite
└── docs/                # Documentation
```

## Need Help?

* Check existing issues and documentation
* Ask questions in GitHub Discussions
* Join our community chat (if available)

## Recognition

Contributors will be recognized in:
* CHANGELOG.md for significant contributions
* README.md contributors section
* Release notes

Thank you for contributing to Lumos Framework! 🌟

