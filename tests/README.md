# Lumos Framework Tests

This directory contains the test suite for the Lumos Framework.

## Structure

```
tests/
├── unit/                    # Unit tests
│   ├── core_library/       # Tests for core library components
│   └── platform_services/  # Tests for platform services
├── integration/            # Integration tests
│   ├── core_library/       # Integration tests for core components
│   └── platform_services/  # Integration tests for platforms
├── test_data/             # Test data files
├── conftest.py            # Pytest fixtures and configuration
└── README.md              # This file
```

## Running Tests

### Run all tests
```bash
pytest
```

### Run with coverage
```bash
pytest --cov=core_library --cov=platform_services --cov-report=html
```

### Run specific test file
```bash
pytest tests/unit/core_library/test_config_manager.py
```

### Run specific test class
```bash
pytest tests/unit/core_library/test_config_manager.py::TestConfigManager
```

### Run specific test method
```bash
pytest tests/unit/core_library/test_config_manager.py::TestConfigManager::test_init_default_config_dir
```

### Run tests with verbose output
```bash
pytest -v
```

### Run tests and stop on first failure
```bash
pytest -x
```

## Writing Tests

### Unit Tests

Unit tests should:
- Test individual components in isolation
- Use mocks for external dependencies
- Be fast and independent
- Focus on a single behavior

Example:
```python
def test_config_manager_loads_config():
    config_manager = ConfigManager()
    config = config_manager.load_config("test.yaml")
    assert config is not None
```

### Integration Tests

Integration tests should:
- Test component interactions
- Use real implementations where possible
- Test end-to-end workflows
- May be slower than unit tests

Example:
```python
def test_csv_ingestion_to_datahub():
    # Setup
    config = load_test_config()
    platform = PlatformFactory.get_instance("datahub", config)
    handler = CSVIngestionHandler(config, platform)
    
    # Execute
    handler.ingest()
    
    # Verify
    # Check that metadata was sent to DataHub
```

## Test Fixtures

Common fixtures are defined in `conftest.py`:

- `test_data_dir`: Path to test data directory
- `sample_csv_config`: Sample CSV ingestion config
- `sample_mongo_config`: Sample MongoDB ingestion config
- `mock_datahub_config`: Mock DataHub configuration
- `sample_lineage_config`: Sample lineage configuration

## Test Data

Test data files should be placed in `tests/test_data/` directory.

## Coverage

Aim for at least 80% code coverage for:
- Core library components
- Platform services
- Critical business logic

To view coverage report:
```bash
pytest --cov=core_library --cov=platform_services --cov-report=html
open htmlcov/index.html
```

## Continuous Integration

Tests are automatically run in CI on:
- Every push to main/develop branches
- Every pull request
- Python versions: 3.8, 3.9, 3.10, 3.11

## Best Practices

1. **Naming**: Test files should start with `test_`
2. **Organization**: Group related tests in classes
3. **Isolation**: Each test should be independent
4. **Clarity**: Test names should describe what is being tested
5. **Assertions**: Use specific assertions (not just `assert True`)
6. **Mocking**: Mock external services and I/O operations
7. **Cleanup**: Use fixtures for setup and teardown
8. **Documentation**: Add docstrings to test classes and methods

