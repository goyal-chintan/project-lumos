## Test suite

This repository uses a **`test/`** directory layout (requested) with subfolders that mirror the codebase.

### Structure

- **`test/core/`**: unit tests targeting `core/`
- **`test/feature/`**: unit tests targeting `feature/`
- **`test/radn/`**: lightweight smoke / R&D tests (imports, quick sanity checks)

### Running tests (uses `.venv`)

From repo root:

```bash
./test/run_tests.sh
```

Or directly:

```bash
./.venv/bin/python -m pytest -c test/pytest.ini test
```


