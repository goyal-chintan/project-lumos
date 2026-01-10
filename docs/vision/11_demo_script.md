## Demo Script (10 minutes, CTO-friendly)

### Demo goal
Show that Lumos:
- automates metadata creation and updates
- can produce a bird's-eye report
- can version and diff metadata (and later gate changes in CI)

### Prerequisites
- DataHub running locally (or in a test env)
- Lumos installed and configs set in `configs/global_settings.yaml`

### Part 1: Show ingestion automation (existing today)
Narration:
"We can ingest metadata from raw sources without asking engineers to manually register assets."

Commands:
- Ingest sample data (existing CLI):
  - `python framework_cli.py ingest:sample_configs_and_templates/ingestion/test_ingestion.json`

Expected output:
- New datasets appear in the catalog.

### Part 2: Show bird's-eye reporting (existing today)
Narration:
"We can extract organization-wide metadata into exec-friendly formats."

Commands:
- Comprehensive extraction:
  - `python framework_cli.py extract:excel-comprehensive`
- Governance extraction:
  - `python framework_cli.py extract:excel-governance`

Expected output:
- A report file in `extracted_data/` showing inventory, ownership, governance signals.

### Part 3: Show metadata versioning (existing today, basic)
Narration:
"We can track dataset version metadata in the catalog and update it centrally."

Commands:
- Version update:
  - `python framework_cli.py version-update`

Expected output:
- Dataset properties updated with version mapping.

### Part 4: Show snapshot + diff + CI gate (planned)
Narration:
"Next we add the key missing piece: portable snapshots and real diffs, so we can enforce safe schema evolution in CI and eliminate vendor lock-in risk."

Planned commands (Phase A/C):
- `python framework_cli.py snapshot:export --datasets all --out snapshots/run_001`
- `python framework_cli.py snapshot:diff --from snapshots/run_000 --to snapshots/run_001`
- `python framework_cli.py version:compute --diff snapshots/diff_001.json --apply`

CI story (Jenkins):
- On pipeline change:
  - export snapshot
  - diff vs baseline
  - fail build if breaking changes detected unless approved


