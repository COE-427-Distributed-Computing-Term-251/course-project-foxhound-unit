# Foxhound Unit — Solar Telemetry Toolkit

A small, lightweight toolkit to simulate per-panel solar telemetry, collect it at the edge, and run simple validation and ingest workflows. This project is intended as a teaching/demo tool and a starting point for building more capable telemetry pipelines.

![System Diagram](diagram.jpeg)

Summary
- Generate realistic synthetic telemetry for solar panels (CSV or JSONL).
- Run a minimal edge collector to accept telemetry from devices.
- Use a central ingest script to validate and process telemetry files or streams.
- Includes small sample data, unit tests, and utilities to validate records.

Important: This repository is licensed under the PolyForm Noncommercial 1.0.0 license — free for academic, research, and personal (noncommercial) use. Commercial or production usage requires a different license; please review the LICENSE file.

What’s in this repo
- src/solar_panel_telemetry.py — Telemetry generator (CSV or JSONL). See the script docstring and --help for examples.
- src/edge_collector_server.py — Minimal edge-level HTTP collector for incoming telemetry.
- src/central_ingest.py — Ingest and processing script (reads files or stdin depending on options).
- src/records_handler.py — Utilities for validating and transforming telemetry records.
- src/ingest_fallback.jsonl — Sample fallback input that the ingest pipeline can use for testing.
- data/ — Example datasets and instructions (small, safe samples).
- tests/ — Unit tests (pytest).
- requirements.txt — Python dependencies.
- LICENSE — PolyForm Noncommercial 1.0.0 license text.
- THIRD_PARTY_NOTICES.md — Third-party notices and license attributions.

Quick start (local, simple)
1. Create an environment and install dependencies (example using venv):
   - Windows (PowerShell)
     ```powershell
     python -m venv .venv
     .\.venv\Scripts\Activate.ps1
     python -m pip install --upgrade pip
     pip install -r requirements.txt
     ```
   - macOS / Linux
     ```bash
     python -m venv .venv
     source .venv/bin/activate
     pip install -r requirements.txt
     ```

2. Generate example telemetry (CSV) for 100 panels over 12 hours at 60s intervals:
   ```bash
   python src/solar_panel_telemetry.py --panels 100 --start "2025-10-18T06:00:00" --hours 12 --step 60 --out telemetry.csv
   ```

3. Run the edge collector:
   ```bash
   python src/edge_collector_server.py
   ```

4. Run central ingest on the generated file:
   ```bash
   python src/central_ingest.py --source telemetry.csv
   ```

Key usage notes
- Each script supports a `--help` flag. Consult it for the most up-to-date options.
- The generator supports CSV and JSONL formats and has a `--seed` option for deterministic output.
- The ingest script can read from files or stdin depending on command-line options.
- Use `src/records_handler.py` to validate or transform records:
  ```bash
  python src/records_handler.py --input telemetry.csv --action validate
  ```

Why this project exists
- Teaching/demo: show how telemetry can be produced, transported, and ingested in a small system.
- Testing: generate deterministic, varied telemetry for unit tests and pipeline development.
- Prototype: a small base you can extend (e.g., add streaming, persistent storage, or stricter schemas).

Reproducibility & determinism
- Use the `--seed` flag with the telemetry generator to get deterministic telemetry.
- For full reproducibility of test runs, record the Python version and pinned dependencies:
  ```bash
  python -V
  pip freeze > reproducible-requirements.txt
  ```
- Note: exact thread scheduling or timing-dependent behavior may vary across platforms; achieving bit-for-bit identical concurrency requires additional synchronization code.

Testing & CI
- Tests live in the `tests/` folder and use pytest.
  ```bash
  pip install pytest
  pytest
  ```
- If you add functionality, include tests and update CI to keep builds green.

Data & models
- Do not commit large or sensitive datasets.
- Keep small sample data in `data/`.
- For large datasets, provide download scripts and document data sources and licenses.

Secrets & credentials
- Never commit secrets. Use .env files that are gitignored, or protect secrets via CI secret stores.
- If your local config files are used, add them to `.gitignore`.

Contributing
- Fork the repo, create a branch, and open a PR with tests and documentation.
- Describe your changes and add a minimal example showing how to reproduce behavior.
- If you want me to open a PR with this README rewrite, tell me the target branch and commit message.

Roadmap ideas
- Add HTTP/gRPC streaming ingestion.
- Provide a Docker Compose setup for local end-to-end testing.
- Add stricter record schemas (e.g., JSON Schema) and more validation rules.
- Implement persistent storage or a simple time-series database sink.

Contact / questions
- If you want help extending this project, adding CI, or packaging it in Docker, say what you’d like and I’ll propose changes or open a PR.

License
- PolyForm Noncommercial 1.0.0 — see LICENSE for details.
