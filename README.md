# Course Project — Foxhound Unit

![System Diagram](diagram.jpeg)

Lightweight toolkit to generate per-panel solar telemetry, ingest it, and run basic validation/processing. This repository includes a telemetry generator, ingest utilities, a simple edge collector server and handlers, and a small set of tests.

Important: The repository is licensed under the PolyForm Noncommercial 1.0.0 license. Code and data are free for academic, research, and personal (noncommercial) use. Commercial/production usage requires a separate agreement.

---

## Contents

- src/solar_panel_telemetry.py — telemetry generator (CSV/JSONL)
- src/edge_collector_server.py — collector
- src/central_ingest.py — MQTT subscriber / ingest
- src/records_handler.py — validation utilities (importable module)
- src/ingest_fallback.jsonl — sample input
- data/ — sample data
- tests/ — pytest tests
- requirements.txt — dependencies
- LICENSE — PolyForm Noncommercial 1.0.0
- THIRD_PARTY_NOTICES.md — third-party notices

## Requirements

- Python 3.8+
- Install dependencies: pip install -r requirements.txt

## Installation

Option A — venv (Windows/macOS/Linux)

- Windows (PowerShell)
  ```powershell
  cd c:\Users\aliob\OneDrive\Desktop\webs\course-project-foxhound-unit-master
  python -m venv .venv
  .\.venv\Scripts\Activate.ps1
  python -m pip install --upgrade pip
  pip install -r requirements.txt
  ```
- macOS / Linux
  ```bash
  cd /path/to/course-project-foxhound-unit-master
  python3 -m venv .venv
  source .venv/bin/activate
  python -m pip install --upgrade pip
  pip install -r requirements.txt
  ```

Option B — conda

```bash
conda create -n foxhound python=3.8
conda activate foxhound
pip install -r requirements.txt
```

## Phase Two — Running Backend & Frontend

### Backend (InfluxDB API)

To run the backend API in Phase Two, navigate to the `web` directory and start the InfluxDB API service:

If you are on any system (Windows / macOS / Linux):
```bash
cd web
python influx_api.py
```

If you are on Windows:
```bash
cd web
py .\influx_api.py
```

### Frontend (Dashboard)

To run the frontend dashboard, navigate to the web directory and open the dashboard file:

```bash
cd web
start dashboard.html
```

## Usage (examples)

Note: Run any script with `--help` to confirm flags.

1. Generate telemetry (CSV)

```bash
python src/solar_panel_telemetry.py --panels 100 --start "2025-10-18T06:00:00" --hours 12 --step 60 --out telemetry.csv
```

2. Generate JSONL (stdout)

```bash
python src/solar_panel_telemetry.py --panels 10 --hours 1 --step 30 --format jsonl
```

3. Start edge collector (example flags)

```bash
python src/edge_collector_server.py --mqtt-host localhost --mqtt-port 1883 --site-id site-01 --port 9000
```

4. Start central ingest (MQTT subscriber)

```bash
python src/central_ingest.py --mqtt-host localhost --mqtt-port 1883
```

## Tests

```bash
pip install pytest
pytest
```

## License

PolyForm Noncommercial 1.0.0 — see LICENSE.
