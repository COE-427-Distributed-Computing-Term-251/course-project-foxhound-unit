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

## Usage (examples)

Note: Run any script with `--help` to confirm flags.

1) Generate telemetry (CSV)
```bash
python src/solar_panel_telemetry.py --panels 100 --start "2025-10-18T06:00:00" --hours 12 --step 60 --out telemetry.csv
```

2) Generate JSONL (stdout)
```bash
python src/solar_panel_telemetry.py --panels 10 --hours 1 --step 30 --format jsonl
```

3) Start edge collector (example flags)
```bash
python src/edge_collector_server.py --mqtt-host localhost --mqtt-port 1883 --site-id site-01 --port 9000
```

4) Start central ingest (MQTT subscriber)
```bash
python src/central_ingest.py --mqtt-host localhost --mqtt-port 1883
```

5) Use records_handler from Python (import)
```bash
python - <<'PY'
from src.records_handler import RecordsHandler
rh = RecordsHandler()
print(rh.process_line('{"timestamp": "..."}'))
PY
```

## Tests
```bash
pip install pytest
pytest
```

## License
PolyForm Noncommercial 1.0.0 — see LICENSE.
