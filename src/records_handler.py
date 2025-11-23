#!/usr/bin/env python3
"""
records_handler.py

Ensures every telemetry record matches the 13-field FINAL schema.
Supports JSONL input or CSV input automatically.

If CSV: header must include all 13 required columns.

Returns: validated JSON string (one line) matching EXACT schema fields.
"""

import json
import csv
from io import StringIO

# --- FINAL expected schema (order preserved) ---
REQUIRED_FIELDS = [
    "timestamp_utc",
    "panel_id",
    "string_id",
    "status",
    "fault",
    "power_w",
    "voltage_v",
    "current_a",
    "irradiance_wm2",
    "ambient_temp_c",
    "cell_temp_c",
    "orientation_deg",
    "tilt_deg",
]


def is_csv_line(line: str) -> bool:
    return "," in line and not line.strip().startswith("{") and not line.strip().startswith("[")


def parse_csv_line(line: str, header: list) -> dict:
    """Parse CSV line using previously detected header."""
    reader = csv.DictReader([line], fieldnames=header)
    row = next(reader)

    # Convert numeric fields
    for k in row:
        if k not in REQUIRED_FIELDS:
            continue

        if k in [
            "power_w", "voltage_v", "current_a",
            "irradiance_wm2", "ambient_temp_c",
            "cell_temp_c", "orientation_deg", "tilt_deg"
        ]:
            try:
                row[k] = float(row[k])
            except:
                row[k] = None

    return row


def validate_and_fix(record: dict) -> dict:
    """
    Ensures all required fields exist.
    No transformations except casting numeric fields.
    """
    cleaned = {}

    for field in REQUIRED_FIELDS:
        if field not in record:
            raise ValueError(f"Missing required field: {field}")

        value = record[field]

        # Cast numeric fields
        if field in [
            "power_w", "voltage_v", "current_a",
            "irradiance_wm2", "ambient_temp_c",
            "cell_temp_c", "orientation_deg", "tilt_deg"
        ]:
            try:
                value = float(value)
            except:
                value = None

        cleaned[field] = value

    return cleaned


class RecordsHandler:
    """
    Handles normalization + schema enforcement for incoming lines.
    Stores CSV header if detected.
    """

    def __init__(self):
        self.csv_header = None

    def process_line(self, line: str) -> str:
        """
        Takes a raw JSONL string OR CSV line.
        Returns validated **JSON string** with correct schema.
        """

        line = line.strip()
        if not line:
            return None

        # ---------------------
        # JSONL record
        # ---------------------
        if line.startswith("{"):
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                raise ValueError("Invalid JSON record")

            cleaned = validate_and_fix(obj)
            return json.dumps(cleaned, separators=(",", ":"))

        # ---------------------
        # CSV record
        # ---------------------
        if is_csv_line(line):
            if self.csv_header is None:
                # first line must be header
                header = [h.strip() for h in line.split(",")]
                missing = [f for f in REQUIRED_FIELDS if f not in header]
                if missing:
                    raise ValueError(f"CSV missing required columns: {missing}")

                self.csv_header = header
                return None  # header line ignored

            # Parse data row
            obj = parse_csv_line(line, self.csv_header)
            cleaned = validate_and_fix(obj)
            return json.dumps(cleaned, separators=(",", ":"))

        raise ValueError("Unknown record format (not JSONL or CSV)")
