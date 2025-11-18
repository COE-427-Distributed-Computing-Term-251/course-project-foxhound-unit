"""
Central Ingest Module

This module handles the central ingestion of data from various sources,
processes it, and stores it in the appropriate databases.

Functions:
- Subscripes to MQTT topics for incoming data.
- decompresses gzip data sent from edge collectors.
- parse JSON payloads.
- deduplicarte entries based on unique identifiers (panel_id and timestamp_utc).
- write processed data to InfluxDB or fallback JSON file if DB is unavailable.
"""


import gzip
import json
import os
import sqlite3
import time
from datetime import datetime, timezone

import paho.mqtt.client as mqtt
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# --- Correct InfluxDB 2.x client ---
from influxdb_client import InfluxDBClient, Point, WritePrecision

# ---------------------------------------------------------
# Configuration
# ---------------------------------------------------------

DEDUP_DB = "ingest_dedup.db"
FALLBACK_FILE = "ingest_fallback.jsonl"


# ---------------------------------------------------------
# Deduplication DB
# ---------------------------------------------------------

def init_dedup_db():
    """Initialize the deduplication SQLite database."""
    conn = sqlite3.connect(DEDUP_DB)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS seen (
            panel_id TEXT NOT NULL,
            ts TEXT NOT NULL,
            PRIMARY KEY (panel_id, ts)
        )
    """)
    conn.commit()
    conn.close()


def is_duplicate(panel_id, ts):
    """Check if a record with (panel_id, ts) was already processed."""
    conn = sqlite3.connect(DEDUP_DB)
    cursor = conn.cursor()

    cursor.execute("SELECT 1 FROM seen WHERE panel_id = ? AND ts = ?", (panel_id, ts))
    result = cursor.fetchone()

    if result:
        conn.close()
        return True

    cursor.execute("INSERT OR IGNORE INTO seen (panel_id, ts) VALUES (?, ?)", (panel_id, ts))
    conn.commit()
    conn.close()
    return False


# ---------------------------------------------------------
# Main Ingest Class
# ---------------------------------------------------------

class CentralIngest:
    def __init__(self, mqtt_host="localhost", mqtt_port=1883):
        self.mqtt = mqtt.Client(client_id=f"central-{int(time.time())}")
        self.mqtt.on_connect = self._on_connect
        self.mqtt.on_message = self._on_message

        self.mqtt_host = mqtt_host
        self.mqtt_port = mqtt_port

        # -------------------------------------------------
        # Configure InfluxDB
        # -------------------------------------------------
        self.influx_client = None
        self.write_api = None

        influx_url = os.environ.get("INFLUX_URL")
        influx_token = os.environ.get("INFLUX_TOKEN")
        influx_org = os.environ.get("INFLUX_ORG")

        if influx_url and influx_token and influx_org:
            try:
                self.influx_client = InfluxDBClient(
                    url=influx_url,
                    token=influx_token,
                    org=influx_org,
                )
                self.write_api = self.influx_client.write_api()
                print("InfluxDB client configured ✔️")
            except Exception as e:
                print("⚠️ InfluxDB init failed:", e)
                self.influx_client = None
        else:
            print("⚠️ Missing INFLUX_* env vars — using fallback JSONL")

    # -----------------------------------------------------
    # MQTT Handlers
    # -----------------------------------------------------

    def _on_connect(self, client, userdata, flags, rc):
        print(f"Central MQTT connected (rc={rc})")
        client.subscribe("panels/+/telemetry", qos=1)

    def _on_message(self, client, userdata, msg):
        """Handle incoming MQTT messages."""
        payload = msg.payload
        try:
            raw_text = gzip.decompress(payload).decode("utf-8")
        except Exception:
            raw_text = payload.decode("utf-8")

        for line in raw_text.strip().splitlines():
            try:
                obj = json.loads(line)
            except Exception:
                print("Skipping invalid JSON:", line)
                continue

            panel_id = obj.get("panel_id", "unknown")
            ts = obj.get("timestamp_utc") or datetime.now(timezone.utc).isoformat()

            if is_duplicate(panel_id, ts):
                continue

            self._write(obj, ts)

    # -----------------------------------------------------
    # Write Data to Influx or Fallback
    # -----------------------------------------------------

    def _write(self, obj, ts):
        """Write telemetry data to InfluxDB or fallback file."""

        # Try Influx first
        if self.write_api is not None:
            try:
                point = (
                    Point("panel")
                    .tag("panel_id", obj["panel_id"])
                    .field("power", float(obj.get("power", 0)))
                    .field("voltage", float(obj.get("voltage", 0)))
                    .field("current", float(obj.get("current", 0)))
                    .field("temperature", float(obj.get("temperature", 0)))
                    .field("irradiance", float(obj.get("irradiance", 0)))
                    .field("status", str(obj.get("status", 0)))
                    .time(ts, WritePrecision.NS)
                )

                self.write_api.write(
                    bucket=os.environ.get("INFLUX_BUCKET"),
                    org=os.environ.get("INFLUX_ORG"),
                    record=point
                )

                print(f"✔️ Wrote to InfluxDB: panel_id={obj['panel_id']} ts={ts}")
                return

            except Exception as e:
                print(" InfluxDB write failed:", e)

        # Fallback JSONL
        try:
            with open(FALLBACK_FILE, "a") as f:
                f.write(json.dumps(obj) + "\n")
            print(f" Wrote to fallback file: panel_id={obj['panel_id']} ts={ts}")
        except Exception as e:
            print(" Fallback write failed:", e)

    # -----------------------------------------------------

    def run(self):
        """Start the MQTT client loop."""
        init_dedup_db()
        self.mqtt.connect(self.mqtt_host, self.mqtt_port, keepalive=60)
        print("Central Ingest service is running...")
        self.mqtt.loop_forever()


# ---------------------------------------------------------
# Entry Point
# ---------------------------------------------------------

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--mqtt-host", default="localhost")
    parser.add_argument("--mqtt-port", type=int, default=1883)
    args = parser.parse_args()

    ingest = CentralIngest(mqtt_host=args.mqtt_host, mqtt_port=args.mqtt_port)
    ingest.run()
