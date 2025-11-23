"""
Central Ingest Module

This module handles the central ingestion of data from various sources,
processes it, and stores it in the appropriate databases.

Functions:
- Subscribes to MQTT topics for incoming data.
- Decompresses gzip data sent from edge collectors.
- Parses JSON or CSV payloads.
- Deduplicates entries using (panel_id, timestamp_utc).
- Writes processed data to InfluxDB or to a fallback JSONL file if the DB is unavailable.

Usage:
    python central_ingest.py --mqtt-host localhost --mqtt-port 1883
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
    """Initialize the deduplication SQLite database and enable WAL."""
    conn = sqlite3.connect(DEDUP_DB, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS seen (
            panel_id TEXT NOT NULL,
            ts TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            PRIMARY KEY (panel_id, ts)
        )
    """)
    conn.commit()
    conn.close()

def is_duplicate(panel_id, ts):
    """Check if a record with (panel_id, ts) was already processed."""
    conn = sqlite3.connect(DEDUP_DB, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL;")
    cursor = conn.cursor()

    cursor.execute("SELECT 1 FROM seen WHERE panel_id = ? AND ts = ?", (panel_id, ts))
    result = cursor.fetchone()

    if result:
        conn.close()
        return True

    try:
        cursor.execute(
            "INSERT OR IGNORE INTO seen (panel_id, ts, created_at) VALUES (?, ?, ?)",
            (panel_id, ts, int(time.time()))
        )
        conn.commit()
    except Exception as e:
        print("⚠️ dedup insert failed:", e)
    finally:
        conn.close()
    return False

# ---------------------------------------------------------
# Main Ingest Class 
# ---------------------------------------------------------

class CentralIngest:
    def __init__(self, mqtt_host="localhost", mqtt_port=1883):
        self.mqtt = mqtt.Client(client_id="central-ingest", clean_session=False)
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
            # ----------------------------
            # JSON FIRST → then CSV fallback
            # ----------------------------
            try:
                obj = json.loads(line)
            except Exception:
                # --- CSV fallback - ---
                parts = line.split(",")
                
                # Skip header row if present
                if parts[0] == "timestamp_utc":
                    continue
                    
                if len(parts) == 13:
                    try:
                        obj = {
                            "timestamp_utc": parts[0],
                            "panel_id": parts[1],
                            "string_id": parts[2],           
                            "status": parts[3],
                            "fault": parts[4],               
                            "power_w": float(parts[5]),     
                            "voltage_v": float(parts[6]),     
                            "current_a": float(parts[7]),    
                            "irradiance_wm2": float(parts[8]),
                            "ambient_temp_c": float(parts[9]), 
                            "cell_temp_c": float(parts[10]),   
                            "orientation_deg": float(parts[11]), 
                            "tilt_deg": float(parts[12])       
                        }
                    except Exception as e:
                        print("Skipping invalid CSV:", line, "ERR:", e)
                        continue
                else:
                    print(f"Skipping invalid line (expected 13 parts, got {len(parts)}):", line)
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
        if self.write_api is not None:
            try:
                point = (
                    Point("panel_telemetry")
                    .tag("panel_id", obj["panel_id"])
                    .tag("string_id", obj.get("string_id", "unknown"))
                    .tag("status", obj.get("status", "UNKNOWN"))
                    .tag("fault", obj.get("fault", "NONE"))
                    .field("power_w", float(obj.get("power_w", 0)))
                    .field("voltage_v", float(obj.get("voltage_v", 0)))
                    .field("current_a", float(obj.get("current_a", 0)))
                    .field("irradiance_wm2", float(obj.get("irradiance_wm2", 0)))
                    .field("ambient_temp_c", float(obj.get("ambient_temp_c", 0)))
                    .field("cell_temp_c", float(obj.get("cell_temp_c", 0)))
                    .field("orientation_deg", float(obj.get("orientation_deg", 0)))
                    .field("tilt_deg", float(obj.get("tilt_deg", 0)))
                    .time(obj["timestamp_utc"])
                )

                self.write_api.write(
                    bucket=os.environ.get("INFLUX_BUCKET"),
                    org=os.environ.get("INFLUX_ORG"),
                    record=point
                )

                print(f"✔️ Wrote to InfluxDB: panel_id={obj['panel_id']} ts={ts}")
                return

            except Exception as e:
                print("❌ InfluxDB write failed:", e)

        # Fallback JSONL
        try:
            with open(FALLBACK_FILE, "a") as f:
                f.write(json.dumps(obj) + "\n")
            print(f"📄 Wrote to fallback file: panel_id={obj['panel_id']} ts={ts}")
        except Exception as e:
            print("❌ Fallback write failed:", e)

    # -----------------------------------------------------

    def run(self):
        """Start the MQTT client loop."""
        init_dedup_db()
        self.mqtt.connect(self.mqtt_host, self.mqtt_port, keepalive=60)
        self.mqtt.loop_start()
        print("Central Ingest service is running...")

        try:
            while True:
                time.sleep(1.0)
        except KeyboardInterrupt:
            print("Shutting down central ingest...")
            self.mqtt.loop_stop()
            self.mqtt.disconnect()

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