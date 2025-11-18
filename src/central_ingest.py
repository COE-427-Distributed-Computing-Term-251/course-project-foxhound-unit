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

USE_INFLUXDB = True
try:
    from influxdb import InfluxDBClient, Point, WritePrecision
except ImportError:
    USE_INFLUXDB = False

DEDUP_DB = 'ingest_dedup.db'
FALLBACK_FILE = 'ingest_fallback.jsonl'

def init_dedup_db():
    """Initialize the deduplication SQLite database."""
    conn = sqlite3.connect(DEDUP_DB)
    cursor = conn.cursor()
    cursor.execute(""""
        CREATE TABLE IF NOT EXISTS seen (
            panel_id TEXT NOT NULL,
            ts TEXT NOT NULL,
            PRIMARY KEY (panel_id, ts)
        )
    """)
    conn.commit()
    conn.close()

def is_duplicate(panel_id, ts):
    """Check if the given panel_id and timestamp combination has been seen before."""
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
class CentralIngest:
    def __init__(self, mqtt_host="localhost", mqtt_port=1883):
        self.mqtt = mqtt.Client(client_id=f"central-{int(time.time())}")
        self.mqtt.on_connect = self._on_connect
        self.mqtt.on_message = self._on_message
        self.mqtt_host = mqtt_host
        self.mqtt_port = mqtt_port

        # Influx config from env
        self.influx_client = None
        self.write_api = None
        if USE_INFLUXDB and os.environ.get("INFLUX_URL"):
            try:
                self.influx_client = InfluxDBClient(url=os.environ["INFLUX_URL"],
                                                    token=os.environ.get("INFLUX_TOKEN", ""),
                                                    org=os.environ.get("INFLUX_ORG", "org"))
                self.write_api = self.influx_client.write_api()
                print("InfluxDB client configured")
            except Exception as e:
                print("InfluxDB init failed:", e)
                self.influx_client = None
        else:
            if not USE_INFLUXDB:
                print("influxdb_client not available; writing fallback JSONL")
            else:
                print("INFLUX_URL not set; writing fallback JSONL")
    def _on_connect(self, client, userdata, flags, rc):
        print("Central MQTT connected (rc=%s)" % rc)
        client.subscribe("panels/+/telemetry", qos=1)

    def _on_message(self, client, userdata, msg):
        """Handle incoming MQTT messages."""
        payload = msg.payload
        try:
            raw_text = gzip.decompress(payload).decode('utf-8')
        except Exception:
            raw_text = payload.decode('utf-8')
        
        for line in raw_text.strip().splitlines():
            try:
                obj = json.loads(line)
            except Exception:
                print("Skipping invalid JSON line", line)
                continue

            panel_id = obj.get("panel_id","unknown")
            ts = obj.get("timestamp_utc") or datetime.now(timezone.utc).isoformat()

            if is_duplicate(panel_id, ts):
                continue

            self._write(obj, ts)

    def _write(self, obj, ts):
        """write telemetry data to InfluxDB or fallback file."""
        # InfluxDB mode:
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
                    bucket=os.environ.get("INFLUX_BUCKET", "telemetry"),
                    org=os.environ.get("INFLUX_ORG", "org"),
                    record=point
                    )
                print(f"Wrote to InfluxDB: panel_id={obj['panel_id']} ts={ts}")
                return
            except Exception as e:
                print("InfluxDB write failed:", e)
                
        # Fallback to JSONL file
        try:
            with open(FALLBACK_FILE, 'a') as f:
                f.write(json.dumps(obj) + '\n')
            print(f"Wrote to fallback file: panel_id={obj['panel_id']} ts={ts}")
        except Exception as e:
            print("Fallback write failed:", e)