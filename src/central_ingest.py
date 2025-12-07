#!/usr/bin/env python3
"""
central_ingest.py

Central ingestion service:
- Subscribes to MQTT topics published by edge collectors
- Decompresses gzip payloads
- Parses JSON/CSV lines
- Deduplicates by (panel_id, timestamp_utc)
- Writes to InfluxDB when available
- On Influx failures: writes to fallback JSONL AND pushes to an SQLite outbox for retry
- On startup: replays fallback file into outbox (so data gets retried)

Usage:
    python3 central_ingest.py --mqtt-host localhost --mqtt-port 1883
"""
import gzip
import json
import os
import sqlite3
import time
import threading
from datetime import datetime, timezone
from typing import Optional, Tuple

import paho.mqtt.client as mqtt
from dotenv import load_dotenv

load_dotenv()

# Optional Influx imports (if available)
try:
    from influxdb_client import InfluxDBClient, Point, WriteOptions
except Exception:
    InfluxDBClient = None
    Point = None
    WriteOptions = None

# ---------------------------
# Files & DB names
# ---------------------------
DEDUP_DB = "ingest_dedup.db"
FALLBACK_FILE = "ingest_fallback.jsonl"      # human-readable fallback
OUTBOX_DB = "ingest_outbox.db"               # sqlite outbox for retries

# ---------------------------
# Outbox helpers (SQLite)
# ---------------------------
def init_outbox_db():
    """Create outbox table for failed writes and enable WAL."""
    conn = sqlite3.connect(OUTBOX_DB, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    c = conn.cursor()
    c.execute("""
    CREATE TABLE IF NOT EXISTS outbox (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        payload TEXT NOT NULL,        -- JSON string of the record to write
        created_at INTEGER NOT NULL,
        attempts INTEGER DEFAULT 0,
        last_attempt INTEGER
    )
    """)
    conn.commit()
    conn.close()

def push_outbox(payload_obj: dict):
    """Append a record to outbox (as JSON string)."""
    try:
        conn = sqlite3.connect(OUTBOX_DB, timeout=30)
        conn.execute("PRAGMA journal_mode=WAL;")
        c = conn.cursor()
        c.execute(
            "INSERT INTO outbox (payload, created_at, attempts) VALUES (?, ?, 0)",
            (json.dumps(payload_obj), int(time.time()))
        )
        conn.commit()
        conn.close()
    except Exception as e:
        print("❌ Failed to push to outbox:", e)

def pop_outbox() -> Tuple[Optional[int], Optional[dict]]:
    """Peek the oldest outbox row without deleting. Returns (id, payload_obj) or (None, None)."""
    conn = sqlite3.connect(OUTBOX_DB, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL;")
    c = conn.cursor()
    c.execute("SELECT id, payload FROM outbox ORDER BY id LIMIT 1")
    row = c.fetchone()
    conn.close()
    if row:
        try:
            return row[0], json.loads(row[1])
        except Exception:
            # payload corrupted — remove it to avoid blocking
            try:
                conn = sqlite3.connect(OUTBOX_DB, timeout=30)
                conn.execute("PRAGMA journal_mode=WAL;")
                c = conn.cursor()
                c.execute("DELETE FROM outbox WHERE id=?", (row[0],))
                conn.commit()
                conn.close()
            except Exception:
                pass
            return None, None
    return None, None

def mark_outbox_attempt(id_: int, success: bool = False):
    """If success: delete row. Else: increment attempts and set last_attempt."""
    conn = sqlite3.connect(OUTBOX_DB, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL;")
    c = conn.cursor()
    if success:
        c.execute("DELETE FROM outbox WHERE id=?", (id_,))
    else:
        now = int(time.time())
        c.execute("UPDATE outbox SET attempts = attempts + 1, last_attempt = ? WHERE id=?", (now, id_))
    conn.commit()
    conn.close()

# ---------------------------
# Dedup DB
# ---------------------------
def init_dedup_db():
    """Initialize deduplication DB (seen table)."""
    conn = sqlite3.connect(DEDUP_DB, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS seen (
            panel_id TEXT NOT NULL,
            ts TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            PRIMARY KEY (panel_id, ts)
        )
    """)
    conn.commit()
    conn.close()

def is_duplicate(panel_id: str, ts: str) -> bool:
    """Return True if (panel_id, ts) was seen before; otherwise record and return False."""
    conn = sqlite3.connect(DEDUP_DB, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL;")
    c = conn.cursor()
    c.execute("SELECT 1 FROM seen WHERE panel_id=? AND ts=?", (panel_id, ts))
    if c.fetchone():
        conn.close()
        return True
    try:
        c.execute("INSERT OR IGNORE INTO seen (panel_id, ts, created_at) VALUES (?, ?, ?)",
                  (panel_id, ts, int(time.time())))
        conn.commit()
    except Exception as e:
        print("⚠️ dedup insert failed:", e)
    finally:
        conn.close()
    return False

# ---------------------------
# Central Ingest Class
# ---------------------------
class CentralIngest:
    def __init__(self, mqtt_host="localhost", mqtt_port=1883):
        self.mqtt_host = mqtt_host
        self.mqtt_port = mqtt_port

        # MQTT client with persistent session
        self.mqtt = mqtt.Client(client_id="central-ingest", clean_session=False)
        self.mqtt.on_connect = self._on_connect
        self.mqtt.on_message = self._on_message
        self.mqtt.on_disconnect = self._on_disconnect

        # Influx client (optional)
        self.influx_client = None
        self.write_api = None
        self._configure_influx()

        # Control
        self.stop_event = threading.Event()

        # Start background thread for outbox replay
        self.outbox_thread = threading.Thread(target=self.send_outbox_loop, daemon=True)

    def _configure_influx(self):
        influx_url = os.environ.get("INFLUX_URL")
        influx_token = os.environ.get("INFLUX_TOKEN")
        influx_org = os.environ.get("INFLUX_ORG")
        influx_bucket = os.environ.get("INFLUX_BUCKET")

        self.influx_bucket = influx_bucket

        if influx_url and influx_token and influx_org and InfluxDBClient is not None and Point is not None:
            try:
                self.influx_client = InfluxDBClient(url=influx_url, token=influx_token, org=influx_org)
                # choose synchronous write_api (you can configure batch settings here)
                self.write_api = self.influx_client.write_api()
                print("InfluxDB client configured ✔️")
            except Exception as e:
                print("⚠️ InfluxDB init failed:", e)
                self.influx_client = None
                self.write_api = None
        else:
            if InfluxDBClient is None or Point is None:
                print("⚠️ InfluxDB client not installed — will use fallback/outbox.")
            else:
                print("⚠️ Missing INFLUX_* env vars — will use fallback/outbox.")

    # -------------------------
    # MQTT handlers
    # -------------------------
    def _on_connect(self, client, userdata, flags, rc):
        print(f"Central MQTT connected (rc={rc})")
        client.subscribe("panels/+/telemetry", qos=1)

    def _on_disconnect(self, client, userdata, rc):
        print(f"Central MQTT disconnected (rc={rc})")

    def _on_message(self, client, userdata, msg):
        payload = msg.payload
        try:
            raw_text = gzip.decompress(payload).decode("utf-8")
        except Exception:
            raw_text = payload.decode("utf-8", errors="ignore")

        for line in raw_text.strip().splitlines():
            obj = self._parse_line(line)
            if obj is None:
                continue

            panel_id = obj.get("panel_id", "unknown")
            ts = obj.get("timestamp_utc") or datetime.now(timezone.utc).isoformat()

            # Deduplicate; if duplicate skip
            if is_duplicate(panel_id, ts):
                continue

            # Try to write; on failure push to outbox + fallback file
            self._attempt_write_or_queue(obj, ts)

    # -------------------------
    # Parsing helpers
    # -------------------------
    def _parse_line(self, line: str) -> Optional[dict]:
        try:
            return json.loads(line)
        except Exception:
            parts = line.split(",")
            if parts and parts[0] == "timestamp_utc":
                return None
            if len(parts) == 13:
                try:
                    return {
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
                    return None
            else:
                print(f"Skipping invalid line (expected 13 parts, got {len(parts)}):", line)
                return None

    # -------------------------
    # Write helpers
    # -------------------------
    def _attempt_write_or_queue(self, obj: dict, ts: str):
        """Try to write to Influx; on failure write fallback file and outbox."""
        if self._write_to_influx(obj, ts):
            return

        # Influx not available or write failed => persist to fallback file AND outbox
        try:
            with open(FALLBACK_FILE, "a") as f:
                f.write(json.dumps(obj) + "\n")
        except Exception as e:
            print("❌ Fallback write failed:", e)

        # Push to outbox for retry
        try:
            push_outbox(obj)
            print(f"📄 Queued to outbox: panel_id={obj.get('panel_id')} ts={ts}")
        except Exception as e:
            print("❌ Failed to push to outbox:", e)

    def _write_to_influx(self, obj: dict, ts: str) -> bool:
        """Return True on success, False on failure or if Influx not configured."""
        if self.write_api is None or Point is None:
            return False
        try:
            p = (Point("panel_telemetry")
                 .tag("panel_id", obj.get("panel_id", "unknown"))
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
                 .time(ts)
                 )
            self.write_api.write(bucket=self.influx_bucket, org=os.environ.get("INFLUX_ORG"), record=p)
            print(f"✔️ Wrote to InfluxDB: panel_id={obj.get('panel_id')} ts={ts}")
            return True
        except Exception as e:
            print("❌ InfluxDB write failed:", e)
            return False

    # -------------------------
    # Outbox retry loop
    # -------------------------
    def send_outbox_loop(self):
        """Continuously retry writing queued messages from outbox to InfluxDB."""
        print("→ Starting central outbox retry loop")
        backoff_base = 1.0
        while not self.stop_event.is_set():
            id_, payload_obj = pop_outbox()
            if id_ is None or payload_obj is None:
                # nothing to do
                time.sleep(1.0)
                continue

            panel_id = payload_obj.get("panel_id", "unknown")
            ts = payload_obj.get("timestamp_utc") or datetime.now(timezone.utc).isoformat()

            # Deduplicate check again before writing (safety)
            if is_duplicate(panel_id, ts):
                # already processed somewhere else; drop it
                mark_outbox_attempt(id_, success=True)
                print(f"→ Dropped duplicate from outbox id={id_} panel_id={panel_id} ts={ts}")
                continue

            success = self._write_to_influx(payload_obj, ts)
            if success:
                mark_outbox_attempt(id_, success=True)
                # reset backoff
                backoff_base = 1.0
                continue
            else:
                # failed -> mark attempt and apply backoff (increasing sleep)
                mark_outbox_attempt(id_, success=False)
                # increase backoff (capped)
                backoff_base = min(backoff_base * 1.5, 30.0)
                time.sleep(backoff_base)

    # -------------------------
    # Fallback replay on startup
    # -------------------------
    def replay_fallback(self):
        """Read fallback JSONL file (if present), push each record into outbox, then truncate file."""
        if not os.path.exists(FALLBACK_FILE):
            return

        print("🔁 Replaying fallback file into outbox...")
        moved = 0
        remaining_lines = []
        try:
            with open(FALLBACK_FILE, "r") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        obj = json.loads(line)
                        # If dedup says seen already, skip
                        panel_id = obj.get("panel_id", "unknown")
                        ts = obj.get("timestamp_utc") or datetime.now(timezone.utc).isoformat()
                        if is_duplicate(panel_id, ts):
                            continue
                        push_outbox(obj)
                        moved += 1
                    except Exception:
                        # can't parse — keep it for manual inspection
                        remaining_lines.append(line)
        except Exception as e:
            print("❌ Error while replaying fallback file:", e)
            return

        # overwrite fallback file with any remaining unparsable lines
        try:
            with open(FALLBACK_FILE, "w") as f:
                for ln in remaining_lines:
                    f.write(ln + "\n")
        except Exception as e:
            print("⚠️ Failed to truncate fallback file:", e)

        print(f"🔁 Moved {moved} records from fallback to outbox.")

    # -------------------------
    # Run
    # -------------------------
    def run(self):
        """Start ingestion: init DBs, replay fallback, start mqtt and outbox thread."""
        init_dedup_db()
        init_outbox_db()
        # Replay fallback -> outbox (so it's retried)
        self.replay_fallback()

        # Start outbox retry thread
        self.outbox_thread.start()

        # Connect MQTT (synchronous connect + background loop)
        try:
            self.mqtt.connect(self.mqtt_host, self.mqtt_port, keepalive=60)
            self.mqtt.loop_start()
            print("Central Ingest service is running...")
            # keep main thread alive
            while not self.stop_event.is_set():
                time.sleep(1.0)
        except KeyboardInterrupt:
            print("Shutting down central ingest...")
        except Exception as e:
            print("❌ MQTT connect/loop failure:", e)
        finally:
            self.stop_event.set()
            try:
                self.mqtt.loop_stop()
                self.mqtt.disconnect()
            except Exception:
                pass

# -------------------------
# CLI entrypoint
# -------------------------
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Central Ingest Service")
    parser.add_argument("--mqtt-host", default="localhost", help="MQTT broker host")
    parser.add_argument("--mqtt-port", type=int, default=1883, help="MQTT broker port")
    args = parser.parse_args()

    ingest = CentralIngest(mqtt_host=args.mqtt_host, mqtt_port=args.mqtt_port)
    ingest.run()
