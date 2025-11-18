#!/usr/bin/env python3
"""
Usage: run per-site. It launches emulator(s) or attaches to their stdout, batches
JSON lines, compresses and publishes to MQTT (QoS=1). On failure it stores batches in local SQLite and retries.
"""
import argparse
import gzip
import json
import sqlite3
import os
import time
from datetime import datetime, timezone
import threading
import queue
import paho.mqtt.client as mqtt
import subprocess
import sys

DB_PATH = "edge_buffer.db"

def init_db():
    """Create outbox table for failed message persistence."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""CREATE TABLE IF NOT EXISTS outbox (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    topic TEXT,
                    payload BLOB,
                    created_at INTEGER
                )""")
    conn.commit()
    conn.close()

def push_outbox(topic, payload):
    """Queue a failed message for retry."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("INSERT INTO outbox (topic, payload, created_at) VALUES (?, ?, ?)",
              (topic, payload, int(time.time())))
    conn.commit()
    conn.close()

def pop_outbox():
    """Retrieve and remove oldest queued message."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT id, topic, payload FROM outbox ORDER BY id LIMIT 1")
    row = c.fetchone()
    if row:
        id_, topic, payload = row
        c.execute("DELETE FROM outbox WHERE id=?", (id_,))
        conn.commit()
    else:
        id_, topic, payload = None, None, None
    conn.close()
    return id_, topic, payload

class EdgeCollector:
    """Batches and publishes telemetry to MQTT with compression and retry."""

    def __init__(self, mqtt_host, mqtt_port, site_id, batch_size=250, batch_secs=2):
        """Initialize collector with MQTT config and batch parameters."""
        self.site_id = site_id
        self.batch_size = batch_size
        self.batch_secs = batch_secs
        self.buffer = []
        self.lock = threading.Lock()
        self.mqtt = mqtt.Client(client_id=f"edge-{site_id}-{int(time.time())}")
        self.mqtt.on_connect = self.on_connect
        self.mqtt.on_disconnect = self.on_disconnect
        self.mqtt_host = mqtt_host
        self.mqtt_port = mqtt_port
        self.connected = False
        self.stop_event = threading.Event()
        self.outbox_q = queue.Queue()

    def on_connect(self, client, userdata, flags, rc):
        """Callback when MQTT connection established."""
        print("MQTT connected")
        self.connected = True

    def on_disconnect(self, client, userdata, rc):
        """Callback when MQTT connection lost."""
        print("MQTT disconnected")
        self.connected = False

    def connect(self):
        """Connect to MQTT broker with auto-reconnect."""
        self.mqtt.reconnect_delay_set(min_delay=1, max_delay=30)
        self.mqtt.connect_async(self.mqtt_host, self.mqtt_port, keepalive=60)
        self.mqtt.loop_start()

    def _send_batch(self, batch):
        """Compress and publish batch, fallback to outbox on failure."""
        topic = f"panels/{self.site_id}/telemetry"
        raw = ("\n".join(batch)).encode("utf-8")
        payload = gzip.compress(raw)
        try:
            if self.connected:
                info = self.mqtt.publish(topic, payload, qos=1)
                info.wait_for_publish()
                if info.rc == mqtt.MQTT_ERR_SUCCESS:
                    # success
                    return
            # if not connected or publish failed:
            raise RuntimeError("MQTT not connected or publish failed")
        except Exception as ex:
            print("Publish failed -> store batch to outbox:", ex)
            push_outbox(topic, payload)

    def send_outbox_loop(self):
        """Continuously retry sending queued messages."""
        while not self.stop_event.is_set():
            # try to send one outbox item
            id_, topic, payload = pop_outbox()
            if id_ is None:
                time.sleep(1.0)
                continue
            try:
                if self.connected:
                    info = self.mqtt.publish(topic, payload, qos=1)
                    info.wait_for_publish()
                    if info.rc != mqtt.MQTT_ERR_SUCCESS:
                        # push back
                        push_outbox(topic, payload)
                        time.sleep(1.0)
                else:
                    push_outbox(topic, payload)
                    time.sleep(1.0)
            except Exception as e:
                push_outbox(topic, payload)
                time.sleep(1.0)

    def add_line(self, line):
        """Add line to buffer, flush if batch size reached."""
        # line expected to be JSON or CSV string; 
        with self.lock:
            self.buffer.append(line)
            if len(self.buffer) >= self.batch_size:
                self._flush_locked()

    def _flush_locked(self):
        """Extract and send buffered batch (must hold lock)."""
        if not self.buffer:
            return
        batch = self.buffer[:]
        self.buffer = []
        # send (non-blocking)
        self._send_batch(batch)

    def flush(self):
        """Thread-safe flush of current buffer."""
        with self.lock:
            self._flush_locked()

    def periodic_flush_loop(self):
        """Flush buffer at regular intervals."""
        while not self.stop_event.is_set():
            time.sleep(self.batch_secs)
            self.flush()

    def spawn_emulator_and_tail(self, emulator_cmd):
        """Launch a single emulator process and read its stdout."""
        p = subprocess.Popen(emulator_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        try:
            while not self.stop_event.is_set():
                if p.stdout is None:
                    continue
                line = p.stdout.readline()
                if not line:
                    if p.poll() is not None:
                        # Process ended
                        break
                    else:
                        continue
                self.add_line(line.strip())
                time.sleep(0.001)  # Prevent CPU spinning
        except KeyboardInterrupt:
            self.stop_event.set()
        finally:
            try:
                p.terminate()
            except Exception:
                pass


    def run(self, emulator_cmds):
        """Main orchestration: init DB, connect MQTT, start threads."""
        init_db()
        self.connect()
        # Start background threads
        t1 = threading.Thread(target=self.periodic_flush_loop, daemon=True)
        t2 = threading.Thread(target=self.send_outbox_loop, daemon=True)
        t1.start()
        t2.start()
        print("Starting emulator tails (press Ctrl-C to stop)...")
        self.spawn_emulator_and_tail(emulator_cmds)
         # Graceful shutdown
        self.stop_event.set()
        self.flush()
        time.sleep(0.5)
        self.mqtt.loop_stop()
        
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mqtt-host", default="localhost")
    parser.add_argument("--mqtt-port", type=int, default=1883)
    parser.add_argument("--site-id", default="site-01")
    parser.add_argument("--batch-size", type=int, default=200)
    parser.add_argument("--batch-secs", type=int, default=2)
    parser.add_argument("--emulator-cmd", action="append", required=True,
                        help="Command to run an emulator instance; pass multiple times")
    args = parser.parse_args()

    collector = EdgeCollector(args.mqtt_host, args.mqtt_port, args.site_id,
                              batch_size=args.batch_size, batch_secs=args.batch_secs)
    collector.run(args.emulator_cmd)