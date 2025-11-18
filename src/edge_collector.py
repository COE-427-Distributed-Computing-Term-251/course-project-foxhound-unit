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
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("INSERT INTO outbox (topic, payload, created_at) VALUES (?, ?, ?)",
              (topic, payload, int(time.time())))
    conn.commit()
    conn.close()

def pop_outbox():
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
    def __init__(self, mqtt_host, mqtt_port, site_id, batch_size=250, batch_secs=2):
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
        print("MQTT connected")
        self.connected = True

    def on_disconnect(self, client, userdata, rc):
        print("MQTT disconnected")
        self.connected = False

    def connect(self):
        self.mqtt.reconnect_delay_set(min_delay=1, max_delay=30)
        self.mqtt.connect_async(self.mqtt_host, self.mqtt_port, keepalive=60)
        self.mqtt.loop_start()

    def _send_batch(self, batch):
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
        # line expected to be JSON or CSV string; 
        with self.lock:
            self.buffer.append(line)
            if len(self.buffer) >= self.batch_size:
                self._flush_locked()

    def _flush_locked(self):
        if not self.buffer:
            return
        batch = self.buffer[:]
        self.buffer = []
        # send (non-blocking)
        self._send_batch(batch)

    def flush(self):
        with self.lock:
            self._flush_locked()

    def periodic_flush_loop(self):
        while not self.stop_event.is_set():
            time.sleep(self.batch_secs)
            self.flush()

    def spawn_emulators_and_tail(self, emulator_cmds):
        # spawn processes and read their stdout lines
        procs = []
        for cmd in emulator_cmds:
            p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            procs.append(p)
        try:
            while not self.stop_event.is_set():
                for p in procs:
                    if p.stdout is None:
                        continue
                    line = p.stdout.readline()
                    if not line:
                        # process may have ended; check return code
                        if p.poll() is not None:
                            # process ended; continue
                            continue
                        else:
                            continue
                    self.add_line(line.strip())
                time.sleep(0.001)
        except KeyboardInterrupt:
            self.stop_event.set()
        finally:
            for p in procs:
                try:
                    p.terminate()
                except Exception:
                    pass

    def run(self, emulator_cmds):
        init_db()
        self.connect()
        t1 = threading.Thread(target=self.periodic_flush_loop, daemon=True)
        t2 = threading.Thread(target=self.send_outbox_loop, daemon=True)
        t1.start()
        t2.start()
        print("Starting emulator tails (press Ctrl-C to stop)...")
        self.spawn_emulators_and_tail(emulator_cmds)
        # stop
        self.stop_event.set()
        self.flush()
        time.sleep(0.5)
        self.mqtt.loop_stop()