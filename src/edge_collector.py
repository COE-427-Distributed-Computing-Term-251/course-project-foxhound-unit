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

# Add class with __init__, on_connect, on_disconnect, connect methods:
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