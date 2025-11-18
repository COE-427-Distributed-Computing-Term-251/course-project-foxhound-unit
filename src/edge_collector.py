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