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