#!/usr/bin/env python3
"""
edge_collector.py
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