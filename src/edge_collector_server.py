#!/usr/bin/env python3
"""
Edge Collector - TCP Server Mode

Acts as a TCP server that accepts connections from emulators running independently.
Each emulator connects and streams telemetry data, which the collector batches,
compresses, and forwards to MQTT.

Usage:
    # Start the edge collector server
    python edge_collector_server.py --mqtt-host localhost --site-id site-01 --port 9000

    # In separate terminals, run emulators that send to this collector
    python solar_panel_telemetry.py --panels 10 --hours 1 --format jsonl | nc localhost 9000
    python solar_panel_telemetry.py --panels 5 --hours 1 --format jsonl | nc localhost 9000
"""

import argparse
import gzip
import json
import os
import sqlite3
import socket
import time
import threading
from datetime import datetime, timezone

import paho.mqtt.client as mqtt

DB_PATH = "edge_buffer.db"
DEFAULT_PORT = 9000
DEFAULT_BATCH_SIZE = 250
DEFAULT_BATCH_SECS = 2


# -------------------------
# SQLite outbox helpers
# -------------------------
def init_db():
    """
    Create/upgrade outbox table for failed message persistence and enable WAL.
    Also ensure a msg_count column exists (migration):
      - If msg_count missing, add it and set existing rows to 1.
    """
    conn = sqlite3.connect(DB_PATH, timeout=30)
    # enable WAL for better concurrency across threads
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    c = conn.cursor()

    # Create base table if missing
    c.execute("""
    CREATE TABLE IF NOT EXISTS outbox (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        topic TEXT NOT NULL,
        payload BLOB NOT NULL,
        created_at INTEGER NOT NULL,
        attempts INTEGER DEFAULT 0,
        last_attempt INTEGER
        -- msg_count will be added if missing
    )
    """)
    conn.commit()

    # Check if msg_count exists; add if missing (migration)
    c.execute("PRAGMA table_info(outbox)")
    cols = [r[1] for r in c.fetchall()]
    if "msg_count" not in cols:
        # Add column with default 1 and set existing rows to 1
        c.execute("ALTER TABLE outbox ADD COLUMN msg_count INTEGER DEFAULT 1")
        # Ensure rows without explicit value have 1 - ALTER TABLE with DEFAULT won't fill
        c.execute("UPDATE outbox SET msg_count = 1 WHERE msg_count IS NULL")
        conn.commit()

    conn.close()


def push_outbox(topic, payload, msg_count=1):
    """
    Queue a failed message for retry (append row).
    msg_count: how many original messages this payload contains (for accurate stats).
    """
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL;")
    c = conn.cursor()
    c.execute(
        "INSERT INTO outbox (topic, payload, created_at, attempts, msg_count) VALUES (?, ?, ?, 0, ?)",
        (topic, payload, int(time.time()), msg_count)
    )
    conn.commit()
    conn.close()


def pop_outbox():
    """
    Retrieve the oldest queued message WITHOUT deleting it.
    Returns (id, topic, payload, msg_count) or (None, None, None, None) if none.
    """
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL;")
    c = conn.cursor()
    c.execute("SELECT id, topic, payload, msg_count FROM outbox ORDER BY id LIMIT 1")
    row = c.fetchone()
    conn.close()
    if row:
        return row[0], row[1], row[2], (row[3] or 1)
    return None, None, None, None


def mark_outbox_attempt(id_, success=False):
    """
    Update attempts/last_attempt and delete on success.
    If success=True the row is removed. On failure we increment attempts and set last_attempt.
    """
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL;")
    c = conn.cursor()
    if success:
        c.execute("DELETE FROM outbox WHERE id=?", (id_,))
    else:
        now = int(time.time())
        c.execute("UPDATE outbox SET attempts = attempts + 1, last_attempt = ? WHERE id=?", (now, id_))
    conn.commit()
    conn.close()


# -------------------------
# EdgeCollectorServer
# -------------------------
class EdgeCollectorServer:
    """TCP server that collects telemetry from remote emulators and forwards via MQTT."""

    def __init__(self, mqtt_host, mqtt_port, site_id, listen_port,
                 batch_size=DEFAULT_BATCH_SIZE, batch_secs=DEFAULT_BATCH_SECS):
        """Initialize collector server with MQTT config and TCP parameters."""
        self.site_id = site_id
        self.listen_port = listen_port
        self.batch_size = batch_size
        self.batch_secs = batch_secs

        # Data buffer for batching
        self.buffer = []
        self.lock = threading.Lock()

        # MQTT setup - use a stable client id and persistent session so broker can queue for offline subscribers
        # Note: stable client id allows some brokers to queue messages for disconnected central subscribers.
        self.mqtt = mqtt.Client(client_id=f"edge-{site_id}", clean_session=False)

        # optional LWT for monitoring
        try:
            self.mqtt.will_set(f"panels/{self.site_id}/status", json.dumps({"status": "offline"}), qos=1, retain=True)
        except Exception:
            pass

        self.mqtt.on_connect = self.on_connect
        self.mqtt.on_disconnect = self.on_disconnect
        self.mqtt_host = mqtt_host
        self.mqtt_port = mqtt_port
        self.connected = False

        # Control
        self.stop_event = threading.Event()

        # Statistics - keep counts updated whenever we queue/send
        self.stats = {
            "messages_received": 0,   # messages read from TCP
            "messages_sent": 0,       # messages successfully published to MQTT (counts original messages inside batches)
            "messages_queued": 0,     # messages persisted to outbox (counts original messages inside payloads)
            "active_connections": 0,
            "bytes_received": 0,
            "bytes_sent": 0
        }
        self.stats_lock = threading.Lock()

    # -------------------------
    # MQTT callbacks
    # -------------------------
    def on_connect(self, client, userdata, flags, rc):
        """Callback when MQTT connection established."""
        print(f"✓ MQTT connected (rc={rc})")
        self.connected = True

    def on_disconnect(self, client, userdata, rc):
        """Callback when MQTT connection lost."""
        print(f"✗ MQTT disconnected (rc={rc})")
        self.connected = False

    def connect_mqtt(self):
        """Connect to MQTT broker with auto-reconnect."""
        # configure reconnect backoff
        self.mqtt.reconnect_delay_set(min_delay=1, max_delay=30)
        # start an async connect and background loop
        self.mqtt.connect_async(self.mqtt_host, self.mqtt_port, keepalive=60)
        self.mqtt.loop_start()
        print(f"→ Connecting to MQTT broker at {self.mqtt_host}:{self.mqtt_port}")

    # -------------------------
    # Sending / batching
    # -------------------------
    def _send_batch(self, batch):
        """
        Compress and publish batch, fallback to outbox on failure.

        Important: `batch` is a list of original lines; use len(batch) as msg_count.
        """
        topic = f"panels/{self.site_id}/telemetry"
        raw = ("\n".join(batch)).encode("utf-8")
        payload = gzip.compress(raw)
        msg_count = len(batch)

        with self.stats_lock:
            self.stats["bytes_sent"] += len(payload)

        try:
            if self.connected:
                info = self.mqtt.publish(topic, payload, qos=1)
                info.wait_for_publish()
                if info.rc == mqtt.MQTT_ERR_SUCCESS:
                    # update messages_sent by msg_count (original messages)
                    with self.stats_lock:
                        self.stats["messages_sent"] += msg_count
                    print(f"✓ Sent batch of {msg_count} messages ({len(payload)} bytes)")
                    return
            # not connected or publish failed -> persist locally
            raise RuntimeError("MQTT not connected or publish failed")
        except Exception as ex:
            print(f"✗ Publish failed, queuing to outbox: {ex}")
            # Persist the compressed payload and the msg_count so later retries update stats accurately
            push_outbox(topic, payload, msg_count=msg_count)
            # reflect queueing immediately in runtime stats
            with self.stats_lock:
                self.stats["messages_queued"] += msg_count

    def add_line(self, line):
        """Add line to buffer, flush if batch size reached."""
        if not line or not line.strip():
            return

        with self.lock:
            self.buffer.append(line.strip())
            with self.stats_lock:
                self.stats["messages_received"] += 1

            if len(self.buffer) >= self.batch_size:
                self._flush_locked()

    def _flush_locked(self):
        """Extract and send buffered batch (must hold lock)."""
        if not self.buffer:
            return
        batch = self.buffer[:]
        self.buffer = []
        self._send_batch(batch)

    def flush(self):
        """Thread-safe flush of current buffer."""
        with self.lock:
            self._flush_locked()

    # -------------------------
    # Periodic & outbox loops
    # -------------------------
    def periodic_flush_loop(self):
        """Flush buffer at regular intervals."""
        print(f"→ Starting periodic flush every {self.batch_secs}s")
        while not self.stop_event.is_set():
            time.sleep(self.batch_secs)
            self.flush()

    def send_outbox_loop(self):
        """
        Continuously retry sending queued messages.

        This loop:
          - peeks the oldest outbox row (pop_outbox returns id, topic, payload, msg_count)
          - attempts to publish the payload to MQTT
          - on success: deletes the row and updates stats (messages_queued -= msg_count, messages_sent += msg_count)
          - on failure: increments attempts and leaves row for later retry
        """
        print("→ Starting outbox retry loop")
        while not self.stop_event.is_set():
            id_, topic, payload, msg_count = pop_outbox()
            if id_ is None:
                time.sleep(1.0)
                continue

            try:
                if self.connected:
                    info = self.mqtt.publish(topic, payload, qos=1)
                    info.wait_for_publish()
                    if info.rc == mqtt.MQTT_ERR_SUCCESS:
                        # remove the row
                        mark_outbox_attempt(id_, success=True)
                        with self.stats_lock:
                            # decrement queued and increment sent by the real msg_count
                            self.stats["messages_queued"] = max(0, self.stats["messages_queued"] - msg_count)
                            self.stats["messages_sent"] += msg_count
                        print(f"✓ Sent queued message from outbox (id={id_}, count={msg_count})")
                    else:
                        # publish failed - mark attempt and backoff
                        mark_outbox_attempt(id_, success=False)
                        time.sleep(1.0)
                else:
                    # not connected -> record attempt/backoff and retry later
                    mark_outbox_attempt(id_, success=False)
                    time.sleep(2.0)
            except Exception as e:
                # record attempt/backoff, don't duplicate rows
                mark_outbox_attempt(id_, success=False)
                time.sleep(2.0)

    # -------------------------
    # TCP client handling
    # -------------------------
    def handle_client(self, conn, addr):
        """Handle a single client connection."""
        client_id = f"{addr[0]}:{addr[1]}"
        print(f"✓ New connection from {client_id}")

        with self.stats_lock:
            self.stats["active_connections"] += 1

        try:
            buffer = ""
            while not self.stop_event.is_set():
                data = conn.recv(4096)
                if not data:
                    break

                chunk = data.decode("utf-8", errors="ignore")
                with self.stats_lock:
                    self.stats["bytes_received"] += len(data)

                buffer += chunk

                # Process complete lines
                while "\n" in buffer:
                    line, buffer = buffer.split("\n", 1)
                    self.add_line(line)

            # Process any remaining data
            if buffer.strip():
                self.add_line(buffer)

        except Exception as e:
            print(f"✗ Error handling client {client_id}: {e}")
        finally:
            conn.close()
            with self.stats_lock:
                self.stats["active_connections"] -= 1
            print(f"✗ Connection closed: {client_id}")

    def tcp_server_loop(self):
        """Main TCP server loop - accept and spawn client handlers."""
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_socket.bind(("0.0.0.0", self.listen_port))
        server_socket.listen(5)
        server_socket.settimeout(1.0)  # Allow periodic checks of stop_event

        print(f"✓ TCP server listening on port {self.listen_port}")
        print(f"→ Emulators can connect with: python solar_panel_telemetry.py ... | nc localhost {self.listen_port}")

        while not self.stop_event.is_set():
            try:
                conn, addr = server_socket.accept()
                # Spawn thread to handle this client
                client_thread = threading.Thread(
                    target=self.handle_client,
                    args=(conn, addr),
                    daemon=True
                )
                client_thread.start()
            except socket.timeout:
                continue
            except Exception as e:
                if not self.stop_event.is_set():
                    print(f"✗ Server error: {e}")

        server_socket.close()
        print("✓ TCP server stopped")

    # -------------------------
    # Stats loop
    # -------------------------
    def stats_loop(self):
        """Periodically print statistics."""
        while not self.stop_event.is_set():
            time.sleep(10)
            with self.stats_lock:
                print(f"\n--- Statistics ---")
                print(f"  Active connections: {self.stats['active_connections']}")
                print(f"  Messages received:  {self.stats['messages_received']}")
                print(f"  Messages sent:      {self.stats['messages_sent']}")
                print(f"  Messages queued:    {self.stats['messages_queued']}")
                print(f"  Bytes received:     {self.stats['bytes_received']:,}")
                print(f"  Bytes sent (gzip):  {self.stats['bytes_sent']:,}")
                if self.stats['bytes_received'] > 0:
                    ratio = (1 - self.stats['bytes_sent'] / self.stats['bytes_received']) * 100
                    print(f"  Compression ratio:  {ratio:.1f}%")
                print(f"  MQTT connected:     {self.connected}")
                print("------------------\n")

    # -------------------------
    # Run
    # -------------------------
    def run(self):
        """Main orchestration: init DB, connect MQTT, start all services."""
        init_db()
        self.connect_mqtt()

        # Start background threads
        threads = [
            threading.Thread(target=self.periodic_flush_loop, daemon=True),
            threading.Thread(target=self.send_outbox_loop, daemon=True),
            threading.Thread(target=self.stats_loop, daemon=True),
        ]

        for t in threads:
            t.start()

        # Run TCP server in main thread (blocking)
        try:
            self.tcp_server_loop()
        except KeyboardInterrupt:
            print("\n→ Shutting down...")
        finally:
            self.stop_event.set()
            self.flush()
            time.sleep(0.5)
            self.mqtt.loop_stop()
            print("✓ Edge collector stopped")


# -------------------------
# CLI entrypoint
# -------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Edge Collector TCP Server - collects telemetry from remote emulators"
    )
    parser.add_argument("--mqtt-host", default="localhost",
                        help="MQTT broker hostname")
    parser.add_argument("--mqtt-port", type=int, default=1883,
                        help="MQTT broker port")
    parser.add_argument("--site-id", default="site-01",
                        help="Site identifier for this edge collector")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT,
                        help=f"TCP port to listen on (default: {DEFAULT_PORT})")
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE,
                        help=f"Messages per batch (default: {DEFAULT_BATCH_SIZE})")
    parser.add_argument("--batch-secs", type=int, default=DEFAULT_BATCH_SECS,
                        help=f"Seconds between flushes (default: {DEFAULT_BATCH_SECS})")

    args = parser.parse_args()

    print("=" * 60)
    print("Edge Collector Server")
    print("=" * 60)
    print(f"Site ID:       {args.site_id}")
    print(f"TCP Port:      {args.port}")
    print(f"MQTT Broker:   {args.mqtt_host}:{args.mqtt_port}")
    print(f"Batch Config:  {args.batch_size} messages / {args.batch_secs}s")
    print("=" * 60)

    collector = EdgeCollectorServer(
        mqtt_host=args.mqtt_host,
        mqtt_port=args.mqtt_port,
        site_id=args.site_id,
        listen_port=args.port,
        batch_size=args.batch_size,
        batch_secs=args.batch_secs
    )

    collector.run()
