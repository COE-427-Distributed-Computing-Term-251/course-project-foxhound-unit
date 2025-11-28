import gzip
import json
import os
import sys
import threading
import time
from types import SimpleNamespace

import pytest

# Add src/ to path
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "src"))

import records_handler as rh
import edge_collector_server as ecs
import solar_panel_telemetry as spt
import central_ingest as ci
import json
import gzip
import central_ingest as ci

# ---------------------------
# Fixtures
# ---------------------------

@pytest.fixture
def records_handler():
    return rh.RecordsHandler()


@pytest.fixture
def isolated_outbox_db(tmp_path, monkeypatch):
    path = str(tmp_path / "edge_buffer.db")
    monkeypatch.setattr(ecs, "DB_PATH", path)
    ecs.init_db()
    return path


@pytest.fixture
def isolated_dedup_db(tmp_path, monkeypatch):
    path = str(tmp_path / "ingest_dedup.db")
    monkeypatch.setattr(ci, "DEDUP_DB", path)
    ci.init_dedup_db()
    return path


def make_test_collector(monkeypatch, batch_size=10, batch_secs=60):
    class FakeMQTT:
        def __init__(self, *a, **k): pass
        def will_set(self, *a, **k): pass
        def connect_async(self, *a, **k): pass
        def loop_start(self): pass
        def reconnect_delay_set(self, *a, **k): pass
        def publish(self, *a, **k):
            info = SimpleNamespace(rc=ecs.mqtt.MQTT_ERR_SUCCESS)
            info.wait_for_publish = lambda: None
            return info

    monkeypatch.setattr(ecs.mqtt, "Client", FakeMQTT)

    return ecs.EdgeCollectorServer(
        mqtt_host="localhost",
        mqtt_port=1883,
        site_id="test-site",
        listen_port=0,
        batch_size=batch_size,
        batch_secs=batch_secs,
    )


# ---------------------------
# RecordsHandler
# ---------------------------

def test_records_handler_valid_json(records_handler):
    rec = {k: 1 for k in rh.REQUIRED_FIELDS}
    rec["timestamp_utc"] = "2025-10-18T08:00:00Z"
    rec["panel_id"] = "P1"
    out = records_handler.process_line(json.dumps(rec))
    assert json.loads(out)["panel_id"] == "P1"


def test_records_handler_csv(records_handler):
    header = ",".join(rh.REQUIRED_FIELDS)
    row = (
        "2025-10-18T08:00:00Z,P1,S01,OK,NONE,"
        "100,40,2,800,25,35,180,25"
    )
    assert records_handler.process_line(header) is None
    out = records_handler.process_line(row)
    assert json.loads(out)["panel_id"] == "P1"


def test_records_handler_missing_field():
    rec = {k: 1 for k in rh.REQUIRED_FIELDS if k != "tilt_deg"}
    with pytest.raises(ValueError):
        rh.validate_and_fix(rec)


def test_records_handler_unknown_format(records_handler):
    with pytest.raises(ValueError):
        records_handler.process_line("not-json-or-csv")


# ---------------------------
# EdgeCollectorServer
# ---------------------------

def test_edge_add_line_valid(monkeypatch):
    c = make_test_collector(monkeypatch, batch_size=5)
    c._send_batch = lambda b: None

    rec = {k: 1 for k in rh.REQUIRED_FIELDS}
    rec["timestamp_utc"] = "t"
    line = json.dumps(rec)

    c.add_line(line)
    c.add_line(line)

    assert c.stats["messages_received"] == 2
    assert c.stats["messages_invalid"] == 0


def test_edge_add_line_invalid(monkeypatch):
    c = make_test_collector(monkeypatch)
    bad = {k: 1 for k in rh.REQUIRED_FIELDS if k != "tilt_deg"}
    c.add_line(json.dumps(bad))
    assert c.stats["messages_invalid"] == 1


def test_edge_batching(monkeypatch):
    c = make_test_collector(monkeypatch, batch_size=2)
    batches = []
    c._send_batch = lambda b: batches.append(b)

    rec = {k: 1 for k in rh.REQUIRED_FIELDS}
    rec["timestamp_utc"] = "t"
    line = json.dumps(rec)

    c.add_line(line)
    assert len(batches) == 0
    c.add_line(line)
    assert len(batches) == 1
    assert len(batches[0]) == 2


def test_edge_flush_empty(monkeypatch):
    c = make_test_collector(monkeypatch)
    c._send_batch = lambda b: (_ for _ in ()).throw(Exception())
    c.flush()  # should not raise


# ---------------------------
# Outbox
# ---------------------------

def test_outbox_push_pop(isolated_outbox_db):
    ecs.push_outbox("t", b"a", 2)
    ecs.push_outbox("t", b"b", 5)

    id1, _, p1, c1 = ecs.pop_outbox()
    assert p1 == b"a"
    assert c1 == 2
    ecs.mark_outbox_attempt(id1, True)

    id2, _, p2, c2 = ecs.pop_outbox()
    assert p2 == b"b"
    assert c2 == 5


def test_send_batch_queues(monkeypatch):
    c = make_test_collector(monkeypatch)
    c.connected = False

    queued = []
    def fake_push_outbox(topic, payload, msg_count=1, **kwargs):
        queued.append((topic, payload, msg_count))

    monkeypatch.setattr(ecs, "push_outbox", fake_push_outbox)

    batch = ['{"x":1}', '{"x":2}']
    c._send_batch(batch)

    assert queued[0][2] == 2
    assert c.stats["messages_queued"] == 2


def test_outbox_retry(monkeypatch, isolated_outbox_db):
    ecs.push_outbox("t", b"a", 4)
    c = make_test_collector(monkeypatch)
    c.connected = True

    sent = []
    c.mqtt.publish = lambda t, p, qos: SimpleNamespace(
        rc=ecs.mqtt.MQTT_ERR_SUCCESS,
        wait_for_publish=lambda: sent.append((t, p, qos))
    )

    real_pop = ecs.pop_outbox
    def pop_once():
        r = real_pop()
        ecs.pop_outbox = lambda: (None, None, None, None)
        return r
    ecs.pop_outbox = pop_once

    t = threading.Thread(target=c.send_outbox_loop, daemon=True)
    t.start()
    time.sleep(0.3)
    c.stop_event.set()
    t.join()

    assert len(sent) == 1
    assert c.stats["messages_sent"] == 4


# ---------------------------
# TCP Client Handling
# ---------------------------

class FakeConn:
    def __init__(self, chunks):
        self.chunks = list(chunks)
    def recv(self, n):
        return self.chunks.pop(0) if self.chunks else b""
    def close(self): pass


def test_handle_client_splits_lines(monkeypatch):
    c = make_test_collector(monkeypatch)
    c._send_batch = lambda b: None

    rec = {k: 1 for k in rh.REQUIRED_FIELDS}
    rec["timestamp_utc"] = "t"
    line = (json.dumps(rec) + "\n") * 2
    data = line.encode()

    chunks = [data[:10], data[10:25], data[25:]]
    c.handle_client(FakeConn(chunks), ("127.0.0.1", 1))

    assert c.stats["messages_received"] == 2


# ---------------------------
# CentralIngest
# ---------------------------

def make_test_ingest(monkeypatch, isolated_dedup_db, tmp_path):
    class FakeMQ:
        def __init__(self, *a, **k):
            self.on_message = None
        def connect(self, *a, **k): pass
        def loop_start(self): pass
        def subscribe(self, *a, **k): pass

    monkeypatch.setattr(ci.mqtt, "Client", FakeMQ)

    fallback = str(tmp_path / "fallback.jsonl")
    monkeypatch.setattr(ci, "FALLBACK_FILE", fallback)

    i = ci.CentralIngest()
    i.influx_client = None
    i.write_api = None
    return i, fallback


def test_central_fallback(monkeypatch, isolated_dedup_db, tmp_path):
    i, fallback = make_test_ingest(monkeypatch, isolated_dedup_db, tmp_path)
    obj = {k: 1 for k in rh.REQUIRED_FIELDS}
    obj["timestamp_utc"] = "t"

    i._write(obj, "t")

    stored = json.loads(open(fallback).read())
    assert stored["timestamp_utc"] == "t"



def test_central_dedup(tmp_path, monkeypatch):
    # Use temp files so we don't touch real project files
    db_path = tmp_path / "dedup_test.db"
    fallback_path = tmp_path / "fallback_test.jsonl"

    # Point module-level constants to temp paths
    monkeypatch.setattr(ci, "DEDUP_DB", str(db_path))
    monkeypatch.setattr(ci, "FALLBACK_FILE", str(fallback_path))

    # Re-init dedup DB on the new path
    ci.init_dedup_db()

    # Build ingest with no real Influx (force fallback JSONL)
    ingest = ci.CentralIngest(mqtt_host="test", mqtt_port=1883)
    ingest.write_api = None  # force _write() to use fallback file

    # One valid JSON line
    line_obj = {
        "timestamp_utc": "2025-01-01T00:00:00Z",
        "panel_id": "P-1",
        "string_id": "S-1",
        "status": "OK",
        "fault": "NONE",
        "power_w": 100.0,
        "voltage_v": 10.0,
        "current_a": 10.0,
        "irradiance_wm2": 1000.0,
        "ambient_temp_c": 25.0,
        "cell_temp_c": 30.0,
        "orientation_deg": 0.0,
        "tilt_deg": 30.0,
    }
    line = json.dumps(line_obj)

    class Msg:
        def __init__(self, payload: bytes):
            self.payload = payload

    # First time → should be written
    msg1 = Msg(payload=line.encode("utf-8"))
    ingest._on_message(None, None, msg1)

    # Second time with same panel_id + timestamp → should be deduplicated
    msg2 = Msg(payload=line.encode("utf-8"))
    ingest._on_message(None, None, msg2)

    # Fallback JSONL file should contain only ONE line
    with open(fallback_path, "r", encoding="utf-8") as f:
        lines = f.readlines()

    assert len(lines) == 1


# ---------------------------
# End-to-End Single Record
# ---------------------------

def test_end_to_end(monkeypatch):
    fleet = spt.generate_panel_fleet(1, 42)
    spec = fleet[0]
    s, e = spt.parse_daylight_window("06:00-18:00")
    ts = spt.utc_now_truncated()

    rec = spt.compute_telemetry(spec, ts, s, e, 0, 1.0)
    line = json.dumps(rec)

    c = make_test_collector(monkeypatch, batch_size=1)
    c.connected = True

    sent = []
    c.mqtt.publish = lambda t, p, qos: SimpleNamespace(
        rc=ecs.mqtt.MQTT_ERR_SUCCESS,
        wait_for_publish=lambda: sent.append((t, p, qos))
    )

    c.add_line(line)

    raw = gzip.decompress(sent[0][1]).decode().strip()
    loaded = json.loads(raw)
    assert loaded["panel_id"] == rec["panel_id"]
