#!/usr/bin/env python3
"""
influx_api.py
Flask-based REST API to query solar panel telemetry from InfluxDB.

Endpoints:
  GET /api/current - Latest readings for all panels
  GET /api/panel/<panel_id> - Historical data for specific panel
  GET /api/aggregate - System-wide aggregated metrics
  GET /api/faults - Current faults and warnings
  GET /api/strings - Per-string aggregated data

Requirements:
  pip install flask flask-cors influxdb-client python-dotenv
"""

import os
from datetime import datetime, timedelta
from flask import Flask, jsonify, request
from flask_cors import CORS
from influxdb_client import InfluxDBClient
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

INFLUX_URL = os.getenv('INFLUX_URL')
INFLUX_TOKEN = os.getenv('INFLUX_TOKEN')
INFLUX_ORG = os.getenv('INFLUX_ORG')
INFLUX_BUCKET = os.getenv('INFLUX_BUCKET')

app = Flask(__name__)
CORS(app)

# Initialize InfluxDB client
client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
query_api = client.query_api()


def parse_time_range(request_args):
    start = request_args.get('start', '-1h')
    stop = request_args.get('stop', 'now()')
    return start, stop


# ============================================================
# /api/current
# ============================================================

@app.route('/api/current', methods=['GET'])
def get_current_readings():
    query = f'''
    from(bucket: "{INFLUX_BUCKET}")
      |> range(start: -5m)
      |> filter(fn: (r) => r["_measurement"] == "panel_telemetry")
      |> group(columns: ["panel_id", "string_id"])
      |> last()
      |> group()
      |> pivot(rowKey:["_time", "panel_id", "string_id"], columnKey: ["_field"], valueColumn: "_value")
    '''

    try:
        tables = query_api.query(query, org=INFLUX_ORG)
        results = []

        for table in tables:
            for record in table.records:
                results.append({
                    'timestamp': record.get_time().isoformat(),
                    'panel_id': record.values.get('panel_id', ''),
                    'string_id': record.values.get('string_id', ''),
                    'status': record.values.get('status', 'UNKNOWN'),
                    'fault': record.values.get('fault', 'NONE'),
                    'power_w': float(record.values.get('power_w', 0)),
                    'voltage_v': float(record.values.get('voltage_v', 0)),
                    'current_a': float(record.values.get('current_a', 0)),
                    'irradiance_wm2': float(record.values.get('irradiance_wm2', 0)),
                    'ambient_temp_c': float(record.values.get('ambient_temp_c', 0)),
                    'cell_temp_c': float(record.values.get('cell_temp_c', 0)),
                    'orientation_deg': float(record.values.get('orientation_deg', 0)),
                    'tilt_deg': float(record.values.get('tilt_deg', 0))
                })

        return jsonify({'data': results, 'count': len(results)})

    except Exception as e:
        return jsonify({'error': str(e)}), 500


# ============================================================
# /api/panel/<id>
# ============================================================

@app.route('/api/panel/<panel_id>', methods=['GET'])
def get_panel_history(panel_id):
    start, stop = parse_time_range(request.args)

    query = f'''
    from(bucket: "{INFLUX_BUCKET}")
      |> range(start: {start}, stop: {stop})
      |> filter(fn: (r) => r["_measurement"] == "panel_telemetry")
      |> filter(fn: (r) => r["panel_id"] == "{panel_id}")
      |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
    '''

    try:
        tables = query_api.query(query, org=INFLUX_ORG)
        result = []

        for table in tables:
            for record in table.records:
                result.append({
                    'timestamp': record.get_time().isoformat(),
                    'power_w': float(record.values.get('power_w', 0)),
                    'voltage_v': float(record.values.get('voltage_v', 0)),
                    'current_a': float(record.values.get('current_a', 0)),
                    'irradiance_wm2': float(record.values.get('irradiance_wm2', 0)),
                    'ambient_temp_c': float(record.values.get('ambient_temp_c', 0)),
                    'cell_temp_c': float(record.values.get('cell_temp_c', 0)),
                    'status': record.values.get('status', 'UNKNOWN'),
                    'fault': record.values.get('fault', 'NONE')
                })

        return jsonify({'panel_id': panel_id, 'data': result})
    except Exception as e:
        return jsonify({'error': str(e)}), 500



# ============================================================
# /api/aggregate
# ============================================================

@app.route('/api/aggregate', methods=['GET'])
def get_aggregate_metrics():
    start, stop = parse_time_range(request.args)

    try:
        # Power timeseries
        ts_query = f'''
        from(bucket: "{INFLUX_BUCKET}")
          |> range(start: {start}, stop: {stop})
          |> filter(fn: (r) => r["_measurement"] == "panel_telemetry")
          |> filter(fn: (r) => r["_field"] == "power_w")
          |> aggregateWindow(every: 1m, fn: mean, createEmpty: false)
          |> group(columns: ["_time"])
          |> sum()
        '''

        power_tables = query_api.query(ts_query, org=INFLUX_ORG)
        power_by_time = {}

        for t in power_tables:
            for r in t.records:
                power_by_time[r.get_time().isoformat()] = float(r.get_value())

        # Irradiance
        irr_query = f'''
        from(bucket: "{INFLUX_BUCKET}")
          |> range(start: {start}, stop: {stop})
          |> filter(fn: (r) => r["_measurement"] == "panel_telemetry")
          |> filter(fn: (r) => r["_field"] == "irradiance_wm2")
          |> aggregateWindow(every: 1m, fn: mean, createEmpty: false)
          |> group(columns: ["_time"])
          |> mean()
        '''

        irr_tables = query_api.query(irr_query, org=INFLUX_ORG)
        irr_by_time = {}

        for t in irr_tables:
            for r in t.records:
                irr_by_time[r.get_time().isoformat()] = float(r.get_value())

        # Temperature
        temp_query = f'''
        from(bucket: "{INFLUX_BUCKET}")
          |> range(start: {start}, stop: {stop})
          |> filter(fn: (r) => r["_measurement"] == "panel_telemetry")
          |> filter(fn: (r) => r["_field"] == "ambient_temp_c")
          |> aggregateWindow(every: 1m, fn: mean, createEmpty: false)
          |> group(columns: ["_time"])
          |> mean()
        '''

        temp_tables = query_api.query(temp_query, org=INFLUX_ORG)
        temp_by_time = {}

        for t in temp_tables:
            for r in t.records:
                temp_by_time[r.get_time().isoformat()] = float(r.get_value())

        all_ts = sorted(set(power_by_time.keys()) |
                        set(irr_by_time.keys()) |
                        set(temp_by_time.keys()))

        output = []
        for ts in all_ts:
            output.append({
                'timestamp': ts,
                'avg_power_w': power_by_time.get(ts, 0),
                'avg_irradiance_wm2': irr_by_time.get(ts, 0),
                'avg_ambient_temp_c': temp_by_time.get(ts, 0)
            })

        return jsonify({'timeseries': output})

    except Exception as e:
        return jsonify({'error': str(e)}), 500



# ============================================================
# /api/faults  (FIXED — NO COMMENTS IN FLUX)
# ============================================================

@app.route('/api/faults', methods=['GET'])
def get_faults():
    # Handle start/stop date params
    start = request.args.get('start', '-7d')
    stop = request.args.get('stop', 'now()')

    # Query:
    # 1) Pivot so fault + status become columns
    # 2) Keep rows where fault != NONE OR status != OK
    query = f'''
    from(bucket: "{INFLUX_BUCKET}")
      |> range(start: {start}, stop: {stop})
      |> filter(fn: (r) => r["_measurement"] == "panel_telemetry")
      |> pivot(rowKey:["_time", "panel_id", "string_id"], columnKey: ["_field"], valueColumn: "_value")
      |> filter(fn: (r) =>
          (exists r.fault and r.fault != "" and r.fault != "NONE") or
          (exists r.status and r.status != "" and r.status != "OK")
      )
      |> sort(columns: ["_time"], desc: true)
      |> limit(n: 1000)
    '''

    try:
        tables = query_api.query(query, org=INFLUX_ORG)
        results = []

        for table in tables:
            for record in table.records:
                results.append({
                    'timestamp': record.get_time().isoformat(),
                    'panel_id': record.values.get('panel_id', 'UNKNOWN'),
                    'string_id': record.values.get('string_id', 'UNKNOWN'),
                    'fault': record.values.get('fault', ''),
                    'status': record.values.get('status', ''),
                    'power_w': float(record.values.get('power_w', 0)),
                    'voltage_v': float(record.values.get('voltage_v', 0)),
                    'current_a': float(record.values.get('current_a', 0)),
                })

        return jsonify({'faults': results, 'count': len(results)})

    except Exception as e:
        return jsonify({'error': str(e)}), 500



# ============================================================
# /api/strings
# ============================================================

@app.route('/api/strings', methods=['GET'])
def get_string_data():
    start, stop = parse_time_range(request.args)

    query = f'''
    from(bucket: "{INFLUX_BUCKET}")
      |> range(start: {start}, stop: {stop})
      |> filter(fn: (r) => r["_measurement"] == "panel_telemetry")
      |> filter(fn: (r) => r["_field"] == "power_w")
      |> aggregateWindow(every: 1m, fn: mean, createEmpty: false)
      |> group(columns: ["string_id", "_time"])
      |> sum()
    '''

    try:
        tables = query_api.query(query, org=INFLUX_ORG)
        results = {}

        for table in tables:
            for record in table.records:
                sid = record.values.get('string_id', 'unknown')

                if sid not in results:
                    results[sid] = []

                results[sid].append({
                    'timestamp': record.get_time().isoformat(),
                    'total_power_w': float(record.get_value())
                })

        return jsonify({'strings': results})
    except Exception as e:
        return jsonify({'error': str(e)}), 500



# ============================================================
# /api/health
# ============================================================

@app.route('/api/health', methods=['GET'])
def health_check():
    try:
        health = client.health()
        return jsonify({
            'status': 'healthy',
            'influxdb': health.status,
            'message': health.message
        })
    except Exception as e:
        return jsonify({'status': 'unhealthy', 'error': str(e)}), 500



# ============================================================
# MAIN
# ============================================================

if __name__ == '__main__':
    if not all([INFLUX_URL, INFLUX_TOKEN, INFLUX_ORG, INFLUX_BUCKET]):
        print("ERROR: Missing required environment variables!")
        exit(1)

    app.run(host='0.0.0.0', port=8080, debug=True)
