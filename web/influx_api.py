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
    """Parse time range from request parameters."""
    start = request_args.get('start', '-1h')
    stop = request_args.get('stop', 'now()')
    return start, stop


@app.route('/api/current', methods=['GET'])
def get_current_readings():
    """Get the latest reading for each panel."""
    query = f'''
    from(bucket: "{INFLUX_BUCKET}")
      |> range(start: -5m)
      |> filter(fn: (r) => r["_measurement"] == "panel_telemetry")
      |> group(columns: ["panel_id", "string_id"])
      |> last()
      |> group()
      |> pivot(rowKey:["_time", "panel_id", "string_id"], columnKey: ["_field"], valueColumn: "_value")
    '''
    
    print(f"\n[/api/current] Executing query")
    
    try:
        tables = query_api.query(query, org=INFLUX_ORG)
        results = []
        
        for table in tables:
            for record in table.records:
                result_data = {
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
                }
                results.append(result_data)
        
        print(f"[/api/current] Total results: {len(results)}")
        if len(results) > 0:
            print(f"[/api/current] Sample: Panel {results[0]['panel_id']}, Power: {results[0]['power_w']}W")
        else:
            print("[/api/current] WARNING: No results found!")
        
        return jsonify({'data': results, 'count': len(results)})
    except Exception as e:
        print(f"[/api/current] ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500


@app.route('/api/panel/<panel_id>', methods=['GET'])
def get_panel_history(panel_id):
    """Get historical data for a specific panel."""
    start, stop = parse_time_range(request.args)
    
    query = f'''
    from(bucket: "{INFLUX_BUCKET}")
      |> range(start: {start}, stop: {stop})
      |> filter(fn: (r) => r["_measurement"] == "panel_telemetry")
      |> filter(fn: (r) => r["panel_id"] == "{panel_id}")
      |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
    '''
    
    print(f"\n[/api/panel/{panel_id}] Executing query for time range: {start} to {stop}")
    
    try:
        tables = query_api.query(query, org=INFLUX_ORG)
        results = []
        
        for table in tables:
            for record in table.records:
                results.append({
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
        
        print(f"[/api/panel/{panel_id}] Returned {len(results)} records")
        
        return jsonify({'panel_id': panel_id, 'data': results})
    except Exception as e:
        print(f"[/api/panel/{panel_id}] ERROR: {str(e)}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/aggregate', methods=['GET'])
def get_aggregate_metrics():
    """Get system-wide aggregated metrics (timeseries + current status)."""
    start, stop = parse_time_range(request.args)

    print(f"\n[/api/aggregate] Time range: {start} to {stop}")

    try:
        # 1️⃣ TIMESERIES - Get all power readings per minute per panel, then sum
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
        
        for table in power_tables:
            for record in table.records:
                ts = record.get_time().isoformat()
                power_by_time[ts] = float(record.get_value())
        
        # Get irradiance timeseries (average)
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
        
        for table in irr_tables:
            for record in table.records:
                ts = record.get_time().isoformat()
                irr_by_time[ts] = float(record.get_value())
        
        # Get temperature timeseries (average)
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
        
        for table in temp_tables:
            for record in table.records:
                ts = record.get_time().isoformat()
                temp_by_time[ts] = float(record.get_value())
        
        # Combine all timeseries
        all_timestamps = set(power_by_time.keys()) | set(irr_by_time.keys()) | set(temp_by_time.keys())
        timeseries = []
        
        for ts in sorted(all_timestamps):
            timeseries.append({
                'timestamp': ts,
                'avg_power_w': power_by_time.get(ts, 0),
                'avg_irradiance_wm2': irr_by_time.get(ts, 0),
                'avg_ambient_temp_c': temp_by_time.get(ts, 0)
            })
        
        print(f"[/api/aggregate] Timeseries points: {len(timeseries)}")

        # 2️⃣ CURRENT STATUS - Get latest readings from ACTUAL current time (last 5 min)
        # This represents the real-time system status, not historical data
        current_query = f'''
        from(bucket: "{INFLUX_BUCKET}")
          |> range(start: -5m)
          |> filter(fn: (r) => r["_measurement"] == "panel_telemetry")
          |> filter(fn: (r) => r["_field"] == "power_w" or r["_field"] == "irradiance_wm2")
          |> group(columns: ["panel_id", "_field"])
          |> last()
          |> group()
          |> pivot(rowKey:["panel_id"], columnKey: ["_field"], valueColumn: "_value")
        '''
        
        current_tables = query_api.query(current_query, org=INFLUX_ORG)
        
        total_power = 0
        panel_count = 0
        active_count = 0
        total_irr = 0
        
        for table in current_tables:
            for record in table.records:
                power_val = float(record.values.get('power_w', 0))
                irr_val = float(record.values.get('irradiance_wm2', 0))
                
                total_power += power_val
                total_irr += irr_val
                panel_count += 1
                
                if power_val > 1:
                    active_count += 1
        
        avg_irradiance = total_irr / panel_count if panel_count > 0 else 0
        
        print(f"[/api/aggregate] Current status - Total power: {total_power}W, Panels: {panel_count}, Active: {active_count}")
        print(f"[/api/aggregate] Current avg irradiance: {avg_irradiance}W/m²")

        return jsonify({
            'timeseries': timeseries,
            'total_power_w': total_power,
            'panel_count': panel_count,
            'active_panels': active_count,
            'avg_irradiance': avg_irradiance
        })

    except Exception as e:
        print(f"[/api/aggregate] ERROR: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500

@app.route('/api/faults', methods=['GET'])
def get_faults():
    """Return ALL fault rows with proper pivot to get all fields."""
    # Get ALL data (not just fault field) and filter later
    query = f'''
    from(bucket: "{INFLUX_BUCKET}")
      |> range(start: -7d)  # Last 7 days
      |> filter(fn: (r) => r["_measurement"] == "panel_telemetry")
      |> pivot(rowKey:["_time", "panel_id", "string_id"], columnKey: ["_field"], valueColumn: "_value")
      |> filter(fn: (r) => r["fault"] != "NONE" and r["fault"] != "")
      |> sort(columns: ["_time"], desc: true)
      |> limit(n: 1000)  # Limit to 1000 rows to avoid overload
    '''

    print(f"\n[/api/faults] Executing fault query for last 7 days")

    try:
        tables = query_api.query(query, org=INFLUX_ORG)
        results = []
        
        for table in tables:
            for record in table.records:
                try:
                    fault_value = record.values.get('fault', 'NONE')
                    
                    if fault_value and fault_value != "NONE":
                        results.append({
                            'timestamp': record.get_time().isoformat(),
                            'panel_id': record.values.get('panel_id', 'UNKNOWN'),
                            'string_id': record.values.get('string_id', 'UNKNOWN'),
                            'status': record.values.get('status', 'UNKNOWN'),
                            'fault': fault_value,
                            'power_w': float(record.values.get('power_w', 0)),
                            'voltage_v': float(record.values.get('voltage_v', 0)),
                            'current_a': float(record.values.get('current_a', 0))
                        })
                except Exception as e:
                    print(f"[/api/faults] Error parsing record: {e}")
                    continue
        
        print(f"[/api/faults] Found {len(results)} fault rows")
        
        # If still no results, try a different approach
        if len(results) == 0:
            print(f"[/api/faults] DEBUG: No faults with pivot method. Trying alternative...")
            return get_faults_alternative()
        
        return jsonify({
            'faults': results,
            'count': len(results),
            'message': f'Found {len(results)} fault records'
        })

    except Exception as e:
        print(f"[/api/faults] ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500


def get_faults_alternative():
    """Alternative method to get faults - query for FAULT status."""
    query = f'''
    from(bucket: "{INFLUX_BUCKET}")
      |> range(start: -7d)
      |> filter(fn: (r) => r["_measurement"] == "panel_telemetry")
      |> pivot(rowKey:["_time", "panel_id", "string_id"], columnKey: ["_field"], valueColumn: "_value")
      |> filter(fn: (r) => r["status"] == "FAULT")
      |> sort(columns: ["_time"], desc: true)
      |> limit(n: 1000)
    '''

    print(f"\n[/api/faults] Alternative: Looking for panels with status=FAULT")

    try:
        tables = query_api.query(query, org=INFLUX_ORG)
        results = []
        
        for table in tables:
            for record in table.records:
                try:
                    status_value = record.values.get('status', 'UNKNOWN')
                    
                    if status_value == "FAULT":
                        results.append({
                            'timestamp': record.get_time().isoformat(),
                            'panel_id': record.values.get('panel_id', 'UNKNOWN'),
                            'string_id': record.values.get('string_id', 'UNKNOWN'),
                            'status': status_value,
                            'fault': record.values.get('fault', 'UNKNOWN_FAULT'),
                            'power_w': float(record.values.get('power_w', 0)),
                            'voltage_v': float(record.values.get('voltage_v', 0)),
                            'current_a': float(record.values.get('current_a', 0))
                        })
                except Exception as e:
                    print(f"[/api/faults] Error parsing record: {e}")
                    continue
        
        print(f"[/api/faults] Alternative found {len(results)} FAULT status rows")
        return jsonify({
            'faults': results,
            'count': len(results),
            'message': f'Found {len(results)} panels with FAULT status'
        })

    except Exception as e:
        print(f"[/api/faults] Alternative ERROR: {str(e)}")
        return jsonify({'error': str(e), 'faults': [], 'count': 0}), 500
@app.route('/api/strings', methods=['GET'])
def get_string_data():
    """Get per-string aggregated power data over time."""
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
    
    print(f"\n[/api/strings] Executing query with time range: {start} to {stop}")
    
    try:
        tables = query_api.query(query, org=INFLUX_ORG)
        results = {}
        
        for table in tables:
            for record in table.records:
                string_id = record.values.get('string_id', 'unknown')
                
                if string_id not in results:
                    results[string_id] = []
                
                results[string_id].append({
                    'timestamp': record.get_time().isoformat(),
                    'total_power_w': float(record.get_value())
                })
        
        print(f"[/api/strings] Found {len(results)} strings")
        for string_id, data in results.items():
            if len(data) > 0:
                latest_power = data[-1]['total_power_w']
                print(f"[/api/strings] {string_id}: {len(data)} points, Latest: {latest_power}W")
        
        return jsonify({'strings': results})
    except Exception as e:
        print(f"[/api/strings] ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500


@app.route('/api/health', methods=['GET'])
def health_check():
    """Health check endpoint."""
    try:
        health = client.health()
        return jsonify({
            'status': 'healthy',
            'influxdb': health.status,
            'message': health.message
        })
    except Exception as e:
        return jsonify({
            'status': 'unhealthy',
            'error': str(e)
        }), 500


if __name__ == '__main__':
    if not all([INFLUX_URL, INFLUX_TOKEN, INFLUX_ORG, INFLUX_BUCKET]):
        print("ERROR: Missing required environment variables!")
        print("Please ensure .env contains: INFLUX_URL, INFLUX_TOKEN, INFLUX_ORG, INFLUX_BUCKET")
        exit(1)
    
    print("="*60)
    print("Solar Panel Telemetry API Server")
    print("="*60)
    print(f"InfluxDB URL: {INFLUX_URL}")
    print(f"Organization: {INFLUX_ORG}")
    print(f"Bucket: {INFLUX_BUCKET}")
    print(f"API Server: http://localhost:8080")
    print("="*60)
    print("\nAvailable Endpoints:")
    print("  GET /api/current - Latest readings for all panels")
    print("  GET /api/aggregate - System-wide aggregated metrics")
    print("  GET /api/faults - Current faults and warnings")
    print("  GET /api/strings - Per-string aggregated data")
    print("  GET /api/health - Health check")
    print("="*60)
    print("\nStarting server...\n")
    
    app.run(host='0.0.0.0', port=8080, debug=True)