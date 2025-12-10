#!/usr/bin/env python3
"""
influx_api.py
Flask-based REST API for InfluxDB 2.x with optimized queries and timeouts

Optimizations:
- Increased connection and read timeouts
- Simplified queries for faster execution
- Better error handling with graceful degradation
- Reduced data ranges for faster queries
"""

import os
from datetime import datetime, timedelta
from flask import Flask, jsonify, request
from flask_cors import CORS
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

INFLUX_URL = os.getenv('INFLUX_URL')
INFLUX_TOKEN = os.getenv('INFLUX_TOKEN')
INFLUX_ORG = os.getenv('INFLUX_ORG')
INFLUX_BUCKET = os.getenv('INFLUX_BUCKET')

app = Flask(__name__)
CORS(app)

# Use InfluxDB 2.x client with Flux and increased timeout
from influxdb_client import InfluxDBClient

# Create client with increased timeout (60 seconds)
client = InfluxDBClient(
    url=INFLUX_URL, 
    token=INFLUX_TOKEN, 
    org=INFLUX_ORG,
    timeout=60_000  # 60 seconds in milliseconds
)
query_api = client.query_api()

print("Using InfluxDB 2.x client with Flux queries (60s timeout)")


def execute_flux(flux_query, timeout_ms=60000):
    """Execute Flux query and return results as list of dicts."""
    try:
        # Query with custom timeout
        tables = query_api.query(flux_query, org=INFLUX_ORG)
        
        results = []
        for table in tables:
            for record in table.records:
                # Convert record to dict
                row_dict = {
                    'time': record.get_time(),
                    '_time': record.get_time(),
                }
                # Add all fields and tags
                row_dict.update(record.values)
                results.append(row_dict)
        
        return results
        
    except Exception as e:
        error_msg = str(e)
        print(f"Flux Query Error: {error_msg}")
        if len(flux_query) < 500:
            print(f"Query was: {flux_query}")
        raise


def safe_float(value, default=0.0):
    """Safely convert to float."""
    if value is None:
        return default
    try:
        return float(value)
    except (ValueError, TypeError):
        return default


def safe_str(value, default=''):
    """Safely convert to string."""
    if value is None:
        return default
    return str(value)


def convert_time_range(start):
    """Convert relative time string to Flux duration."""
    time_map = {
        '-5m': '5m',
        '-15m': '15m',
        '-1h': '1h',
        '-6h': '6h',
        '-24h': '24h',
        '-7d': '7d'
    }
    return time_map.get(start, '1h')


# ============================================================
# /api/debug
# ============================================================

@app.route('/api/debug', methods=['GET'])
def debug_info():
    """Debug endpoint to check database connectivity and data."""
    info = {
        'influx_version': '2.x (Flux)',
        'config': {
            'url': INFLUX_URL,
            'org': INFLUX_ORG,
            'bucket': INFLUX_BUCKET,
        },
        'tests': {}
    }
    
    try:
        # Test 1: Count recent data
        flux = f'''
        from(bucket: "{INFLUX_BUCKET}")
          |> range(start: -1m)
          |> filter(fn: (r) => r._measurement == "panel_telemetry")
          |> count()
          |> limit(n: 1)
        '''
        result = execute_flux(flux)
        info['tests']['recent_data_points'] = len(result)
        
        # Test 2: Get sample panel IDs
        flux = f'''
        from(bucket: "{INFLUX_BUCKET}")
          |> range(start: -2m)
          |> filter(fn: (r) => r._measurement == "panel_telemetry")
          |> keep(columns: ["panel_id"])
          |> distinct(column: "panel_id")
          |> limit(n: 5)
        '''
        result = execute_flux(flux)
        info['tests']['sample_panels'] = [r.get('panel_id') for r in result if r.get('panel_id')]
        
        # Test 3: Get RAW sample data to see structure
        flux = f'''
        from(bucket: "{INFLUX_BUCKET}")
          |> range(start: -1m)
          |> filter(fn: (r) => r._measurement == "panel_telemetry")
          |> limit(n: 3)
        '''
        result = execute_flux(flux)
        info['tests']['raw_sample'] = result
        
        # Test 4: Check if status/fault are fields or tags
        flux = f'''
        from(bucket: "{INFLUX_BUCKET}")
          |> range(start: -1m)
          |> filter(fn: (r) => r._measurement == "panel_telemetry")
          |> filter(fn: (r) => r._field == "status" or r._field == "fault")
          |> limit(n: 3)
        '''
        result = execute_flux(flux)
        info['tests']['status_fault_as_fields'] = result
        
        info['status'] = 'connected'
        
    except Exception as e:
        info['status'] = 'error'
        info['error'] = str(e)
    
    return jsonify(info)


# ============================================================
# /api/current
# ============================================================

@app.route('/api/current', methods=['GET'])
def get_current_readings():
    """Get latest readings for all panels - OPTIMIZED."""
    
    # Get all fields for all panels
    flux = f'''
    from(bucket: "{INFLUX_BUCKET}")
      |> range(start: -2m)
      |> filter(fn: (r) => r._measurement == "panel_telemetry")
      |> group(columns: ["panel_id", "_field"])
      |> last()
    '''
    
    try:
        results = execute_flux(flux)
        
        # Manually pivot the data in Python
        panels_dict = {}
        
        for row in results:
            panel_id = safe_str(row.get('panel_id'), 'UNKNOWN')
            field_name = row.get('_field')
            value = row.get('_value')
            
            if panel_id not in panels_dict:
                panels_dict[panel_id] = {
                    'panel_id': panel_id,
                    'timestamp': row.get('_time'),
                    'string_id': safe_str(row.get('string_id'), 'UNKNOWN'),
                    'status': safe_str(row.get('status'), 'UNKNOWN'),  # Try to get from tags
                    'fault': safe_str(row.get('fault'), 'NONE')        # Try to get from tags
                }
            
            # Store the field value
            if field_name:
                # Check if this is a status/fault field
                if field_name == 'status':
                    panels_dict[panel_id]['status'] = safe_str(value, 'UNKNOWN')
                elif field_name == 'fault':
                    panels_dict[panel_id]['fault'] = safe_str(value, 'NONE')
                else:
                    panels_dict[panel_id][field_name] = value
            
            # Update timestamp and tags to latest
            if row.get('_time') and (not panels_dict[panel_id]['timestamp'] or row.get('_time') > panels_dict[panel_id]['timestamp']):
                panels_dict[panel_id]['timestamp'] = row.get('_time')
                panels_dict[panel_id]['string_id'] = safe_str(row.get('string_id'), panels_dict[panel_id]['string_id'])
                # Update status and fault from tags if present
                if row.get('status'):
                    panels_dict[panel_id]['status'] = safe_str(row.get('status'), panels_dict[panel_id]['status'])
                if row.get('fault'):
                    panels_dict[panel_id]['fault'] = safe_str(row.get('fault'), panels_dict[panel_id]['fault'])
        
        # Convert to list with proper types
        panel_list = []
        for panel_id, data in panels_dict.items():
            panel_list.append({
                'timestamp': data.get('timestamp').isoformat() if data.get('timestamp') else None,
                'panel_id': panel_id,
                'string_id': data.get('string_id', 'UNKNOWN'),
                'status': data.get('status', 'UNKNOWN'),
                'fault': data.get('fault', 'NONE'),
                'power_w': safe_float(data.get('power_w')),
                'voltage_v': safe_float(data.get('voltage_v')),
                'current_a': safe_float(data.get('current_a')),
                'irradiance_wm2': safe_float(data.get('irradiance_wm2')),
                'ambient_temp_c': safe_float(data.get('ambient_temp_c')),
                'cell_temp_c': safe_float(data.get('cell_temp_c')),
                'orientation_deg': safe_float(data.get('orientation_deg')),
                'tilt_deg': safe_float(data.get('tilt_deg'))
            })
        
        return jsonify({
            'data': panel_list,
            'count': len(panel_list)
        })
        
    except Exception as e:
        print(f"Error in /api/current: {e}")
        return jsonify({
            'error': str(e),
            'data': [],
            'count': 0
        }), 500


# ============================================================
# /api/panel/<id>
# ============================================================

@app.route('/api/panel/<panel_id>', methods=['GET'])
def get_panel_history(panel_id):
    """Get historical data for a specific panel."""
    
    start = request.args.get('start', '-1h')
    duration = convert_time_range(start)
    
    # Simplified: Get last N points instead of full range
    limit = 100
    
    flux = f'''
    from(bucket: "{INFLUX_BUCKET}")
      |> range(start: -{duration})
      |> filter(fn: (r) => r._measurement == "panel_telemetry")
      |> filter(fn: (r) => r.panel_id == "{panel_id}")
      |> sort(columns: ["_time"], desc: true)
      |> limit(n: {limit})
      |> sort(columns: ["_time"])
    '''
    
    try:
        results = execute_flux(flux)
        
        # Manual pivot in Python
        time_points = {}
        for row in results:
            time_key = row.get('_time')
            field_name = row.get('_field')
            value = row.get('_value')
            
            if time_key not in time_points:
                time_points[time_key] = {
                    'timestamp': time_key,
                    'status': safe_str(row.get('status'), 'UNKNOWN'),
                    'fault': safe_str(row.get('fault'), 'NONE')
                }
            
            if field_name:
                time_points[time_key][field_name] = value
        
        # Convert to list
        data = []
        for time_key in sorted(time_points.keys()):
            point = time_points[time_key]
            data.append({
                'timestamp': point['timestamp'].isoformat() if point['timestamp'] else None,
                'power_w': safe_float(point.get('power_w')),
                'voltage_v': safe_float(point.get('voltage_v')),
                'current_a': safe_float(point.get('current_a')),
                'irradiance_wm2': safe_float(point.get('irradiance_wm2')),
                'ambient_temp_c': safe_float(point.get('ambient_temp_c')),
                'cell_temp_c': safe_float(point.get('cell_temp_c')),
                'status': point['status'],
                'fault': point['fault']
            })
        
        return jsonify({
            'panel_id': panel_id,
            'data': data,
            'count': len(data)
        })
        
    except Exception as e:
        print(f"Error in /api/panel/{panel_id}: {e}")
        return jsonify({'error': str(e), 'data': [], 'count': 0}), 500


# ============================================================
# /api/aggregate
# ============================================================

@app.route('/api/aggregate', methods=['GET'])
def get_aggregate_metrics():
    """Get aggregated system-wide metrics - OPTIMIZED."""
    
    start = request.args.get('start', '-1h')
    duration = convert_time_range(start)
    
    # Determine window interval
    window_map = {
        '5m': '30s',
        '15m': '1m',
        '1h': '2m',
        '6h': '10m',
        '24h': '30m',
        '7d': '2h'
    }
    window = window_map.get(duration, '2m')
    
    # Simplified: Get power only, limit points
    flux = f'''
    from(bucket: "{INFLUX_BUCKET}")
      |> range(start: -{duration})
      |> filter(fn: (r) => r._measurement == "panel_telemetry")
      |> filter(fn: (r) => r._field == "power_w" or r._field == "irradiance_wm2" or r._field == "ambient_temp_c")
      |> aggregateWindow(every: {window}, fn: mean, createEmpty: false)
      |> group(columns: ["_time", "_field"])
      |> sum()
      |> limit(n: 200)
    '''
    
    try:
        results = execute_flux(flux)
        
        # Organize by time and field
        time_data = {}
        for row in results:
            time_key = row.get('_time')
            field = row.get('_field')
            value = safe_float(row.get('_value'))
            
            if time_key not in time_data:
                time_data[time_key] = {}
            
            if field == 'power_w':
                # Power is summed across all panels
                time_data[time_key]['power_w'] = value
            elif field == 'irradiance_wm2':
                # Store for later averaging
                if 'irradiance_sum' not in time_data[time_key]:
                    time_data[time_key]['irradiance_sum'] = 0
                    time_data[time_key]['irradiance_count'] = 0
                time_data[time_key]['irradiance_sum'] += value
                time_data[time_key]['irradiance_count'] += 1
            elif field == 'ambient_temp_c':
                if 'temp_sum' not in time_data[time_key]:
                    time_data[time_key]['temp_sum'] = 0
                    time_data[time_key]['temp_count'] = 0
                time_data[time_key]['temp_sum'] += value
                time_data[time_key]['temp_count'] += 1
        
        # Convert to timeseries
        timeseries = []
        for time_key in sorted(time_data.keys()):
            data = time_data[time_key]
            
            irr_avg = data['irradiance_sum'] / data['irradiance_count'] if data.get('irradiance_count', 0) > 0 else 0
            temp_avg = data['temp_sum'] / data['temp_count'] if data.get('temp_count', 0) > 0 else 0
            
            timeseries.append({
                'timestamp': time_key.isoformat() if time_key else None,
                'avg_power_w': data.get('power_w', 0),
                'avg_irradiance_wm2': irr_avg,
                'avg_ambient_temp_c': temp_avg,
                'sample_count': 1
            })
        
        return jsonify({
            'timeseries': timeseries,
            'count': len(timeseries),
            'bucket_interval': window
        })
        
    except Exception as e:
        print(f"Error in /api/aggregate: {e}")
        return jsonify({
            'error': str(e),
            'timeseries': [],
            'count': 0
        }), 500


# ============================================================
# /api/faults
# ============================================================

@app.route('/api/faults', methods=['GET'])
def get_faults():
    """Get current faults and warnings - OPTIMIZED."""
    
    start = request.args.get('start', '-1h')
    duration = convert_time_range(start)
    
    # Get all data and filter in Python for better reliability
    flux = f'''
    from(bucket: "{INFLUX_BUCKET}")
      |> range(start: -{duration})
      |> filter(fn: (r) => r._measurement == "panel_telemetry")
      |> sort(columns: ["_time"], desc: true)
      |> limit(n: 1000)
    '''
    
    try:
        results = execute_flux(flux)
        
        # Manual pivot and filter
        time_points = {}
        for row in results:
            key = (row.get('_time'), row.get('panel_id'))
            field = row.get('_field')
            
            if key not in time_points:
                time_points[key] = {
                    'timestamp': row.get('_time'),
                    'panel_id': safe_str(row.get('panel_id'), 'UNKNOWN'),
                    'string_id': safe_str(row.get('string_id'), 'UNKNOWN'),
                    'status': safe_str(row.get('status'), 'OK'),  # Try to get from tags
                    'fault': safe_str(row.get('fault'), 'NONE')   # Try to get from tags
                }
            
            # Store field values
            if field:
                if field == 'status':
                    time_points[key]['status'] = safe_str(row.get('_value'), 'OK')
                elif field == 'fault':
                    time_points[key]['fault'] = safe_str(row.get('_value'), 'NONE')
                elif field in ['power_w', 'voltage_v', 'current_a']:
                    time_points[key][field] = row.get('_value')
            
            # Also try to get status/fault from tags
            if row.get('status'):
                time_points[key]['status'] = safe_str(row.get('status'), time_points[key]['status'])
            if row.get('fault'):
                time_points[key]['fault'] = safe_str(row.get('fault'), time_points[key]['fault'])
        
        # Filter for faults and convert to list
        faults = []
        seen_entries = set()
        
        for point in time_points.values():
            status = point.get('status', 'OK')
            fault = point.get('fault', 'NONE')
            
            # Check if this is actually a fault/warning
            has_fault = (
                (fault and fault != 'NONE' and fault.upper() != 'NONE') or 
                (status and status != 'OK' and status.upper() != 'OK')
            )
            
            if has_fault:
                # Create unique key to avoid duplicates
                entry_key = (point['panel_id'], status, fault, point['timestamp'])
                if entry_key not in seen_entries:
                    seen_entries.add(entry_key)
                    
                    faults.append({
                        'timestamp': point['timestamp'].isoformat() if point.get('timestamp') else None,
                        'panel_id': point['panel_id'],
                        'string_id': point['string_id'],
                        'fault': fault,
                        'status': status,
                        'power_w': safe_float(point.get('power_w')),
                        'voltage_v': safe_float(point.get('voltage_v')),
                        'current_a': safe_float(point.get('current_a'))
                    })
        
        # Sort by timestamp desc
        faults.sort(key=lambda x: x['timestamp'] if x['timestamp'] else '', reverse=True)
        
        # Limit to most recent 100
        faults = faults[:100]
        
        print(f"Found {len(faults)} faults/warnings")
        
        return jsonify({
            'faults': faults,
            'count': len(faults)
        })
        
    except Exception as e:
        print(f"Error in /api/faults: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e), 'faults': [], 'count': 0}), 500


@app.route('/api/faults/debug', methods=['GET'])
def debug_faults():
    """Debug endpoint to check fault detection."""
    
    duration = '5m'
    
    # Get sample of all status and fault values
    flux = f'''
    from(bucket: "{INFLUX_BUCKET}")
      |> range(start: -{duration})
      |> filter(fn: (r) => r._measurement == "panel_telemetry")
      |> limit(n: 100)
    '''
    
    try:
        results = execute_flux(flux)
        
        # Analyze the data
        debug_info = {
            'total_rows': len(results),
            'unique_statuses': set(),
            'unique_faults': set(),
            'sample_data': [],
            'status_field_found': False,
            'fault_field_found': False,
            'status_tag_found': False,
            'fault_tag_found': False
        }
        
        for row in results[:20]:
            # Check fields
            if row.get('_field') == 'status':
                debug_info['status_field_found'] = True
                debug_info['unique_statuses'].add(str(row.get('_value')))
            if row.get('_field') == 'fault':
                debug_info['fault_field_found'] = True
                debug_info['unique_faults'].add(str(row.get('_value')))
            
            # Check tags
            if row.get('status'):
                debug_info['status_tag_found'] = True
                debug_info['unique_statuses'].add(str(row.get('status')))
            if row.get('fault'):
                debug_info['fault_tag_found'] = True
                debug_info['unique_faults'].add(str(row.get('fault')))
            
            # Sample data
            if len(debug_info['sample_data']) < 5:
                debug_info['sample_data'].append({
                    'panel_id': row.get('panel_id'),
                    'field': row.get('_field'),
                    'value': str(row.get('_value')),
                    'status_tag': row.get('status'),
                    'fault_tag': row.get('fault'),
                    'time': row.get('_time').isoformat() if row.get('_time') else None
                })
        
        # Convert sets to lists for JSON
        debug_info['unique_statuses'] = list(debug_info['unique_statuses'])
        debug_info['unique_faults'] = list(debug_info['unique_faults'])
        
        return jsonify(debug_info)
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# ============================================================
# /api/strings
# ============================================================

@app.route('/api/strings', methods=['GET'])
def get_string_data():
    """Get per-string aggregated power data - OPTIMIZED."""
    
    start = request.args.get('start', '-1h')
    duration = convert_time_range(start)
    
    # Determine window interval
    window_map = {
        '5m': '30s',
        '15m': '1m',
        '1h': '2m',
        '6h': '10m',
        '24h': '30m',
        '7d': '2h'
    }
    window = window_map.get(duration, '2m')
    
    # Simplified query
    flux = f'''
    from(bucket: "{INFLUX_BUCKET}")
      |> range(start: -{duration})
      |> filter(fn: (r) => r._measurement == "panel_telemetry")
      |> filter(fn: (r) => r._field == "power_w")
      |> aggregateWindow(every: {window}, fn: mean, createEmpty: false)
      |> group(columns: ["string_id", "_time"])
      |> sum()
      |> limit(n: 100)
    '''
    
    try:
        results = execute_flux(flux)
        
        # Organize by string_id
        strings = {}
        for row in results:
            sid = safe_str(row.get('string_id'), 'unknown')
            if sid not in strings:
                strings[sid] = []
            
            time_val = row.get('_time')
            strings[sid].append({
                'timestamp': time_val.isoformat() if time_val else None,
                'total_power_w': safe_float(row.get('_value'))
            })
        
        return jsonify({
            'strings': strings,
            'string_count': len(strings)
        })
        
    except Exception as e:
        print(f"Error in /api/strings: {e}")
        return jsonify({'error': str(e), 'strings': {}, 'string_count': 0}), 500


# ============================================================
# /api/health
# ============================================================

@app.route('/api/health', methods=['GET'])
def health_check():
    """Check API and database health."""
    try:
        # Quick query
        flux = f'''
        from(bucket: "{INFLUX_BUCKET}")
          |> range(start: -30s)
          |> filter(fn: (r) => r._measurement == "panel_telemetry")
          |> limit(n: 1)
        '''
        result = execute_flux(flux)
        
        return jsonify({
            'status': 'healthy',
            'influx_version': '2.x (Flux)',
            'bucket': INFLUX_BUCKET,
            'data_available': len(result) > 0
        })
    except Exception as e:
        return jsonify({
            'status': 'unhealthy',
            'error': str(e)
        }), 500


# ============================================================
# MAIN
# ============================================================

if __name__ == '__main__':
    print("=" * 60)
    print("Solar Panel Telemetry API (Optimized Flux for InfluxDB 2.x)")
    print("=" * 60)
    
    if not all([INFLUX_URL, INFLUX_TOKEN, INFLUX_ORG, INFLUX_BUCKET]):
        print("\nERROR: Missing required environment variables!")
        print(f"  INFLUX_URL: {'SET' if INFLUX_URL else 'MISSING'}")
        print(f"  INFLUX_TOKEN: {'SET' if INFLUX_TOKEN else 'MISSING'}")
        print(f"  INFLUX_ORG: {'SET' if INFLUX_ORG else 'MISSING'}")
        print(f"  INFLUX_BUCKET: {'SET' if INFLUX_BUCKET else 'MISSING'}")
        exit(1)
    
    print(f"\nConfiguration:")
    print(f"  InfluxDB URL: {INFLUX_URL}")
    print(f"  Organization: {INFLUX_ORG}")
    print(f"  Bucket: {INFLUX_BUCKET}")
    print(f"  Query Timeout: 60 seconds")
    print(f"  Query Language: Flux (Optimized)")
    
    print(f"\nOptimizations:")
    print(f"  ✓ Increased timeout to 60s")
    print(f"  ✓ Reduced query complexity")
    print(f"  ✓ Limited result sets")
    print(f"  ✓ Manual pivoting in Python")
    print(f"  ✓ Graceful error handling")
    
    print(f"\nEndpoints:")
    print(f"  GET /api/current   - Latest panel readings (last 2min)")
    print(f"  GET /api/panel/<id> - Panel history (last 100 points)")
    print(f"  GET /api/aggregate - System metrics (max 200 points)")
    print(f"  GET /api/faults    - Faults and warnings (max 100)")
    print(f"  GET /api/strings   - Per-string data (max 100 points)")
    print(f"  GET /api/health    - Health check")
    print(f"  GET /api/debug     - Debug info")
    
    print(f"\nStarting server on port 8080...")
    print(f"Test at: http://localhost:8080/api/health")
    app.run(host='0.0.0.0', port=8080, debug=True)
