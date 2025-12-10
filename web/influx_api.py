#!/usr/bin/env python3
"""
influx_api.py
Flask-based REST API for InfluxDB 3.x using SQL queries.

InfluxDB 3.x uses SQL instead of Flux. This API is designed for that.

Endpoints:
  GET /api/current - Latest readings for all panels
  GET /api/panel/<panel_id> - Historical data for specific panel
  GET /api/aggregate - System-wide aggregated metrics
  GET /api/faults - Current faults and warnings
  GET /api/strings - Per-string aggregated data
  GET /api/health - Health check
  GET /api/debug - Debug info

Requirements:
  pip install flask flask-cors influxdb-client-3 python-dotenv pyarrow
  
  OR for older client:
  pip install flask flask-cors influxdb-client python-dotenv
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
# For InfluxDB 3.x, bucket is sometimes called database
INFLUX_DATABASE = os.getenv('INFLUX_DATABASE', INFLUX_BUCKET)

app = Flask(__name__)
CORS(app)

# Try to use InfluxDB 3 client first, fall back to v2 client
try:
    from influxdb_client_3 import InfluxDBClient3
    client = InfluxDBClient3(
        host=INFLUX_URL.replace('https://', '').replace('http://', '').rstrip('/'),
        token=INFLUX_TOKEN,
        database=INFLUX_DATABASE
    )
    INFLUX_VERSION = 3
    print("Using InfluxDB 3.x client")
except ImportError:
    from influxdb_client import InfluxDBClient
    client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
    INFLUX_VERSION = 2
    print("Using InfluxDB 2.x client (will try SQL via Flight)")


def execute_sql(sql_query):
    """Execute SQL query and return results as list of dicts."""
    try:
        if INFLUX_VERSION == 3:
            # InfluxDB 3.x native client
            table = client.query(sql_query)
            df = table.to_pandas()
            return df.to_dict('records')
        else:
            # Try using the v2 client with Flight SQL
            # This requires the database to support SQL
            from influxdb_client.client.flux_table import FluxStructureEncoder
            import json
            
            # For InfluxDB Cloud/3.x accessed via v2 client
            # We need to use a different approach
            query_api = client.query_api()
            
            # Try Flux query that mimics SQL behavior
            # This is a fallback - won't work as well
            raise Exception("SQL not supported with v2 client, needs Flux")
            
    except Exception as e:
        print(f"SQL Query Error: {e}")
        print(f"Query was: {sql_query}")
        raise


def execute_flux(flux_query):
    """Execute Flux query (fallback for v2)."""
    if INFLUX_VERSION == 3:
        raise Exception("Flux not supported in v3 client")
    
    query_api = client.query_api()
    tables = query_api.query(flux_query, org=INFLUX_ORG)
    
    results = []
    for table in tables:
        for record in table.records:
            results.append(dict(record.values))
    return results


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


# ============================================================
# /api/debug
# ============================================================

@app.route('/api/debug', methods=['GET'])
def debug_info():
    """Debug endpoint to check database connectivity and data."""
    info = {
        'influx_version': INFLUX_VERSION,
        'config': {
            'url': INFLUX_URL,
            'org': INFLUX_ORG,
            'bucket': INFLUX_BUCKET,
            'database': INFLUX_DATABASE,
        },
        'tests': {}
    }
    
    try:
        # Test 1: Simple count
        sql = f'''
        SELECT COUNT(*) as cnt 
        FROM "panel_telemetry" 
        WHERE time >= now() - interval '1 hour'
        '''
        result = execute_sql(sql)
        info['tests']['count_1h'] = result[0]['cnt'] if result else 0
        
        # Test 2: Sample data
        sql = f'''
        SELECT * 
        FROM "panel_telemetry" 
        WHERE time >= now() - interval '1 minute'
        ORDER BY time DESC
        LIMIT 5
        '''
        result = execute_sql(sql)
        info['tests']['sample_data'] = result
        
        # Test 3: Unique panels
        sql = f'''
        SELECT DISTINCT panel_id 
        FROM "panel_telemetry" 
        WHERE time >= now() - interval '5 minutes'
        '''
        result = execute_sql(sql)
        info['tests']['unique_panels'] = [r['panel_id'] for r in result]
        
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
    """Get latest readings for all panels."""
    
    # Get the most recent reading for each panel
    sql = f'''
    SELECT 
        time,
        panel_id,
        string_id,
        status,
        fault,
        power_w,
        voltage_v,
        current_a,
        irradiance_wm2,
        ambient_temp_c,
        cell_temp_c,
        orientation_deg,
        tilt_deg
    FROM "panel_telemetry"
    WHERE time >= now() - interval '5 minutes'
    ORDER BY time DESC
    '''
    
    try:
        results = execute_sql(sql)
        
        # Get only the latest reading per panel
        latest_by_panel = {}
        for row in results:
            panel_id = row.get('panel_id', 'UNKNOWN')
            if panel_id not in latest_by_panel:
                latest_by_panel[panel_id] = {
                    'timestamp': row.get('time').isoformat() if hasattr(row.get('time'), 'isoformat') else str(row.get('time')),
                    'panel_id': panel_id,
                    'string_id': safe_str(row.get('string_id'), 'UNKNOWN'),
                    'status': safe_str(row.get('status'), 'UNKNOWN'),
                    'fault': safe_str(row.get('fault'), 'NONE'),
                    'power_w': safe_float(row.get('power_w')),
                    'voltage_v': safe_float(row.get('voltage_v')),
                    'current_a': safe_float(row.get('current_a')),
                    'irradiance_wm2': safe_float(row.get('irradiance_wm2')),
                    'ambient_temp_c': safe_float(row.get('ambient_temp_c')),
                    'cell_temp_c': safe_float(row.get('cell_temp_c')),
                    'orientation_deg': safe_float(row.get('orientation_deg')),
                    'tilt_deg': safe_float(row.get('tilt_deg'))
                }
        
        panel_list = list(latest_by_panel.values())
        
        return jsonify({
            'data': panel_list,
            'count': len(panel_list),
            'total_rows_queried': len(results)
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# ============================================================
# /api/panel/<id>
# ============================================================

@app.route('/api/panel/<panel_id>', methods=['GET'])
def get_panel_history(panel_id):
    """Get historical data for a specific panel."""
    
    # Parse time range
    start = request.args.get('start', '-1h')
    
    # Convert relative time to SQL interval
    interval_map = {
        '-5m': '5 minutes',
        '-15m': '15 minutes',
        '-1h': '1 hour',
        '-6h': '6 hours',
        '-24h': '24 hours',
        '-7d': '7 days'
    }
    interval = interval_map.get(start, '1 hour')
    
    sql = f'''
    SELECT 
        time,
        power_w,
        voltage_v,
        current_a,
        irradiance_wm2,
        ambient_temp_c,
        cell_temp_c,
        status,
        fault
    FROM "panel_telemetry"
    WHERE panel_id = '{panel_id}'
      AND time >= now() - interval '{interval}'
    ORDER BY time ASC
    '''
    
    try:
        results = execute_sql(sql)
        
        data = []
        for row in results:
            data.append({
                'timestamp': row.get('time').isoformat() if hasattr(row.get('time'), 'isoformat') else str(row.get('time')),
                'power_w': safe_float(row.get('power_w')),
                'voltage_v': safe_float(row.get('voltage_v')),
                'current_a': safe_float(row.get('current_a')),
                'irradiance_wm2': safe_float(row.get('irradiance_wm2')),
                'ambient_temp_c': safe_float(row.get('ambient_temp_c')),
                'cell_temp_c': safe_float(row.get('cell_temp_c')),
                'status': safe_str(row.get('status'), 'UNKNOWN'),
                'fault': safe_str(row.get('fault'), 'NONE')
            })
        
        return jsonify({
            'panel_id': panel_id,
            'data': data,
            'count': len(data)
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# ============================================================
# /api/aggregate
# ============================================================

@app.route('/api/aggregate', methods=['GET'])
def get_aggregate_metrics():
    """Get aggregated system-wide metrics."""
    
    start = request.args.get('start', '-1h')
    
    interval_map = {
        '-5m': ('5 minutes', '10 seconds'),
        '-15m': ('15 minutes', '30 seconds'),
        '-1h': ('1 hour', '1 minute'),
        '-6h': ('6 hours', '5 minutes'),
        '-24h': ('24 hours', '15 minutes'),
        '-7d': ('7 days', '1 hour')
    }
    
    time_range, bucket_interval = interval_map.get(start, ('1 hour', '1 minute'))
    
    sql = f'''
    SELECT 
        DATE_BIN(interval '{bucket_interval}', time) as time_bucket,
        SUM(power_w) as total_power_w,
        AVG(irradiance_wm2) as avg_irradiance_wm2,
        AVG(ambient_temp_c) as avg_ambient_temp_c,
        COUNT(*) as sample_count
    FROM "panel_telemetry"
    WHERE time >= now() - interval '{time_range}'
    GROUP BY time_bucket
    ORDER BY time_bucket ASC
    '''
    
    try:
        results = execute_sql(sql)
        
        timeseries = []
        for row in results:
            time_val = row.get('time_bucket')
            timeseries.append({
                'timestamp': time_val.isoformat() if hasattr(time_val, 'isoformat') else str(time_val),
                'avg_power_w': safe_float(row.get('total_power_w')),
                'avg_irradiance_wm2': safe_float(row.get('avg_irradiance_wm2')),
                'avg_ambient_temp_c': safe_float(row.get('avg_ambient_temp_c')),
                'sample_count': int(row.get('sample_count', 0))
            })
        
        return jsonify({
            'timeseries': timeseries,
            'count': len(timeseries),
            'bucket_interval': bucket_interval
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# ============================================================
# /api/faults
# ============================================================

@app.route('/api/faults', methods=['GET'])
def get_faults():
    """Get current faults and warnings."""
    
    start = request.args.get('start', '-1h')
    
    interval_map = {
        '-5m': '5 minutes',
        '-15m': '15 minutes',
        '-1h': '1 hour',
        '-6h': '6 hours',
        '-24h': '24 hours',
        '-7d': '7 days'
    }
    interval = interval_map.get(start, '1 hour')
    
    sql = f'''
    SELECT 
        time,
        panel_id,
        string_id,
        fault,
        status,
        power_w,
        voltage_v,
        current_a
    FROM "panel_telemetry"
    WHERE time >= now() - interval '{interval}'
      AND (fault != 'NONE' OR status != 'OK')
    ORDER BY time DESC
    LIMIT 1000
    '''
    
    try:
        results = execute_sql(sql)
        
        faults = []
        for row in results:
            faults.append({
                'timestamp': row.get('time').isoformat() if hasattr(row.get('time'), 'isoformat') else str(row.get('time')),
                'panel_id': safe_str(row.get('panel_id'), 'UNKNOWN'),
                'string_id': safe_str(row.get('string_id'), 'UNKNOWN'),
                'fault': safe_str(row.get('fault')),
                'status': safe_str(row.get('status')),
                'power_w': safe_float(row.get('power_w')),
                'voltage_v': safe_float(row.get('voltage_v')),
                'current_a': safe_float(row.get('current_a'))
            })
        
        return jsonify({
            'faults': faults,
            'count': len(faults)
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# ============================================================
# /api/strings
# ============================================================

@app.route('/api/strings', methods=['GET'])
def get_string_data():
    """Get per-string aggregated power data."""
    
    start = request.args.get('start', '-1h')
    
    interval_map = {
        '-5m': ('5 minutes', '10 seconds'),
        '-15m': ('15 minutes', '30 seconds'),
        '-1h': ('1 hour', '1 minute'),
        '-6h': ('6 hours', '5 minutes'),
        '-24h': ('24 hours', '15 minutes'),
        '-7d': ('7 days', '1 hour')
    }
    
    time_range, bucket_interval = interval_map.get(start, ('1 hour', '1 minute'))
    
    sql = f'''
    SELECT 
        string_id,
        DATE_BIN(interval '{bucket_interval}', time) as time_bucket,
        SUM(power_w) as total_power_w
    FROM "panel_telemetry"
    WHERE time >= now() - interval '{time_range}'
    GROUP BY string_id, time_bucket
    ORDER BY string_id, time_bucket ASC
    '''
    
    try:
        results = execute_sql(sql)
        
        # Organize by string_id
        strings = {}
        for row in results:
            sid = safe_str(row.get('string_id'), 'unknown')
            if sid not in strings:
                strings[sid] = []
            
            time_val = row.get('time_bucket')
            strings[sid].append({
                'timestamp': time_val.isoformat() if hasattr(time_val, 'isoformat') else str(time_val),
                'total_power_w': safe_float(row.get('total_power_w'))
            })
        
        return jsonify({
            'strings': strings,
            'string_count': len(strings)
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# ============================================================
# /api/health
# ============================================================

@app.route('/api/health', methods=['GET'])
def health_check():
    """Check API and database health."""
    try:
        # Try a simple query
        sql = "SELECT 1 as test"
        result = execute_sql(sql)
        
        return jsonify({
            'status': 'healthy',
            'influx_version': INFLUX_VERSION,
            'database': INFLUX_DATABASE
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
    print("Solar Panel Telemetry API (SQL Version for InfluxDB 3.x)")
    print("=" * 60)
    
    if not all([INFLUX_URL, INFLUX_TOKEN, INFLUX_DATABASE]):
        print("\nERROR: Missing required environment variables!")
        print(f"  INFLUX_URL: {'SET' if INFLUX_URL else 'MISSING'}")
        print(f"  INFLUX_TOKEN: {'SET' if INFLUX_TOKEN else 'MISSING'}")
        print(f"  INFLUX_ORG: {'SET' if INFLUX_ORG else 'MISSING (optional for v3)'}")
        print(f"  INFLUX_BUCKET/DATABASE: {'SET' if INFLUX_DATABASE else 'MISSING'}")
        exit(1)
    
    print(f"\nConfiguration:")
    print(f"  InfluxDB URL: {INFLUX_URL}")
    print(f"  Database: {INFLUX_DATABASE}")
    print(f"  Client Version: {INFLUX_VERSION}")
    
    print(f"\nEndpoints:")
    print(f"  GET /api/current   - Latest panel readings")
    print(f"  GET /api/panel/<id> - Panel history")
    print(f"  GET /api/aggregate - System metrics")
    print(f"  GET /api/faults    - Faults and warnings")
    print(f"  GET /api/strings   - Per-string data")
    print(f"  GET /api/health    - Health check")
    print(f"  GET /api/debug     - Debug info")
    
    print(f"\nStarting server on port 8080...")
    app.run(host='0.0.0.0', port=8080, debug=True)