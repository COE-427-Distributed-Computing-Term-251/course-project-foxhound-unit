import os
import json
import gzip
from datetime import datetime, timedelta
from collections import defaultdict
from flask import Flask, jsonify, render_template, send_from_directory, request
from flask_cors import CORS

try:
    from influxdb_client import InfluxDBClient
    INFLUX_AVAILABLE = True
except ImportError:
    INFLUX_AVAILABLE = False
    print("influxdb-client not installed, using JSONL fallback only")

#===========================================================================
# SETUP
#===========================================================================

app = Flask(__name__, static_folder='static', template_folder='templates')
CORS(app)

# Configuration
#NEEDS UPDATE!!!
INFLUX_URL = os.getenv("INFLUX_URL")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN")
INFLUX_ORG = os.getenv("INFLUX_ORG")
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET")
FALLBACK_FILE = 'ingest_fallback.jsonl'

# InfluxDB client
influx_client = None
query_api = None
try:
    influx_client = InfluxDBClient(
        url=INFLUX_URL,
        token=INFLUX_TOKEN,
        org=INFLUX_ORG
    )
    query_api = influx_client.query_api()
    print("InfluxDB connected!!")
except Exception as e:
    print("X InfluxDB disabled:", e)




# ============================================================================
# INFLUXDB FUNCTIONS
# ============================================================================

def init_influx():
    global influx_client, query_api
    
    if not INFLUX_AVAILABLE:
        print("InfluxDB client library not available")
        return False
    
    if not INFLUX_TOKEN or not INFLUX_URL:
        print("InfluxDB not configured")
        return False
    
    try:
        influx_client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
        query_api = influx_client.query_api()
        # Test connection
        query_api.query(f'from(bucket:"{INFLUX_BUCKET}") |> range(start: -1s) |> limit(n:1)')
        print(f"Connected to InfluxDB at {INFLUX_URL}!")
        return True
    except Exception as e:
        print(f"X Failed to connect to InfluxDB: {e}")
        influx_client = None
        query_api = None
        return False

def read_jsonl_fallback(minutes=30, max_lines=10000):
    if not os.path.exists(FALLBACK_FILE):
        return []
    
    cutoff = datetime.now(datetime.timezone.utc) - timedelta(minutes=minutes)
    records = []
    
    try:
        with open(FALLBACK_FILE, 'rb') as f:
            # Seek to end and read backwards
            f.seek(0, 2)
            file_size = f.tell()
            block_size = 8192
            lines = []
            
            position = file_size
            while position > 0 and len(lines) < max_lines:
                read_size = min(block_size, position)
                position -= read_size
                f.seek(position)
                chunk = f.read(read_size).decode('utf-8', errors='ignore')
                lines = chunk.split('\n') + lines
            
            for line in lines[-max_lines:]:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                    ts_str = obj.get('timestamp_utc', '')
                    if ts_str:
                        ts = datetime.fromisoformat(ts_str.replace('Z', '+00:00'))
                        if ts >= cutoff:
                            records.append(obj)
                except Exception:
                    continue
                    
    except Exception as e:
        print(f"Error reading fallback file: {e}")
    
    return records



# ============================================================================
# INFLUXDB QUERY FUNCTIONS
# ============================================================================

def query_influx_summary():
    """Get summary metrics from InfluxDB"""
    if not query_api:
        return None
    
    try:
        # Total power (last 30s average)
        power_query = f'''
from(bucket: "{INFLUX_BUCKET}")
  |> range(start: -30s)
  |> filter(fn: (r) => r["_measurement"] == "panel_telemetry")
  |> filter(fn: (r) => r["_field"] == "power_w")
  |> mean()
  |> group()
  |> sum()
'''
        power_result = query_api.query(power_query)
        total_power = 0.0
        for table in power_result:
            for record in table.records:
                total_power += record.get_value()
        
        # Active panels (distinct in last 2 min)
        panels_query = f'''
from(bucket: "{INFLUX_BUCKET}")
  |> range(start: -2m)
  |> filter(fn: (r) => r["_measurement"] == "panel_telemetry")
  |> keep(columns: ["panel_id"])
  |> distinct(column: "panel_id")
  |> count()
'''
        panels_result = query_api.query(panels_query)
        active_panels = 0
        for table in panels_result:
            for record in table.records:
                active_panels = record.get_value()
        
        # Fault counts (last hour)
        faults_query = f'''
from(bucket: "{INFLUX_BUCKET}")
  |> range(start: -1h)
  |> filter(fn: (r) => r["_measurement"] == "panel_telemetry")
  |> filter(fn: (r) => r["fault"] != "NONE")
  |> group(columns: ["fault"])
  |> count()
'''
        faults_result = query_api.query(faults_query)
        fault_counts = {}
        for table in faults_result:
            for record in table.records:
                fault_type = record.values.get('fault', 'UNKNOWN')
                fault_counts[fault_type] = record.get_value()
        
        # Status distribution (last 5 min)
        status_query = f'''
from(bucket: "{INFLUX_BUCKET}")
  |> range(start: -5m)
  |> filter(fn: (r) => r["_measurement"] == "panel_telemetry")
  |> keep(columns: ["status"])
  |> group(columns: ["status"])
  |> count()
'''
        status_result = query_api.query(status_query)
        status_counts = {}
        for table in status_result:
            for record in table.records:
                status = record.values.get('status', 'UNKNOWN')
                status_counts[status] = record.get_value()
        
        return {
            'total_power_w': round(total_power, 2),
            'active_panels': active_panels,
            'fault_counts': fault_counts,
            'status_counts': status_counts,
            'timestamp': datetime.utcnow().isoformat() + 'Z'
        }
        
    except Exception as e:
        print(f"Error querying InfluxDB summary: {e}")
        return None

def query_influx_strings():
    """Get per-string aggregates from InfluxDB"""
    if not query_api:
        return None
    
    try:
        # Power per string (last 30s)
        query = f'''
from(bucket: "{INFLUX_BUCKET}")
  |> range(start: -30s)
  |> filter(fn: (r) => r["_measurement"] == "panel_telemetry")
  |> filter(fn: (r) => r["_field"] == "power_w")
  |> group(columns: ["string_id"])
  |> mean()
  |> group(columns: ["string_id"])
  |> sum()
'''
        result = query_api.query(query)
        strings = {}
        
        for table in result:
            for record in table.records:
                string_id = record.values.get('string_id', 'UNKNOWN')
                power = record.get_value()
                
                if string_id not in strings:
                    strings[string_id] = {
                        'string_id': string_id,
                        'total_power_w': 0.0,
                        'panel_count': 0
                    }
                strings[string_id]['total_power_w'] = round(power, 2)
        
        # Panel counts per string
        count_query = f'''
from(bucket: "{INFLUX_BUCKET}")
  |> range(start: -2m)
  |> filter(fn: (r) => r["_measurement"] == "panel_telemetry")
  |> keep(columns: ["string_id", "panel_id"])
  |> group(columns: ["string_id"])
  |> distinct(column: "panel_id")
  |> count()
'''
        count_result = query_api.query(count_query)
        for table in count_result:
            for record in table.records:
                string_id = record.values.get('string_id', 'UNKNOWN')
                if string_id in strings:
                    strings[string_id]['panel_count'] = record.get_value()
        
        return list(strings.values())
        
    except Exception as e:
        print(f"Error querying InfluxDB strings: {e}")
        return None

def query_influx_timeseries(entity_type='panel', entity_id=None, minutes=30):
    """Get time-series data from InfluxDB"""
    if not query_api or not entity_id:
        return None
    
    try:
        filter_col = "panel_id" if entity_type == "panel" else "string_id"
        
        query = f'''
from(bucket: "{INFLUX_BUCKET}")
  |> range(start: -{minutes}m)
  |> filter(fn: (r) => r["_measurement"] == "panel_telemetry")
  |> filter(fn: (r) => r["{filter_col}"] == "{entity_id}")
  |> filter(fn: (r) => r["_field"] == "power_w" or r["_field"] == "irradiance_wm2" or 
                       r["_field"] == "ambient_temp_c" or r["_field"] == "cell_temp_c")
  |> aggregateWindow(every: 1m, fn: mean, createEmpty: false)
'''
        result = query_api.query(query)
        
        series = defaultdict(list)
        
        for table in result:
            field = table.records[0].get_field() if table.records else None
            for record in table.records:
                ts = record.get_time().isoformat()
                val = record.get_value()
                series[field].append({'timestamp': ts, 'value': round(val, 2)})
        
        return dict(series)
        
    except Exception as e:
        print(f"Error querying InfluxDB timeseries: {e}")
        return None

# ==========================
# JSONL FALLBACK FUNCTIONS
# ==========================

def compute_fallback_summary(records):
    """
    Compute summary from JSONL records
    """
    if not records:
        return {
            'total_power_w': 0.0,
            'active_panels': 0,
            'fault_counts': {},
            'status_counts': {},
            'timestamp': datetime.utcnow().isoformat() + 'Z'
        }
    
    total_power = 0.0
    panels = set()
    fault_counts = defaultdict(int)
    status_counts = defaultdict(int)
    
    # Get most recent record per panel
    latest_by_panel = {}
    for rec in records:
        panel_id = rec.get('panel_id')
        ts_str = rec.get('timestamp_utc', '')
        if panel_id and ts_str:
            ts = datetime.fromisoformat(ts_str.replace('Z', '+00:00'))
            if panel_id not in latest_by_panel or ts > latest_by_panel[panel_id]['ts']:
                latest_by_panel[panel_id] = {'ts': ts, 'rec': rec}
    
    for panel_data in latest_by_panel.values():
        rec = panel_data['rec']
        panels.add(rec.get('panel_id'))
        total_power += float(rec.get('power_w', 0))
        
        fault = rec.get('fault', 'NONE')
        if fault and fault != 'NONE':
            fault_counts[fault] += 1
        
        status = rec.get('status', 'UNKNOWN')
        if status:
            status_counts[status] += 1
    
    return {
        'total_power_w': round(total_power, 2),
        'active_panels': len(panels),
        'fault_counts': dict(fault_counts),
        'status_counts': dict(status_counts),
        'timestamp': datetime.utcnow().isoformat() + 'Z'
    }

def compute_fallback_strings(records):
    """
    Compute per-string aggregates from JSONL records
    """
    strings = defaultdict(lambda: {'total_power_w': 0.0, 'panels': set()})
    
    #latest record per panel
    latest_by_panel = {}
    for rec in records:
        panel_id = rec.get('panel_id')
        ts_str = rec.get('timestamp_utc', '')
        if panel_id and ts_str:
            ts = datetime.fromisoformat(ts_str.replace('Z', '+00:00'))
            if panel_id not in latest_by_panel or ts > latest_by_panel[panel_id]['ts']:
                latest_by_panel[panel_id] = {'ts': ts, 'rec': rec}
    
    for panel_data in latest_by_panel.values():
        rec = panel_data['rec']
        string_id = rec.get('string_id', 'UNKNOWN')
        power = float(rec.get('power_w', 0))
        panel_id = rec.get('panel_id')
        
        strings[string_id]['total_power_w'] += power
        strings[string_id]['panels'].add(panel_id)
    
    result = []
    for string_id, data in strings.items():
        result.append({
            'string_id': string_id,
            'total_power_w': round(data['total_power_w'], 2),
            'panel_count': len(data['panels'])
        })
    
    return result

def compute_fallback_timeseries(records, entity_type, entity_id):
    """
    Compute time-series from JSONL records
    """
    filter_key = 'panel_id' if entity_type == 'panel' else 'string_id'
    filtered = [r for r in records if r.get(filter_key) == entity_id]
    
    # Group 1-minute buckets
    buckets = defaultdict(lambda: defaultdict(list))
    
    for rec in filtered:
        ts_str = rec.get('timestamp_utc', '')
        if not ts_str:
            continue
        
        ts = datetime.fromisoformat(ts_str.replace('Z', '+00:00'))
        bucket_key = ts.replace(second=0, microsecond=0).isoformat()
        
        for field in ['power_w', 'irradiance_wm2', 'ambient_temp_c', 'cell_temp_c']:
            val = rec.get(field)
            if val is not None:
                buckets[bucket_key][field].append(float(val))
    
    # Compute averages
    series = defaultdict(list)
    for bucket_ts, fields in sorted(buckets.items()):
        for field, values in fields.items():
            avg = sum(values) / len(values)
            series[field].append({
                'timestamp': bucket_ts,
                'value': round(avg, 2)
            })
    
    return dict(series)

# ============================================================================
# API ENDPOINTS
# ============================================================================

@app.route('/')
def index():
    """
    Serve dashboard UI
    """
    return render_template('index.html')

@app.route('/api/summary')
def api_summary():
    """
    Get real-time summary metrics
    """
    # Try InfluxDB first
    data = query_influx_summary()
    
    # Fall back to JSONL
    if data is None:
        records = read_jsonl_fallback(minutes=30)
        data = compute_fallback_summary(records)
        data['source'] = 'jsonl_fallback'
    else:
        data['source'] = 'influxdb'
    
    return jsonify(data)

@app.route('/api/strings')
def api_strings():
    """
    Get per-string aggregates
    """
    # Try InfluxDB first
    data = query_influx_strings()
    
    # Fall back to JSONL
    if data is None:
        records = read_jsonl_fallback(minutes=5)
        data = compute_fallback_strings(records)
        source = 'jsonl_fallback'
    else:
        source = 'influxdb'
    
    return jsonify({'strings': data, 'source': source})

@app.route('/api/timeseries')
def api_timeseries():
    """
    Get time-series data for a panel or string
    """
    entity_type = request.args.get('type', 'panel')  # 'panel' or 'string'
    entity_id = request.args.get('id')
    minutes = int(request.args.get('minutes', 30))
    
    if not entity_id:
        return jsonify({'error': 'Missing id parameter'}), 400
    
    # Try InfluxDB first
    data = query_influx_timeseries(entity_type, entity_id, minutes)
    
    # Fall back to JSONL
    if data is None:
        records = read_jsonl_fallback(minutes=minutes)
        data = compute_fallback_timeseries(records, entity_type, entity_id)
        source = 'jsonl_fallback'
    else:
        source = 'influxdb'
    
    return jsonify({
        'entity_type': entity_type,
        'entity_id': entity_id,
        'series': data,
        'source': source
    })

@app.route('/api/panels')
def api_panels():
    """
    Get list of all panels with metadata
    """
    # Simple implementation: get distinct panels from recent data
    if query_api:
        try:
            query = f'''
from(bucket: "{INFLUX_BUCKET}")
  |> range(start: -5m)
  |> filter(fn: (r) => r["_measurement"] == "panel_telemetry")
  |> keep(columns: ["panel_id", "string_id"])
  |> distinct(column: "panel_id")
  |> limit(n: 1000)
'''
            result = query_api.query(query)
            panels = []
            seen = set()
            for table in result:
                for record in table.records:
                    panel_id = record.values.get('panel_id')
                    string_id = record.values.get('string_id')
                    if panel_id and panel_id not in seen:
                        panels.append({'panel_id': panel_id, 'string_id': string_id})
                        seen.add(panel_id)
            return jsonify({'panels': panels, 'source': 'influxdb'})
        except Exception:
            pass
    
    # Fallback
    records = read_jsonl_fallback(minutes=5)
    panels_dict = {}
    for rec in records:
        panel_id = rec.get('panel_id')
        string_id = rec.get('string_id')
        if panel_id and panel_id not in panels_dict:
            panels_dict[panel_id] = {'panel_id': panel_id, 'string_id': string_id}
    
    return jsonify({'panels': list(panels_dict.values()), 'source': 'jsonl_fallback'})

# ============================================================================
# MAIN
# ============================================================================

if __name__ == '__main__':
    print("=" * 60)
    print("Solar Telemetry Dashboard Backend")
    print("=" * 60)
    
    # Initialize InfluxDB connection
    influx_ok = init_influx()
    
    if not influx_ok:
        print("Running with JSONL fallback mode only")
        print(f"Reading from: {FALLBACK_FILE}")
    
    print("\nStarting Flask server on http://localhost:5000")
    print("   Dashboard: http://localhost:5000/")
    print("=" * 60 + "\n")
    
    app.run(host='0.0.0.0', port=5000, debug=True)
