#!/usr/bin/env python3
"""
influx_writer.py
Continuously generate and write solar panel telemetry to InfluxDB in real-time.

This script bridges the telemetry generator with InfluxDB, allowing you to:
1. Generate realistic solar panel data
2. Write it to InfluxDB as it's generated
3. See live updates in your dashboard

Usage:
    # First writer: 10 panels → P001..P010
    python influx_writer.py --panels 10 --interval 10 --offset 0

    # Second writer: 20 panels → P011..P030
    python influx_writer.py --panels 20 --interval 10 --offset 10
        
    # Backfill historical data for last 24 hours
    python influx_writer.py --panels 50 --backfill 24h --seed 123
Requirements:
    pip install influxdb-client python-dotenv
"""

import argparse
import os
import sys
import time
import math
import random
from datetime import datetime, timedelta, timezone
from typing import Dict, List
from dataclasses import dataclass

from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

INFLUX_URL = os.getenv('INFLUX_URL')
INFLUX_TOKEN = os.getenv('INFLUX_TOKEN')
INFLUX_ORG = os.getenv('INFLUX_ORG')
INFLUX_BUCKET = os.getenv('INFLUX_BUCKET')

# ----------------------------
# Solar Panel Models (from generator)
# ----------------------------

@dataclass
class PanelSpec:
    panel_id: str
    p_stc_w: float
    v_mppt: float
    i_stc_a: float
    temp_coeff_p: float
    degradation_per_day: float
    efficiency_jitter: float
    orientation_deg: float
    tilt_deg: float
    string_id: str

FAULT_CATALOG = [
    ("NONE", "OK", 0.97),          # 97% no fault
    ("SHADING", "WARNING", 0.015),
    ("HOTSPOT", "FAULT", 0.005),
    ("STRING_OPEN", "FAULT", 0.002),
    ("INVERTER_TRIP", "FAULT", 0.003),
    ("SOILING", "WARNING", 0.005),
]

def generate_panel_fleet(n: int, seed: int, offset: int = 0) -> List[PanelSpec]:
    """Generate a fleet of solar panels with realistic specs."""
    random.seed(seed)
    fleet = []
    for i in range(n):
        global_index = offset + i          # 0-based index across all panels

        p_stc = random.uniform(370, 430)
        v_mppt = random.uniform(33, 40)
        i_stc = p_stc / v_mppt * random.uniform(0.95, 1.05)
        coeff = random.uniform(-0.0045, -0.0035)
        degr = random.uniform(0.00003, 0.00007)
        jitter = random.uniform(0.98, 1.02)
        orient = random.choice([150, 165, 180, 195, 210]) + random.uniform(-5, 5)
        tilt = random.choice([15, 20, 25, 30, 35]) + random.uniform(-2, 2)

        # 5 panels per string, continuous across offsets
        string = f"S{1 + (global_index // 5):02d}"

        fleet.append(PanelSpec(
            panel_id=f"P{global_index + 1:03d}",
            p_stc_w=p_stc,
            v_mppt=v_mppt,
            i_stc_a=i_stc,
            temp_coeff_p=coeff,
            degradation_per_day=degr,
            efficiency_jitter=jitter,
            orientation_deg=orient,
            tilt_deg=tilt,
            string_id=string
        ))
    return fleet

def seconds_since_midnight(t: datetime) -> int:
    """Get seconds elapsed since midnight."""
    return t.hour * 3600 + t.minute * 60 + t.second

def smoothstep(x: float) -> float:
    """Quintic smoothstep for smooth transitions."""
    return 6*x**5 - 15*x**4 + 10*x**3

def diurnal_irradiance_factor(ts: datetime, dl_start: int = 6*3600, dl_end: int = 18*3600) -> float:
    """Calculate solar irradiance factor based on time of day (0..1)."""
    ssm = seconds_since_midnight(ts)
    if ssm <= dl_start or ssm >= dl_end:
        return 0.0
    
    span = dl_end - dl_start
    x = (ssm - dl_start) / span
    base = math.sin(math.pi * x)
    if base < 0:
        return 0.0
    
    shaped = base ** 1.5
    edge = smoothstep(x)
    return max(0.0, min(1.0, shaped * (0.7 + 0.3 * edge)))

def cloud_cover_factor() -> float:
    """Random cloud cover effect (0.6..1.0)."""
    light = random.uniform(0.85, 1.0)
    occasional = 1.0 - random.random() ** 6 * 0.25
    return max(0.6, min(1.0, (light * 0.7 + occasional * 0.3)))

def ambient_temperature_c(ts: datetime, base: float = 26.0) -> float:
    """Calculate ambient temperature based on time of day."""
    ssm = seconds_since_midnight(ts)
    phase = (ssm / 86400.0) * 2 * math.pi
    return base + 8.0 * math.sin(phase - math.pi / 6)

def panel_cell_temp_c(ambient_c: float, irradiance_wm2: float) -> float:
    """Calculate panel cell temperature."""
    return ambient_c + (irradiance_wm2 / 800.0) * 20.0

def assign_fault() -> tuple:
    """Randomly assign a fault based on probabilities."""
    r = random.random()
    acc = 0.0
    for fault, status, p in FAULT_CATALOG:
        acc += p
        if r <= acc:
            return fault, status
    return "NONE", "OK"

def orientation_tilt_modifier(orientation_deg: float, tilt_deg: float) -> float:
    """Calculate efficiency modifier based on orientation and tilt."""
    orient_score = 1.0 - (abs(180 - orientation_deg) / 180.0) * 0.1
    tilt_score = 1.0 - (abs(25 - tilt_deg) / 25.0) * 0.08
    return max(0.85, min(1.05, orient_score * tilt_score))

def compute_telemetry(spec: PanelSpec, ts: datetime, cloud_factor: float) -> Dict:
    """Compute telemetry for a single panel at a given time."""
    # Solar irradiance
    sun_f = diurnal_irradiance_factor(ts)
    irr = 1000.0 * sun_f * cloud_factor
    
    # Temperature
    amb_c = ambient_temperature_c(ts)
    cell_c = panel_cell_temp_c(amb_c, irr)
    
    # Orientation modifier
    orient_mod = orientation_tilt_modifier(spec.orientation_deg, spec.tilt_deg)
    
    # Power calculation
    p_raw = spec.p_stc_w * (irr / 1000.0) * spec.efficiency_jitter * orient_mod
    delta_t = cell_c - 25.0
    p_temp = p_raw * (1.0 + spec.temp_coeff_p * delta_t)
    p_noisy = p_temp * random.uniform(0.98, 1.02)
    p_dc = max(0.0, min(spec.p_stc_w * 1.10, p_noisy))
    
    # Voltage and current
    if p_dc <= 0.1:
        v = spec.v_mppt * random.uniform(0.92, 1.05)
        i = 0.0
    else:
        v = spec.v_mppt * random.uniform(0.97, 1.03)
        i = p_dc / v
    
    if irr < 1.0:
        irr = 0.0
    
    # Faults
    fault, status = assign_fault()
    if fault == "SHADING":
        p_dc *= random.uniform(0.6, 0.9)
    elif fault == "HOTSPOT":
        p_dc *= random.uniform(0.4, 0.8)
        cell_c += random.uniform(3, 8)
    elif fault in ["STRING_OPEN", "INVERTER_TRIP"]:
        p_dc = 0.0
        i = 0.0
    elif fault == "SOILING":
        p_dc *= random.uniform(0.8, 0.95)
    
    if v > 0:
        i = p_dc / v
    
    return {
        "timestamp": ts,
        "panel_id": spec.panel_id,
        "string_id": spec.string_id,
        "status": status,
        "fault": fault,
        "power_w": round(p_dc, 2),
        "voltage_v": round(v, 2),
        "current_a": round(i, 3),
        "irradiance_wm2": round(irr, 1),
        "ambient_temp_c": round(amb_c, 2),
        "cell_temp_c": round(cell_c, 2),
        "orientation_deg": round(spec.orientation_deg, 1),
        "tilt_deg": round(spec.tilt_deg, 1)
    }

def write_to_influx(client, bucket: str, org: str, fleet: List[PanelSpec], timestamp: datetime):
    """Generate telemetry for all panels and write to InfluxDB."""
    write_api = client.write_api(write_options=SYNCHRONOUS)
    
    # Shared cloud factor with per-string variance
    site_cloud = cloud_cover_factor()
    string_clouds = {}
    
    points = []
    for spec in fleet:
        if spec.string_id not in string_clouds:
            string_clouds[spec.string_id] = max(0.6, min(1.0, site_cloud * random.uniform(0.95, 1.05)))
        
        data = compute_telemetry(spec, timestamp, string_clouds[spec.string_id])
        
        point = Point("panel_telemetry") \
            .tag("panel_id", data["panel_id"]) \
            .tag("string_id", data["string_id"]) \
            .tag("status", data["status"]) \
            .tag("fault", data["fault"]) \
            .field("power_w", data["power_w"]) \
            .field("voltage_v", data["voltage_v"]) \
            .field("current_a", data["current_a"]) \
            .field("irradiance_wm2", data["irradiance_wm2"]) \
            .field("ambient_temp_c", data["ambient_temp_c"]) \
            .field("cell_temp_c", data["cell_temp_c"]) \
            .field("orientation_deg", data["orientation_deg"]) \
            .field("tilt_deg", data["tilt_deg"]) \
            .time(timestamp, WritePrecision.S)
        
        points.append(point)
    
    write_api.write(bucket=bucket, org=org, record=points)
    
    # Calculate summary stats
    total_power = sum(p._fields['power_w'] for p in points)
    active_count = sum(1 for p in points if p._fields['power_w'] > 1)
    avg_irr = sum(p._fields['irradiance_wm2'] for p in points) / len(points)
    
    return total_power, active_count, avg_irr

def parse_duration(duration_str: str) -> timedelta:
    """Parse duration string like '24h', '2d', '30m'."""
    if duration_str.endswith('h'):
        return timedelta(hours=int(duration_str[:-1]))
    elif duration_str.endswith('d'):
        return timedelta(days=int(duration_str[:-1]))
    elif duration_str.endswith('m'):
        return timedelta(minutes=int(duration_str[:-1]))
    else:
        raise ValueError(f"Invalid duration format: {duration_str}")

def main():
    parser = argparse.ArgumentParser(description="Write solar telemetry to InfluxDB")
    parser.add_argument("--panels", type=int, default=5, help="Number of panels to simulate")
    parser.add_argument("--interval", type=int, default=10, help="Seconds between updates")
    parser.add_argument("--duration", type=int, help="Minutes to run (omit for continuous)")
    parser.add_argument("--backfill", type=str, help="Backfill historical data (e.g., '24h', '7d')")
    parser.add_argument("--seed", type=int, default=42, help="Random seed")
    parser.add_argument(
        "--offset",
        type=int,
        default=0,
        help="Panel ID offset (for running multiple writers)"
    )
    args = parser.parse_args()
    
    # Validate environment
    if not all([INFLUX_URL, INFLUX_TOKEN, INFLUX_ORG, INFLUX_BUCKET]):
        print("ERROR: Missing InfluxDB environment variables!")
        print("Please ensure .env contains: INFLUX_URL, INFLUX_TOKEN, INFLUX_ORG, INFLUX_BUCKET")
        sys.exit(1)
    
    # Initialize
    client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
    fleet = generate_panel_fleet(args.panels, args.seed, args.offset)

    
    print("=" * 60)
    print("Solar Panel Telemetry Writer")
    print("=" * 60)
    print(f"Panels: {args.panels} (offset {args.offset})")
    print(f"Strings: {len(set(p.string_id for p in fleet))}")
    print(f"Update Interval: {args.interval}s")
    print(f"InfluxDB: {INFLUX_URL}")
    print(f"Bucket: {INFLUX_BUCKET}")
    print("=" * 60)
    
    iteration = 0  # Initialize before try block
    
    try:
        # Backfill mode
        if args.backfill:
            duration = parse_duration(args.backfill)
            start_time = datetime.now(timezone.utc) - duration
            current_time = start_time
            end_time = datetime.now(timezone.utc)
            
            print(f"\n📊 Backfilling data from {start_time} to {end_time}")
            print(f"This may take a few moments...\n")
            
            count = 0
            while current_time <= end_time:
                write_to_influx(client, INFLUX_BUCKET, INFLUX_ORG, fleet, current_time)
                count += 1
                if count % 10 == 0:
                    print(f"Written {count} time points... (current: {current_time.strftime('%H:%M:%S')})")
                current_time += timedelta(seconds=args.interval)
            
            print(f"\n✅ Backfill complete! Written {count} time points.")
        
        # Live mode
        else:
            print("\n🔴 Starting live telemetry generation...")
            print("Press Ctrl+C to stop\n")
            
            start_time = time.time()
            iteration = 0
            
            while True:
                timestamp = datetime.now(timezone.utc)
                total_power, active_count, avg_irr = write_to_influx(
                    client, INFLUX_BUCKET, INFLUX_ORG, fleet, timestamp
                )
                
                iteration += 1
                print(f"[{timestamp.strftime('%H:%M:%S')}] "
                      f"Power: {total_power/1000:.2f}kW | "
                      f"Active: {active_count}/{args.panels} | "
                      f"Irradiance: {avg_irr:.1f}W/m²")
                
                # Check duration limit
                if args.duration and (time.time() - start_time) >= args.duration * 60:
                    print(f"\n✅ Completed {args.duration} minute run.")
                    break
                
                time.sleep(args.interval)
    
    except KeyboardInterrupt:
        print("\n\n⏹️  Stopped by user")
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        client.close()
        print("\n" + "=" * 60)
        print(f"Total iterations: {iteration}")
        print("=" * 60)

if __name__ == "__main__":
    main()