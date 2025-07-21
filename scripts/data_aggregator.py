import os
import httpx
import json
import asyncio
import math
import copy
from datetime import datetime, timezone

PROCESSOR_URL = os.getenv("PROCESSOR_URL", "http://localhost:8001")
DATA_DIR = "/opt/hoarder_map/frontend/data"
MAX_JUMP_KM = 5.0
RDP_EPSILON = 0.00008
CHAIKIN_ITERATIONS = 4

def deep_merge(source, destination):
    for key, value in source.items():
        if isinstance(value, dict):
            node = destination.setdefault(key, {})
            deep_merge(value, node)
        else:
            destination[key] = value
    return destination

def haversine(lon1, lat1, lon2, lat2):
    R = 6371.0
    try:
        lon1, lat1, lon2, lat2 = map(math.radians, [float(lon1), float(lat1), float(lon2), float(lat2)])
        dlon, dlat = lon2 - lon1, lat2 - lat1
        a = math.sin(dlat / 2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2)**2
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        return R * c
    except (ValueError, TypeError):
        return float('inf')

def prune_state_for_frontend(state):
    if not isinstance(state, dict): return {}
    pruned = {}
    if state.get('identity'): pruned['identity'] = {'device_name': state['identity'].get('device_name')}
    if state.get('power'): pruned['power'] = {'battery_percent': state['power'].get('battery_percent')}
    network_state = state.get('network', {})
    if isinstance(network_state, dict):
        pruned['network'] = {'type': network_state.get('type'), 'operator': network_state.get('operator')}
        if isinstance(network_state.get('cellular'), dict):
            pruned['network']['signal_strength'] = network_state['cellular'].get('signal_strength')
    env_state = state.get('environment', {})
    if isinstance(env_state, dict):
        pruned['environment'] = {}
        if isinstance(env_state.get('weather'), dict):
            pruned['environment']['weather'] = {k: env_state['weather'].get(k) for k in ['description', 'temperature', 'assessment', 'humidity']}
        if isinstance(env_state.get('wind'), dict):
            pruned['environment']['wind'] = {k: env_state['wind'].get(k) for k in ['speed', 'description', 'direction']}
    return pruned

def extract_point(record_state):
    try:
        loc = record_state['location']
        ts_str = record_state['diagnostics']['timestamps']['device_event_timestamp_utc']
        return {'lon': float(loc['longitude']), 'lat': float(loc['latitude']), 'ts': datetime.strptime(ts_str, '%d.%m.%Y %H:%M:%S %Z').replace(tzinfo=timezone.utc).isoformat(), 'state': record_state}
    except (KeyError, TypeError, ValueError, AttributeError):
        return None

def perpendicular_distance(pt, start, end):
    try:
        return abs((end['lat'] - start['lat']) * pt['lon'] - (end['lon'] - start['lon']) * pt['lat'] + end['lon'] * start['lat'] - end['lat'] * start['lon']) / math.sqrt((end['lat'] - start['lat'])**2 + (end['lon'] - start['lon'])**2)
    except (ValueError, TypeError, ZeroDivisionError):
        return 0

def rdp_simplify(points, epsilon):
    if len(points) < 3: return points
    dmax, index = 0, 0
    for i in range(1, len(points) - 1):
        d = perpendicular_distance(points[i], points[0], points[-1])
        if d > dmax: index, dmax = i, d
    if dmax > epsilon:
        return rdp_simplify(points[:index+1], epsilon)[:-1] + rdp_simplify(points[index:], epsilon)
    else:
        return [points[0], points[-1]]

def chaikin_smooth(points, iterations):
    for _ in range(iterations):
        if len(points) < 2: return points
        smoothed = [points[0]]
        for i in range(len(points) - 1):
            p0, p1 = points[i], points[i+1]
            dt0, dt1 = datetime.fromisoformat(p0['ts']).timestamp(), datetime.fromisoformat(p1['ts']).timestamp()
            q = {'lon': 0.75*p0['lon'] + 0.25*p1['lon'], 'lat': 0.75*p0['lat'] + 0.25*p1['lat'], 'ts': datetime.fromtimestamp(0.75*dt0 + 0.25*dt1, tz=timezone.utc).isoformat(), 'state': p0['state']}
            r = {'lon': 0.25*p0['lon'] + 0.75*p1['lon'], 'lat': 0.25*p0['lat'] + 0.75*p1['lat'], 'ts': datetime.fromtimestamp(0.25*dt0 + 0.75*dt1, tz=timezone.utc).isoformat(), 'state': p1['state']}
            smoothed.extend([q, r])
        smoothed.append(points[-1])
        points = smoothed
    return points

def segment_and_process_points(points):
    if not points: return []
    all_features, current_segment, last_point = [], [], None
    for point in points:
        if last_point and haversine(last_point['lon'], last_point['lat'], point['lon'], point['lat']) > MAX_JUMP_KM:
            if len(current_segment) > 2:
                simplified = rdp_simplify(current_segment, RDP_EPSILON)
                smoothed = chaikin_smooth(simplified, CHAIKIN_ITERATIONS)
                all_features.append({"type": "Feature", "geometry": {"type": "LineString", "coordinates": [[p['lon'], p['lat']] for p in smoothed]}, "properties": {"time": [p['ts'] for p in smoothed], "states": [prune_state_for_frontend(p['state']) for p in smoothed]}})
            current_segment = []
        current_segment.append(point)
        last_point = point
    if len(current_segment) > 2:
        simplified = rdp_simplify(current_segment, RDP_EPSILON)
        smoothed = chaikin_smooth(simplified, CHAIKIN_ITERATIONS)
        all_features.append({"type": "Feature", "geometry": {"type": "LineString", "coordinates": [[p['lon'], p['lat']] for p in smoothed]}, "properties": {"time": [p['ts'] for p in smoothed], "states": [prune_state_for_frontend(p['state']) for p in smoothed]}})
    return all_features

async def process_device(client, device):
    device_id = device['device_id']
    print(f"  Processing device: {device_id}...")
    all_points, last_full_state = [], {}
    next_url = device['links']['history'] + "&limit=500"
    try:
        while next_url:
            resp = await client.get(next_url, timeout=60.0)
            resp.raise_for_status()
            page_data = resp.json()
            for record_delta in reversed(page_data.get('data', [])):
                current_state = deep_merge(record_delta.get('changes', {}), copy.deepcopy(last_full_state))
                if 'diagnostics' in record_delta: current_state['diagnostics'] = record_delta['diagnostics']
                point = extract_point(current_state)
                if point: all_points.append(point)
                last_full_state = current_state
            next_url = page_data.get('navigation', {}).get('next_page')
    except (httpx.RequestError, httpx.HTTPStatusError) as e:
        print(f"    Error fetching data for {device_id}, skipping: {e}")
    features = segment_and_process_points(all_points)
    if not features: print(f"    No valid track for {device_id}. Writing empty file.")
    geojson = {"type": "FeatureCollection", "features": features}
    output_path = os.path.join(DATA_DIR, f"{device_id}.json")
    with open(output_path, 'w') as f: json.dump(geojson, f)
    if features: print(f"    Saved simplified and smoothed track with {len(features)} segments.")

async def main():
    print("Starting data aggregation run (with simplification)...")
    os.makedirs(DATA_DIR, exist_ok=True)
    async with httpx.AsyncClient(timeout=30.0) as client:
        try:
            resp = await client.get(f"{PROCESSOR_URL}/data/devices?limit=100")
            resp.raise_for_status()
            devices = resp.json()
        except (httpx.RequestError, httpx.HTTPStatusError) as e:
            print(f"Could not fetch device list: {e}"); return
        tasks = [process_device(client, device) for device in devices]
        await asyncio.gather(*tasks)
    print("Aggregation run finished.")

if __name__ == "__main__":
    asyncio.run(main())
