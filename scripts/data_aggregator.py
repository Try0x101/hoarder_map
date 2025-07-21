import os
import httpx
import json
import asyncio
import math
import copy
from datetime import datetime, timezone

PROCESSOR_URL = os.getenv("PROCESSOR_URL", "http://localhost:8001")
DATA_DIR = "/opt/hoarder_map/frontend/data"
MAX_JUMP_KM = 5.0 # Reduced for better city-level accuracy
SMOOTHING_WINDOW = 11 # Increased for a smoother line

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

def extract_point(record_state):
    try:
        location = record_state.get('location', {})
        lat = float(location['latitude'])
        lon = float(location['longitude'])
        
        ts_str = record_state['diagnostics']['timestamps']['device_event_timestamp_utc']
        dt_obj = datetime.strptime(ts_str, '%d.%m.%Y %H:%M:%S %Z')
        iso_ts = dt_obj.replace(tzinfo=timezone.utc).isoformat()
        return {'lon': lon, 'lat': lat, 'ts': iso_ts}
    except (KeyError, TypeError, ValueError, AttributeError):
        return None

def smooth_points(points, window_size=SMOOTHING_WINDOW):
    if len(points) < window_size:
        return points
    
    smoothed = []
    for i in range(len(points)):
        start = max(0, i - window_size // 2)
        end = min(len(points), i + window_size // 2 + 1)
        window = points[start:end]
        
        avg_lon = sum(p['lon'] for p in window) / len(window)
        avg_lat = sum(p['lat'] for p in window) / len(window)
        
        smoothed.append({'lon': round(avg_lon, 6), 'lat': round(avg_lat, 6), 'ts': points[i]['ts']})
    return smoothed

def segment_and_smooth_points(points):
    if not points:
        return []

    all_features = []
    current_segment = []
    last_point = None

    for point in points:
        if last_point:
            dist = haversine(last_point['lon'], last_point['lat'], point['lon'], point['lat'])
            if dist > MAX_JUMP_KM:
                if len(current_segment) > 1:
                    smoothed = smooth_points(current_segment)
                    coords = [[p['lon'], p['lat']] for p in smoothed]
                    times = [p['ts'] for p in smoothed]
                    all_features.append({
                        "type": "Feature",
                        "geometry": {"type": "LineString", "coordinates": coords},
                        "properties": {"time": times}
                    })
                current_segment = []
        
        current_segment.append(point)
        last_point = point

    if len(current_segment) > 1:
        smoothed = smooth_points(current_segment)
        coords = [[p['lon'], p['lat']] for p in smoothed]
        times = [p['ts'] for p in smoothed]
        all_features.append({
            "type": "Feature",
            "geometry": {"type": "LineString", "coordinates": coords},
            "properties": {"time": times}
        })
    return all_features

async def process_device(client, device):
    device_id = device['device_id']
    print(f"  Processing device: {device_id}...")
    
    all_points = []
    last_full_state = {}
    next_url = device['links']['history'] + "&limit=500"

    while next_url:
        try:
            resp = await client.get(next_url, timeout=60.0)
            resp.raise_for_status()
            page_data = resp.json()
        except (httpx.RequestError, httpx.HTTPStatusError) as e:
            print(f"    Error fetching data for {device_id}: {e}")
            return
        
        records = page_data.get('data', [])
        for record_delta in reversed(records):
            current_state = deep_merge(record_delta.get('changes', {}), copy.deepcopy(last_full_state))
            if 'diagnostics' in record_delta:
                current_state['diagnostics'] = record_delta['diagnostics']
            point = extract_point(current_state)
            if point:
                all_points.append(point)
            last_full_state = current_state

        nav = page_data.get('navigation', {})
        next_url = nav.get('next_page')

    if not all_points:
        print(f"    No valid location data found for {device_id}.")
        return

    features = segment_and_smooth_points(all_points)
    geojson = {"type": "FeatureCollection", "features": features}

    output_path = os.path.join(DATA_DIR, f"{device_id}.json")
    with open(output_path, 'w') as f:
        json.dump(geojson, f)
    print(f"    Saved segmented and smoothed track with {len(features)} segments to {output_path}")

async def main():
    print("Starting refined data aggregation run...")
    os.makedirs(DATA_DIR, exist_ok=True)
    async with httpx.AsyncClient() as client:
        try:
            resp = await client.get(f"{PROCESSOR_URL}/data/devices?limit=100")
            resp.raise_for_status()
            devices = resp.json()
        except (httpx.RequestError, httpx.HTTPStatusError) as e:
            print(f"Could not fetch device list: {e}")
            return
        tasks = [process_device(client, device) for device in devices]
        await asyncio.gather(*tasks)
    print("Aggregation run finished.")

if __name__ == "__main__":
    asyncio.run(main())
