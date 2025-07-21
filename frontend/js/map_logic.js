const map = L.map('map', {
    zoom: 12,
    fullscreenControl: true,
    timeDimension: true,
    timeDimensionControl: true,
    timeDimensionControlOptions: {
        position: 'bottomleft',
        autoPlay: true,
        loopButton: true,
        timeSliderDragUpdate: true,
        speedSlider: true,
        playerOptions: {
            transitionTime: 250,
            startOver: true
        },
        formatDate: function(date) {
            return new Date(date).toLocaleString('en-GB', {
                hour: 'numeric', minute: 'numeric', second: 'numeric',
                year: 'numeric', month: 'short', day: 'numeric',
                timeZone: 'UTC'
            });
        }
    }
});

L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
    attribution: '© <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors © <a href="https://carto.com/attributions">CARTO</a>',
    subdomains: 'abcd',
    maxZoom: 20
}).addTo(map);

const deviceSelector = document.getElementById('device-selector');
const statsContent = document.getElementById('stats-content');
let staticTrackLayer = null, timedLayer = null, underlyingPointLayer = null;
let pollingInterval = null, lastKnownTimestamp = null, currentDeviceGeoJson = null;
let liveTrailSegments = [], liveLatLngs = [];

const SHARED_STYLE_PROPS = { lineCap: 'round', lineJoin: 'round' };
const HISTORICAL_STYLE = { color: '#00bcd4', weight: 3, opacity: 0.3, ...SHARED_STYLE_PROPS };
const LIVE_TRAIL_STYLES = [
    { color: '#00e5ff', weight: 4, opacity: 0.9, ...SHARED_STYLE_PROPS }, 
    { color: '#00d4e5', weight: 3.5, opacity: 0.7, ...SHARED_STYLE_PROPS },
    { color: '#00b5cc', weight: 3, opacity: 0.5, ...SHARED_STYLE_PROPS }
];

function universalDateParser(ts_str) {
    if (!ts_str) return null;
    const date = new Date(ts_str.replace(' UTC', 'Z').replace(/(\d{2})\.(\d{2})\.(\d{4})/, '$3-$2-$1'));
    return isNaN(date) ? null : date;
}

function renderStats(state) {
    if (!state || Object.keys(state).length === 0) { statsContent.innerHTML = 'No stat data.'; return; }
    let html = '';
    const s = (val, fallback = 'N/A') => val || fallback;
    const p = state.power || {};
    html += `<div class="stat-group"><h4>Device</h4><div class="stat-item"><span>Battery</span><span>${s(p.battery_percent)}</span></div></div>`;
    const n = state.network || {};
    html += `<div class="stat-group"><h4>Network</h4><div class="stat-item"><span>Type</span><span>${s(n.type)}</span></div><div class="stat-item"><span>Operator</span><span>${s(n.operator)}</span></div><div class="stat-item"><span>Signal</span><span>${s(n.signal_strength)}</span></div></div>`;
    const e = state.environment || {}, w = e.weather || {}, wind = e.wind || {};
    html += `<div class="stat-group"><h4>Environment</h4><div class="stat-item"><span>Weather</span><span>${s(w.description)}</span></div><div class="stat-item"><span>Temp</span><span>${s(w.temperature)}</span></div><div class="stat-item"><span>Feels</span><span>${s(w.assessment)}</span></div><div class="stat-item"><span>Wind</span><span>${s(wind.speed)} (${s(wind.direction)})</span></div><div class="stat-item"><span></span><span style="font-style: italic;">${s(wind.description,'')}</span></div></div>`;
    statsContent.innerHTML = html;
}

function findStateForTime(time) {
    if (!currentDeviceGeoJson) return null;
    const isoTime = new Date(time).toISOString();
    for (const feature of currentDeviceGeoJson.features) {
        const index = feature.properties.time.indexOf(isoTime);
        if (index !== -1) return feature.properties.states[index];
    }
    return null;
}

function getCatmullRomPoint(t, p0, p1, p2, p3) {
    const t2 = t * t, t3 = t2 * t;
    const lat = 0.5 * ((2 * p1.lat) + (-p0.lat + p2.lat) * t + (2 * p0.lat - 5 * p1.lat + 4 * p2.lat - p3.lat) * t2 + (-p0.lat + 3 * p1.lat - 3 * p2.lat + p3.lat) * t3);
    const lng = 0.5 * ((2 * p1.lng) + (-p0.lng + p2.lng) * t + (2 * p0.lng - 5 * p1.lng + 4 * p2.lng - p3.lng) * t2 + (-p0.lng + 3 * p1.lng - 3 * p2.lng + p3.lng) * t3);
    return L.latLng(lat, lng);
}

async function startPolling(deviceId) {
    if (pollingInterval) clearInterval(pollingInterval);
    liveTrailSegments = []; liveLatLngs = [];
    const MAX_SPEED_MPS = 250; 

    pollingInterval = setInterval(async () => {
        try {
            const response = await fetch(`/api/latest/${deviceId}`);
            if (!response.ok) return;
            const latestData = await response.json();
            const newTimestamp = universalDateParser(latestData?.diagnostics?.timestamps?.device_event_timestamp_utc);
            if (!newTimestamp || (lastKnownTimestamp && newTimestamp.getTime() <= lastKnownTimestamp.getTime())) return;
            const lat = parseFloat(latestData?.location?.latitude), lon = parseFloat(latestData?.location?.longitude);
            if (isNaN(lat) || isNaN(lon)) return;
            const newLatLng = L.latLng(lat, lon);

            let isJump = false;
            const lastPoint = liveLatLngs.length > 0 ? liveLatLngs[liveLatLngs.length - 1] : null;
            if (lastPoint && lastKnownTimestamp) {
                const timeDiff = (newTimestamp.getTime() - lastKnownTimestamp.getTime()) / 1000;
                if (timeDiff > 0.1) {
                    if ((lastPoint.distanceTo(newLatLng) / timeDiff) > MAX_SPEED_MPS) isJump = true;
                }
            }
            if (isJump) {
                liveTrailSegments.forEach(segment => segment.setStyle(HISTORICAL_STYLE));
                liveTrailSegments = []; liveLatLngs = [];
            }
            liveLatLngs.push(newLatLng);
            if (liveLatLngs.length > 4) liveLatLngs.shift();

            if (!isJump) {
                if (liveLatLngs.length < 4) {
                    if (liveLatLngs.length > 1) liveTrailSegments.unshift(L.polyline(liveLatLngs.slice(-2), LIVE_TRAIL_STYLES[0]).addTo(staticTrackLayer));
                } else {
                    if (liveTrailSegments.length > 0) liveTrailSegments.shift().remove();
                    const [p0, p1, p2, p3] = liveLatLngs;
                    const curve = [p1];
                    for (let i = 1; i <= 10; i++) curve.push(getCatmullRomPoint(i / 10, p0, p1, p2, p3));
                    liveTrailSegments.unshift(L.polyline(curve, LIVE_TRAIL_STYLES[1]).addTo(staticTrackLayer));
                    liveTrailSegments.unshift(L.polyline([p2, p3], LIVE_TRAIL_STYLES[0]).addTo(staticTrackLayer));
                }
                if (liveTrailSegments.length > LIVE_TRAIL_STYLES.length + 1) liveTrailSegments.pop().setStyle(HISTORICAL_STYLE);
                liveTrailSegments.forEach((seg, i) => seg.setStyle(LIVE_TRAIL_STYLES[i] || LIVE_TRAIL_STYLES.slice(-1)[0]));
            }
            underlyingPointLayer.addData({ type: 'Feature', geometry: { type: 'Point', coordinates: [lon, lat] }, properties: { time: newTimestamp.toISOString() }});
            lastKnownTimestamp = newTimestamp;
            map.timeDimension.setCurrentTime(newTimestamp.getTime());
            map.setView(newLatLng, map.getZoom(), { animate: true, pan: { duration: 1.0 } });
            renderStats(latestData);
        } catch (e) { console.error("Polling error:", e); }
    }, 5000);
}

async function loadDeviceTrack(deviceId) {
    if (pollingInterval) clearInterval(pollingInterval);
    liveTrailSegments = []; liveLatLngs = []; currentDeviceGeoJson = null;
    if (staticTrackLayer) map.removeLayer(staticTrackLayer);
    if (timedLayer) timedLayer.remove();
    if (!deviceId) { statsContent.innerHTML = 'Select a device.'; map.timeDimension.setAvailableTimes([], 'replace'); return; }
    statsContent.innerHTML = `Loading track data...`;
    try {
        const response = await fetch(`/data/${deviceId}.json?v=${new Date().getTime()}`);
        if (!response.ok) throw new Error(`Data file not found (status: ${response.status}).`);
        const geoJsonData = await response.json();
        if (!geoJsonData || !Array.isArray(geoJsonData.features) || geoJsonData.features.length === 0) throw new Error("GeoJSON data empty.");
        currentDeviceGeoJson = geoJsonData;
        const lastFeature = currentDeviceGeoJson.features.slice(-1)[0];
        const lastCoords = lastFeature.geometry.coordinates;
        liveLatLngs = [L.latLng(lastCoords.slice(-1)[0][1], lastCoords.slice(-1)[0][0])];
        lastKnownTimestamp = universalDateParser(lastFeature.properties.time.slice(-1)[0]);
        renderStats(lastFeature.properties.states.slice(-1)[0]);
        staticTrackLayer = L.geoJSON(currentDeviceGeoJson, { style: HISTORICAL_STYLE }).addTo(map);
        const points = currentDeviceGeoJson.features.flatMap(f => f.geometry.coordinates.map((c, i) => ({type: 'Feature', geometry: {type:'Point', coordinates: c}, properties: {time: f.properties.time[i]}})));
        underlyingPointLayer = L.geoJSON({type:'FeatureCollection', features: points}, { pointToLayer: (f, l) => L.circleMarker(l, { radius: 7, color: '#fff', weight: 2, fillColor: '#00e5ff', fillOpacity: 1 }) });
        timedLayer = L.timeDimension.layer.geoJson(underlyingPointLayer, { updateTimeDimension: true, updateTimeDimensionMode: 'replace', duration: 'PT0S', addlastPoint: true }).addTo(map);
        map.fitBounds(staticTrackLayer.getBounds(), { padding: [50, 50] });
        startPolling(deviceId);
    } catch (error) {
        console.error(`Failed to load track for ${deviceId}:`, error);
        statsContent.innerHTML = `No valid track data found.`;
        map.timeDimension.setAvailableTimes([], 'replace');
    }
}

async function init() {
    map.setView([20, 0], 2);
    try {
        const response = await fetch('/api/devices');
        if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`);
        const devices = await response.json();
        deviceSelector.innerHTML = '<option value="">Select a device</option>';
        devices.forEach(device => {
            const option = document.createElement('option');
            option.value = device.device_id;
            option.textContent = device.device_name || device.device_id;
            deviceSelector.appendChild(option);
        });
        deviceSelector.onchange = (e) => loadDeviceTrack(e.target.value);
        map.on('timeload', (e) => renderStats(findStateForTime(e.time)));
        if (devices.length > 0) {
            deviceSelector.value = devices[0].device_id;
            loadDeviceTrack(devices[0].device_id);
        } else {
            statsContent.innerHTML = 'No devices found.';
        }
    } catch (e) {
        console.error("Failed to initialize devices:", e);
        statsContent.innerHTML = 'Error loading device list.';
        deviceSelector.innerHTML = '<option>Error loading devices</option>';
    }
}

document.addEventListener('DOMContentLoaded', init);
