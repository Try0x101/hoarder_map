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
let staticTrackLayer = null;
let timedLayer = null;
let underlyingPointLayer = null;
let pollingInterval = null;
let lastKnownTimestamp = null;
let liveTrailSegments = [];

const HISTORICAL_STYLE = { color: '#00bcd4', weight: 3, opacity: 0.3 };
const LIVE_TRAIL_STYLES = [
    { color: '#00e5ff', weight: 4, opacity: 0.9 },
    { color: '#00d4e5', weight: 3.5, opacity: 0.7 },
    { color: '#00b5cc', weight: 3, opacity: 0.5 }
];

function universalDateParser(ts_str) {
    if (!ts_str) return null;
    const date = new Date(ts_str.replace(' UTC', 'Z').replace(/(\d{2})\.(\d{2})\.(\d{4})/, '$3-$2-$1'));
    return isNaN(date) ? null : date;
}

function convertLineStringToPoints(feature) {
    const points = [];
    const coords = feature.geometry.coordinates;
    const times = feature.properties.time;
    if (coords.length !== times.length) return [];
    for (let i = 0; i < coords.length; i++) {
        points.push({
            type: 'Feature',
            geometry: { type: 'Point', coordinates: coords[i] },
            properties: { time: times[i] }
        });
    }
    return points;
}

async function startPolling(deviceId) {
    if (pollingInterval) clearInterval(pollingInterval);
    liveTrailSegments = [];

    pollingInterval = setInterval(async () => {
        try {
            const response = await fetch(`/api/latest/${deviceId}`);
            if (!response.ok) return;
            const latestData = await response.json();
            
            const newTimestamp = universalDateParser(latestData?.diagnostics?.timestamps?.device_event_timestamp_utc);
            if (!newTimestamp || (lastKnownTimestamp && newTimestamp <= lastKnownTimestamp)) return;

            const lat = parseFloat(latestData?.location?.latitude);
            const lon = parseFloat(latestData?.location?.longitude);
            if (isNaN(lat) || isNaN(lon)) return;

            const allPointLayers = underlyingPointLayer.getLayers();
            const lastPointLayer = allPointLayers[allPointLayers.length - 1];
            const lastLatLng = lastPointLayer ? lastPointLayer.getLatLng() : null;
            const newLatLng = L.latLng(lat, lon);

            if (lastLatLng) {
                const distanceMeters = lastLatLng.distanceTo(newLatLng);
                if (distanceMeters < 50000) {
                    const newSegment = L.polyline([lastLatLng, newLatLng], LIVE_TRAIL_STYLES[0]);
                    newSegment.addTo(staticTrackLayer);
                    liveTrailSegments.unshift(newSegment);

                    if (liveTrailSegments.length > LIVE_TRAIL_STYLES.length) {
                        const oldestSegment = liveTrailSegments.pop();
                        oldestSegment.setStyle(HISTORICAL_STYLE);
                    }
                    
                    liveTrailSegments.forEach((segment, index) => {
                        segment.setStyle(LIVE_TRAIL_STYLES[index]);
                    });
                } else {
                    liveTrailSegments.forEach(segment => segment.setStyle(HISTORICAL_STYLE));
                    liveTrailSegments = [];
                }
            }
            
            const newPointFeature = {
                type: 'Feature',
                geometry: { type: 'Point', coordinates: [lon, lat] },
                properties: { time: newTimestamp.toISOString() }
            };
            underlyingPointLayer.addData(newPointFeature);
            
            lastKnownTimestamp = newTimestamp;
            map.timeDimension.setCurrentTime(newTimestamp.getTime());
            map.setView(newLatLng, map.getZoom(), { animate: true, pan: { duration: 1.0 } });

        } catch (e) {
            console.error("Polling error:", e);
        }
    }, 5000);
}

async function loadDeviceTrack(deviceId) {
    if (pollingInterval) clearInterval(pollingInterval);
    liveTrailSegments = [];
    if (staticTrackLayer) map.removeLayer(staticTrackLayer);
    if (timedLayer) timedLayer.remove();
    
    if (!deviceId) return;

    const response = await fetch(`/data/${deviceId}.json?v=${new Date().getTime()}`);
    if (!response.ok) return;
    const geoJsonData = await response.json();
    if (!geoJsonData || !geoJsonData.features || geoJsonData.features.length === 0) {
        map.timeDimension.setAvailableTimes([], 'replace');
        return;
    }
    
    const lastFeature = geoJsonData.features.slice(-1)[0];
    const lastTimeStr = lastFeature.properties.time.slice(-1)[0];
    lastKnownTimestamp = universalDateParser(lastTimeStr);

    staticTrackLayer = L.geoJSON(geoJsonData, { style: HISTORICAL_STYLE }).addTo(map);

    const pointGeoJson = {
        type: 'FeatureCollection',
        features: geoJsonData.features.flatMap(convertLineStringToPoints)
    };
    
    underlyingPointLayer = L.geoJSON(pointGeoJson, {
        pointToLayer: (feature, latlng) => L.circleMarker(latlng, {
            radius: 7, color: '#ffffff', weight: 2, fillColor: '#00e5ff', fillOpacity: 1
        })
    });

    timedLayer = L.timeDimension.layer.geoJson(underlyingPointLayer, {
        updateTimeDimension: true, updateTimeDimensionMode: 'replace',
        duration: 'PT0S', addlastPoint: true,
        lastPointStyle: { color: '#00e5ff', weight: 3, opacity: 0.8 }
    }).addTo(map);

    map.fitBounds(staticTrackLayer.getBounds(), { padding: [50, 50] });
    startPolling(deviceId);
}

async function init() {
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
        
        if (devices.length > 0) {
            deviceSelector.value = devices[0].device_id;
            loadDeviceTrack(devices[0].device_id);
        }
    } catch (e) {
        console.error("Failed to initialize devices:", e);
        deviceSelector.innerHTML = '<option>Error loading devices</option>';
    }
}

document.addEventListener('DOMContentLoaded', init);
