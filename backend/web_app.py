import os
import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

APP_DIR = os.path.dirname(os.path.abspath(__file__))
FRONTEND_DIR = os.path.join(APP_DIR, '..', 'frontend')
PROCESSOR_URL = os.getenv("PROCESSOR_URL", "http://localhost:8001")

app = FastAPI()

@app.get("/api/devices")
async def list_devices():
    async with httpx.AsyncClient() as client:
        try:
            resp = await client.get(f"{PROCESSOR_URL}/data/devices")
            resp.raise_for_status()
            return resp.json()
        except httpx.RequestError as e:
            raise HTTPException(status_code=503, detail=f"Cannot connect to processor: {e}")
        except httpx.HTTPStatusError as e:
            raise HTTPException(status_code=e.response.status_code, detail=e.response.json())

@app.get("/api/latest/{device_id}")
async def get_latest(device_id: str):
    async with httpx.AsyncClient() as client:
        try:
            resp = await client.get(f"{PROCESSOR_URL}/data/latest/{device_id}")
            resp.raise_for_status()
            return resp.json()
        except httpx.RequestError as e:
            raise HTTPException(status_code=503, detail=f"Cannot connect to processor: {e}")
        except httpx.HTTPStatusError as e:
            raise HTTPException(status_code=e.response.status_code, detail=e.response.json())

app.mount("/data", StaticFiles(directory=os.path.join(FRONTEND_DIR, "data")), name="data")
app.mount("/js", StaticFiles(directory=os.path.join(FRONTEND_DIR, "js")), name="js")

@app.get("/")
async def get_index():
    return FileResponse(os.path.join(FRONTEND_DIR, 'index.html'))
