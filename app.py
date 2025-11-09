from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
from functools import lru_cache
from typing import Optional
from datetime import datetime

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=[""], allow_methods=[""], allow_headers=["*"])

# Load CSV once
DF = pd.read_csv('.csv_file name', parse_dates=['timestamp'])


def _normalize_date(s: Optional[str]):
    return None if s is None else pd.to_datetime(s)

# simple cache dict
_cache = {}

def cache_key(location, sensor, start_date, end_date):
    return (location or "", sensor or "", str(start_date) or "", str(end_date) or "")

@app.get("/stats")
def stats(response: Response, location: Optional[str] = None, sensor: Optional[str] = None,
          start_date: Optional[str] = None, end_date: Optional[str] = None):
    sd = _normalize_date(start_date)
    ed = _normalize_date(end_date)
    key = cache_key(location, sensor, sd, ed)
    if key in _cache:
        response.headers["X-Cache"] = "HIT"
        return {"stats": _cache[key]}
    df = DF
    if location:
        df = df[df['location'] == location]
    if sensor:
        df = df[df['sensor'] == sensor]
    if sd is not None:
        df = df[df['timestamp'] >= sd]
    if ed is not None:
        df = df[df['timestamp'] <= ed]
    values = df['value'].astype(float)
    stats_obj = {
        "count": int(values.count()),
        "avg": round(float(values.mean()) if not values.empty else 0.0, 2),
        "min": round(float(values.min()) if not values.empty else 0.0, 2),
        "max": round(float(values.max()) if not values.empty else 0.0, 2)
    }
    _cache[key] = stats_obj
    response.headers["X-Cache"] = "MISS"
    return {"stats": stats_obj}

# Run: uvicorn app:app --reload --host 127.0.0.1 --port 8000
