from fastapi import FastAPI, Query, Response
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd

app = FastAPI()

# ✅ Enable CORS (allow all origins)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ✅ Load CSV
df = pd.read_csv("q-fastapi-timeseries-cache.csv")

# ✅ Convert timestamp column to datetime
df["timestamp"] = pd.to_datetime(df["timestamp"])

# ✅ Simple dictionary cache
cache = {}

def compute_stats(location, sensor, start_date, end_date):
    key = (location, sensor, start_date, end_date)

    # 🔁 If already computed → return from cache
    if key in cache:
        return cache[key], "HIT"

    data = df.copy()

    # ✅ Apply filters (all optional)
    if location:
        data = data[data["location"] == location]

    if sensor:
        data = data[data["sensor"] == sensor]

    if start_date:
        data = data[data["timestamp"] >= pd.to_datetime(start_date)]

    if end_date:
        data = data[data["timestamp"] <= pd.to_datetime(end_date)]

    # ✅ Compute stats
    if len(data) == 0:
        result = {
            "count": 0,
            "avg": 0,
            "min": 0,
            "max": 0
        }
    else:
        result = {
            "count": int(data["value"].count()),
            "avg": float(data["value"].mean()),
            "min": float(data["value"].min()),
            "max": float(data["value"].max())
        }

    # 💾 Save in cache
    cache[key] = result

    return result, "MISS"


@app.get("/stats")
def get_stats(
    response: Response,
    location: str = Query(None),
    sensor: str = Query(None),
    start_date: str = Query(None),
    end_date: str = Query(None)
):
    stats, cache_status = compute_stats(location, sensor, start_date, end_date)

    # ✅ Add cache header
    response.headers["X-Cache"] = cache_status

    return {"stats": stats}


# ✅ Required for Render deployment
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=10000)
