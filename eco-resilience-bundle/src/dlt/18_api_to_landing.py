# Databricks notebook source
# MAGIC %md
# MAGIC # 18 — API → JSON landing volume
# MAGIC
# MAGIC Producer for the Auto-Loader-driven SDP pipeline (notebook 17).
# MAGIC
# MAGIC Every run:
# MAGIC   1. Fetches Open-Meteo forecasts for the 15 seeded NSW locations.
# MAGIC   2. Fetches all 4 TfNSW hazard feeds (incident / flood / fire / roadwork).
# MAGIC   3. Writes each batch as a **single JSON file** to:
# MAGIC      ```
# MAGIC      /Volumes/eco_resilience/dlt/landing/weather/YYYY/MM/DD/HHMMSS.json
# MAGIC      /Volumes/eco_resilience/dlt/landing/hazards/YYYY/MM/DD/HHMMSS.json
# MAGIC      ```
# MAGIC
# MAGIC ### Why this notebook exists
# MAGIC
# MAGIC SDP's `@dp.append_flow` requires a **streaming source** (Auto Loader, Kafka,
# MAGIC or another streaming table). HTTP fetches return batch DataFrames, which SDP
# MAGIC rejects with `CREATE_APPEND_ONCE_FLOW_FROM_BATCH_QUERY_NOT_ALLOWED`.
# MAGIC
# MAGIC The production-grade workaround: decouple the producer from the pipeline.
# MAGIC This notebook is the producer — pure Python, scheduled by Workflows.
# MAGIC Notebook 17's SDP pipeline is the consumer — picks up files via Auto Loader.
# MAGIC
# MAGIC ### Schedule
# MAGIC
# MAGIC Workflow `eco_resilience_landing_fetcher`, every 3 hours.
# MAGIC The SDP pipeline runs every 3 hours offset by ~15 minutes so it always
# MAGIC ingests the latest batch.
# MAGIC
# MAGIC ### Prerequisites
# MAGIC
# MAGIC Volume must exist:
# MAGIC ```sql
# MAGIC CREATE VOLUME IF NOT EXISTS eco_resilience.dlt.landing
# MAGIC   COMMENT 'JSON landing zone for SDP pipeline.';
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## §1 — Imports + configuration

# COMMAND ----------

import json
import requests
from datetime import datetime, timezone

LANDING_BASE  = "/Volumes/eco_resilience/dlt/landing"
SECRET_SCOPE  = "eco_resilience"
SECRET_KEY    = "tfnsw_api_key"

# Forecast horizon — Open-Meteo allows up to 16; we pull 7 days hourly.
FORECAST_DAYS = 7

OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"
TFNSW_API_BASE = "https://api.transport.nsw.gov.au/v1/live/hazards"
HAZARD_TYPES   = ["incident", "flood", "fire", "roadwork"]

# Seeded NSW locations — MUST match notebook 17's SEEDED_LOCATIONS and
# notebook 02's LOCATIONS. The "Sydney CBD" name is the canonical form used by
# eco_resilience.silver.poa_to_weather_location.
SEEDED_LOCATIONS = [
    ("Sydney CBD",     -33.86, 151.21),
    ("Bathurst",       -33.42, 149.58),
    ("Newcastle",      -32.93, 151.78),
    ("Wollongong",     -34.42, 150.89),
    ("Canberra",       -35.31, 149.13),
    ("Lismore",        -28.81, 153.28),
    ("Coffs Harbour",  -30.30, 153.12),
    ("Tweed Heads",    -28.18, 153.55),
    ("Dubbo",          -32.24, 148.61),
    ("Wagga Wagga",    -35.12, 147.36),
    ("Tamworth",       -31.09, 150.93),
    ("Albury",         -36.07, 146.92),
    ("Orange",         -33.28, 149.10),
    ("Broken Hill",    -31.95, 141.47),
    ("Goulburn",       -34.75, 149.72),
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## §2 — API fetchers
# MAGIC
# MAGIC These are the same shapes as notebook 17 §2. Moved here so they live
# MAGIC closer to the API-call side rather than inside an SDP pipeline notebook
# MAGIC that can't actually call them outside a pipeline run.

# COMMAND ----------

def _as_float(v): return None if v is None else float(v)
def _as_int(v):   return None if v is None else int(v)
def _as_bool(v):  return None if v is None else bool(v)


def fetch_open_meteo_for_seeds():
    """Pull next 7 days of hourly weather for every seeded NSW location.
    Returns a flat list-of-dicts ready for JSON serialization."""
    all_rows = []
    for name, lat, lon in SEEDED_LOCATIONS:
        params = {
            "latitude":      lat,
            "longitude":     lon,
            "hourly":        "precipitation,windspeed_10m,temperature_2m,relative_humidity_2m,weather_code",
            "timezone":      "Australia/Sydney",
            "forecast_days": FORECAST_DAYS,
        }
        resp = requests.get(OPEN_METEO_URL, params=params, timeout=15)
        resp.raise_for_status()
        hourly = resp.json()["hourly"]

        for i in range(len(hourly["time"])):
            all_rows.append({
                "location_name":    name,
                "latitude":         _as_float(lat),
                "longitude":        _as_float(lon),
                "forecast_time":    hourly["time"][i],
                "precipitation_mm": _as_float(hourly["precipitation"][i]),
                "windspeed_kmh":    _as_float(hourly["windspeed_10m"][i]),
                "temperature_c":    _as_float(hourly["temperature_2m"][i]),
                "humidity_pct":     _as_float(hourly["relative_humidity_2m"][i]),
                "weather_code":     _as_int(hourly["weather_code"][i]),
            })
    return all_rows


def fetch_tfnsw_hazards_all_feeds(api_key: str):
    """Pull active hazards from all 4 TfNSW feeds. Returns flat list-of-dicts."""
    all_rows = []
    for hazard_type in HAZARD_TYPES:
        url = f"{TFNSW_API_BASE}/{hazard_type}/open"
        resp = requests.get(
            url,
            headers={"Authorization": f"apikey {api_key}"},
            timeout=15,
        )
        resp.raise_for_status()
        features = resp.json().get("features", []) or []

        for feat in features:
            props  = feat.get("properties", {}) or {}
            geom   = feat.get("geometry") or {}
            coords = geom.get("coordinates") or [None, None]
            coords_ok = isinstance(coords, list) and len(coords) >= 2

            all_rows.append({
                "hazard_type":            hazard_type,
                "hazard_id":              _as_int(feat.get("id")),
                "main_category":          props.get("mainCategory"),
                "display_name":           props.get("displayName"),
                "headline":               props.get("headline") or None,
                "expected_delay_min":     _as_float(props.get("expectedDelay")),
                "impacting_network":      _as_bool(props.get("impactingNetwork")),
                "is_major":               _as_bool(props.get("isMajor")),
                "ended":                  _as_bool(props.get("ended")),
                "advice_a":               props.get("adviceA"),
                "advice_b":               props.get("adviceB"),
                "advice_c":               props.get("adviceC"),
                "other_advice":           props.get("otherAdvice"),
                "created_ms":             _as_int(props.get("created")),
                "last_updated_ms":        _as_int(props.get("lastUpdated")),
                "longitude":              _as_float(coords[0]) if coords_ok else None,
                "latitude":               _as_float(coords[1]) if coords_ok else None,
                "encoded_polylines_json": json.dumps(props.get("encodedPolylines") or []),
                "roads_json":             json.dumps(props.get("roads") or []),
            })
    return all_rows

# COMMAND ----------

# MAGIC %md
# MAGIC ## §3 — Landing-write helper
# MAGIC
# MAGIC One JSON file per fetch per source. File naming `HHMMSS.json` is unique
# MAGIC at second granularity — collisions impossible at the configured 3h cadence.
# MAGIC
# MAGIC **`overwrite=False`** is deliberate: if two fetcher runs ever raced on the
# MAGIC same filename, we want the second to error loudly rather than silently
# MAGIC clobber the earlier write.

# COMMAND ----------

def write_batch_to_landing(rows: list, subdir: str) -> str:
    """Serialize rows to a single JSON file at landing/<subdir>/YYYY/MM/DD/HHMMSS.json.
    Returns the path written."""
    if not rows:
        print(f"[{subdir}] zero rows — nothing to write")
        return ""

    now = datetime.now(timezone.utc)
    yyyy   = now.strftime("%Y")
    mm     = now.strftime("%m")
    dd     = now.strftime("%d")
    hhmmss = now.strftime("%H%M%S")

    target_dir  = f"{LANDING_BASE}/{subdir}/{yyyy}/{mm}/{dd}"
    target_file = f"{target_dir}/{hhmmss}.json"

    dbutils.fs.mkdirs(target_dir)
    dbutils.fs.put(target_file, json.dumps(rows), overwrite=False)

    print(f"[{subdir}] wrote {len(rows)} rows → {target_file}")
    return target_file

# COMMAND ----------

# MAGIC %md
# MAGIC ## §4 — Run

# COMMAND ----------

# Weather — no auth.
weather_rows = fetch_open_meteo_for_seeds()
write_batch_to_landing(weather_rows, "weather")

# COMMAND ----------

# Hazards — auth via secret scope.
tfnsw_api_key = dbutils.secrets.get(scope=SECRET_SCOPE, key=SECRET_KEY)
hazard_rows   = fetch_tfnsw_hazards_all_feeds(tfnsw_api_key)
write_batch_to_landing(hazard_rows, "hazards")

# COMMAND ----------

# MAGIC %md
# MAGIC ## §5 — Notes
# MAGIC
# MAGIC ### What this notebook does NOT do
# MAGIC
# MAGIC | Concern | Where it lives |
# MAGIC |---|---|
# MAGIC | Schema enforcement | Auto Loader (in notebook 17) infers schema from landing files |
# MAGIC | H3 cell assignment | Done in notebook 17's silver layer, not here |
# MAGIC | Type coercion of weather_code, etc. | Already done in fetchers above via `_as_*` helpers |
# MAGIC | Deduplication | Bronze is append-only; silver does latest-batch isolation |
# MAGIC | Retention | Deferred. Landing files + bronze grow forever for now |
# MAGIC
# MAGIC ### Operating with the SDP pipeline (notebook 17)
# MAGIC
# MAGIC - This notebook can run **independently** of the SDP pipeline — it just writes files.
# MAGIC - The SDP pipeline reads landing files at its own cadence via Auto Loader checkpoints.
# MAGIC - To replay history, **delete the Auto Loader checkpoint** (Full Refresh in SDP UI). Landing files survive; bronze rebuilds from them.
# MAGIC - To start fresh on the landing side: `rm -rf` the volume subdirs. Then Full Refresh the SDP pipeline.