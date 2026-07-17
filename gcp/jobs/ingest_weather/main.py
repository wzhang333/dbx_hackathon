"""Cloud Run Job: Open-Meteo forecasts → BigQuery bronze + silver.

GCP port of eco-resilience-bundle/src/ingestion/ingest_open_meteo.py:
  spark.createDataFrame + Delta append      → bq.load_table_from_json (append)
  h3_longlatash3 SQL                        → h3.latlng_to_cell (Python)
  CREATE OR REPLACE TABLE ... CLUSTER BY    → CREATE OR REPLACE TABLE (BQ SQL)
  Databricks Job schedule                   → Cloud Scheduler → this job

Runs to completion and exits 0/1 — Cloud Run Jobs retries on failure.
"""

import logging
import sys
import time
from datetime import datetime, timezone

import h3
import requests
from google.cloud import bigquery

sys.path.insert(0, "/srv")
from agent import config  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("ingest_weather")

API_URL = "https://api.open-meteo.com/v1/forecast"
FORECAST_DAYS = 7

BRONZE_TABLE = config.bq_table(config.BRONZE, "open_meteo_forecast")
SILVER_TABLE = config.bq_table(config.SILVER, "weather_current")
MAPPING_TABLE = config.bq_table(config.SILVER, "poa_to_weather_location")
CENTROIDS_TABLE = config.bq_table(config.SILVER, "poa_centroids")

# Curated NSW seed locations — demo anchor (Bathurst), major centres,
# flood-prone north coast, drought-prone inland. Same list as the notebook.
LOCATIONS = [
    ("Bathurst", -33.42, 149.58),
    ("Sydney CBD", -33.86, 151.21),
    ("Newcastle", -32.93, 151.78),
    ("Wollongong", -34.42, 150.89),
    ("Canberra", -35.31, 149.13),
    ("Lismore", -28.81, 153.28),
    ("Coffs Harbour", -30.30, 153.12),
    ("Tweed Heads", -28.18, 153.55),
    ("Dubbo", -32.24, 148.61),
    ("Wagga Wagga", -35.12, 147.36),
    ("Tamworth", -31.09, 150.93),
    ("Albury", -36.07, 146.92),
    ("Orange", -33.28, 149.10),
    ("Broken Hill", -31.95, 141.47),
    ("Goulburn", -34.75, 149.72),
]


def fetch_forecast(name: str, lat: float, lon: float) -> list[dict]:
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": "precipitation,windspeed_10m,temperature_2m,relative_humidity_2m,weather_code",
        "timezone": "Australia/Sydney",
        "forecast_days": FORECAST_DAYS,
    }
    resp = requests.get(API_URL, params=params, timeout=15)
    resp.raise_for_status()
    hourly = resp.json()["hourly"]
    return [
        {
            "location_name": name,
            "latitude": lat,
            "longitude": lon,
            "forecast_time": hourly["time"][i],
            "precipitation_mm": hourly["precipitation"][i],
            "windspeed_kmh": hourly["windspeed_10m"][i],
            "temperature_c": hourly["temperature_2m"][i],
            "humidity_pct": hourly["relative_humidity_2m"][i],
            "weather_code": hourly["weather_code"][i],
        }
        for i in range(len(hourly["time"]))
    ]


def main() -> int:
    bq = bigquery.Client(project=config.PROJECT_ID, location=config.BQ_LOCATION)
    ingest_time = datetime.now(timezone.utc).isoformat()

    t0 = time.time()
    all_rows = []
    for name, lat, lon in LOCATIONS:
        rows = fetch_forecast(name, lat, lon)
        for r in rows:
            r["h3_cell"] = h3.latlng_to_cell(lat, lon, config.H3_RESOLUTION)
            r["_source"] = "open-meteo"
            r["_ingest_time"] = ingest_time
        all_rows.extend(rows)
        log.info(f"  {name:20s} → {len(rows)} hourly rows")
    log.info(f"Fetched {len(all_rows):,} rows in {time.time() - t0:.1f}s")

    # Bronze: append-only load (batch load, not streaming inserts — free and
    # immediately consistent for downstream DML).
    job = bq.load_table_from_json(
        all_rows, BRONZE_TABLE,
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_APPEND"),
    )
    job.result()
    log.info(f"Appended {len(all_rows)} rows to {BRONZE_TABLE}")

    # Silver: latest ingest batch only (Liquid Clustering → BQ CLUSTER BY)
    bq.query(f"""
        CREATE OR REPLACE TABLE `{SILVER_TABLE}`
        CLUSTER BY h3_cell AS
        SELECT * FROM `{BRONZE_TABLE}`
        WHERE _ingest_time = (SELECT MAX(_ingest_time) FROM `{BRONZE_TABLE}`)
    """).result()
    log.info(f"Rebuilt {SILVER_TABLE}")

    # Postcode → nearest seeded weather location (haversine over centroids,
    # same logic as the notebook; poa_centroids is built by seed_reference).
    bq.query(f"""
        CREATE OR REPLACE TABLE `{MAPPING_TABLE}`
        CLUSTER BY poa_code AS
        WITH weather_seeds AS (
            SELECT DISTINCT location_name, latitude AS seed_lat, longitude AS seed_lon
            FROM `{SILVER_TABLE}`
        ),
        distances AS (
            SELECT pc.poa_code, ws.location_name,
                   2 * 6371 * ASIN(SQRT(
                       POW(SIN((ws.seed_lat - pc.poa_lat) * ACOS(-1) / 360), 2) +
                       COS(pc.poa_lat * ACOS(-1) / 180) * COS(ws.seed_lat * ACOS(-1) / 180) *
                       POW(SIN((ws.seed_lon - pc.poa_lon) * ACOS(-1) / 360), 2)
                   )) AS distance_km
            FROM `{CENTROIDS_TABLE}` pc
            CROSS JOIN weather_seeds ws
        )
        SELECT poa_code,
               location_name AS nearest_weather_location,
               ROUND(distance_km, 1) AS distance_km
        FROM distances
        QUALIFY ROW_NUMBER() OVER (PARTITION BY poa_code ORDER BY distance_km) = 1
    """).result()
    log.info(f"Rebuilt {MAPPING_TABLE}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
