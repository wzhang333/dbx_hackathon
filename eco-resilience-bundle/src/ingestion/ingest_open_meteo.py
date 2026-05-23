# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Ingest Open-Meteo forecasts → Bronze + Silver
# MAGIC
# MAGIC **What this notebook does**
# MAGIC
# MAGIC Pulls 7-day weather forecasts from the free Open-Meteo API for a curated
# MAGIC seed list of NSW locations, indexes each forecast point to an H3 cell, and
# MAGIC lands them as Delta tables ready for the agent's `get_weather_forecast` tool.
# MAGIC
# MAGIC **Pattern**
# MAGIC
# MAGIC This is our first **incremental** ingestion. Each run *appends* a snapshot
# MAGIC to Bronze, tagged with `_ingest_time`. Silver is rebuilt from "latest ingest
# MAGIC only", so re-running this notebook always produces a fresh `weather_current`.
# MAGIC
# MAGIC **Outputs**
# MAGIC - `<catalog>.bronze.open_meteo_forecast` — append-only history of forecasts (Delta)
# MAGIC - `<catalog>.silver.weather_current` — latest snapshot, one row per (location, forecast_time)
# MAGIC
# MAGIC **Schedule (later):** wire to a Databricks Job, run every 6h.
# MAGIC
# MAGIC **API:** `https://api.open-meteo.com/v1/forecast` — free, no key, ~1s per location.
# MAGIC
# MAGIC **Compute**
# MAGIC - **Serverless** (project-wide). Photon-on by default, native H3 SQL available.
# MAGIC - No extra `%pip install` needed — `requests` is in the serverless image.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration

# COMMAND ----------

CATALOG       = "eco_resilience"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"

H3_RESOLUTION = 8
FORECAST_DAYS = 7

API_URL = "https://api.open-meteo.com/v1/forecast"

BRONZE_TABLE  = f"{CATALOG}.{BRONZE_SCHEMA}.open_meteo_forecast"
SILVER_TABLE  = f"{CATALOG}.{SILVER_SCHEMA}.weather_current"
MAPPING_TABLE = f"{CATALOG}.{SILVER_SCHEMA}.poa_to_weather_location"

print(f"BRONZE_TABLE  = {BRONZE_TABLE}")
print(f"SILVER_TABLE  = {SILVER_TABLE}")
print(f"MAPPING_TABLE = {MAPPING_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Seed locations
# MAGIC
# MAGIC Curated set of NSW locations to pre-fetch. The agent calls Open-Meteo
# MAGIC directly at runtime for any postcode not represented here.
# MAGIC
# MAGIC | Why this set | Examples |
# MAGIC |---|---|
# MAGIC | Demo anchor | Bathurst (Tom's story) |
# MAGIC | Major urban centres | Sydney, Newcastle, Wollongong, Canberra |
# MAGIC | Flood-prone | Lismore, Tweed Heads, Coffs Harbour |
# MAGIC | Drought / inland | Broken Hill, Dubbo, Wagga Wagga |
# MAGIC | Regional spread | Tamworth, Albury, Goulburn, Orange |

# COMMAND ----------

LOCATIONS = [
    # (name,            lat,     lon)
    ("Bathurst",       -33.42,  149.58),
    ("Sydney CBD",     -33.86,  151.21),
    ("Newcastle",      -32.93,  151.78),
    ("Wollongong",     -34.42,  150.89),
    ("Canberra",       -35.31,  149.13),
    ("Lismore",        -28.81,  153.28),
    ("Coffs Harbour",  -30.30,  153.12),
    ("Tweed Heads",    -28.18,  153.55),
    ("Dubbo",          -32.24,  148.61),
    ("Wagga Wagga",    -35.12,  147.36),
    ("Tamworth",       -31.09,  150.93),
    ("Albury",         -36.07,  146.92),
    ("Orange",         -33.28,  149.10),
    ("Broken Hill",    -31.95,  141.47),
    ("Goulburn",       -34.75,  149.72),
]

print(f"Will fetch forecasts for {len(LOCATIONS)} locations")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Fetch forecasts from Open-Meteo
# MAGIC
# MAGIC Sequential `requests` on the driver. 15 calls × ~1s = ~15s. The function
# MAGIC flattens the API's parallel-arrays response into a list of dicts —
# MAGIC easier for Spark to ingest than nested JSON.
# MAGIC
# MAGIC **Variables we pull:**
# MAGIC - `precipitation` (mm, hourly) — flood signal
# MAGIC - `windspeed_10m` (km/h, hourly) — storm signal
# MAGIC - `temperature_2m` (°C, hourly)
# MAGIC - `relative_humidity_2m` (%, hourly) — fire signal
# MAGIC - `weather_code` (WMO numeric code, hourly) — categorical state

# COMMAND ----------

import requests
import time
from datetime import datetime, timezone

def fetch_forecast(name: str, lat: float, lon: float, days: int = FORECAST_DAYS) -> list[dict]:
    """One API call → list of flat hourly rows for this location."""
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": "precipitation,windspeed_10m,temperature_2m,relative_humidity_2m,weather_code",
        "timezone": "Australia/Sydney",
        "forecast_days": days,
    }
    resp = requests.get(API_URL, params=params, timeout=15)
    resp.raise_for_status()
    data = resp.json()

    hourly = data["hourly"]
    return [
        {
            "location_name":   name,
            "latitude":        lat,
            "longitude":       lon,
            "forecast_time":   hourly["time"][i],
            "precipitation_mm":     hourly["precipitation"][i],
            "windspeed_kmh":        hourly["windspeed_10m"][i],
            "temperature_c":        hourly["temperature_2m"][i],
            "humidity_pct":         hourly["relative_humidity_2m"][i],
            "weather_code":         hourly["weather_code"][i],
        }
        for i in range(len(hourly["time"]))
    ]

# Pull all locations
t0 = time.time()
all_rows: list[dict] = []
for name, lat, lon in LOCATIONS:
    rows = fetch_forecast(name, lat, lon)
    all_rows.extend(rows)
    print(f"  {name:20s} → {len(rows):>3} hourly rows")

elapsed = time.time() - t0
print(f"\nFetched {len(all_rows):,} rows from {len(LOCATIONS)} locations in {elapsed:.1f}s")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Bronze — append to history
# MAGIC
# MAGIC Build a Spark DataFrame, attach the H3 cell with the **point-flavored**
# MAGIC `h3_longlatash3(lat, lon, res)` (the sibling of `h3_polyfillash3` you saw
# MAGIC in notebook 01), then **append** to the Bronze table. Each run adds a new
# MAGIC batch tagged by `_ingest_time` — Delta time travel preserves all history.

# COMMAND ----------

from pyspark.sql import functions as F


df = (
    spark.createDataFrame(all_rows)
    .withColumn("forecast_time", F.to_timestamp("forecast_time"))
    .withColumn(
        "h3_cell",
        F.expr(f"h3_longlatash3(longitude, latitude, {H3_RESOLUTION})"),
    )
    .withColumn("_source",       F.lit("open-meteo"))
    .withColumn("_ingest_time",  F.current_timestamp())
)

(
    df.write.format("delta")
      .mode("append")
      .option("mergeSchema", "true")
      .saveAsTable(BRONZE_TABLE)
)

print(f"✅ Appended {df.count()} rows to {BRONZE_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bronze sanity check

# COMMAND ----------

print(f"Bronze total rows (all ingest batches): {spark.table(BRONZE_TABLE).count():,}")
print(f"Distinct ingest batches:                {spark.table(BRONZE_TABLE).select('_ingest_time').distinct().count()}")

display(
    spark.table(BRONZE_TABLE)
    .select(
        "location_name", "latitude", "longitude", "h3_cell",
        "forecast_time", "precipitation_mm", "windspeed_kmh",
        "temperature_c", "_ingest_time",
    )
    .orderBy(F.col("_ingest_time").desc(), "location_name", "forecast_time")
    .limit(5)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Silver — latest snapshot
# MAGIC
# MAGIC Silver = "the most recent ingest batch only". Cheap to rebuild on every run.
# MAGIC The agent always reads Silver, never Bronze.
# MAGIC
# MAGIC If we wanted to surface forecast-vs-actual divergence later, we'd add a
# MAGIC second Silver view that ranks rows by `_ingest_time` per `forecast_time` —
# MAGIC but for now, latest-only is the right level of complexity.

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE TABLE {SILVER_TABLE}
    CLUSTER BY (h3_cell) AS                              -- Liquid Clustering, serverless-idiomatic
    SELECT *
    FROM   {BRONZE_TABLE}
    WHERE  _ingest_time = (SELECT MAX(_ingest_time) FROM {BRONZE_TABLE})
""")
print(f"✅ {SILVER_TABLE} rebuilt with Liquid Clustering on h3_cell")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5b. Postcode → nearest weather location lookup
# MAGIC
# MAGIC The cell-equality join (`weather.h3_cell = poa_h3_lookup.h3_cell`) is fragile:
# MAGIC only one of a postcode's ~850 H3 cells matches a seeded weather point, and
# MAGIC even *that* match can fail at polygon-boundary cells due to H3's
# MAGIC center-of-cell containment rule.
# MAGIC
# MAGIC The right semantic is **"closest seeded forecast to this postcode"**.
# MAGIC We pre-compute one row per NSW POA pointing at the nearest seed location,
# MAGIC and the agent joins through *this* table instead of through `h3_cell`.
# MAGIC
# MAGIC | Approach | What it gives | When the agent uses it |
# MAGIC |---|---|---|
# MAGIC | `weather.h3_cell = poa.h3_cell` | "Forecast in this exact 0.7 km² hex" | Never — too brittle |
# MAGIC | `weather.location_name = mapping.nearest_weather_location` | "Forecast at the closest seeded town" | Always |

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE TABLE {MAPPING_TABLE}
    CLUSTER BY (poa_code) AS
    WITH
    poa_cell_centers AS (
        -- Every POA's H3 cells decoded to lat/lon (centroid of each hex)
        SELECT p.poa_code,
               CAST(get_json_object(h3_centerasgeojson(p.h3_cell), '$.coordinates[1]') AS DOUBLE) AS cell_lat,
               CAST(get_json_object(h3_centerasgeojson(p.h3_cell), '$.coordinates[0]') AS DOUBLE) AS cell_lon
        FROM   {CATALOG}.{SILVER_SCHEMA}.poa_h3_lookup p
    ),
    poa_centroids AS (
        -- Postcode "centre" = mean of its cell centres. Good enough for nearest-seed ranking.
        SELECT poa_code,
               AVG(cell_lat) AS poa_lat,
               AVG(cell_lon) AS poa_lon
        FROM   poa_cell_centers
        GROUP  BY poa_code
    ),
    weather_seeds AS (
        SELECT DISTINCT
               location_name,
               latitude  AS seed_lat,
               longitude AS seed_lon
        FROM   {SILVER_TABLE}
    ),
    distances AS (
        -- Haversine distance, km. Cross-join is fine: ~600 POAs × 15 seeds = 9k pairs.
        SELECT pc.poa_code,
               ws.location_name,
               2 * 6371 * ASIN(SQRT(
                   POWER(SIN(RADIANS((ws.seed_lat - pc.poa_lat) / 2)), 2) +
                   COS(RADIANS(pc.poa_lat)) * COS(RADIANS(ws.seed_lat)) *
                   POWER(SIN(RADIANS((ws.seed_lon - pc.poa_lon) / 2)), 2)
               )) AS distance_km
        FROM   poa_centroids pc
        CROSS JOIN weather_seeds ws
    )
    SELECT poa_code,
           location_name      AS nearest_weather_location,
           ROUND(distance_km, 1) AS distance_km
    FROM   distances
    QUALIFY ROW_NUMBER() OVER (PARTITION BY poa_code ORDER BY distance_km) = 1
""")
print(f"✅ {MAPPING_TABLE} built")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Lookup sanity check — what's the nearest seed for Bathurst, Mudgee, Lismore?

# COMMAND ----------

display(spark.sql(f"""
    SELECT poa_code, nearest_weather_location, distance_km
    FROM   {MAPPING_TABLE}
    WHERE  poa_code IN ('2795', '2850', '2480', '2640', '2770')
    ORDER  BY poa_code
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Smoke tests
# MAGIC
# MAGIC ### 6a — direct location lookup (Bathurst, next 24h precipitation)

# COMMAND ----------

display(spark.sql(f"""
    SELECT location_name,
           forecast_time,
           ROUND(temperature_c,    1) AS temp_c,
           ROUND(precipitation_mm, 1) AS rain_mm,
           ROUND(windspeed_kmh,    1) AS wind_kmh,
           ROUND(humidity_pct,     0) AS humidity_pct
    FROM   {SILVER_TABLE}
    WHERE  location_name = 'Bathurst'
    ORDER  BY forecast_time
    LIMIT  24
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6b — the actual agent flow: postcode → nearest weather location → forecast
# MAGIC
# MAGIC This is the join the `get_weather_forecast(poa_code)` tool will use.
# MAGIC Goes through `poa_to_weather_location` (the lookup we just built) instead
# MAGIC of cell-equality. Always returns rows for any NSW postcode.

# COMMAND ----------

display(spark.sql(f"""
    SELECT m.poa_code,
           m.nearest_weather_location AS forecast_for,
           m.distance_km,
           w.forecast_time,
           ROUND(w.temperature_c,    1) AS temp_c,
           ROUND(w.precipitation_mm, 1) AS rain_mm,
           ROUND(w.windspeed_kmh,    1) AS wind_kmh
    FROM   {SILVER_TABLE} w
    JOIN   {MAPPING_TABLE} m
      ON   w.location_name = m.nearest_weather_location
    WHERE  m.poa_code = '2795'                  -- Bathurst (Tom's farm)
    ORDER  BY w.forecast_time
    LIMIT  12
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Done
# MAGIC
# MAGIC | Layer | Table | Pattern | Refresh |
# MAGIC |---|---|---|---|
# MAGIC | Bronze | `bronze.open_meteo_forecast` | Append-only with `_ingest_time` | Every run |
# MAGIC | Silver | `silver.weather_current` | Latest ingest snapshot, `CLUSTER BY (h3_cell)` | Rebuilt every run |
# MAGIC | Silver | `silver.poa_to_weather_location` | One row per NSW POA → nearest seed location | Rebuilt every run |
# MAGIC
# MAGIC ### What's next
# MAGIC
# MAGIC - **Schedule:** wire this notebook to a Databricks Job running every 6h. The
# MAGIC   spec recommends 6h granularity for weather; faster is wasteful, slower
# MAGIC   misses storm events.
# MAGIC - **TODO — flood telemetry:** Open-Meteo has a separate `flood-api` endpoint
# MAGIC   for river discharge; great for the Macquarie River readout in the demo.
# MAGIC - **Next ingestion notebook:** `03_ingest_tfnsw_hazards.py` — same pattern,
# MAGIC   plus auth via Databricks Secrets.