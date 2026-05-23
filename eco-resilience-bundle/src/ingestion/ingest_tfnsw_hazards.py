# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Ingest TfNSW Live Traffic Hazards → Bronze + Silver
# MAGIC
# MAGIC **What this notebook does**
# MAGIC
# MAGIC Pulls the four live-hazard feeds from Transport for NSW (`incident`,
# MAGIC `flood`, `fire`, `roadwork`), unions them into one Bronze table, indexes
# MAGIC each hazard's location to an H3 cell, and rebuilds Silver as the latest
# MAGIC snapshot for the agent's `get_active_hazards` tool.
# MAGIC
# MAGIC **Pattern**
# MAGIC
# MAGIC Same as 02:
# MAGIC - Bronze: append-only, every run tagged with `_ingest_time`.
# MAGIC - Silver: rebuilt as "the latest ingest only", `CLUSTER BY (h3_cell)`.
# MAGIC
# MAGIC **Outputs**
# MAGIC - `<catalog>.bronze.tfnsw_hazards` — append-only history of all 4 hazard types
# MAGIC - `<catalog>.silver.hazards_current` — latest snapshot, Liquid-clustered on h3_cell
# MAGIC
# MAGIC **Compute**
# MAGIC - Serverless. `requests` is in the image; no `%pip install` needed.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Pre-requisite — API key in Databricks Secrets
# MAGIC
# MAGIC Run **once** in a shell with the Databricks CLI configured:
# MAGIC
# MAGIC ```bash
# MAGIC databricks secrets create-scope eco_resilience
# MAGIC databricks secrets put-secret eco_resilience tfnsw_api_key
# MAGIC # paste the key when prompted
# MAGIC ```
# MAGIC
# MAGIC The notebook reads the key via `dbutils.secrets.get()`. The value is
# MAGIC automatically masked in notebook output.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration

# COMMAND ----------

CATALOG       = "eco_resilience"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"

H3_RESOLUTION = 8

# Secret scope + key holding the TfNSW API key
SECRET_SCOPE = "eco_resilience"
SECRET_KEY   = "tfnsw_api_key"

# The four hazard endpoints we care about. All return the same GeoJSON shape;
# we union them into one Bronze table tagged with `hazard_type`.
HAZARD_TYPES = ["incident", "flood", "fire", "roadwork"]

API_BASE = "https://api.transport.nsw.gov.au/v1/live/hazards"

BRONZE_TABLE = f"{CATALOG}.{BRONZE_SCHEMA}.tfnsw_hazards"
SILVER_TABLE = f"{CATALOG}.{SILVER_SCHEMA}.hazards_current"

print(f"BRONZE_TABLE = {BRONZE_TABLE}")
print(f"SILVER_TABLE = {SILVER_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Read the API key from Databricks Secrets

# COMMAND ----------

api_key = dbutils.secrets.get(scope=SECRET_SCOPE, key=SECRET_KEY)
# Don't print the key — Databricks masks `dbutils.secrets.get()` results in output
# anyway, but we print only its length to confirm it loaded.
assert api_key, f"API key not found at {SECRET_SCOPE}/{SECRET_KEY}"
print(f"✅ API key loaded (length: {len(api_key)} chars)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Fetch the four hazard feeds
# MAGIC
# MAGIC One helper, called four times. Each feature is flattened to a flat dict
# MAGIC with the fields the agent will actually use; the rich nested bits
# MAGIC (`encodedPolylines`, `roads`) are preserved as JSON strings in Bronze for
# MAGIC later use without needing to re-fetch.

# COMMAND ----------

import json
import time
import requests

def fetch_hazards(hazard_type: str, key: str) -> list[dict]:
    """One API call → list of flat hazard dicts for a given type."""
    url = f"{API_BASE}/{hazard_type}/open"
    resp = requests.get(
        url,
        headers={"Authorization": f"apikey {key}"},
        timeout=15,
    )
    resp.raise_for_status()
    payload = resp.json()
    features = payload.get("features", []) or []

    rows: list[dict] = []
    for feat in features:
        props = feat.get("properties", {}) or {}
        geom = feat.get("geometry") or {}
        coords = geom.get("coordinates") or [None, None]

        rows.append({
            "hazard_type":           hazard_type,
            "hazard_id":             feat.get("id"),
            "main_category":         props.get("mainCategory"),
            "display_name":          props.get("displayName"),
            "headline":              props.get("headline") or None,
            "expected_delay_min":    props.get("expectedDelay"),
            "impacting_network":     props.get("impactingNetwork"),
            "is_major":              props.get("isMajor"),
            "ended":                 props.get("ended"),
            "advice_a":              props.get("adviceA"),
            "advice_b":              props.get("adviceB"),
            "advice_c":              props.get("adviceC"),
            "other_advice":          props.get("otherAdvice"),
            "created_ms":            props.get("created"),
            "last_updated_ms":       props.get("lastUpdated"),
            "longitude":             coords[0] if isinstance(coords, list) else None,
            "latitude":              coords[1] if isinstance(coords, list) else None,
            "encoded_polylines_json": json.dumps(props.get("encodedPolylines") or []),
            "roads_json":             json.dumps(props.get("roads") or []),
        })
    return rows

# Pull all four feeds
t0 = time.time()
all_rows: list[dict] = []
for ht in HAZARD_TYPES:
    rows = fetch_hazards(ht, api_key)
    all_rows.extend(rows)
    print(f"  /{ht}/open  → {len(rows):>4} features")

elapsed = time.time() - t0
print(f"\nFetched {len(all_rows):,} total features in {elapsed:.1f}s")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Bronze — append the snapshot
# MAGIC
# MAGIC Three transformations on the way in:
# MAGIC 1. Cast epoch-ms columns to real `TIMESTAMP`s.
# MAGIC 2. Compute `h3_cell = h3_longlatash3(lon, lat, 8)` — point-flavored H3.
# MAGIC    Hazards with NULL geometry (statewide announcements) get NULL h3_cell;
# MAGIC    Bronze keeps them for audit, Silver will too.
# MAGIC 3. Stamp `_source` and `_ingest_time` for lineage.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, DoubleType, BooleanType
)

# Make sure schemas exist
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SILVER_SCHEMA}")

# Explicit schema avoids LongType/DoubleType merge conflicts during inference
schema = StructType([
    StructField("hazard_type",           StringType(),  True),
    StructField("hazard_id",             StringType(),  True),
    StructField("main_category",         StringType(),  True),
    StructField("display_name",          StringType(),  True),
    StructField("headline",              StringType(),  True),
    StructField("expected_delay_min",    DoubleType(),  True),
    StructField("impacting_network",     BooleanType(), True),
    StructField("is_major",              BooleanType(), True),
    StructField("ended",                 BooleanType(), True),
    StructField("advice_a",              StringType(),  True),
    StructField("advice_b",              StringType(),  True),
    StructField("advice_c",              StringType(),  True),
    StructField("other_advice",          StringType(),  True),
    StructField("created_ms",            LongType(),    True),
    StructField("last_updated_ms",       LongType(),    True),
    StructField("longitude",             DoubleType(),  True),
    StructField("latitude",              DoubleType(),  True),
    StructField("encoded_polylines_json", StringType(), True),
    StructField("roads_json",             StringType(), True),
])

if not all_rows:
    print("⚠️  No hazards returned by any endpoint — skipping write.")
else:
    df = (
        spark.createDataFrame(all_rows, schema=schema)
        .withColumn("created_ts",      F.to_timestamp(F.col("created_ms") / 1000))
        .withColumn("last_updated_ts", F.to_timestamp(F.col("last_updated_ms") / 1000))
        .withColumn(
            "h3_cell",
            F.expr(f"h3_longlatash3(longitude, latitude, {H3_RESOLUTION})"),
        )
        .withColumn("_source",      F.lit("tfnsw"))
        .withColumn("_ingest_time", F.current_timestamp())
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
# MAGIC ### Bronze sanity check — counts by hazard type

# COMMAND ----------

display(spark.sql(f"""
    SELECT hazard_type,
           COUNT(*)                          AS hazards,
           SUM(IF(impacting_network, 1, 0))  AS network_impacting,
           SUM(IF(is_major, 1, 0))           AS major,
           SUM(IF(h3_cell IS NULL, 1, 0))    AS missing_geometry
    FROM   {BRONZE_TABLE}
    WHERE  _ingest_time = (SELECT MAX(_ingest_time) FROM {BRONZE_TABLE})
    GROUP  BY hazard_type
    ORDER  BY hazards DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Silver — latest snapshot, Liquid-clustered

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE TABLE {SILVER_TABLE}
    CLUSTER BY (h3_cell) AS
    SELECT *
    FROM   {BRONZE_TABLE}
    WHERE  _ingest_time = (SELECT MAX(_ingest_time) FROM {BRONZE_TABLE})
""")
print(f"✅ {SILVER_TABLE} rebuilt with Liquid Clustering on h3_cell")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Smoke tests
# MAGIC
# MAGIC ### 6a — current hazard mix in NSW

# COMMAND ----------

display(spark.sql(f"""
    SELECT hazard_type, main_category, COUNT(*) AS n
    FROM   {SILVER_TABLE}
    GROUP  BY hazard_type, main_category
    ORDER  BY n DESC
    LIMIT  20
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6b — agent flow: hazards near Bathurst (postcode 2795)
# MAGIC
# MAGIC The same join pattern the `get_active_hazards(poa_code)` tool will use:
# MAGIC postcode → H3 cells (from `silver.poa_h3_lookup`) → matching hazards.

# COMMAND ----------

display(spark.sql(f"""
    SELECT h.hazard_type,
           h.main_category,
           h.display_name,
           h.advice_a,
           h.expected_delay_min,
           h.impacting_network,
           h.last_updated_ts,
           h.latitude, h.longitude
    FROM   {SILVER_TABLE} h
    JOIN   {CATALOG}.{SILVER_SCHEMA}.poa_h3_lookup p
      ON   h.h3_cell = p.h3_cell
    WHERE  p.poa_code = '2795'
    ORDER  BY h.impacting_network DESC, h.expected_delay_min DESC NULLS LAST
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6c — top network-impacting hazards across NSW

# COMMAND ----------

display(spark.sql(f"""
    SELECT hazard_type, display_name, advice_a,
           expected_delay_min, latitude, longitude
    FROM   {SILVER_TABLE}
    WHERE  impacting_network = true
      AND  expected_delay_min IS NOT NULL
    ORDER  BY expected_delay_min DESC
    LIMIT  10
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Done
# MAGIC
# MAGIC | Layer | Table | Pattern | Refresh |
# MAGIC |---|---|---|---|
# MAGIC | Bronze | `bronze.tfnsw_hazards` | Append-only with `_ingest_time` | Every run |
# MAGIC | Silver | `silver.hazards_current` | Latest snapshot, `CLUSTER BY (h3_cell)` | Rebuilt every run |
# MAGIC
# MAGIC ### What's next
# MAGIC
# MAGIC - **Schedule:** wire to a Databricks Job, every 30 min during demo window
# MAGIC   (hazards change quickly), every 1–2h otherwise.
# MAGIC - **TODO — polyline decoding.** `encoded_polylines_json` holds Google-encoded
# MAGIC   polylines for affected road segments. When we want road-level
# MAGIC   visualization or "is this hazard *on* my route", we'll add a notebook
# MAGIC   that decodes them and polyfills via `h3_polyfillash3`.
# MAGIC - **TODO — k-ring proximity.** Right now the agent only finds hazards in
# MAGIC   the *exact* H3 cells of a postcode. Real users care about hazards
# MAGIC   *near* their postcode. Add `h3_kring(h3_cell, 1)` expansion in the agent
# MAGIC   tool, not in this notebook (keep Silver clean).
# MAGIC - **Next ingestion notebook:** `04_ingest_csiro_stations.py` — point data
# MAGIC   from CSV files in the Volume, plus a "nearest station per POA" lookup.