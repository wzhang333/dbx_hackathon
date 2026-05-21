# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — Ingest CSIRO Climate Projections → Bronze + Silver
# MAGIC
# MAGIC **What this notebook does**
# MAGIC
# MAGIC Loads the six CMIP5 station-projection CSVs CSIRO publishes via Climate
# MAGIC Change in Australia (CCiA) from a UC Volume, creates one Bronze Delta
# MAGIC table per CSV (no transformation), then unions and unpivots them into a
# MAGIC single long-format Silver table the agent can query for projections like
# MAGIC *"by 2050 it'll be X°C in Bathurst"*.
# MAGIC
# MAGIC **Why this is different from 02 and 03**
# MAGIC
# MAGIC Climate projections are **static reference data**:
# MAGIC - One-shot load (`CREATE OR REPLACE`) — no append-only history needed
# MAGIC - No scheduled refresh — CSIRO publishes a new edition only every few years
# MAGIC - The big work is the wide → long **unpivot** (20 time-aggregation columns
# MAGIC   per row become 20 rows)
# MAGIC
# MAGIC **Outputs**
# MAGIC - Six `<catalog>.bronze.csiro_<var>` tables (one per source CSV, audit-pure)
# MAGIC - `<catalog>.silver.csiro_projections` — long-format, NSW-filtered,
# MAGIC   `CLUSTER BY (h3_cell, rcp)` — the table the agent reads
# MAGIC - `<catalog>.silver.poa_to_csiro_station` — one row per NSW POA →
# MAGIC   nearest station (Haversine), same pattern as `poa_to_weather_location`
# MAGIC
# MAGIC **Demo default scenario:** rcp45 (moderate emissions) — the credible
# MAGIC middle. All four RCPs (rcp26, rcp45, rcp60, rcp85) are kept in Silver so
# MAGIC the agent can cite rcp85 as a "worst case" callout without a separate query.
# MAGIC
# MAGIC **Compute:** Serverless. No `%pip install` needed.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration

# COMMAND ----------

# MAGIC %md
# MAGIC ## CSIRO climate variables — quick reference
# MAGIC
# MAGIC Each of the six source CSVs measures one physical climate variable. The codes (`tas`, `tasmax`, …) follow the CMIP / CF (Climate and Forecast) Conventions — the same standard codes used across all major international climate datasets, so they integrate cleanly with other CMIP data later if needed.
# MAGIC
# MAGIC ### What each variable means
# MAGIC
# MAGIC | `var` | Full name | Units | What it measures | Why it matters for the agent |
# MAGIC |---|---|---|---|---|
# MAGIC | **`tas`** | Temperature, Air, Surface (2m above ground) | °C | Daily mean temperature, averaged over a season/year | Headline climate-change number for the pitch |
# MAGIC | **`tasmax`** | tas + maximum | °C | Daily *peak* temperature, averaged over a period | Heat-stress signal — when does it hit 40°C+? |
# MAGIC | **`tasmin`** | tas + minimum | °C | Daily *lowest* temperature, averaged | Frost risk for crops; cold extremes |
# MAGIC | **`hurs9`** | Humidity, Relative, Surface, at 9am | % | Morning humidity (when soil moisture is high) | Drying trend / evaporation context |
# MAGIC | **`hurs15`** | hurs at 15:00 (3pm) | % | Afternoon humidity (when fire risk is highest) | Bushfire severity proxy |
# MAGIC | **`pan_evap`** | Pan evaporation | mm | Total water evaporated from a standard pan over time | Drought signal — more pan-evap = drier landscape |
# MAGIC
# MAGIC ### How the agent uses each variable
# MAGIC
# MAGIC | Variable | Agent's likely sentence |
# MAGIC |---|---|
# MAGIC | `tas` | *"Average annual temperature in your postcode is projected to rise by ~2°C by 2080 under rcp45."* |
# MAGIC | `tasmax` | *"Peak summer days will average ~32°C by 2050, up from ~29°C today."* |
# MAGIC | `tasmin` | *"Frost risk decreases — coldest winter mornings warm by ~1.5°C."* |
# MAGIC | `hurs9` / `hurs15` | *"Afternoon humidity drops from 45% to 38% in summer — fire-prone conditions extend by 3 weeks."* |
# MAGIC | `pan_evap` | *"Annual evaporation increases ~12%, so existing irrigation delivers ~12% less moisture per dollar."* |
# MAGIC
# MAGIC `tas` is the headline; `tasmax` + `hurs15` together drive the fire-risk story; `pan_evap` drives the drought story; `tasmin` is the most farmer-relevant for frost-sensitive crops. The agent picks the variables relevant to the user's industry — heat for dairy, frost for vineyards, evaporation for broadacre cropping.
# MAGIC

# COMMAND ----------

CATALOG       = "eco_resilience"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
VOLUME_NAME   = "raw_climate"       

H3_RESOLUTION = 8

# NSW lat/lon bbox — same one we used during day-1 CSIRO inspection
NSW_LAT_MIN, NSW_LAT_MAX = -37.5, -28.0
NSW_LON_MIN, NSW_LON_MAX = 141.0, 154.0

# (filename, variable code, aggregation type, units)
# pan-evap is *summed* per season (water flux); the others are *averaged*.
# Keep the aggregation flag in Silver so downstream consumers don't blindly AVG.

CSIRO_FILES = [
    ("tas_aus-station_r1i1p1_CSIRO-MnCh-wrt-1986-2005-Scl_v1_mon_seasavg-clim_1.csv",  "tas",      "mean", "celsius"),
    ("tasmax_aus-station_r1i1p1_CSIRO-MnCh-wrt-1986-2005-Scl_v1_mon_seasavg-clim.csv", "tasmax",   "mean", "celsius"),
    ("tasmin_aus-station_r1i1p1_CSIRO-MnCh-wrt-1986-2005-Scl_v1_mon_seasavg-clim.csv", "tasmin",   "mean", "celsius"),
    ("hurs9_aus-station_r1i1p1_CSIRO-MnCh-wrt-1986-2005-Scl_v1_mon_seasavg-clim.csv",  "hurs9",    "mean", "percent"),
    ("hurs15_aus-station_r1i1p1_CSIRO-MnCh-wrt-1986-2005-Scl_v1_mon_seasavg-clim.csv", "hurs15",   "mean", "percent"),
    ("pan-evap_aus-station_r1i1p1_CSIRO-MnCh-wrt-1986-2005-Scl_v1_mon_seassum-clim.csv","pan_evap", "sum",  "mm"),
]

# Wide value columns to be unpivoted into rows
TIME_AGG_COLS = [
    "Annual",
    "DJF", "MAM", "JJA", "SON",        # 4 seasons
    "NDJFMA", "MJJASO",                # 2 half-years
    "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December",
]

VOLUME_PATH              = f"/Volumes/{CATALOG}/{BRONZE_SCHEMA}/{VOLUME_NAME}"
SILVER_PROJECTIONS_TABLE = f"{CATALOG}.{SILVER_SCHEMA}.csiro_projections"
SILVER_LOOKUP_TABLE      = f"{CATALOG}.{SILVER_SCHEMA}.poa_to_csiro_station"

print(f"VOLUME_PATH              = {VOLUME_PATH}")
print(f"SILVER_PROJECTIONS_TABLE = {SILVER_PROJECTIONS_TABLE}")
print(f"SILVER_LOOKUP_TABLE      = {SILVER_LOOKUP_TABLE}")
print(f"Files to load: {len(CSIRO_FILES)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Sanity check — files are in the Volume

# COMMAND ----------

import os

for fname, var, agg, units in CSIRO_FILES:
    path = f"{VOLUME_PATH}/{fname}"
    # assert <condition>, <message> raise AssertionError if condition false with a custom failure message
    assert os.path.exists(path), f"Missing: {path}"
    size_mb = os.path.getsize(path) / 1024 / 1024
    print(f"  ✅ {var:10s} ({agg:4s}, {units:7s})  {size_mb:>5.1f} MB")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Bronze — six tables, one per source CSV
# MAGIC
# MAGIC No transformations except the standard `_source_file` / `_ingest_time`
# MAGIC audit columns. Each Bronze table preserves the raw wide layout (27 cols)
# MAGIC so we can always go back to source if the unpivot/union does the wrong thing.

# COMMAND ----------

from pyspark.sql import functions as F

bronze_tables: list[tuple[str, str, str, str]] = []   # (full_table_name, var, agg, units)

for fname, var, agg, units in CSIRO_FILES:
    src_path     = f"{VOLUME_PATH}/{fname}"
    bronze_table = f"{CATALOG}.{BRONZE_SCHEMA}.csiro_{var}"
    bronze_tables.append((bronze_table, var, agg, units))

    df = (
        spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .csv(src_path)
            .withColumn("_source_file", F.lit(fname))
            .withColumn("_ingest_time", F.current_timestamp())
    )
    df.write.format("delta").mode("overwrite").saveAsTable(bronze_table)
    print(f"  ✅ {bronze_table}  ({df.count():>6,} rows)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bronze sanity check — row counts match day-1 inspection
# MAGIC
# MAGIC Expected (from day-1 inspection of source CSVs):
# MAGIC ```
# MAGIC tas      11,648  | tasmax  11,200  | tasmin  11,200
# MAGIC hurs9     4,160  | hurs15   4,160  | pan_evap 5,512
# MAGIC ```

# COMMAND ----------

for bronze_table, var, agg, units in bronze_tables:
    cnt = spark.table(bronze_table).count()
    print(f"  {var:10s} {agg:5s} {units:8s}  {cnt:>6,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Silver — unpivot + union + NSW filter + H3 index
# MAGIC
# MAGIC Strategy:
# MAGIC 1. For each Bronze table, build a SQL fragment that **`UNPIVOT`s** the 20 time-
# MAGIC    aggregation columns into rows, tagging with `variable` / `aggregation` / `units` literals.
# MAGIC 2. **`UNION ALL`** the six fragments.
# MAGIC 3. Filter NSW lat/lon bbox.
# MAGIC 4. Add `h3_cell = h3_longlatash3(lon, lat, 8)` — note: longitude first.
# MAGIC 5. `CLUSTER BY (h3_cell, rcp)` — the two columns the agent will filter on.

# COMMAND ----------

# Build the UNPIVOT IN-clause once
unpivot_in_clause = ", ".join([f"`{c}`" for c in TIME_AGG_COLS])

# Build six SQL fragments — one per Bronze table
fragments = []
for bronze_table, var, agg, units in bronze_tables:
    fragments.append(f"""
        SELECT
            STATION_NAME           AS station_name,
            STN_ID                 AS station_id,
            CAST(LAT AS DOUBLE)    AS lat,
            CAST(LON AS DOUBLE)    AS lon,
            ENSEMBLE               AS ensemble,
            RCP                    AS rcp,
            MODEL                  AS model,
            CLIMATOLOGY            AS period,
            '{var}'                AS variable,
            '{agg}'                AS aggregation,
            '{units}'              AS units,
            time_aggregation,
            CAST(value AS DOUBLE)  AS value
        FROM {bronze_table}
        UNPIVOT (
            value FOR time_aggregation IN ({unpivot_in_clause})
        )
    """)

union_sql = "\n        UNION ALL\n".join(fragments)

silver_sql = f"""
    CREATE OR REPLACE TABLE {SILVER_PROJECTIONS_TABLE}
    CLUSTER BY (h3_cell, rcp) AS
    SELECT
        station_name, station_id, lat, lon,
        h3_longlatash3(lon, lat, {H3_RESOLUTION}) AS h3_cell,   -- LON first!
        ensemble, rcp, model, period,
        variable, aggregation, units,
        time_aggregation, value
    FROM (
        {union_sql}
    )
    WHERE lat BETWEEN {NSW_LAT_MIN} AND {NSW_LAT_MAX}
      AND lon BETWEEN {NSW_LON_MIN} AND {NSW_LON_MAX}
"""

spark.sql(silver_sql)
print(f"✅ {SILVER_PROJECTIONS_TABLE} built (Liquid Clustering on h3_cell, rcp)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Silver sanity check — variable × scenario × model coverage

# COMMAND ----------

display(spark.sql(f"""
    SELECT variable,
           COUNT(*)                          AS rows,
           COUNT(DISTINCT station_name)      AS nsw_stations,
           COUNT(DISTINCT model)             AS models,
           COUNT(DISTINCT rcp)               AS rcps,
           COUNT(DISTINCT period)            AS time_periods
    FROM   {SILVER_PROJECTIONS_TABLE}
    GROUP  BY variable
    ORDER  BY variable
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Silver — postcode → nearest CSIRO station lookup
# MAGIC
# MAGIC Same Haversine nearest-neighbour pattern as `silver.poa_to_weather_location`.
# MAGIC One row per NSW POA. The agent's `get_climate_projection(poa_code)` joins
# MAGIC through this table — never on `h3_cell` directly (cell-equality is fragile,
# MAGIC and there are only ~30 NSW stations vs ~600 POAs anyway).

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE TABLE {SILVER_LOOKUP_TABLE}
    CLUSTER BY (poa_code) AS
    WITH
    poa_cell_centers AS (
        SELECT p.poa_code,
               CAST(get_json_object(h3_centerasgeojson(p.h3_cell), '$.coordinates[1]') AS DOUBLE) AS cell_lat,
               CAST(get_json_object(h3_centerasgeojson(p.h3_cell), '$.coordinates[0]') AS DOUBLE) AS cell_lon
        FROM   {CATALOG}.{SILVER_SCHEMA}.poa_h3_lookup p
    ),
    poa_centroids AS (
        SELECT poa_code,
               AVG(cell_lat) AS poa_lat,
               AVG(cell_lon) AS poa_lon
        FROM   poa_cell_centers
        GROUP  BY poa_code
    ),
    csiro_stations AS (
        SELECT DISTINCT
               station_name,
               lat AS station_lat,
               lon AS station_lon
        FROM   {SILVER_PROJECTIONS_TABLE}
    ),
    distances AS (
        SELECT pc.poa_code,
               cs.station_name,
               2 * 6371 * ASIN(SQRT(
                   POWER(SIN(RADIANS((cs.station_lat - pc.poa_lat) / 2)), 2) +
                   COS(RADIANS(pc.poa_lat)) * COS(RADIANS(cs.station_lat)) *
                   POWER(SIN(RADIANS((cs.station_lon - pc.poa_lon) / 2)), 2)
               )) AS distance_km
        FROM   poa_centroids pc
        CROSS JOIN csiro_stations cs
    )
    SELECT poa_code,
           station_name           AS nearest_csiro_station,
           ROUND(distance_km, 1)  AS distance_km
    FROM   distances
    QUALIFY ROW_NUMBER() OVER (PARTITION BY poa_code ORDER BY distance_km) = 1
""")

print(f"✅ {SILVER_LOOKUP_TABLE} built")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Lookup sanity check — nearest station for representative POAs

# COMMAND ----------

display(spark.sql(f"""
    SELECT poa_code, nearest_csiro_station, distance_km
    FROM   {SILVER_LOOKUP_TABLE}
    WHERE  poa_code IN ('2795', '2480', '2640', '2770', '2850', '2450', '2000')
    ORDER  BY poa_code
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Smoke tests — the demo narrative
# MAGIC
# MAGIC ### 6a — Bathurst, mean annual temperature, demo default scenario (rcp45)

# COMMAND ----------

display(spark.sql(f"""
    SELECT period,
           model,
           ROUND(value, 2) AS annual_mean_temp_c
    FROM   {SILVER_PROJECTIONS_TABLE}
    WHERE  station_name     = 'BATHURST-AGRICULTURAL-STATION'
      AND  variable         = 'tas'
      AND  rcp              = 'rcp45'
      AND  time_aggregation = 'Annual'
    ORDER  BY period, model
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6b — model-median across the 8 GCMs (Global Climate Model) ("the honest single number")
# MAGIC
# MAGIC The agent should report the median + min/max range — never a single model.
# MAGIC This produces statements like *"projections range from 22.4 to 24.0°C
# MAGIC by 2080-2099 under rcp45, median 23.1°C"*.

# COMMAND ----------

display(spark.sql(f"""
    SELECT period,
           ROUND(percentile_approx(value, 0.5), 2) AS median_temp_c,
           ROUND(MIN(value), 2)                    AS min_model,
           ROUND(MAX(value), 2)                    AS max_model,
           COUNT(DISTINCT model)                   AS n_models
    FROM   {SILVER_PROJECTIONS_TABLE}
    WHERE  station_name     = 'BATHURST-AGRICULTURAL-STATION'
      AND  variable         = 'tas'
      AND  rcp              = 'rcp45'
      AND  time_aggregation = 'Annual'
    GROUP  BY period
    ORDER  BY period
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6c — agent flow: postcode → nearest station → multi-variable projections (rcp45)

# COMMAND ----------

display(spark.sql(f"""
    SELECT m.poa_code,
           m.nearest_csiro_station,
           m.distance_km,
           c.period,
           c.variable,
           c.units,
           ROUND(percentile_approx(c.value, 0.5), 2) AS median_value
    FROM   {SILVER_LOOKUP_TABLE} m
    JOIN   {SILVER_PROJECTIONS_TABLE} c
      ON   m.nearest_csiro_station = c.station_name
    WHERE  m.poa_code           = '2795'              -- Bathurst (Tom)
      AND  c.rcp                = 'rcp45'
      AND  c.time_aggregation   = 'Annual'
      AND  c.variable           IN ('tas', 'tasmax', 'tasmin', 'pan_evap')
    GROUP  BY m.poa_code, m.nearest_csiro_station, m.distance_km,
              c.period, c.variable, c.units
    ORDER  BY c.period, c.variable
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6d — RCP scenario comparison: how much warmer is rcp85 than rcp45 at Bathurst by 2080?
# MAGIC
# MAGIC Stronger pitch material than picking just one scenario — *"under moderate
# MAGIC emissions, +X°C; under high emissions, +Y°C"*.

# COMMAND ----------

display(spark.sql(f"""
    SELECT rcp,
           ROUND(percentile_approx(value, 0.5), 2) AS median_temp_c,
           ROUND(MIN(value), 2)                    AS min_model,
           ROUND(MAX(value), 2)                    AS max_model
    FROM   {SILVER_PROJECTIONS_TABLE}
    WHERE  station_name     = 'BATHURST-AGRICULTURAL-STATION'
      AND  variable         = 'tas'
      AND  time_aggregation = 'Annual'
      AND  period            = '2080-2099'
    GROUP  BY rcp
    ORDER  BY rcp
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Done
# MAGIC
# MAGIC | Layer | Table | Pattern |
# MAGIC |---|---|---|
# MAGIC | Bronze | `bronze.csiro_<var>` (×6) | One-shot CREATE OR REPLACE per CSV |
# MAGIC | Silver | `silver.csiro_projections` | Long-format unpivot+union, NSW-filtered, `CLUSTER BY (h3_cell, rcp)` |
# MAGIC | Silver | `silver.poa_to_csiro_station` | One row per NSW POA → nearest station, Haversine |
# MAGIC
# MAGIC ### What's next
# MAGIC
# MAGIC - **Fire FFDI xlsx** — not loaded here. If the demo needs explicit fire
# MAGIC   projections, add a small follow-up notebook that parses
# MAGIC   `NRM_fire_proj_summary.xlsx` (needs `%pip install openpyxl`).
# MAGIC - **Confidence bands beyond min/median/max** — CSIRO publishes percentiles
# MAGIC   in a different download. Skip unless the agent's output specifically
# MAGIC   wants 10th/90th percentile language.
# MAGIC - **Per-month seasonality** — kept in Silver via `time_aggregation`. Demo
# MAGIC   queries should filter to `'Annual'` or one season; full monthly noise
# MAGIC   isn't useful for the pitch.
# MAGIC - **Next:** `05_abr_business_runtime.py` — runtime tool only (no Bronze /
# MAGIC   Silver). Translates ABN → postcode → all three of: H3 cells, nearest
# MAGIC   weather location, nearest CSIRO station. That's the stitched data path
# MAGIC   for `verify_abn` plus the agent's spatial setup.