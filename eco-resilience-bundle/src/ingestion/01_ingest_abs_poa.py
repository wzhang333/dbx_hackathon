# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Ingest ABS POA boundaries → Bronze + Silver
# MAGIC
# MAGIC **What this notebook does**
# MAGIC
# MAGIC Reads the Australian Bureau of Statistics POA (Postal Area) 2021 shapefile
# MAGIC from a Unity Catalog Volume, lands it as a Delta table, then builds the
# MAGIC NSW-only `(postcode → H3 cells)` lookup table the agent will use for every
# MAGIC spatial join in the project.
# MAGIC
# MAGIC **Inputs**
# MAGIC - `POA_2021_AUST_GDA2020_SHP.zip` already uploaded to a UC Volume
# MAGIC
# MAGIC **Outputs**
# MAGIC - `<catalog>.bronze.abs_poa_2021` — full national POA polygons + attributes (Delta)
# MAGIC - `<catalog>.silver.poa_h3_lookup` — NSW POA → H3 cell exploded lookup (Delta, Z-ordered)
# MAGIC
# MAGIC **Cluster requirements**
# MAGIC - DBR **13.3+** with Photon (for native `h3_polyfillash3` / `h3_longlatash3`)
# MAGIC - Standard cluster is fine; a 53 MB / 2,650-polygon file is not a Spark workload
# MAGIC
# MAGIC **Expected runtime:** ~1 minute end-to-end

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Install dependencies
# MAGIC
# MAGIC `geopandas` reads the shapefile on the driver. `pyogrio` is the modern, fast
# MAGIC backend (replaces `fiona`). Both pure-Python wheels, no compile.

# COMMAND ----------

# MAGIC %pip install geopandas pyogrio --quiet

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Configuration
# MAGIC
# MAGIC Edit these to match your workspace. The defaults assume the spec layout
# MAGIC (`eco_resilience.bronze.raw_geo`); change if you uploaded the zip elsewhere.

# COMMAND ----------

CATALOG       = "eco_resilience"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
VOLUME_NAME   = "raw_geo"
ZIP_FILENAME  = "POA_2021_AUST_GDA2020_SHP.zip"

# H3 resolution — spec says 8 (~0.7 km² hexes). Don't change without good reason.
H3_RESOLUTION = 8

# NSW filter. The POA shapefile has NO state column (POAs don't strictly nest
# inside states), so we filter by postcode prefix. NSW postcodes are 2xxx,
# which also includes ACT (~30 POAs around Canberra). Acceptable for our demo.
NSW_POSTCODE_PREFIX = "2"

# Derived paths — usually no need to edit
VOLUME_PATH   = f"/Volumes/{CATALOG}/{BRONZE_SCHEMA}/{VOLUME_NAME}"
ZIP_PATH      = f"{VOLUME_PATH}/{ZIP_FILENAME}"
BRONZE_TABLE  = f"{CATALOG}.{BRONZE_SCHEMA}.abs_poa_2021"
SILVER_TABLE  = f"{CATALOG}.{SILVER_SCHEMA}.poa_h3_lookup"

print(f"ZIP_PATH      = {ZIP_PATH}")
print(f"BRONZE_TABLE  = {BRONZE_TABLE}")
print(f"SILVER_TABLE  = {SILVER_TABLE}")
print(f"H3_RESOLUTION = {H3_RESOLUTION}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Sanity check — confirm the zip is in the Volume
# MAGIC
# MAGIC If this fails, either upload the file via the UC Volume browser or update
# MAGIC the config above. Volumes are POSIX-mounted, so plain `os.path` works.

# COMMAND ----------

import os

assert os.path.exists(ZIP_PATH), (
    f"Shapefile not found at {ZIP_PATH}\n"
    f"Either upload it to the Volume or fix the config above."
)
size_mb = os.path.getsize(ZIP_PATH) / 1024 / 1024
print(f"✅ Found {ZIP_FILENAME} ({size_mb:.1f} MB)")

# Make sure the destination schemas exist (no-op if they do)
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SILVER_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Bronze — read shapefile, write full national Delta table
# MAGIC
# MAGIC Driver-side read with GeoPandas. Geometry is converted to **WKT (string)**
# MAGIC so Spark can store it without a native geometry type. We add audit columns
# MAGIC (`_source_file`, `_ingest_time`) for lineage.
# MAGIC
# MAGIC Expected: ~2,650 rows nationally.

# COMMAND ----------

import time
import geopandas as gpd
from pyspark.sql import functions as F

t0 = time.time()
gdf = gpd.read_file(f"zip://{ZIP_PATH}")
elapsed = time.time() - t0

print(f"Read {len(gdf):,} rows in {elapsed:.1f}s")
print(f"CRS:     {gdf.crs}")
print(f"Columns: {list(gdf.columns)}")
gdf.head(3)

# COMMAND ----------

# Convert geometry → WKT, drop the GeoSeries (Spark doesn't know what to do with it)
gdf["geometry_wkt"] = gdf.geometry.to_wkt()
gdf_attrs = gdf.drop(columns="geometry")

(
    spark.createDataFrame(gdf_attrs)
    .withColumn("_source_file", F.lit(ZIP_FILENAME))
    .withColumn("_ingest_time", F.current_timestamp())
    .write.format("delta")
    .mode("overwrite")
    .saveAsTable(BRONZE_TABLE)
)

print(f"✅ Wrote {BRONZE_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bronze validation
# MAGIC
# MAGIC Confirm the row count matches GeoPandas, the schema is sensible, and
# MAGIC `geometry_wkt` is parseable WKT.

# COMMAND ----------

print(f"Bronze rows: {spark.table(BRONZE_TABLE).count():,}")
display(
    spark.table(BRONZE_TABLE)
    .select(
        "POA_CODE21", "POA_NAME21",
        "AREASQKM21", "LOCI_URI21",
        F.length("geometry_wkt").alias("wkt_chars"),
    )
    .orderBy("POA_CODE21")
    .limit(5)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Silver — NSW filter + H3 polyfill
# MAGIC
# MAGIC Native `h3_polyfillash3(geometry_wkt, resolution)` returns the array of
# MAGIC H3 cell IDs covering each polygon. We `EXPLODE` so the result is one row
# MAGIC per (postcode, H3 cell) pair — that's the shape the agent's tools want.
# MAGIC
# MAGIC NSW only here; we keep the full national set in Bronze for re-use.

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE TABLE {SILVER_TABLE} AS
    SELECT
        POA_CODE21                                         AS poa_code,
        POA_NAME21                                         AS poa_name,
        AREASQKM21                                         AS area_sqkm,
        explode(h3_polyfillash3(geometry_wkt, {H3_RESOLUTION})) AS h3_cell
    FROM   {BRONZE_TABLE}
    -- POA shapefile has no state column. NSW postcodes start with '2'
    -- (this also includes ACT's ~30 POAs, which is fine for the demo).
    WHERE  POA_CODE21 LIKE '{NSW_POSTCODE_PREFIX}%'
""")

print(f"✅ Wrote {SILVER_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Silver validation — counts, distribution, Bathurst spot-check

# COMMAND ----------

silver = spark.table(SILVER_TABLE)
print(f"Silver rows (postcode × h3 cell): {silver.count():,}")
print(f"Distinct NSW postcodes:          {silver.select('poa_code').distinct().count():,}")

# COMMAND ----------

# Distribution: how many H3 cells per postcode? (small CBDs vs huge rural POAs)
display(spark.sql(f"""
    SELECT poa_code, area_sqkm, COUNT(*) AS h3_cells
    FROM   {SILVER_TABLE}
    GROUP  BY poa_code, area_sqkm
    ORDER  BY h3_cells DESC
    LIMIT  10
"""))

# COMMAND ----------

# Sanity check: Bathurst (POA 2795) — the Tom-the-farmer demo postcode
display(spark.sql(f"""
    SELECT poa_code, area_sqkm, COUNT(*) AS h3_cells
    FROM   {SILVER_TABLE}
    WHERE  poa_code = '2795'
    GROUP  BY poa_code, area_sqkm
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Optimize for the join pattern
# MAGIC
# MAGIC Every agent tool that needs spatial reasoning will join `silver.poa_h3_lookup`
# MAGIC against weather/hazard tables on `h3_cell`. Z-ordering colocates rows with
# MAGIC nearby H3 cells on disk → faster joins.

# COMMAND ----------

spark.sql(f"OPTIMIZE {SILVER_TABLE} ZORDER BY (h3_cell)")
print(f"✅ Optimized {SILVER_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Smoke test — simulate the agent's lookup
# MAGIC
# MAGIC The whole point of this notebook is to make this query trivial and fast:
# MAGIC "Tom enters postcode 2795 → give me the H3 cells covering his farm."
# MAGIC The agent calls this exact query in production.

# COMMAND ----------

import time
t0 = time.time()
cells = spark.sql(f"""
    SELECT h3_cell
    FROM   {SILVER_TABLE}
    WHERE  poa_code = '2795'
""").collect()
elapsed_ms = (time.time() - t0) * 1000

print(f"Bathurst (POA 2795) → {len(cells)} H3 cells in {elapsed_ms:.0f} ms")
print(f"First 5 cells: {[c.h3_cell for c in cells[:5]]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Done
# MAGIC
# MAGIC | Layer | Table | Rows | Use |
# MAGIC |---|---|---|---|
# MAGIC | Bronze | `eco_resilience.bronze.abs_poa_2021` | ~2,650 | Full national POAs (audit/re-use) |
# MAGIC | Silver | `eco_resilience.silver.poa_h3_lookup` | ~500K | NSW (postcode → H3 cells), Z-ordered |
# MAGIC
# MAGIC ### What's next
# MAGIC
# MAGIC With this lookup in place, the next ingestion notebooks can land their data
# MAGIC pre-keyed to H3 and join trivially:
# MAGIC
# MAGIC - `02_ingest_open_meteo.py` — weather points → `h3_longlatash3(lat, lon, 8)`
# MAGIC - `03_ingest_tfnsw_hazards.py` — hazard polylines → polyfill via `h3_polyfillash3`
# MAGIC - `04_ingest_csiro_stations.py` — station points → `h3_longlatash3` then nearest-station per POA
# MAGIC - `05_ingest_abr_business.py` — at runtime: ABN → postcode → this lookup → cells