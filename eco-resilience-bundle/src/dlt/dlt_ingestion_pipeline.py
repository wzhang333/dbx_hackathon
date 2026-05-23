# Databricks notebook source
# MAGIC %md
# MAGIC # 17 — Lakeflow Spark Declarative Pipelines (SDP) — Ingestion Refactor
# MAGIC
# MAGIC Declarative re-implementation of notebooks **02 (weather)** + **03 (hazards)**
# MAGIC + the gold view from **16** using **Lakeflow Spark Declarative Pipelines**
# MAGIC — the framework formerly known as Delta Live Tables (DLT), renamed in 2026.
# MAGIC Same data, same shape, same end-state — different paradigm (declarative vs imperative).
# MAGIC
# MAGIC ### What changed when DLT became Lakeflow SDP
# MAGIC
# MAGIC | Old (still works for backwards compat) | New (this notebook uses) |
# MAGIC |---|---|
# MAGIC | `import dlt` | `from pyspark import pipelines as dp` |
# MAGIC | `@dp.materialized_view` (for full-rebuild tables) | `@dp.materialized_view` ← clearer semantics |
# MAGIC | `@dp.materialized_view` (for streaming reads) | `@dp.table` (still streaming-only) |
# MAGIC | `@dlt.view` | `@dp.temporary_view` |
# MAGIC | `@dp.expect_*` | `@dp.expect_*` |
# MAGIC | `dp.read("name")` | `dp.read("name")` |
# MAGIC
# MAGIC Existing `@dlt.*` code keeps working — but the new API ships with Apache
# MAGIC Spark Declarative Pipelines 4.1+ and is the going-forward standard.
# MAGIC
# MAGIC ### Why this exists
# MAGIC
# MAGIC The hackathon's deployed agent already works on the **original** Silver tables
# MAGIC (refreshed by Workflow jobs from notebook 14). This SDP version is purely
# MAGIC **additive** — it writes to a new schema `eco_resilience.dlt.*` so the
# MAGIC deployed agent stays stable while you learn the SDP paradigm side-by-side.
# MAGIC
# MAGIC ### Two-component architecture
# MAGIC
# MAGIC ```
# MAGIC notebook 18 (Workflow job)     →  /Volumes/eco_resilience/dlt/landing/*.json
# MAGIC                                                  ↓ Auto Loader
# MAGIC notebook 17 (this SDP pipeline)  →  bronze ─┬─→ silver_*_history ─┬─→ gold_*_current
# MAGIC                                              │                    └─→ gold_*_history
# MAGIC                                              └─→ silver_*_current
# MAGIC ```
# MAGIC
# MAGIC ### Learning callouts to look for as you read
# MAGIC
# MAGIC | Concept | Where to look |
# MAGIC |---|---|
# MAGIC | `dp.create_streaming_table` + `@dp.append_flow` | §3 bronze — true append-only with Auto Loader as the streaming source |
# MAGIC | `spark.readStream.format("cloudFiles")` | §3 bronze — Auto Loader options + `_metadata.file_modification_time` |
# MAGIC | `@dp.materialized_view` | §4 silver, §5 gold — used for batch rebuild semantics |
# MAGIC | History vs current split | §4 silver, §5 gold — `_history` for trend questions, `_current` for "what's now" |
# MAGIC | `dense_rank()` for latest-batch isolation | §4 silver — picks the latest batch from accumulating history |
# MAGIC | `@dp.expect` / `_or_drop` / `_or_fail` | Three quality levels — declared inline in `dp.create_streaming_table()` for bronze, stacked as decorators for materialized views |
# MAGIC | `dp.read("name")` vs `spark.table()` | SDP-managed read vs external read — only `dp.read` adds an edge to the DAG |
# MAGIC | `cluster_by=[...]` | Liquid Clustering, declarative form |
# MAGIC
# MAGIC ### Pipeline configuration (set in the UI, NOT in this notebook)
# MAGIC
# MAGIC | Setting | Value |
# MAGIC |---|---|
# MAGIC | Pipeline name | `eco_resilience_dlt_ingestion` |
# MAGIC | Pipeline mode | Triggered |
# MAGIC | Target schema | `eco_resilience.dlt` |
# MAGIC | Compute | Serverless |
# MAGIC | Channel | Current |
# MAGIC | Schedule | every 3h, offset ~15 min from notebook 18's fetcher |
# MAGIC
# MAGIC Secrets are NOT needed in this pipeline anymore — they live in the notebook 18
# MAGIC fetcher Workflow. SDP only reads landing files; no API auth from this side.
# MAGIC
# MAGIC ### IMPORTANT — this notebook only RUNS inside a Lakeflow SDP pipeline
# MAGIC
# MAGIC Trying to run cells individually in an interactive notebook **won't work** —
# MAGIC the `@dp.materialized_view` decorators register tables, they don't execute.
# MAGIC To see this notebook do anything: create the pipeline in the UI (see steps
# MAGIC above), point it at this notebook, and click **Start**.

# COMMAND ----------

# MAGIC %md
# MAGIC ## §1 — Imports + configuration

# COMMAND ----------

# The new Lakeflow Spark Declarative Pipelines (SDP) import — replaces `import dlt`.
# Available in Databricks pipelines runtime + Apache Spark 4.1+.
# `import dlt` still works for backwards compat, but `dp` is the modern alias.
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Spatial resolution — matches the rest of the project (h3 res-8 = ~1km cells)
H3_RESOLUTION = 8

# JSON landing volume written by notebook 18. Auto Loader watches this path.
LANDING_BASE = "/Volumes/eco_resilience/dlt/landing"

# Number of seeded weather locations — defines the silver_weather_current contract
# (one row per seeded location). Keep in sync with notebook 18's SEEDED_LOCATIONS
# and notebook 02's LOCATIONS. See [[feedback-seed-name-vocabulary]].
EXPECTED_SEED_COUNT = 15

# Existing (non-DLT) Silver tables we read from for the gold composition.
SILVER_POA_H3      = "eco_resilience.silver.poa_h3_lookup"
SILVER_POA_WEATHER = "eco_resilience.silver.poa_to_weather_location"
SILVER_POA_CSIRO   = "eco_resilience.silver.poa_to_csiro_station"
SILVER_CSIRO       = "eco_resilience.silver.csiro_projections"

# CSIRO periods (Baseline vs Future)
CSIRO_BASELINE_PERIOD = "2020-2039"
CSIRO_FUTURE_PERIOD   = "2080-2099"

# COMMAND ----------

# MAGIC %md
# MAGIC ## §2 — No fetcher helpers here anymore
# MAGIC
# MAGIC In an earlier iteration of this notebook the @dp.materialized_view bronze
# MAGIC tables called HTTP-fetch helpers inline. That worked, but it meant **the
# MAGIC pipeline IS the fetcher** — every run hit the upstream APIs, with no
# MAGIC replay path and no append-only history.
# MAGIC
# MAGIC The current design moves all fetching to **`notebooks/18_api_to_landing.py`**
# MAGIC (scheduled via Workflows). That notebook writes JSON files to the landing
# MAGIC volume, and this pipeline picks them up via Auto Loader as a streaming
# MAGIC source. Three immediate wins:
# MAGIC
# MAGIC 1. **Replayability** — landing files survive checkpoint resets; rebuild bronze any time.
# MAGIC 2. **`@dp.append_flow` becomes legal** — Auto Loader is a streaming source, so SDP accepts it where it rejected our previous batch fetch.
# MAGIC 3. **Decoupled cadences** — fetcher can run hourly while pipeline runs every 3-6 hours, or vice versa.

# COMMAND ----------

# MAGIC %md
# MAGIC ## §3 — Bronze layer: streaming tables fed by Auto Loader
# MAGIC
# MAGIC ### What changed
# MAGIC
# MAGIC Bronze used to be `@dp.materialized_view` calling HTTP fetchers inline —
# MAGIC every pipeline run rebuilt from scratch with no history. Now:
# MAGIC
# MAGIC ```
# MAGIC nb 18 (Workflow) → JSON files in /Volumes/eco_resilience/dlt/landing/
# MAGIC                       ↓ Auto Loader watches for new files
# MAGIC          dp.create_streaming_table  ←  @dp.append_flow
# MAGIC ```
# MAGIC
# MAGIC ### Why this is the SDP-blessed pattern
# MAGIC
# MAGIC `@dp.append_flow` requires a **streaming source** (Kafka, Auto Loader, or
# MAGIC another streaming table). Auto Loader's `cloudFiles` format qualifies, so
# MAGIC SDP accepts the append-once semantic that it rejected with our previous
# MAGIC batch fetch (`CREATE_APPEND_ONCE_FLOW_FROM_BATCH_QUERY_NOT_ALLOWED`).
# MAGIC
# MAGIC ### Auto Loader options worth understanding
# MAGIC
# MAGIC | Option | Why |
# MAGIC |---|---|
# MAGIC | `cloudFiles.format = "json"` | We write JSON arrays from notebook 18 |
# MAGIC | `cloudFiles.schemaLocation = ".../_schema/<source>"` | Auto Loader needs writable storage for its inferred schema + state. Underscore prefix keeps these files out of the data scan |
# MAGIC | `cloudFiles.inferColumnTypes = true` | Types like `temperature_c` come out as DOUBLE not STRING |
# MAGIC | `multiLine = true` | Critical: notebook 18 writes ONE JSON array per file. Without this, Auto Loader treats each char as a row |
# MAGIC | `cloudFiles.schemaEvolutionMode = "addNewColumns"` | If the API adds a field, the pipeline keeps running; the new column is added with NULL backfill |
# MAGIC
# MAGIC ### Quality levels still demonstrated
# MAGIC
# MAGIC | Decorator | Behavior on failure | Use for |
# MAGIC |---|---|---|
# MAGIC | `@dp.expect(name, expr)` | Log + flag; row passes through | Soft signals you want to monitor |
# MAGIC | `@dp.expect_or_drop(name, expr)` | Drop the failing row, continue | Data hygiene before silver |
# MAGIC | `@dp.expect_or_fail(name, expr)` | Abort the pipeline run | Contract violations — never silently swallow |
# MAGIC
# MAGIC Expectations on a streaming table work the same way they do on a materialized
# MAGIC view — they're enforced row-by-row as Auto Loader appends.

# COMMAND ----------

# ─── Bronze: weather (Auto Loader → streaming append) ─────────────────
dp.create_streaming_table(
    name="bronze_weather_dlt",
    comment=(
        "Append-only history of every Open-Meteo response, ingested via Auto "
        "Loader from /Volumes/eco_resilience/dlt/landing/weather/. One bronze "
        "row per (location, forecast_time) per fetcher batch. Cleared by Full "
        "Refresh; replayable from landing files."
    ),
    table_properties={
        "quality":                        "bronze",
        "pipelines.autoOptimize.managed": "true",
        "delta.enableChangeDataFeed":     "true",
    },
    expect_all_or_drop={
        "valid_temperature":   "temperature_c BETWEEN -50 AND 60",
        "valid_precipitation": "precipitation_mm >= 0 OR precipitation_mm IS NULL",
        "valid_windspeed":     "windspeed_kmh   >= 0 OR windspeed_kmh   IS NULL",
    },
    expect_all={
        "forecast_recent": "forecast_time IS NOT NULL",
    },
)


@dp.append_flow(target="bronze_weather_dlt")
def bronze_weather_flow():
    """Auto Loader stream off the weather/ landing subdir."""
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format",              "json")
        .option("cloudFiles.schemaLocation",      f"{LANDING_BASE}/_schema/weather")
        .option("cloudFiles.inferColumnTypes",    "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("multiLine",                      "true")
        .load(f"{LANDING_BASE}/weather/")
        # Cast forecast_time from ISO string to timestamp
        .withColumn("forecast_time", F.to_timestamp("forecast_time"))
        # Prefer the source file's modification time for replay-safe history;
        # current_timestamp() is fine for first-ingest semantics.
        .withColumn("_ingest_time",  F.col("_metadata.file_modification_time"))
        .withColumn("_source_file",  F.col("_metadata.file_path"))
        .withColumn("_source",       F.lit("open-meteo"))
    )

# COMMAND ----------

# DBTITLE 1,Untitled
# ─── Bronze: hazards (Auto Loader → streaming append + h3 derivation) ──
dp.create_streaming_table(
    name="bronze_hazards_dlt",
    comment=(
        "Append-only history of every TfNSW hazard payload, ingested via Auto "
        "Loader from /Volumes/eco_resilience/dlt/landing/hazards/. Adds h3_cell "
        "spatial index + parsed timestamps. Cleared by Full Refresh; replayable "
        "from landing files."
    ),
    table_properties={
        "quality":                        "bronze",
        "pipelines.autoOptimize.managed": "true",
        "delta.enableChangeDataFeed":     "true",
    },
    expect_all_or_drop={
        "known_hazard_type": "hazard_type IN ('flood', 'fire', 'roadwork', 'incident')",
        "longitude_in_nsw":  "longitude IS NULL OR longitude BETWEEN 140.0 AND 154.0",
        "latitude_in_nsw":   "latitude  IS NULL OR latitude  BETWEEN -37.5 AND -28.0",
    },
    expect_all={
        "has_geometry":      "longitude IS NOT NULL AND latitude IS NOT NULL",
    },
)


@dp.append_flow(target="bronze_hazards_dlt")
def bronze_hazards_flow():
    """Auto Loader stream off the hazards/ landing subdir, with h3 derivation."""
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format",              "json")
        .option("cloudFiles.schemaLocation",      f"{LANDING_BASE}/_schema/hazards")
        .option("cloudFiles.inferColumnTypes",    "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("multiLine",                      "true")
        .load(f"{LANDING_BASE}/hazards/")
        # API delivers epoch ms — convert to timestamp
        .withColumn("created_ts",      F.to_timestamp(F.col("created_ms")      / 1000))
        .withColumn("last_updated_ts", F.to_timestamp(F.col("last_updated_ms") / 1000))
        # Spatial H3 — Databricks SQL takes LON first
        .withColumn(
            "h3_cell",
            F.when(
                F.col("longitude").isNotNull() & F.col("latitude").isNotNull(),
                F.expr(f"h3_longlatash3(longitude, latitude, {H3_RESOLUTION})"),
            ),
        )
        .withColumn("_ingest_time", F.col("_metadata.file_modification_time"))
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_source",      F.lit("tfnsw"))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## §4 — Silver layer: split into `_history` + `_current`
# MAGIC
# MAGIC Bronze now accumulates every fetch forever (Auto Loader append). Silver
# MAGIC fans it out into **two question shapes**:
# MAGIC
# MAGIC | Table | Grain | Use for |
# MAGIC |---|---|---|
# MAGIC | `silver_weather_history_dlt` | One row per (location, forecast_time, batch) | Trend analysis: "how did our forecast for 2480 evolve over the past week?" |
# MAGIC | `silver_weather_current_dlt` | 15 rows — one per seeded location | "What's the weather like right now and in the next 24h?" |
# MAGIC | `silver_hazards_history_dlt` | One row per hazard payload per batch | "How often does postcode X appear in hazards over time?" |
# MAGIC | `silver_hazards_current_dlt` | Latest batch only, active + h3-joinable | "What's active right now?" |
# MAGIC
# MAGIC ### Why split?
# MAGIC
# MAGIC A single silver table with both shapes would force every consumer to filter
# MAGIC `WHERE _ingest_time = (SELECT MAX(...))` to get "current" answers. Easy
# MAGIC footgun, easy to forget, and Genie's text-to-SQL gets it wrong half the
# MAGIC time. Two tables make the question explicit in the name.
# MAGIC
# MAGIC ### Latest-batch isolation — `dense_rank()`, not `row_number()`
# MAGIC
# MAGIC `_current` tables read from `_history` and filter to the latest batch using
# MAGIC `dense_rank() OVER (ORDER BY _ingest_time DESC) = 1`. `dense_rank` (not
# MAGIC `row_number`) is the right choice because every row in one batch shares the
# MAGIC same `_ingest_time` — we want ALL rows from the latest batch, not just one.
# MAGIC
# MAGIC ### Quality contracts to notice
# MAGIC
# MAGIC - `silver_weather_current_dlt` carries `@dp.expect_or_fail("expected_15_locations", ...)` — if the latest batch lost a location, the pipeline halts. Better to fail loud than silently downgrade.
# MAGIC - `silver_hazards_current_dlt` uses `_or_drop` for `ended` / `h3_cell` / `severity_score` — these are row-hygiene checks, not pipeline contracts.
# MAGIC
# MAGIC ### `dp.read()` vs `spark.table()`
# MAGIC
# MAGIC `dp.read("name")` is how SDP discovers cross-table dependencies. Using
# MAGIC `spark.table("eco_resilience.dlt.name")` works at runtime but doesn't appear
# MAGIC in the SDP DAG, breaking the auto-orchestration story.

# COMMAND ----------

# ─── Silver: weather HISTORY (passthrough of bronze) ──────────────────
@dp.materialized_view(
    name="silver_weather_history_dlt",
    comment=(
        "Every Open-Meteo forecast row ever ingested, with parsed forecast_time. "
        "Grain: one row per (location_name, forecast_time, _ingest_time). Use "
        "this for trend / forecast-skill / historical questions. For 'what's the "
        "weather now?', use silver_weather_current_dlt."
    ),
    table_properties={"quality": "silver"},
    cluster_by=["_ingest_time", "location_name"],
)
def silver_weather_history_dlt():
    return dp.read("bronze_weather_dlt")

# COMMAND ----------

# ─── Silver: hazards HISTORY (passthrough of bronze) ──────────────────
@dp.materialized_view(
    name="silver_hazards_history_dlt",
    comment=(
        "Every TfNSW hazard payload ever ingested. Grain: one row per (hazard_id, "
        "_ingest_time). Use this for 'how often does postcode X show up in "
        "hazards over the past N months?' For 'what's active right now?', use "
        "silver_hazards_current_dlt."
    ),
    table_properties={"quality": "silver"},
    cluster_by=["_ingest_time", "h3_cell"],
)
def silver_hazards_history_dlt():
    return dp.read("bronze_hazards_dlt")

# COMMAND ----------

# ─── Silver: weather CURRENT (latest batch, aggregated to 15 rows) ─────
@dp.materialized_view(
    name="silver_weather_current_dlt",
    comment=(
        "Per-location weather summary — one row per seeded NSW location. "
        "Reads the latest batch from silver_weather_history_dlt and aggregates "
        "to 'current observation + next-24h outlook'. 'Current' = forecast row "
        "closest to NOW; '24h' = stats over forecast_time in [now, now+24h)."
    ),
    table_properties={"quality": "silver"},
    cluster_by=["location_name"],
)
@dp.expect_or_drop("current_temp_valid",
                   "current_temperature_c IS NULL OR current_temperature_c BETWEEN -50 AND 60")
def silver_weather_current_dlt():
    """Latest-batch aggregation of weather history into per-location summaries."""
    history = dp.read("silver_weather_history_dlt")

    # ── Isolate the latest batch using dense_rank on _ingest_time ──
    latest_batch = (
        history
        .withColumn(
            "_batch_rank",
            F.dense_rank().over(Window.orderBy(F.desc("_ingest_time"))),
        )
        .filter(F.col("_batch_rank") == 1)
        .drop("_batch_rank")
    )

    # ── Part A: "current" — forecast row closest to NOW per location ──
    now = F.current_timestamp()
    w_partition = Window.partitionBy("location_name").orderBy(
        F.abs(F.unix_timestamp("forecast_time") - F.unix_timestamp(now))
    )
    current = (
        latest_batch
        .withColumn("_rn", F.row_number().over(w_partition))
        .filter(F.col("_rn") == 1)
        .select(
            "location_name", "latitude", "longitude",
            F.col("forecast_time").alias("current_observed_at"),
            F.col("temperature_c").alias("current_temperature_c"),
            F.col("precipitation_mm").alias("current_precipitation_mm"),
            F.col("windspeed_kmh").alias("current_windspeed_kmh"),
            F.col("humidity_pct").alias("current_humidity_pct"),
            F.col("weather_code").alias("current_weather_code"),
        )
    )

    # ── Part B: next-24h aggregate stats per location ──
    next_24h = (
        latest_batch
        .filter(F.col("forecast_time") >= now)
        .filter(F.col("forecast_time") <  now + F.expr("INTERVAL 24 HOURS"))
        .groupBy("location_name")
        .agg(
            F.max("temperature_c").alias("next_24h_max_temp_c"),
            F.min("temperature_c").alias("next_24h_min_temp_c"),
            F.round(F.sum("precipitation_mm"), 2).alias("next_24h_total_rain_mm"),
            F.max("windspeed_kmh").alias("next_24h_max_wind_kmh"),
            F.round(F.avg("humidity_pct"), 1).alias("next_24h_avg_humidity_pct"),
        )
    )

    return current.join(next_24h, on="location_name", how="left")

# COMMAND ----------

# ─── Silver: hazards CURRENT (latest batch, active + h3-joinable) ──────
@dp.materialized_view(
    name="silver_hazards_current_dlt",
    comment=(
        "Active, spatially-joinable TfNSW hazards with computed severity_score, "
        "limited to the latest batch from silver_hazards_history_dlt. Filters "
        "ended=false AND h3_cell IS NOT NULL (drops statewide announcements with "
        "no geometry). severity_score: 1=routine, 2=impacting network, 3=major."
    ),
    table_properties={"quality": "silver"},
    cluster_by=["h3_cell"],
)
@dp.expect_or_drop("only_active",          "ended = false")
@dp.expect_or_drop("has_h3_cell",          "h3_cell IS NOT NULL")
@dp.expect_or_drop("valid_severity_score", "severity_score IN (1, 2, 3)")
def silver_hazards_current_dlt():
    """Latest-batch filter + severity_score derivation."""
    history = dp.read("silver_hazards_history_dlt")

    latest_batch = (
        history
        .withColumn(
            "_batch_rank",
            F.dense_rank().over(Window.orderBy(F.desc("_ingest_time"))),
        )
        .filter(F.col("_batch_rank") == 1)
        .drop("_batch_rank")
    )

    return (
        latest_batch
        .filter(F.col("ended") == False)
        .filter(F.col("h3_cell").isNotNull())
        .withColumn(
            "severity_score",
            F.when(F.col("is_major") == True, F.lit(3))
             .when(F.col("impacting_network") == True, F.lit(2))
             .otherwise(F.lit(1)),
        )
        .select(
            "hazard_type", "hazard_id", "main_category", "display_name",
            "headline", "is_major", "impacting_network", "severity_score",
            "ended",
            "h3_cell", "longitude", "latitude",
            "created_ts", "last_updated_ts",
            "_ingest_time",
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## §5 — Gold layer: split into `_current` + `_history`
# MAGIC
# MAGIC Same split principle as silver, applied to the analytical join:
# MAGIC
# MAGIC | Table | Grain | Use for |
# MAGIC |---|---|---|
# MAGIC | `gold_nsw_postcode_resilience_current_dlt` | One row per postcode, "right now" | Operational questions — what's the state of 2795 today? |
# MAGIC | `gold_nsw_postcode_resilience_history_dlt` | One row per (postcode, snapshot_date) where hazards were observed | Trend questions — has 2480's flood frequency changed since March? |
# MAGIC
# MAGIC ### Why two tables, not one with a snapshot_date filter
# MAGIC
# MAGIC Genie text-to-SQL gets ambiguous when one table answers both shapes. Two
# MAGIC tables make the intent explicit in the name; Genie's LLM picks the right
# MAGIC one from phrasing without needing a `WHERE snapshot_date = MAX(...)` hint.
# MAGIC The downstream agent reads `_current` (deterministic, fast). Genie can hit
# MAGIC either when its sample-questions library covers both shapes.
# MAGIC
# MAGIC ### What gold_history intentionally drops
# MAGIC
# MAGIC - **Climate (CSIRO) columns** — invariant per postcode; rejoin from `_current` if needed for cross-reference.
# MAGIC - **Forward-looking weather forecast fields** — `_history` snapshots are about what was OBSERVED, not what was predicted. Weather columns there are aggregated past observations only.

# COMMAND ----------

# ─── Gold: CURRENT (same logic as before — renamed for parallelism with _history) ───
@dp.materialized_view(
    name="gold_nsw_postcode_resilience_current_dlt",
    comment=(
        "Per-postcode resilience snapshot for NSW — current state. One row per "
        "postcode, joining latest-batch hazards + weather with non-DLT silver "
        "tables for POA + CSIRO climate. Materialized for sub-second Genie reads. "
        "For trend questions, use gold_nsw_postcode_resilience_history_dlt."
    ),
    table_properties={"quality": "gold"},
)
@dp.expect("warming_within_sanity",
            "warming_2080s_rcp85_c IS NULL OR warming_2080s_rcp85_c BETWEEN -2 AND 10")
def gold_nsw_postcode_resilience_current_dlt():
    weather = dp.read("silver_weather_current_dlt")   # 15 rows, one per location, pre-aggregated
    hazards = dp.read("silver_hazards_current_dlt")   # active hazards only, with severity_score

    # Non-DLT silver tables — these stay in eco_resilience.silver
    poa_h3   = spark.table(SILVER_POA_H3)
    poa_w    = spark.table(SILVER_POA_WEATHER)
    poa_c    = spark.table(SILVER_POA_CSIRO)
    csiro    = spark.table(SILVER_CSIRO)

    # ── Cell count per postcode ──
    cell_count = poa_h3.groupBy("poa_code").agg(
        F.countDistinct("h3_cell").alias("h3_cells_count")
    )

    # ── Weather per postcode — silver already aggregated to 1 row per location ──
    # No more Window / row_number gymnastics: silver did the work upstream.
    weather_per_poa = (
        poa_w
        .join(weather,
              poa_w["nearest_weather_location"] == weather["location_name"],
              "left")
        .select(
            poa_w["poa_code"],
            poa_w["nearest_weather_location"].alias("weather_station_name"),
            F.col("current_temperature_c"),
            F.col("current_precipitation_mm"),
            F.col("current_windspeed_kmh"),
            F.col("current_weather_code"),
            F.col("current_observed_at").alias("weather_observed_at"),
            F.col("next_24h_max_temp_c"),
            F.col("next_24h_min_temp_c"),
            F.col("next_24h_total_rain_mm"),
            F.col("next_24h_max_wind_kmh"),
        )
    )

    # ── Hazard counts per postcode (silver already filtered to active + h3-joinable) ──
    poa_x_hazard = (
        poa_h3.alias("p")
        .join(hazards.alias("h"),
              F.col("p.h3_cell") == F.col("h.h3_cell"),
              "left")
        .groupBy("p.poa_code")
        .agg(
            F.countDistinct(F.when(F.col("h.hazard_type") == "flood",    F.col("h.h3_cell"))).alias("active_flood_count"),
            F.countDistinct(F.when(F.col("h.hazard_type") == "fire",     F.col("h.h3_cell"))).alias("active_fire_count"),
            F.countDistinct(F.when(F.col("h.hazard_type") == "roadwork", F.col("h.h3_cell"))).alias("active_roadwork_count"),
            F.countDistinct(F.when(F.col("h.hazard_type") == "incident", F.col("h.h3_cell"))).alias("active_incident_count"),
            F.countDistinct(F.when(F.col("h.is_major") == True,          F.col("h.h3_cell"))).alias("major_hazards_count"),
            F.countDistinct(F.col("h.h3_cell")).alias("total_active_hazards"),
            F.max(F.col("h.severity_score")).alias("max_severity_score"),
        )
    )

    # ── Climate projections per postcode (2020s baseline vs 2080s RCP scenarios) ──
    csiro_pivot = (
        poa_c
        .join(csiro,
              (poa_c["nearest_csiro_station"] == csiro["station_name"]) &
              (csiro["variable"]        == F.lit("tas")) &
              (csiro["aggregation"]     == F.lit("mean")) &
              (csiro["time_aggregation"] == F.lit("Annual")),
              "left")
        .groupBy(poa_c["poa_code"], poa_c["nearest_csiro_station"])
        .agg(
            F.max(F.when((csiro["period"] == CSIRO_BASELINE_PERIOD) & (csiro["rcp"] == "rcp45"),
                         csiro["value"])).alias("climate_temp_2020s"),
            F.max(F.when((csiro["period"] == CSIRO_FUTURE_PERIOD) & (csiro["rcp"] == "rcp45"),
                         csiro["value"])).alias("climate_temp_2080s_rcp45"),
            F.max(F.when((csiro["period"] == CSIRO_FUTURE_PERIOD) & (csiro["rcp"] == "rcp85"),
                         csiro["value"])).alias("climate_temp_2080s_rcp85"),
        )
    )

    # ── Final compose ──
    return (
        cell_count
        .join(weather_per_poa, "poa_code", "left")
        .join(poa_x_hazard,    "poa_code", "left")
        .join(csiro_pivot,     "poa_code", "left")
        .withColumn("postcode", F.col("poa_code"))
        .withColumn("state",    F.lit("NSW"))
        # Wrap nulls to 0 for cleaner Genie SQL
        .withColumn("active_flood_count",     F.coalesce("active_flood_count",    F.lit(0)))
        .withColumn("active_fire_count",      F.coalesce("active_fire_count",     F.lit(0)))
        .withColumn("active_roadwork_count",  F.coalesce("active_roadwork_count", F.lit(0)))
        .withColumn("active_incident_count",  F.coalesce("active_incident_count", F.lit(0)))
        .withColumn("major_hazards_count",    F.coalesce("major_hazards_count",   F.lit(0)))
        .withColumn("total_active_hazards",   F.coalesce("total_active_hazards",  F.lit(0)))
        .withColumn("max_severity_score",     F.coalesce("max_severity_score",    F.lit(0)))
        # Composite risk level — same rule as notebook 16
        .withColumn("risk_level", F.expr("""
            CASE
                WHEN major_hazards_count > 0                                 THEN 'Critical'
                WHEN active_flood_count + active_fire_count > 0              THEN 'High'
                WHEN total_active_hazards > 0                                THEN 'Moderate'
                ELSE                                                              'Low'
            END
        """))
        # Derived warming column
        .withColumn(
            "warming_2080s_rcp85_c",
            F.col("climate_temp_2080s_rcp85") - F.col("climate_temp_2020s"),
        )
        .select(
            "postcode", "state", "h3_cells_count",
            "weather_station_name",
            "current_temperature_c", "current_precipitation_mm",
            "current_windspeed_kmh", "current_weather_code", "weather_observed_at",
            "next_24h_max_temp_c", "next_24h_min_temp_c",
            "next_24h_total_rain_mm", "next_24h_max_wind_kmh",
            "active_flood_count", "active_fire_count", "active_roadwork_count",
            "active_incident_count", "major_hazards_count", "total_active_hazards",
            "max_severity_score", "risk_level",
            "nearest_csiro_station", "climate_temp_2020s",
            "climate_temp_2080s_rcp45", "climate_temp_2080s_rcp85", "warming_2080s_rcp85_c",
        )
    )

# COMMAND ----------

# ─── Gold: HISTORY (per-postcode trend table, hazards + daily weather aggregates) ───
@dp.materialized_view(
    name="gold_nsw_postcode_resilience_history_dlt",
    comment=(
        "Per-postcode trend table for NSW resilience. One row per (postcode, "
        "snapshot_date) summarizing OBSERVED hazards + daily weather aggregates "
        "from silver_*_history. Used for 'has 2480's flood risk changed?' / "
        "'how often did X show up in hazards last month?' questions. Climate "
        "columns intentionally absent (invariant per postcode) — join "
        "gold_*_current for those."
    ),
    table_properties={"quality": "gold"},
    cluster_by=["snapshot_date", "postcode"],
)
@dp.expect("snapshot_date_not_null", "snapshot_date IS NOT NULL")
def gold_nsw_postcode_resilience_history_dlt():
    """Hazard + weather aggregates per (postcode, snapshot_date)."""
    hazards_h = (
        dp.read("silver_hazards_history_dlt")
        .filter(F.col("h3_cell").isNotNull())
        .filter(F.col("ended") == False)
        .withColumn("snapshot_date", F.to_date("_ingest_time"))
    )
    weather_h = (
        dp.read("silver_weather_history_dlt")
        .withColumn("snapshot_date", F.to_date("_ingest_time"))
    )

    poa_h3 = spark.table(SILVER_POA_H3)
    poa_w  = spark.table(SILVER_POA_WEATHER)

    # ── Hazards per (postcode, snapshot_date) ──
    # countDistinct on hazard_id dedups the same hazard appearing in multiple
    # batches within one day (8 batches × same hazard = 1 count).
    poa_x_hazard_history = (
        poa_h3.alias("p")
        .join(hazards_h.alias("h"),
              F.col("p.h3_cell") == F.col("h.h3_cell"),
              "inner")
        .groupBy("p.poa_code", "h.snapshot_date")
        .agg(
            F.countDistinct(F.when(F.col("h.hazard_type") == "flood",    F.col("h.hazard_id"))).alias("active_flood_count"),
            F.countDistinct(F.when(F.col("h.hazard_type") == "fire",     F.col("h.hazard_id"))).alias("active_fire_count"),
            F.countDistinct(F.when(F.col("h.hazard_type") == "roadwork", F.col("h.hazard_id"))).alias("active_roadwork_count"),
            F.countDistinct(F.when(F.col("h.hazard_type") == "incident", F.col("h.hazard_id"))).alias("active_incident_count"),
            F.countDistinct(F.when(F.col("h.is_major") == True,          F.col("h.hazard_id"))).alias("major_hazards_count"),
            F.countDistinct(F.col("h.hazard_id")).alias("total_active_hazards"),
        )
    )

    # ── Weather per (postcode, snapshot_date) — daily aggregates ──
    weather_daily = (
        weather_h
        .groupBy("location_name", "snapshot_date")
        .agg(
            F.round(F.avg("temperature_c"), 1).alias("daily_avg_temp_c"),
            F.max("temperature_c").alias("daily_max_temp_c"),
            F.min("temperature_c").alias("daily_min_temp_c"),
            F.round(F.sum("precipitation_mm"), 2).alias("daily_total_rain_mm"),
            F.max("windspeed_kmh").alias("daily_max_wind_kmh"),
        )
    )
    weather_per_poa_date = (
        poa_w.alias("pw")
        .join(weather_daily.alias("wd"),
              F.col("pw.nearest_weather_location") == F.col("wd.location_name"),
              "inner")
        .select(
            F.col("pw.poa_code").alias("poa_code"),
            F.col("wd.snapshot_date").alias("snapshot_date"),
            F.col("pw.nearest_weather_location").alias("weather_station_name"),
            "daily_avg_temp_c", "daily_max_temp_c", "daily_min_temp_c",
            "daily_total_rain_mm", "daily_max_wind_kmh",
        )
    )

    # ── Union by (postcode, snapshot_date): full outer so days with weather
    #    but no hazards (or vice versa) still produce a row.
    combined = (
        poa_x_hazard_history.alias("h")
        .join(weather_per_poa_date.alias("w"),
              (F.col("h.poa_code")      == F.col("w.poa_code")) &
              (F.col("h.snapshot_date") == F.col("w.snapshot_date")),
              "full_outer")
        .select(
            F.coalesce(F.col("h.poa_code"),      F.col("w.poa_code"))     .alias("postcode"),
            F.coalesce(F.col("h.snapshot_date"), F.col("w.snapshot_date")).alias("snapshot_date"),
            "weather_station_name",
            F.coalesce(F.col("active_flood_count"),    F.lit(0)).alias("active_flood_count"),
            F.coalesce(F.col("active_fire_count"),     F.lit(0)).alias("active_fire_count"),
            F.coalesce(F.col("active_roadwork_count"), F.lit(0)).alias("active_roadwork_count"),
            F.coalesce(F.col("active_incident_count"), F.lit(0)).alias("active_incident_count"),
            F.coalesce(F.col("major_hazards_count"),   F.lit(0)).alias("major_hazards_count"),
            F.coalesce(F.col("total_active_hazards"),  F.lit(0)).alias("total_active_hazards"),
            "daily_avg_temp_c", "daily_max_temp_c", "daily_min_temp_c",
            "daily_total_rain_mm", "daily_max_wind_kmh",
        )
    )

    return (
        combined
        .withColumn("state", F.lit("NSW"))
        # Same risk_level rule as gold_current, applied per-snapshot
        .withColumn("risk_level", F.expr("""
            CASE
                WHEN major_hazards_count > 0                       THEN 'Critical'
                WHEN active_flood_count + active_fire_count > 0    THEN 'High'
                WHEN total_active_hazards > 0                      THEN 'Moderate'
                ELSE                                                    'Low'
            END
        """))
        .select(
            "postcode", "state", "snapshot_date",
            "weather_station_name",
            "active_flood_count", "active_fire_count", "active_roadwork_count",
            "active_incident_count", "major_hazards_count", "total_active_hazards",
            "risk_level",
            "daily_avg_temp_c", "daily_max_temp_c", "daily_min_temp_c",
            "daily_total_rain_mm", "daily_max_wind_kmh",
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## §6 — What this notebook does NOT contain (intentional)
# MAGIC
# MAGIC | Concern | Where it lives |
# MAGIC |---|---|
# MAGIC | **API fetching** | Notebook 18 (`18_api_to_landing.py`), scheduled by Workflows. Writes JSON to landing volume. |
# MAGIC | **Scheduling** | SDP pipeline UI's "Schedule" tab. No cron in this file. |
# MAGIC | **Secrets** | Handled in notebook 18; SDP pipeline only reads files. |
# MAGIC | **Schema creation** | SDP auto-creates `eco_resilience.dlt.*` based on the pipeline target. |
# MAGIC | **Backfill** | Full Refresh from the pipeline UI clears Auto Loader's checkpoint; landing files survive and replay. |
# MAGIC | **Production swap** | The deployed agent reads `eco_resilience.silver.*` (non-DLT). This pipeline writes to `eco_resilience.dlt.*` — purely additive. |
# MAGIC
# MAGIC ## §7 — Verifying the run (queries to use AFTER the first pipeline run)
# MAGIC
# MAGIC Run these in a regular notebook (not in the SDP pipeline) after the pipeline completes:
# MAGIC
# MAGIC ```sql
# MAGIC -- 1. All 7 tables created?
# MAGIC SHOW TABLES IN eco_resilience.dlt;
# MAGIC
# MAGIC -- 2. Row counts — the medallion shape should look like this AFTER 1 fetch:
# MAGIC --     bronze_weather         ~2520    (15 locations × 168 hourly forecasts — accumulates over time)
# MAGIC --     bronze_hazards         ~50-500  (depends on active TfNSW activity — accumulates over time)
# MAGIC --     silver_weather_history = bronze_weather (passthrough)
# MAGIC --     silver_hazards_history = bronze_hazards (passthrough)
# MAGIC --     silver_weather_current = 15     (latest batch aggregated to per-location summary)
# MAGIC --     silver_hazards_current ≤ bronze_hazards (latest batch + active + h3-joinable)
# MAGIC --     gold_*_current         ~600    (one row per NSW postcode, current state)
# MAGIC --     gold_*_history         600 × (distinct snapshot_dates we have data for)
# MAGIC -- If silver_weather_current != 15 the @dp.expect_or_fail will halt the pipeline.
# MAGIC SELECT 'bronze_weather'         AS t, COUNT(*) AS n FROM eco_resilience.dlt.bronze_weather_dlt
# MAGIC UNION ALL SELECT 'bronze_hazards',         COUNT(*) FROM eco_resilience.dlt.bronze_hazards_dlt
# MAGIC UNION ALL SELECT 'silver_weather_history', COUNT(*) FROM eco_resilience.dlt.silver_weather_history_dlt
# MAGIC UNION ALL SELECT 'silver_hazards_history', COUNT(*) FROM eco_resilience.dlt.silver_hazards_history_dlt
# MAGIC UNION ALL SELECT 'silver_weather_current', COUNT(*) FROM eco_resilience.dlt.silver_weather_current_dlt
# MAGIC UNION ALL SELECT 'silver_hazards_current', COUNT(*) FROM eco_resilience.dlt.silver_hazards_current_dlt
# MAGIC UNION ALL SELECT 'gold_current',           COUNT(*) FROM eco_resilience.dlt.gold_nsw_postcode_resilience_current_dlt
# MAGIC UNION ALL SELECT 'gold_history',           COUNT(*) FROM eco_resilience.dlt.gold_nsw_postcode_resilience_history_dlt;
# MAGIC
# MAGIC -- 3. Bronze accumulates batches across runs (this is THE point of the refactor)
# MAGIC SELECT COUNT(DISTINCT _ingest_time) AS num_batches,
# MAGIC        COUNT(*)                     AS total_rows
# MAGIC FROM eco_resilience.dlt.bronze_weather_dlt;
# MAGIC
# MAGIC -- 4. Landing files accumulating
# MAGIC -- LIST '/Volumes/eco_resilience/dlt/landing/weather/' RECURSIVE
# MAGIC -- LIST '/Volumes/eco_resilience/dlt/landing/hazards/' RECURSIVE
# MAGIC
# MAGIC -- 5. Bathurst spot-check — current shape
# MAGIC SELECT * FROM eco_resilience.dlt.gold_nsw_postcode_resilience_current_dlt
# MAGIC WHERE postcode = '2795';
# MAGIC
# MAGIC -- 6. Bathurst trend — the question that history unlocks
# MAGIC SELECT snapshot_date, total_active_hazards, risk_level
# MAGIC FROM eco_resilience.dlt.gold_nsw_postcode_resilience_history_dlt
# MAGIC WHERE postcode = '2795'
# MAGIC ORDER BY snapshot_date DESC;
# MAGIC
# MAGIC -- 7. Did the expectations fire? (Event log query)
# MAGIC SELECT timestamp, details:flow_progress.metrics, details
# MAGIC FROM event_log("eco_resilience_dlt_ingestion")
# MAGIC WHERE event_type = 'expectation_result'
# MAGIC ORDER BY timestamp DESC LIMIT 50;
# MAGIC
# MAGIC -- 8. Auto Loader file ingestion counter (proves the streaming-source story)
# MAGIC SELECT timestamp, details:flow_progress.metrics
# MAGIC FROM event_log("eco_resilience_dlt_ingestion")
# MAGIC WHERE event_type = 'flow_progress' AND details:flow_name LIKE 'bronze_%_flow'
# MAGIC ORDER BY timestamp DESC LIMIT 20;
# MAGIC ```
# MAGIC
# MAGIC ## §8 — DLT vs Workflows comparison
# MAGIC
# MAGIC After your first successful pipeline run, write your observations to
# MAGIC `doc/dlt_vs_workflows_notes.md`. Suggested headings:
# MAGIC - Code shape (declarative vs imperative)
# MAGIC - Dependency declaration (explicit reads vs auto-DAG)
# MAGIC - Quality enforcement (custom asserts vs @dp.expect)
# MAGIC - Observability (job logs vs event_log)
# MAGIC - Refresh model (cron vs pipeline schedule)
# MAGIC - When YOU would pick each, in a future project