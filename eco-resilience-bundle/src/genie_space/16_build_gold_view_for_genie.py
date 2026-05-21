# Databricks notebook source
# MAGIC %md
# MAGIC # 16 — Gold View for Genie Space (Phase 5)
# MAGIC
# MAGIC Builds `eco_resilience.gold.nsw_postcode_resilience` — a curated per-postcode
# MAGIC view that joins our spatial + weather + hazard + climate Silver tables into
# MAGIC one analyst-friendly row, with strong column comments that Genie reads as
# MAGIC semantic grounding.
# MAGIC
# MAGIC ### Why this view exists
# MAGIC
# MAGIC The deployed agent + Lakehouse App serve the **business-owner** persona
# MAGIC (Tom enters ABN → gets Magic Moment grant draft). This view powers the
# MAGIC complementary **analyst / council** persona: ask NL questions like
# MAGIC *"which postcodes have active flood hazards?"* and let Genie write the SQL.
# MAGIC
# MAGIC ### What this notebook does
# MAGIC
# MAGIC 1. Creates the `eco_resilience.gold` schema if missing
# MAGIC 2. Inspects Silver dependencies (column names + sample rows)
# MAGIC 3. Creates `gold.nsw_postcode_resilience` as a VIEW with full column comments
# MAGIC 4. Runs validation queries (Bathurst 2795 spot-check)
# MAGIC 5. Documents Genie Space setup steps (manual UI walkthrough)
# MAGIC 6. Lists 10 sample NL questions to test the Genie Space with
# MAGIC
# MAGIC ### Compute
# MAGIC Serverless. No `%pip install` needed.

# COMMAND ----------

# MAGIC %md
# MAGIC ## §1 — Configuration

# COMMAND ----------

CATALOG       = "eco_resilience"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA   = "gold"
VIEW_NAME     = "nsw_postcode_resilience"
FQN           = f"{CATALOG}.{GOLD_SCHEMA}.{VIEW_NAME}"

# Ensure the gold schema exists (teammate's app already uses gold.* tables so
# this is usually a no-op; harmless if it's already there).
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{GOLD_SCHEMA}")

print(f"Will create:      {FQN}")
print(f"Silver inputs in: {CATALOG}.{SILVER_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## §2 — Pre-flight: verify Silver inputs exist + inspect their schemas
# MAGIC
# MAGIC The view joins 6 Silver tables. If any is missing or has unexpected
# MAGIC columns, this step surfaces it before we attempt the `CREATE VIEW`.
# MAGIC
# MAGIC Also useful as a reference: shows exactly which columns we're consuming
# MAGIC from each Silver dependency.

# COMMAND ----------

SILVER_INPUTS = {
    "poa_h3_lookup":           "POA → H3 cell mapping (every NSW postcode × its constituent cells)",
    "poa_to_weather_location": "POA → nearest seeded weather station",
    "weather_current":          "Current/recent forecast per seeded station (refreshed every 6h)",
    "hazards_current":          "Currently-active TfNSW hazards w/ h3_cell (refreshed every 3h)",
    "poa_to_csiro_station":     "POA → nearest CSIRO climate station",
    "csiro_projections":        "Long-form climate projections (variable × period × rcp × value)",
}

for table_short, purpose in SILVER_INPUTS.items():
    fq = f"{CATALOG}.{SILVER_SCHEMA}.{table_short}"
    count = spark.table(fq).count()
    print(f"  ✅ {fq}  ({count:,} rows)  — {purpose}")

# COMMAND ----------

# Spot-check column names — fail loudly if a key column was renamed.
EXPECTED_COLUMNS = {
    "poa_h3_lookup":           ["poa_code", "h3_cell"],
    "poa_to_weather_location": ["poa_code", "nearest_weather_location"],
    "weather_current":          ["location_name", "forecast_time", "temperature_c", "precipitation_mm", "windspeed_kmh"],
    "hazards_current":          ["hazard_type", "h3_cell", "ended", "is_major"],
    "poa_to_csiro_station":     ["poa_code", "nearest_csiro_station"],
    "csiro_projections":        ["station_name", "variable", "aggregation", "time_aggregation", "rcp", "period", "value"],
}

for table_short, cols in EXPECTED_COLUMNS.items():
    fq = f"{CATALOG}.{SILVER_SCHEMA}.{table_short}"
    actual = {f.name for f in spark.table(fq).schema.fields}
    missing = [c for c in cols if c not in actual]
    if missing:
        raise RuntimeError(
            f"❌ Table {fq} is missing expected columns: {missing}. "
            f"Found: {sorted(actual)}"
        )
    print(f"  ✅ {table_short:<30}  all expected columns present")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Probe CSIRO period values
# MAGIC
# MAGIC The view filters CSIRO projections to baseline `2020s` and target `2080s`
# MAGIC decades. The CSIRO data uses `YYYY-YYYY` period strings. Print the actual
# MAGIC values so we filter to the right ones.

# COMMAND ----------

display(spark.sql(f"""
    SELECT period, COUNT(*) AS n
    FROM   {CATALOG}.{SILVER_SCHEMA}.csiro_projections
    WHERE  variable = 'tas'
      AND  aggregation = 'mean'
      AND  time_aggregation = 'Annual'
    GROUP BY period
    ORDER BY period
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## §3 — Create the Gold view
# MAGIC
# MAGIC ### Design summary
# MAGIC
# MAGIC One row per NSW postcode. Five logical groups of columns:
# MAGIC
# MAGIC | Group | Columns |
# MAGIC |---|---|
# MAGIC | Identity | `postcode`, `state`, `h3_cells_count` |
# MAGIC | Weather (latest) | `weather_station_name`, `current_temperature_c`, `current_precipitation_mm`, `current_windspeed_kmh`, `current_weather_code`, `weather_observed_at` |
# MAGIC | Hazards (active) | `active_flood_count`, `active_fire_count`, `active_roadwork_count`, `active_incident_count`, `major_hazards_count`, `total_active_hazards` |
# MAGIC | Composite | `risk_level` (Low / Moderate / High / Critical) |
# MAGIC | Climate | `nearest_csiro_station`, `climate_temp_2020s`, `climate_temp_2080s_rcp45`, `climate_temp_2080s_rcp85`, `warming_2080s_rcp85_c` |
# MAGIC
# MAGIC ### Period selection
# MAGIC
# MAGIC `2020-2039` for the baseline decade and `2080-2099` for the future decade.
# MAGIC If §2's probe showed different period strings, update the CASE WHEN clauses
# MAGIC below to match what's in your CSIRO data.

# COMMAND ----------

CSIRO_BASELINE_PERIOD = "2020-2039"
CSIRO_FUTURE_PERIOD   = "2080-2099"

create_view_sql = f"""
CREATE OR REPLACE VIEW {FQN} AS
WITH
-- Latest weather observation per seeded station (one row per station)
latest_weather AS (
    SELECT location_name,
           temperature_c,
           precipitation_mm,
           windspeed_kmh,
           weather_code,
           forecast_time
    FROM (
        SELECT location_name, temperature_c, precipitation_mm, windspeed_kmh,
               weather_code, forecast_time,
               ROW_NUMBER() OVER (
                   PARTITION BY location_name
                   ORDER BY forecast_time DESC
               ) AS rn
        FROM {CATALOG}.{SILVER_SCHEMA}.weather_current
        WHERE forecast_time <= current_timestamp()
    )
    WHERE rn = 1
),

-- Current weather joined to each postcode via its nearest seeded location
weather_now AS (
    SELECT pwl.poa_code,
           pwl.nearest_weather_location AS weather_station_name,
           lw.temperature_c             AS current_temperature_c,
           lw.precipitation_mm          AS current_precipitation_mm,
           lw.windspeed_kmh             AS current_windspeed_kmh,
           lw.weather_code              AS current_weather_code,
           lw.forecast_time             AS weather_observed_at
    FROM {CATALOG}.{SILVER_SCHEMA}.poa_to_weather_location pwl
    LEFT JOIN latest_weather lw
      ON pwl.nearest_weather_location = lw.location_name
),

-- Hazard counts per postcode by type + severity
hazard_agg AS (
    SELECT p.poa_code,
           COUNT(DISTINCT CASE WHEN h.hazard_type = 'flood'    THEN h.h3_cell END) AS active_flood_count,
           COUNT(DISTINCT CASE WHEN h.hazard_type = 'fire'     THEN h.h3_cell END) AS active_fire_count,
           COUNT(DISTINCT CASE WHEN h.hazard_type = 'roadwork' THEN h.h3_cell END) AS active_roadwork_count,
           COUNT(DISTINCT CASE WHEN h.hazard_type = 'incident' THEN h.h3_cell END) AS active_incident_count,
           COUNT(DISTINCT CASE WHEN h.is_major = true          THEN h.h3_cell END) AS major_hazards_count,
           COUNT(DISTINCT h.h3_cell)                                                AS total_active_hazards
    FROM {CATALOG}.{SILVER_SCHEMA}.poa_h3_lookup p
    LEFT JOIN {CATALOG}.{SILVER_SCHEMA}.hazards_current h
      ON p.h3_cell = h.h3_cell AND h.ended = false
    GROUP BY p.poa_code
),

-- Climate projections: pivot baseline + 2080s × RCP scenarios
climate_proj AS (
    SELECT pcs.poa_code,
           pcs.nearest_csiro_station,
           MAX(CASE WHEN cp.period = '{CSIRO_BASELINE_PERIOD}' AND cp.rcp = 'rcp45'
                    THEN cp.value END) AS climate_temp_2020s,
           MAX(CASE WHEN cp.period = '{CSIRO_FUTURE_PERIOD}'   AND cp.rcp = 'rcp45'
                    THEN cp.value END) AS climate_temp_2080s_rcp45,
           MAX(CASE WHEN cp.period = '{CSIRO_FUTURE_PERIOD}'   AND cp.rcp = 'rcp85'
                    THEN cp.value END) AS climate_temp_2080s_rcp85
    FROM {CATALOG}.{SILVER_SCHEMA}.poa_to_csiro_station pcs
    LEFT JOIN {CATALOG}.{SILVER_SCHEMA}.csiro_projections cp
      ON pcs.nearest_csiro_station = cp.station_name
     AND cp.variable        = 'tas'
     AND cp.aggregation     = 'mean'
     AND cp.time_aggregation = 'Annual'
    GROUP BY pcs.poa_code, pcs.nearest_csiro_station
),

-- One row per postcode with its H3 cell count
cell_count AS (
    SELECT poa_code, COUNT(DISTINCT h3_cell) AS h3_cells_count
    FROM {CATALOG}.{SILVER_SCHEMA}.poa_h3_lookup
    GROUP BY poa_code
)

SELECT
    cc.poa_code  AS postcode,
    'NSW'        AS state,
    cc.h3_cells_count,

    -- Weather
    wn.weather_station_name,
    wn.current_temperature_c,
    wn.current_precipitation_mm,
    wn.current_windspeed_kmh,
    wn.current_weather_code,
    wn.weather_observed_at,

    -- Hazards
    COALESCE(ha.active_flood_count,    0) AS active_flood_count,
    COALESCE(ha.active_fire_count,     0) AS active_fire_count,
    COALESCE(ha.active_roadwork_count, 0) AS active_roadwork_count,
    COALESCE(ha.active_incident_count, 0) AS active_incident_count,
    COALESCE(ha.major_hazards_count,   0) AS major_hazards_count,
    COALESCE(ha.total_active_hazards,  0) AS total_active_hazards,

    -- Composite risk level (rule-based)
    CASE
        WHEN COALESCE(ha.major_hazards_count, 0) > 0
            THEN 'Critical'
        WHEN COALESCE(ha.active_flood_count, 0) + COALESCE(ha.active_fire_count, 0) > 0
            THEN 'High'
        WHEN COALESCE(ha.total_active_hazards, 0) > 0
            THEN 'Moderate'
        ELSE
            'Low'
    END AS risk_level,

    -- Climate
    cp.nearest_csiro_station,
    cp.climate_temp_2020s,
    cp.climate_temp_2080s_rcp45,
    cp.climate_temp_2080s_rcp85,
    cp.climate_temp_2080s_rcp85 - cp.climate_temp_2020s AS warming_2080s_rcp85_c

FROM cell_count cc
LEFT JOIN weather_now wn  ON cc.poa_code = wn.poa_code
LEFT JOIN hazard_agg  ha  ON cc.poa_code = ha.poa_code
LEFT JOIN climate_proj cp ON cc.poa_code = cp.poa_code
"""

spark.sql(create_view_sql)
print(f"✅ Created view {FQN}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## §4 — Add column + table comments (the Genie-critical step)
# MAGIC
# MAGIC Genie reads these comments to ground its NL→SQL. Each comment should:
# MAGIC - Define the unit / data type when relevant ("in Celsius", "count of...")
# MAGIC - Mention which Silver source it comes from + refresh cadence
# MAGIC - Call out edge cases (zero, null, business rules)
# MAGIC - Avoid jargon — Genie is good at semantic matching but better with plain English

# COMMAND ----------

# Table-level comment — most important; describes the GRAIN of the view.
spark.sql(f"""
    COMMENT ON TABLE {FQN} IS
    'One row per NSW postcode (4-digit Australian POA_CODE21). Combines real-time
TfNSW hazards, latest Open-Meteo weather at the nearest seeded station, CSIRO climate
projections, and H3 cell geography. Designed for the EcoResilience AI analyst persona:
ask natural-language questions in Genie about disaster risk, current conditions, and
long-term climate exposure at the postcode level. Silver dependencies auto-refresh every
3-6 hours so the view stays current without manual maintenance.'
""")

# COMMAND ----------

# Column comments — one per column. Keyed by column name for maintainability.
COLUMN_COMMENTS = {
    "postcode":
        "4-digit Australian postcode (POA_CODE21 from ABS). NSW only. Primary key for this view.",

    "state":
        "Australian state code. Always 'NSW' in this view because weather and hazard data is NSW-only.",

    "h3_cells_count":
        "Number of H3 resolution-8 cells that fall inside this postcode boundary. Higher = larger geographic area. "
        "Used internally for spatial joins; not typically of analyst interest.",

    "weather_station_name":
        "Name of the seeded Open-Meteo weather location nearest to this postcode (e.g. 'Bathurst', 'Sydney'). "
        "Each NSW postcode is mapped to one of 15 seeded NSW stations.",

    "current_temperature_c":
        "Most recent ambient temperature in Celsius at the nearest seeded weather station. "
        "Sourced from Open-Meteo, refreshed every 6 hours by the refresh_weather Workflow.",

    "current_precipitation_mm":
        "Most recent precipitation reading in millimetres at the nearest seeded weather station. "
        "Higher values indicate active or recent rainfall.",

    "current_windspeed_kmh":
        "Most recent wind speed in kilometres per hour at the nearest seeded weather station.",

    "current_weather_code":
        "Open-Meteo weather code (numeric). Maps to qualitative descriptions like 'clear', 'rain', 'thunderstorm'. "
        "See https://open-meteo.com/en/docs#weathervariables for the full mapping.",

    "weather_observed_at":
        "Timestamp when the current weather reading was observed (UTC). "
        "Used to detect stale data — if older than ~6 hours, the refresh job may be lagging.",

    "active_flood_count":
        "Count of currently-active TfNSW flood-type hazards within this postcode boundary. "
        "Zero means no flood events reported by TfNSW. Refreshed every 3 hours by refresh_hazards Workflow.",

    "active_fire_count":
        "Count of currently-active TfNSW fire-type hazards within this postcode boundary. "
        "Includes bushfires and structure fires. Zero means none reported.",

    "active_roadwork_count":
        "Count of currently-active TfNSW roadwork-type hazards. Includes planned roadworks affecting traffic. "
        "Higher count = more disruption to surface transport in the postcode.",

    "active_incident_count":
        "Count of currently-active TfNSW general incidents (vehicle accidents, road closures, etc.). "
        "Distinct from flood/fire/roadwork — covers everything else.",

    "major_hazards_count":
        "Count of currently-active hazards flagged as 'major' severity by TfNSW (the highest severity level). "
        "Drives the 'Critical' risk_level when greater than zero.",

    "total_active_hazards":
        "Total number of active TfNSW hazards in this postcode across all types. "
        "Sum of flood + fire + roadwork + incident counts.",

    "risk_level":
        "Composite disaster risk level. One of: 'Low' (no active hazards), 'Moderate' (some hazards but none flood/fire/major), "
        "'High' (active flood or fire), 'Critical' (one or more major-severity hazards). Rule-based, not ML-derived.",

    "nearest_csiro_station":
        "Name of the CSIRO climate-projection station nearest to this postcode. Each NSW postcode maps to one of ~60 stations.",

    "climate_temp_2020s":
        "Projected annual mean temperature in Celsius for the 2020s decade (baseline) at the nearest CSIRO station. "
        "Under RCP 4.5 moderate-emissions scenario. Use as comparison baseline for warming calculations.",

    "climate_temp_2080s_rcp45":
        "Projected annual mean temperature in Celsius for the 2080s decade at the nearest CSIRO station, "
        "under RCP 4.5 (moderate-emissions, ~2°C global warming) scenario. From CSIRO Climate Change in Australia data.",

    "climate_temp_2080s_rcp85":
        "Projected annual mean temperature in Celsius for the 2080s decade at the nearest CSIRO station, "
        "under RCP 8.5 (high-emissions, ~4°C global warming) scenario. From CSIRO Climate Change in Australia data.",

    "warming_2080s_rcp85_c":
        "Projected warming in Celsius from the 2020s baseline to the 2080s under the high-emissions scenario (RCP 8.5). "
        "Identifies postcodes facing the largest climate exposure. Positive values indicate warming.",
}

for col, comment in COLUMN_COMMENTS.items():
    # Escape single quotes in the comment for SQL safety
    safe = comment.replace("'", "''")
    spark.sql(f"COMMENT ON COLUMN {FQN}.{col} IS '{safe}'")

print(f"✅ Set {len(COLUMN_COMMENTS)} column comments on {FQN}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## §5 — Validation queries

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5a — Row count + general shape

# COMMAND ----------

display(spark.sql(f"""
    SELECT COUNT(*) AS total_postcodes,
           COUNT(DISTINCT weather_station_name) AS distinct_weather_stations,
           COUNT(DISTINCT nearest_csiro_station) AS distinct_csiro_stations,
           SUM(total_active_hazards) AS total_active_hazards_nsw,
           SUM(CASE WHEN risk_level = 'Critical' THEN 1 ELSE 0 END) AS critical_postcodes,
           SUM(CASE WHEN risk_level = 'High'     THEN 1 ELSE 0 END) AS high_risk_postcodes
    FROM {FQN}
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5b — Bathurst (2795) spot-check
# MAGIC
# MAGIC The demo's headline postcode. Expect:
# MAGIC - `weather_station_name = 'Bathurst'`
# MAGIC - Non-null `climate_temp_2020s` and `climate_temp_2080s_rcp85`
# MAGIC - `nearest_csiro_station` something like 'BATHURST-AGRICULTURAL-STATION'

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {FQN} WHERE postcode = '2795'"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5c — Top 10 most-warming postcodes
# MAGIC
# MAGIC Demonstrates the climate-exposure ordering Genie will use for warming-related questions.

# COMMAND ----------

display(spark.sql(f"""
    SELECT postcode, weather_station_name, nearest_csiro_station,
           ROUND(climate_temp_2020s, 2)        AS temp_2020s,
           ROUND(climate_temp_2080s_rcp85, 2)  AS temp_2080s_high,
           ROUND(warming_2080s_rcp85_c, 2)     AS warming_c
    FROM {FQN}
    WHERE warming_2080s_rcp85_c IS NOT NULL
    ORDER BY warming_2080s_rcp85_c DESC
    LIMIT 10
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5d — Postcodes with active hazards right now
# MAGIC
# MAGIC Will be empty if there are no active TfNSW hazards in NSW at query time
# MAGIC (rare but possible). Useful smoke test of the hazard join.

# COMMAND ----------

display(spark.sql(f"""
    SELECT postcode, risk_level,
           active_flood_count, active_fire_count, active_roadwork_count,
           active_incident_count, major_hazards_count
    FROM {FQN}
    WHERE total_active_hazards > 0
    ORDER BY major_hazards_count DESC, total_active_hazards DESC
    LIMIT 20
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5e — Verify column comments landed (what Genie will read)

# COMMAND ----------

display(spark.sql(f"DESCRIBE TABLE EXTENDED {FQN}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## §6 — Genie Space setup (UI walkthrough)
# MAGIC
# MAGIC Genie Spaces are created via the workspace UI — programmatic creation is
# MAGIC limited in the current Databricks SDK. Follow these steps:
# MAGIC
# MAGIC ### 1. Open Genie
# MAGIC Workspace sidebar → **Genie** → **+ New Space** (top right)
# MAGIC
# MAGIC ### 2. Configure the space
# MAGIC | Field | Value |
# MAGIC |---|---|
# MAGIC | **Name** | `EcoResilience NSW Resilience Explorer` |
# MAGIC | **Description** | `Ask natural-language questions about NSW postcode-level disaster risk, current weather, active TfNSW hazards, and long-term climate projections.` |
# MAGIC | **Default warehouse** | Your serverless SQL warehouse (e.g. `dc189fe4fd0f924b`) |
# MAGIC
# MAGIC ### 3. Add the data asset
# MAGIC - **Add tables → Browse**
# MAGIC - Navigate to `eco_resilience` → `gold` → `nsw_postcode_resilience`
# MAGIC - Click **Add**
# MAGIC
# MAGIC ### 4. Add semantic instructions (the secret sauce)
# MAGIC Click **Instructions** in the space settings and paste:
# MAGIC
# MAGIC ```text
# MAGIC This space answers questions about NSW postcode-level disaster risk and resilience.
# MAGIC The single table 'nsw_postcode_resilience' has one row per NSW postcode.
# MAGIC
# MAGIC When users say:
# MAGIC   - "disaster" or "emergency" → interpret as active hazards (look at active_flood_count, active_fire_count, major_hazards_count).
# MAGIC   - "high risk" or "risky" → filter on risk_level IN ('Critical', 'High').
# MAGIC   - "warming" or "climate change exposure" → use warming_2080s_rcp85_c.
# MAGIC   - "current weather" or "weather right now" → use current_temperature_c, current_precipitation_mm, current_windspeed_kmh.
# MAGIC   - "raining" → current_precipitation_mm > 0.
# MAGIC   - "Bathurst" → postcode = '2795'. "Sydney" → postcode = '2000'. "Newcastle" → '2300'.
# MAGIC
# MAGIC Always show the postcode column in disaster-related answers.
# MAGIC Always round temperatures to 1 decimal place and warming to 2 decimal places.
# MAGIC Never sum postcodes — they're identifiers, not measures.
# MAGIC ```
# MAGIC
# MAGIC ### 5. Add example questions
# MAGIC In the **Sample Questions** tab, paste each of these as a separate row.
# MAGIC See §7 below for the full list.
# MAGIC
# MAGIC ### 6. Save + test
# MAGIC Click **Save**. Open the chat interface inside the space and ask
# MAGIC *"Which postcodes have active flood hazards?"* — Genie should produce
# MAGIC valid SQL filtering on `active_flood_count > 0`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## §7 — 10 sample NL questions to seed the Genie Space
# MAGIC
# MAGIC Copy these into the Genie Space's **Sample Questions** section. They
# MAGIC exercise different parts of the view (filtering, sorting, aggregation,
# MAGIC time-series, climate projections) and serve as a curated starting point
# MAGIC for council members exploring the data.

# COMMAND ----------

SAMPLE_QUESTIONS = [
    # Filter / direct lookup
    "Which postcodes have active flood hazards right now?",
    "What is the current weather for postcode 2795?",
    "How many NSW postcodes have a Critical risk level?",

    # Sorting / ranking
    "Show me the 10 postcodes facing the most projected warming by 2080.",
    "Which postcode has the most active TfNSW roadworks right now?",

    # Multi-condition
    "List postcodes that have BOTH active floods AND major hazards.",
    "Show postcodes where projected 2080s warming exceeds 3 degrees Celsius.",

    # Aggregation
    "What is the average current temperature across all NSW postcodes?",
    "How many postcodes have no active hazards at all?",

    # Climate exposure
    "Compare the 2020s vs 2080s temperature for Sydney (postcode 2000) under the high-emissions scenario.",
]

for i, q in enumerate(SAMPLE_QUESTIONS, start=1):
    print(f"  {i:2d}. {q}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## §8 — Done
# MAGIC
# MAGIC | What we built | Why it matters |
# MAGIC |---|---|
# MAGIC | `eco_resilience.gold.nsw_postcode_resilience` VIEW | Single per-postcode row joining hazards, weather, climate. Analyst-friendly. |
# MAGIC | 21 column COMMENTs + table COMMENT | Genie's semantic grounding — accuracy of NL→SQL depends on these. |
# MAGIC | Rule-based `risk_level` | Simple, defensible, demoable. ML upgrade is future polish. |
# MAGIC | `warming_2080s_rcp85_c` derived column | Direct answer to "which postcode is worst-exposed?" — no math required. |
# MAGIC
# MAGIC ### What's next
# MAGIC
# MAGIC 1. **Create the Genie Space** in the UI per §6
# MAGIC 2. **Test 5-10 of the §7 sample questions** in Genie chat
# MAGIC 3. **(Optional polish)** Add a link from the Streamlit app to the Genie Space — pasting the Space URL into `frontend/index.html` as a header button
# MAGIC
# MAGIC After this lands, the demo has two complementary persona surfaces:
# MAGIC - **Business owner** → Streamlit + Magic Moment grant PDF
# MAGIC - **Analyst / council** → Genie + free-form NL exploration over the same underlying data
# MAGIC
# MAGIC The pitch beat: *"Same Unity Catalog data, two interfaces — agent-driven for action, Genie-driven for exploration."*

# COMMAND ----------

# MAGIC %md
# MAGIC ## §9 — Iteration log
# MAGIC
# MAGIC Fill this in as you work through the §7 sample questions. For each question
# MAGIC where Genie produced wrong or awkward SQL: what was asked, what Genie did
# MAGIC wrong, which knob you tuned (a column COMMENT in §4 above, the General
# MAGIC Instructions in the Genie UI, or a new synonym), whether the fix worked.
# MAGIC
# MAGIC This log is the durable artifact of the lab — the actual deliverable.
# MAGIC
# MAGIC | # | Question | Wrong SQL behaviour | Fix applied | Worked? |
# MAGIC |---|---|---|---|---|
# MAGIC |   |   |   |   |   |
# MAGIC
# MAGIC Target: ≥3 entries before declaring this lab "done". Most Genie spaces need
# MAGIC 5-10 iterations on their first build before the NL→SQL is consistently correct.