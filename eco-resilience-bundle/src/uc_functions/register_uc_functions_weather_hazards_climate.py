# Databricks notebook source
# MAGIC %md
# MAGIC # 08 — Register Data-Layer Agent Tools as UC Functions
# MAGIC ## Phase 4 Step 3a — Three pure-SQL UDFs over Silver
# MAGIC
# MAGIC **What this notebook does**
# MAGIC
# MAGIC Registers three Unity Catalog **SQL** functions the Mosaic AI agent can
# MAGIC discover and call. All three are pure compositions of Silver tables we
# MAGIC already built in notebooks 01–04 — no external API, no Python sandbox.
# MAGIC
# MAGIC | Function | Purpose | Underlying tables |
# MAGIC |---|---|---|
# MAGIC | `silver.get_weather_forecast(postcode)` | Next 12 hours of weather + 24h summary stats | `weather_current`, `poa_to_weather_location` |
# MAGIC | `silver.get_active_hazards(postcode)` | Live TfNSW hazards in the postcode boundary | `hazards_current`, `poa_h3_lookup` |
# MAGIC | `silver.get_climate_projection(postcode)` | 2020s vs 2080s annual mean temp (rcp45 + rcp85 callout) | `csiro_projections`, `poa_to_csiro_station` |
# MAGIC
# MAGIC **Why pure SQL UDFs**
# MAGIC
# MAGIC The UC sandbox bug we hit with `verify_abn` (Python UDFs can't access
# MAGIC `spark` or `dbutils`) doesn't apply here — these tools only read Silver
# MAGIC tables. SQL is simpler, faster, more maintainable, no secret() needed,
# MAGIC no Python imports, and the agent gets the same STRUCT outputs.
# MAGIC
# MAGIC **What we do after this notebook**
# MAGIC
# MAGIC Update `notebooks/07_minimal_agent.py` to include all four tools and
# MAGIC test multi-tool reasoning ("verify ABN, then summarise weather + hazards
# MAGIC + climate for that location").
# MAGIC
# MAGIC **Compute:** Serverless. No `%pip install` needed — pure SQL.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration

# COMMAND ----------

CATALOG       = "eco_resilience"
SILVER_SCHEMA = "silver"

WEATHER_FN = f"{CATALOG}.{SILVER_SCHEMA}.get_weather_forecast"
HAZARDS_FN = f"{CATALOG}.{SILVER_SCHEMA}.get_active_hazards"
CLIMATE_FN = f"{CATALOG}.{SILVER_SCHEMA}.get_climate_projection"

print("Will register:")
print(f"  • {WEATHER_FN}")
print(f"  • {HAZARDS_FN}")
print(f"  • {CLIMATE_FN}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Pre-flight — required Silver tables exist
# MAGIC
# MAGIC Surface any missing dependency now rather than after function creation.

# COMMAND ----------

required_tables = [
    f"{CATALOG}.{SILVER_SCHEMA}.poa_h3_lookup",
    f"{CATALOG}.{SILVER_SCHEMA}.poa_to_weather_location",
    f"{CATALOG}.{SILVER_SCHEMA}.poa_to_csiro_station",
    f"{CATALOG}.{SILVER_SCHEMA}.weather_current",
    f"{CATALOG}.{SILVER_SCHEMA}.hazards_current",
    f"{CATALOG}.{SILVER_SCHEMA}.csiro_projections",
]
for t in required_tables:
    cnt = spark.table(t).count()
    print(f"  ✅ {t}  ({cnt:,} rows)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Tool descriptions — the LLM-facing `COMMENT ON FUNCTION`
# MAGIC
# MAGIC These strings are the agent's primary signal for **when to call which tool**.
# MAGIC Each one mentions the concrete user phrasings that should trigger it.
# MAGIC No single quotes inside (SQL escaping rules) — phrasings re-worded accordingly.

# COMMAND ----------

WEATHER_DESC = (
    "Returns weather forecast for the next 12 hours at the seeded station "
    "nearest the user NSW postcode, plus 24-hour summary statistics for "
    "rainfall and wind speed. Use this when the user asks about current "
    "weather, upcoming conditions, todays forecast, rain expected, wind, "
    "or temperature near their business. Argument is a 4-digit NSW postcode "
    "(typically the postcode field returned by verify_abn)."
)

HAZARDS_DESC = (
    "Returns currently-active TfNSW road hazards (incidents, floods, fires, "
    "roadworks) within the user NSW postcode boundary. Use this when the "
    "user asks about disruptions, road closures, fires, floods, blocked "
    "roads, active emergencies, or anything affecting transportation right "
    "now near their business. Argument is a 4-digit NSW postcode."
)

CLIMATE_DESC = (
    "Returns long-term climate projections for the user NSW postcode — "
    "median annual mean temperature in the 2020s vs the 2080s, computed "
    "across 8 global climate models under moderate (rcp45) and high (rcp85) "
    "emissions scenarios. Use this when the user asks about long-term "
    "climate trends, future warming, strategic planning for 2030 or 2050 "
    "or 2080, or whether the climate is changing in their region. Argument "
    "is a 4-digit NSW postcode."
)

print("Weather:",  len(WEATHER_DESC),  "chars")
print("Hazards:",  len(HAZARDS_DESC),  "chars")
print("Climate:",  len(CLIMATE_DESC),  "chars")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Register `get_weather_forecast`
# MAGIC
# MAGIC Argument is named `postcode` (not `poa_code`) to avoid a name collision
# MAGIC with the `poa_code` *column* in `poa_to_weather_location`. Inside the
# MAGIC body `poa_code` always means the column; `postcode` always means the
# MAGIC function argument.

# COMMAND ----------

weather_sql = f"""
CREATE OR REPLACE FUNCTION {WEATHER_FN}(
  postcode STRING COMMENT 'NSW postcode (4 digits) — typically passed from verify_abn output'
)
RETURNS STRUCT<
  poa_code              STRING,
  forecast_location     STRING,
  next_12h              ARRAY<STRUCT<
                          forecast_time TIMESTAMP,
                          temp_c        DOUBLE,
                          rain_mm       DOUBLE,
                          wind_kmh      DOUBLE
                        >>,
  max_rain_24h_mm       DOUBLE,
  max_wind_24h_kmh      DOUBLE,
  error                 STRING
>
COMMENT '{WEATHER_DESC}'
RETURN (
  WITH
  loc AS (
    SELECT nearest_weather_location
    FROM   {CATALOG}.{SILVER_SCHEMA}.poa_to_weather_location
    WHERE  poa_code = postcode
  ),
  hourly AS (
    SELECT
      w.forecast_time,
      w.temperature_c   AS temp_c,
      w.precipitation_mm AS rain_mm,
      w.windspeed_kmh   AS wind_kmh
    FROM {CATALOG}.{SILVER_SCHEMA}.weather_current w
    JOIN loc ON w.location_name = loc.nearest_weather_location
    WHERE w.forecast_time >= current_timestamp()
      AND w.forecast_time <  current_timestamp() + INTERVAL 24 HOURS
  ),
  next_12 AS (
    SELECT array_agg(
             named_struct(
               'forecast_time', forecast_time,
               'temp_c',        temp_c,
               'rain_mm',       rain_mm,
               'wind_kmh',      wind_kmh
             )
           ) AS rows_arr
    FROM (
      SELECT * FROM hourly ORDER BY forecast_time LIMIT 12
    )
  ),
  summary AS (
    SELECT MAX(rain_mm) AS max_rain, MAX(wind_kmh) AS max_wind FROM hourly
  )
  SELECT named_struct(
    'poa_code',          postcode,
    'forecast_location', (SELECT nearest_weather_location FROM loc),
    'next_12h',          COALESCE((SELECT rows_arr FROM next_12), array()),
    'max_rain_24h_mm',   (SELECT max_rain FROM summary),
    'max_wind_24h_kmh',  (SELECT max_wind FROM summary),
    'error',             CASE
                            WHEN (SELECT nearest_weather_location FROM loc) IS NULL
                            THEN concat('Postcode ', postcode, ' not found in NSW weather lookup')
                            ELSE NULL
                          END
  )
)
"""

spark.sql(weather_sql)
print(f"✅ Registered {WEATHER_FN}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Register `get_active_hazards`
# MAGIC
# MAGIC Joins the postcode's H3 cells against the live hazards table. Limits
# MAGIC the array to 20 most-network-impacting hazards so the LLM doesn't get
# MAGIC overwhelmed when a region has many simultaneous events.

# COMMAND ----------

hazards_sql = f"""
CREATE OR REPLACE FUNCTION {HAZARDS_FN}(
  postcode STRING COMMENT 'NSW postcode (4 digits) — typically passed from verify_abn output'
)
RETURNS STRUCT<
  poa_code     STRING,
  hazard_count INT,
  hazards      ARRAY<STRUCT<
                 hazard_type        STRING,
                 main_category      STRING,
                 display_name       STRING,
                 advice_a           STRING,
                 expected_delay_min DOUBLE,
                 impacting_network  BOOLEAN,
                 last_updated_ts    TIMESTAMP
               >>,
  error        STRING
>
COMMENT '{HAZARDS_DESC}'
RETURN (
  WITH
  cells AS (
    SELECT h3_cell
    FROM   {CATALOG}.{SILVER_SCHEMA}.poa_h3_lookup
    WHERE  poa_code = postcode
  ),
  matched AS (
    SELECT
      h.hazard_type,
      h.main_category,
      h.display_name,
      h.advice_a,
      h.expected_delay_min,
      h.impacting_network,
      h.last_updated_ts
    FROM {CATALOG}.{SILVER_SCHEMA}.hazards_current h
    JOIN cells c ON h.h3_cell = c.h3_cell
    ORDER BY h.impacting_network DESC NULLS LAST, h.last_updated_ts DESC NULLS LAST
    LIMIT 20
  ),
  total AS (
    SELECT COUNT(*) AS n
    FROM {CATALOG}.{SILVER_SCHEMA}.hazards_current h
    JOIN cells c ON h.h3_cell = c.h3_cell
  ),
  agg AS (
    SELECT array_agg(
             named_struct(
               'hazard_type',        hazard_type,
               'main_category',      main_category,
               'display_name',       display_name,
               'advice_a',           advice_a,
               'expected_delay_min', expected_delay_min,
               'impacting_network',  impacting_network,
               'last_updated_ts',    last_updated_ts
             )
           ) AS hazards_arr
    FROM matched
  )
  SELECT named_struct(
    'poa_code',     postcode,
    'hazard_count', CAST(COALESCE((SELECT n FROM total), 0) AS INT),
    'hazards',      COALESCE((SELECT hazards_arr FROM agg), array()),
    'error',        CASE
                      WHEN NOT EXISTS (SELECT 1 FROM cells)
                      THEN concat('Postcode ', postcode, ' not found in NSW H3 lookup')
                      ELSE NULL
                    END
  )
)
"""

spark.sql(hazards_sql)
print(f"✅ Registered {HAZARDS_FN}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Register `get_climate_projection`
# MAGIC
# MAGIC Groups by (rcp, period) to compute the median across the 8 GCMs per
# MAGIC combination, then pivots into a single-row STRUCT. Returns the headline
# MAGIC pitch numbers: today vs 2080s under rcp45, plus rcp85 as the worst-case
# MAGIC callout.

# COMMAND ----------

climate_sql = f"""
CREATE OR REPLACE FUNCTION {CLIMATE_FN}(
  postcode STRING COMMENT 'NSW postcode (4 digits) — typically passed from verify_abn output'
)
RETURNS STRUCT<
  poa_code                       STRING,
  station_name                   STRING,
  current_2020s_temp_c           DOUBLE,
  future_2080s_temp_c            DOUBLE,
  warming_delta_c                DOUBLE,
  worst_case_2080s_rcp85_temp_c  DOUBLE,
  models_n                       INT,
  error                          STRING
>
COMMENT '{CLIMATE_DESC}'
RETURN (
  WITH
  st AS (
    SELECT nearest_csiro_station
    FROM   {CATALOG}.{SILVER_SCHEMA}.poa_to_csiro_station
    WHERE  poa_code = postcode
  ),
  filtered AS (
    SELECT c.rcp, c.period, c.value, c.model
    FROM   {CATALOG}.{SILVER_SCHEMA}.csiro_projections c
    JOIN   st ON c.station_name = st.nearest_csiro_station
    WHERE  c.variable         = 'tas'
      AND  c.time_aggregation = 'Annual'
      AND  c.rcp              IN ('rcp45', 'rcp85')
      AND  c.period           IN ('2020-2039', '2080-2099')
  ),
  medians AS (
    SELECT
      rcp,
      period,
      percentile_approx(value, 0.5)  AS median_temp,
      COUNT(DISTINCT model)          AS n_models
    FROM filtered
    GROUP BY rcp, period
  ),
  pivot AS (
    SELECT
      MAX(CASE WHEN rcp = 'rcp45' AND period = '2020-2039' THEN median_temp END) AS current_2020s,
      MAX(CASE WHEN rcp = 'rcp45' AND period = '2080-2099' THEN median_temp END) AS future_2080s,
      MAX(CASE WHEN rcp = 'rcp85' AND period = '2080-2099' THEN median_temp END) AS worst_2080s,
      MAX(n_models)                                                              AS n_models
    FROM medians
  )
  SELECT named_struct(
    'poa_code',                       postcode,
    'station_name',                   (SELECT nearest_csiro_station FROM st),
    'current_2020s_temp_c',           ROUND((SELECT current_2020s FROM pivot), 2),
    'future_2080s_temp_c',            ROUND((SELECT future_2080s  FROM pivot), 2),
    'warming_delta_c',                ROUND((SELECT future_2080s - current_2020s FROM pivot), 2),
    'worst_case_2080s_rcp85_temp_c',  ROUND((SELECT worst_2080s   FROM pivot), 2),
    'models_n',                       CAST((SELECT n_models FROM pivot) AS INT),
    'error',                          CASE
                                        WHEN (SELECT nearest_csiro_station FROM st) IS NULL
                                        THEN concat('Postcode ', postcode, ' not found in NSW CSIRO lookup')
                                        ELSE NULL
                                      END
  )
)
"""

spark.sql(climate_sql)
print(f"✅ Registered {CLIMATE_FN}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Smoke tests via SQL
# MAGIC
# MAGIC Each test against postcode **2795 (Bathurst)** — the Tom-the-farmer
# MAGIC demo location, which all three Silver lookups resolve cleanly.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7a — Weather forecast for Bathurst
# MAGIC
# MAGIC Expected: `forecast_location='Bathurst'`, 12 rows in `next_12h`,
# MAGIC plausible numbers (temp 0-40°C, rain ≥ 0, wind ≥ 0), null error.

# COMMAND ----------

display(spark.sql(f"select results.* from (SELECT {WEATHER_FN}('2795') as results)"))

# COMMAND ----------

# MAGIC %sql
# MAGIC select results.next_12h from (SELECT eco_resilience.silver.get_weather_forecast('2795') as results)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7b — Active hazards for Bathurst
# MAGIC
# MAGIC Expected: integer `hazard_count` (could be 0 on a quiet afternoon —
# MAGIC still a valid result), `hazards` array matching the count, null error.

# COMMAND ----------

display(spark.sql(f"SELECT results.* from (select {HAZARDS_FN}('2795') as results)"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7c — Climate projection for Bathurst
# MAGIC
# MAGIC Expected: `station_name='BATHURST-AGRICULTURAL-STATION'`,
# MAGIC `current_2020s_temp_c` around 14–16°C, `future_2080s_temp_c` ~1.5–2°C
# MAGIC higher under rcp45, `worst_case_2080s_rcp85_temp_c` further ~1.5°C
# MAGIC higher, `models_n` around 6–8, null error.

# COMMAND ----------

display(spark.sql(f"select results.* from (SELECT {CLIMATE_FN}('2795') as results)"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7d — Error path (unknown postcode)
# MAGIC
# MAGIC Each function should surface a clear error message rather than NULL.

# COMMAND ----------

# MAGIC %sql
# MAGIC select '7d-weather' as test, eco_resilience.silver.get_weather_forecast('9999').error as error_msg
# MAGIC union all
# MAGIC select '7d-hazards', eco_resilience.silver.get_active_hazards('9999').error
# MAGIC union all
# MAGIC select '7d-climate', eco_resilience.silver.get_climate_projection('9999').error

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Discoverability — what the Mosaic AI Agent Framework sees

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog eco_resilience

# COMMAND ----------

display(spark.sql(f"SHOW USER FUNCTIONS IN {CATALOG}.{SILVER_SCHEMA}"))

# COMMAND ----------

display(spark.sql(f"DESCRIBE FUNCTION EXTENDED {WEATHER_FN}"))

# COMMAND ----------

display(spark.sql(f"DESCRIBE FUNCTION EXTENDED {HAZARDS_FN}"))

# COMMAND ----------

display(spark.sql(f"DESCRIBE FUNCTION EXTENDED {CLIMATE_FN}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Done
# MAGIC
# MAGIC | Function | Pattern | Demo question it unlocks |
# MAGIC |---|---|---|
# MAGIC | `silver.get_weather_forecast` | Pure SQL UDF over `weather_current` + `poa_to_weather_location` | "What's the weather like for my business right now?" |
# MAGIC | `silver.get_active_hazards` | Pure SQL UDF over `hazards_current` + `poa_h3_lookup` | "Are there any active road closures or floods near me?" |
# MAGIC | `silver.get_climate_projection` | Pure SQL UDF over `csiro_projections` + `poa_to_csiro_station` | "How is the climate going to change here over the next 50 years?" |
# MAGIC
# MAGIC ### What's next
# MAGIC
# MAGIC **Update `notebooks/07_minimal_agent.py`:**
# MAGIC 1. Extend `TOOL_FUNCTIONS` to include the three new function names.
# MAGIC 2. Update the system prompt to teach the agent about the new tools and
# MAGIC    the recommended call order (verify_abn FIRST, then any of the others).
# MAGIC 3. Add a multi-tool smoke test that exercises the whole chain:
# MAGIC    *"Verify ABN 42173522302, then summarise the weather, hazards and
# MAGIC    climate outlook for that location."*
# MAGIC 4. The agent should call all four tools in sequence and produce a
# MAGIC    coherent multi-paragraph summary.
# MAGIC
# MAGIC **Then Step 3b — `query_nema_guidelines`** (Vector Search RAG tool) and
# MAGIC **Step 3c — `generate_grant_pdf`** (Jinja2 template renderer).