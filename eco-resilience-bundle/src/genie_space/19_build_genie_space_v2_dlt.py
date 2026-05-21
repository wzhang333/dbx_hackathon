# Databricks notebook source
# MAGIC %md
# MAGIC # 19 — Genie Space v2 over the DLT Gold Tables
# MAGIC
# MAGIC Wires a NEW, additive Genie space ("v2") to the two DLT gold tables built by
# MAGIC notebook 17's SDP pipeline:
# MAGIC
# MAGIC - `eco_resilience.dlt.gold_nsw_postcode_resilience_current_dlt`  — one row per postcode (right now)
# MAGIC - `eco_resilience.dlt.gold_nsw_postcode_resilience_history_dlt`  — one row per (postcode, snapshot_date)
# MAGIC
# MAGIC The existing v1 Genie space + view from notebook 16 stay untouched — this
# MAGIC notebook is purely additive so the pitch demo stays stable.
# MAGIC
# MAGIC ### What this notebook owns vs what's UI-only
# MAGIC
# MAGIC | Codeable here | UI-only |
# MAGIC |---|---|
# MAGIC | Per-column `COMMENT` via `ALTER TABLE ... ALTER COLUMN ...` | Genie space creation + name + warehouse selection |
# MAGIC | Table-level `COMMENT ON TABLE` with Genie routing instructions | Adding the tables to a space |
# MAGIC | The Python `SAMPLE_QUESTIONS` list (copy-pasteable into the UI) | The "General Instructions" field text (copy-pasteable from §6 below) |
# MAGIC
# MAGIC Genie reads table + column comments as semantic grounding. The table comment
# MAGIC is the most important lever — it tells the LLM **which table to pick** for a
# MAGIC given question. Get the comments right and Genie will route current vs
# MAGIC history questions correctly without per-question hardcoding.
# MAGIC
# MAGIC ### Compute
# MAGIC Serverless. No `%pip install` needed.

# COMMAND ----------

# MAGIC %md
# MAGIC ## §1 — Configuration

# COMMAND ----------

CATALOG     = "eco_resilience"
DLT_SCHEMA  = "dlt"
CURRENT_TBL = "gold_nsw_postcode_resilience_current_dlt"
HISTORY_TBL = "gold_nsw_postcode_resilience_history_dlt"

CURRENT_FQN = f"{CATALOG}.{DLT_SCHEMA}.{CURRENT_TBL}"
HISTORY_FQN = f"{CATALOG}.{DLT_SCHEMA}.{HISTORY_TBL}"

# Pre-flight: both tables must exist (run the SDP pipeline first)
for fqn in (CURRENT_FQN, HISTORY_FQN):
    n = spark.table(fqn).count()
    print(f"  {fqn}  ({n:,} rows)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## §2 — Column comments on `gold_*_current_dlt` (26 columns)
# MAGIC
# MAGIC Where columns overlap with notebook 16's v1 view, the comment text is reused
# MAGIC verbatim for project consistency. New columns from the DLT version
# MAGIC (`next_24h_*`, `max_severity_score`) get fresh comments.

# COMMAND ----------

CURRENT_COLUMN_COMMENTS = {
    # Identity
    "postcode":
        "4-digit Australian postcode (POA_CODE21 from ABS). NSW only. Primary key for this table — one row per postcode.",

    "state":
        "Australian state code. Always 'NSW' in this table because weather and hazard data is NSW-only.",

    "h3_cells_count":
        "Number of H3 resolution-8 cells that fall inside this postcode boundary. Higher = larger geographic area. "
        "Used internally for spatial joins; not typically of analyst interest.",

    # Weather (current snapshot)
    "weather_station_name":
        "Name of the seeded Open-Meteo weather location nearest to this postcode (e.g. 'Bathurst', 'Sydney CBD'). "
        "Each NSW postcode is mapped to one of 15 seeded NSW stations.",

    "current_temperature_c":
        "Most recent ambient temperature in Celsius at the nearest seeded weather station. "
        "Sourced from Open-Meteo, refreshed every 3 hours by the SDP pipeline.",

    "current_precipitation_mm":
        "Most recent precipitation reading in millimetres at the nearest seeded weather station. "
        "Higher values indicate active or recent rainfall.",

    "current_windspeed_kmh":
        "Most recent wind speed in kilometres per hour at the nearest seeded weather station.",

    "current_weather_code":
        "Open-Meteo weather code (numeric). Maps to qualitative descriptions like 'clear', 'rain', 'thunderstorm'. "
        "See https://open-meteo.com/en/docs#weathervariables for the full mapping.",

    "weather_observed_at":
        "Timestamp of the current weather observation (UTC). "
        "Used to detect stale data — if older than ~6 hours, the SDP pipeline may be lagging.",

    # Weather (next 24h forecast aggregates — NEW in DLT version)
    "next_24h_max_temp_c":
        "Maximum forecast temperature in Celsius over the next 24 hours at the nearest seeded weather station. "
        "Use for 'heat exposure' questions about tomorrow.",

    "next_24h_min_temp_c":
        "Minimum forecast temperature in Celsius over the next 24 hours. Use for overnight low / frost questions.",

    "next_24h_total_rain_mm":
        "Total forecast precipitation in millimetres over the next 24 hours. "
        "Use this for 'how much rain is coming?' questions. Higher values indicate impending heavy rain.",

    "next_24h_max_wind_kmh":
        "Maximum forecast wind speed in km/h over the next 24 hours. Use for storm / damaging-wind questions.",

    # Hazards (active TfNSW)
    "active_flood_count":
        "Count of currently-active TfNSW flood-type hazards within this postcode boundary. "
        "Zero means no flood events reported by TfNSW. Refreshed every 3 hours by the SDP pipeline.",

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

    # New severity column from DLT silver
    "max_severity_score":
        "Highest severity score among hazards currently active in this postcode. Range 1-3: "
        "1 = routine, 2 = impacting network, 3 = major. 0 if no active hazards. "
        "Stronger signal than the binary major_hazards_count for graded risk.",

    "risk_level":
        "Composite disaster risk level. One of: 'Low' (no active hazards), 'Moderate' (some hazards but none flood/fire/major), "
        "'High' (active flood or fire), 'Critical' (one or more major-severity hazards). Rule-based, not ML-derived.",

    # Climate (CSIRO long-term projections)
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




for col, comment in CURRENT_COLUMN_COMMENTS.items():
    # Escape single quotes in the comment for SQL safety
    safe = comment.replace("'", "''")
    spark.sql(f"COMMENT ON COLUMN {CURRENT_FQN}.{col} IS '{safe}'")

print(f"✅ Set {len(CURRENT_COLUMN_COMMENTS)} column comments on {CURRENT_FQN}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## §3 — Column comments on `gold_*_history_dlt` (16 columns)
# MAGIC
# MAGIC The `snapshot_date` comment is the most important one here — it's what tells
# MAGIC Genie's LLM "this is the grain that distinguishes _history from _current".

# COMMAND ----------

HISTORY_COLUMN_COMMENTS = {
    # Identity
    "postcode":
        "4-digit Australian postcode (POA_CODE21 from ABS). NSW only. Part of the composite key (postcode, snapshot_date).",

    "state":
        "Australian state code. Always 'NSW' in this table.",

    "snapshot_date":
        "UTC date this row represents. THE GRAIN COLUMN — every postcode that had data on a given day gets one row. "
        "Use this for trend questions: GROUP BY snapshot_date, ORDER BY snapshot_date, or filter to a date range. "
        "ALWAYS reference this column when querying this table.",

    "weather_station_name":
        "Name of the seeded Open-Meteo weather location nearest to this postcode (e.g. 'Bathurst', 'Sydney CBD'). "
        "Constant per postcode across snapshots — same mapping as the current table.",

    # Hazards (active on snapshot_date, deduped within day)
    "active_flood_count":
        "Distinct count of TfNSW flood hazards observed in this postcode on this snapshot_date. "
        "Deduped via DISTINCT hazard_id — same hazard appearing in multiple intra-day batches counts ONCE.",

    "active_fire_count":
        "Distinct count of TfNSW fire hazards observed in this postcode on this snapshot_date. "
        "Includes bushfires and structure fires.",

    "active_roadwork_count":
        "Distinct count of TfNSW roadwork-type hazards observed in this postcode on this snapshot_date.",

    "active_incident_count":
        "Distinct count of TfNSW general incidents observed in this postcode on this snapshot_date. "
        "Vehicle accidents, road closures, etc.",

    "major_hazards_count":
        "Distinct count of major-severity hazards observed in this postcode on this snapshot_date.",

    "total_active_hazards":
        "Total distinct hazards observed in this postcode across all types on this snapshot_date.",

    "risk_level":
        "Composite risk level for this postcode on this snapshot_date. One of 'Low', 'Moderate', 'High', 'Critical'. "
        "Same rule as the current table, just applied per snapshot.",

    # Weather (daily aggregates)
    "daily_avg_temp_c":
        "Average temperature in Celsius across all hourly forecast rows that landed on this snapshot_date.",

    "daily_max_temp_c":
        "Maximum temperature in Celsius across all hourly forecasts on this snapshot_date. Use for 'hottest day' questions.",

    "daily_min_temp_c":
        "Minimum temperature in Celsius across all hourly forecasts on this snapshot_date. Use for 'coldest day' / overnight low questions.",

    "daily_total_rain_mm":
        "Sum of precipitation in millimetres across all hourly forecasts on this snapshot_date. "
        "Use for 'how much rain did postcode X get?' trend questions.",

    "daily_max_wind_kmh":
        "Maximum wind speed in km/h observed across all hourly forecasts on this snapshot_date.",
}



for col, comment in HISTORY_COLUMN_COMMENTS.items():
    # Escape single quotes in the comment for SQL safety
    safe = comment.replace("'", "''")
    spark.sql(f"COMMENT ON COLUMN {HISTORY_FQN}.{col} IS '{safe}'")

print(f"✅ Set {len(HISTORY_COLUMN_COMMENTS)} column comments on {HISTORY_FQN}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## §4 — Table-level comments — the Genie routing instructions
# MAGIC
# MAGIC These tell Genie's LLM **which table to pick** for a given question. The
# MAGIC LLM reads these as semantic grounding when deciding between the two tables.
# MAGIC Strong instructions here are the difference between Genie consistently
# MAGIC routing trend questions to `_history` and Genie defaulting to `_current`
# MAGIC and silently returning the wrong answer.

# COMMAND ----------

CURRENT_TABLE_COMMENT = (
    "Per-NSW-postcode resilience snapshot — CURRENT state only. One row per postcode (~600 rows). "
    "Use this table for OPERATIONAL questions about right now: 'what is the weather in 2795?', "
    "'which postcodes have active floods?', 'show me postcodes with the worst risk level', "
    "'what is the 24-hour forecast for X?', 'which postcodes face the most projected warming?'. "
    "For TREND / HISTORICAL / OVER-TIME questions (how has X changed, compare today vs last week, "
    "show the past month, daily/weekly aggregates), use the sibling table "
    "gold_nsw_postcode_resilience_history_dlt instead. "
    "Refreshed every 3 hours by the SDP pipeline (notebook 17)."
)

HISTORY_TABLE_COMMENT = (
    "Per-NSW-postcode resilience TREND table. One row per (postcode, snapshot_date) — i.e. one row per "
    "postcode per UTC date for days where data was observed. Use this table EXCLUSIVELY for TREND, "
    "HISTORICAL, or OVER-TIME questions: 'how has 2480's flood count changed?', 'compare hazards today "
    "vs last week', 'show the daily rainfall trend for X', 'which postcodes had Critical risk on any "
    "day this week?'. ALWAYS GROUP BY or filter on snapshot_date in queries against this table. "
    "For CURRENT STATE questions ('what is the weather right now?', 'which postcodes have active hazards?'), "
    "use the sibling table gold_nsw_postcode_resilience_current_dlt instead — it has more columns "
    "(climate projections, 24h forecast) and is faster. Climate (climate_temp_*, warming_*) columns are "
    "intentionally absent here: they're invariant per postcode, so they live only in the _current table."
)

spark.sql(f"COMMENT ON TABLE {CURRENT_FQN} IS '{CURRENT_TABLE_COMMENT.replace(chr(39), chr(39)+chr(39))}'")
spark.sql(f"COMMENT ON TABLE {HISTORY_FQN} IS '{HISTORY_TABLE_COMMENT.replace(chr(39), chr(39)+chr(39))}'")
print(f"Set table-level COMMENTs on both DLT gold tables")

# COMMAND ----------

# MAGIC %md
# MAGIC ## §5 — Sample questions for the Genie Space (12, mixing both shapes)
# MAGIC
# MAGIC Copy these into the Genie Space's **Sample Questions** section, one per row.
# MAGIC Mix is roughly 5 current / 5 history / 2 cross-cutting — fewer than this
# MAGIC starves the LLM of routing examples; more dilutes signal.

# COMMAND ----------

SAMPLE_QUESTIONS = [
    # ── Current shape (operational "right now") ──
    "Which postcodes have active flood hazards right now?",
    "What is the current temperature and 24-hour rainfall forecast for postcode 2795?",
    "Show me NSW postcodes with the most active hazards.",
    "How many postcodes have a Critical risk level today?",
    "Which 10 postcodes face the highest projected warming by 2080?",

    # ── History shape (trend, over-time) ──
    "How has the total active hazard count for postcode 2480 changed over the past 7 days?",
    "Show me the daily flood count trend for postcodes in the Northern Rivers region.",
    "Which postcodes had Critical risk on any day in the past week?",
    "What was the maximum daily rainfall recorded for postcode 2480 over the past month?",
    "Compare today's total active hazards across NSW vs the average daily count over the past 30 days.",

    # ── Cross-cutting (forces Genie to choose carefully) ──
    "For postcode 2795, what's the current risk level and how does it compare to last week's average?",
    "Find postcodes that have had floods on more than 3 different days in the past month.",
]

for i, q in enumerate(SAMPLE_QUESTIONS, start=1):
    print(f"  {i:2d}. {q}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## §6 — Genie Space UI setup walkthrough
# MAGIC
# MAGIC Genie spaces are created via the Databricks UI — programmatic creation is
# MAGIC limited in the current SDK. Follow these steps once after running this
# MAGIC notebook.
# MAGIC
# MAGIC ### 1. Open Genie
# MAGIC Workspace sidebar → **AI/BI** → **Genie** → **+ New Space** (top right)
# MAGIC
# MAGIC ### 2. Configure the space
# MAGIC
# MAGIC | Field | Value |
# MAGIC |---|---|
# MAGIC | **Name** | `EcoResilience NSW Resilience Explorer v2 (DLT)` |
# MAGIC | **Description** | `Ask natural-language questions about NSW postcode-level resilience. Supports both 'right now' and 'over time' shapes — Genie picks the right table from your phrasing.` |
# MAGIC | **Default warehouse** | Your serverless SQL warehouse |
# MAGIC
# MAGIC ### 3. Add the two DLT gold tables
# MAGIC
# MAGIC - **Add data → Browse**
# MAGIC - Navigate to `eco_resilience` → `dlt`
# MAGIC - Add `gold_nsw_postcode_resilience_current_dlt`
# MAGIC - Add `gold_nsw_postcode_resilience_history_dlt`
# MAGIC
# MAGIC ### 4. Paste General Instructions
# MAGIC
# MAGIC Click **Instructions** in the space settings and paste the text below.
# MAGIC This is the LLM-facing "system prompt" for the space:
# MAGIC
# MAGIC ```text
# MAGIC This space answers questions about NSW postcode resilience — combining
# MAGIC real-time hazards (TfNSW), weather (Open-Meteo), and climate projections (CSIRO).
# MAGIC
# MAGIC You have TWO tables for the same domain:
# MAGIC   - gold_nsw_postcode_resilience_current_dlt  — RIGHT NOW state, one row per postcode.
# MAGIC   - gold_nsw_postcode_resilience_history_dlt  — TREND state, one row per (postcode, snapshot_date).
# MAGIC
# MAGIC Pick by question shape:
# MAGIC   - "what is X right now?", "show me current Y", "which postcodes have active Z?",
# MAGIC     "highest projected warming", "next 24 hours"
# MAGIC       → use the CURRENT table.
# MAGIC   - "how has X changed?", "trend over time", "compare today vs last week",
# MAGIC     "history of Y", "past N days/weeks", "daily/weekly aggregates"
# MAGIC       → use the HISTORY table.
# MAGIC   - For HISTORY queries, ALWAYS group by or filter on snapshot_date.
# MAGIC
# MAGIC Synonyms / mapping:
# MAGIC   - "disaster", "emergency"        → active hazards (counts in either table).
# MAGIC   - "high risk", "risky"            → risk_level IN ('Critical', 'High').
# MAGIC   - "warming", "climate change"     → warming_2080s_rcp85_c (CURRENT table only).
# MAGIC   - "raining"                       → precipitation > 0.
# MAGIC   - "Bathurst" → postcode = '2795'. "Sydney" → postcode = '2000'.
# MAGIC     "Newcastle" → '2300'. "Lismore" → '2480'.
# MAGIC
# MAGIC Formatting rules:
# MAGIC   - Always include the postcode column in hazard-related answers.
# MAGIC   - Round temperatures to 1 decimal place, warming to 2 decimal places.
# MAGIC   - Never SUM the postcode column — it's an identifier, not a measure.
# MAGIC
# MAGIC Climate columns (climate_temp_*, warming_2080s_rcp85_c) ONLY exist in the
# MAGIC CURRENT table — they're invariant per postcode so we didn't duplicate them
# MAGIC across snapshots. If asked a climate question, always use the CURRENT table.
# MAGIC ```
# MAGIC
# MAGIC ### 5. Paste Sample Questions
# MAGIC
# MAGIC In the **Sample Questions** tab, paste each line from §5 above as a separate row.
# MAGIC
# MAGIC ### 6. Save + smoke-test
# MAGIC
# MAGIC Click **Save**. Open the chat interface and test by asking:
# MAGIC - *"What is the current temperature in 2795?"* → expect SQL against `_current` table
# MAGIC - *"How has 2480's hazard count changed this week?"* → expect SQL against `_history` table with snapshot_date filter
# MAGIC
# MAGIC If either question routes to the wrong table, refine the table-level COMMENT
# MAGIC (§4 above) — the comment is the strongest routing signal. Re-run §4 and
# MAGIC ask Genie again; no need to recreate the space.

# COMMAND ----------

# MAGIC %md
# MAGIC ## §7 — Verification: confirm comments landed

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7a — Per-table EXTENDED descriptions

# COMMAND ----------

display(spark.sql(f"DESCRIBE TABLE EXTENDED {CURRENT_FQN}"))

# COMMAND ----------

display(spark.sql(f"DESCRIBE TABLE EXTENDED {HISTORY_FQN}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7b — Confirm snapshot_date diversity in history
# MAGIC
# MAGIC Genie's trend questions need ≥2 distinct snapshot_dates to demo well. If
# MAGIC this query returns 1, run the fetcher Workflow + SDP pipeline once more on
# MAGIC a different UTC date before testing trend questions in the Genie UI.

# COMMAND ----------

display(spark.sql(f"""
    SELECT COUNT(DISTINCT snapshot_date) AS distinct_dates,
           MIN(snapshot_date)            AS earliest,
           MAX(snapshot_date)            AS latest,
           COUNT(*)                      AS total_rows
    FROM {HISTORY_FQN}
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7c — Sample SQL that a Genie trend question should produce
# MAGIC
# MAGIC Compare what you'd hope Genie writes to what it actually generates. If they
# MAGIC differ a lot, that's a signal to tighten the table COMMENT or add a more
# MAGIC pointed sample question to the space.

# COMMAND ----------

display(spark.sql(f"""
    -- Hopeful SQL for "How has 2480's hazard count changed in the past 7 days?"
    SELECT snapshot_date, total_active_hazards, risk_level
    FROM   {HISTORY_FQN}
    WHERE  postcode = '2480'
      AND  snapshot_date >= current_date() - INTERVAL 7 DAYS
    ORDER BY snapshot_date
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## §8 — Iteration log
# MAGIC
# MAGIC Fill this in as you smoke-test the §5 sample questions. For each question
# MAGIC where Genie produced wrong / awkward SQL: what was asked, what Genie did
# MAGIC wrong, which knob you tuned (a column COMMENT in §2/§3 above, the table
# MAGIC COMMENT in §4, the General Instructions in the UI, or a new synonym),
# MAGIC whether the fix worked.
# MAGIC
# MAGIC This log is the durable artifact of the lab — the actual deliverable.
# MAGIC
# MAGIC | # | Question | Wrong behaviour | Fix applied | Worked? |
# MAGIC |---|---|---|---|---|
# MAGIC |   |   |   |   |   |
# MAGIC
# MAGIC Target: ≥3 entries before declaring this lab "done". Most Genie spaces need
# MAGIC 5-10 iterations on their first build before the NL→SQL is consistently
# MAGIC correct — especially when there are two sibling tables to route between.

# COMMAND ----------

# MAGIC %md
# MAGIC ## §9 — Done
# MAGIC
# MAGIC | What we built | Why it matters |
# MAGIC |---|---|
# MAGIC | 26 column COMMENTs on `gold_*_current_dlt` | Genie's semantic grounding for "right now" questions |
# MAGIC | 16 column COMMENTs on `gold_*_history_dlt` | Genie's semantic grounding for "over time" questions |
# MAGIC | 2 table COMMENTs with explicit routing instructions | The LLM picks the right table from question phrasing |
# MAGIC | 12 sample questions, ~50/50 split | Anchors Genie's NL→SQL with examples for both shapes |
# MAGIC | UI walkthrough (§6) | Anyone reading the notebook can recreate the space |
# MAGIC
# MAGIC ### Side-by-side comparison vs v1
# MAGIC
# MAGIC Open both spaces and ask the same trend question:
# MAGIC   *"How has 2480's flood count changed over the past 7 days?"*
# MAGIC
# MAGIC - **v1 (notebook 16's view)** — can't answer; no history.
# MAGIC - **v2 (this notebook's space)** — answers cleanly from `_history`.
# MAGIC
# MAGIC That's the demo-able win: the medallion history/current split unlocks an
# MAGIC entire question shape Genie couldn't address before. Spend 30 min after
# MAGIC the lab writing this up in `doc/dlt_vs_workflows_notes.md` — that
# MAGIC comparison doc is what a hiring manager actually reads.

# COMMAND ----------

# MAGIC %md
# MAGIC ## §10 — Re-applying comments after a Full Refresh
# MAGIC
# MAGIC SDP `Full Refresh all` rebuilds the gold tables, which **drops** the
# MAGIC `ALTER TABLE`-applied COMMENTs (Delta-table-level metadata doesn't survive
# MAGIC the rebuild). After every Full Refresh, re-run this notebook (it's
# MAGIC idempotent — re-applying COMMENTs is harmless).
# MAGIC
# MAGIC A more durable solution: move COMMENTs inline into the
# MAGIC `@dp.materialized_view(comment=...)` decorator. Table COMMENT IS supported
# MAGIC there; per-column comments are NOT (as of 2026). For full survival across
# MAGIC Full Refreshes, a future polish would be to define the gold tables via
# MAGIC `CREATE TABLE ... USING DELTA AS SELECT ...` outside SDP, but that loses
# MAGIC the declarative-DAG win.