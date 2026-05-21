# Databricks notebook source
# MAGIC %md
# MAGIC # 10 — Ingest ABS `AUSTRALIAN_INDUSTRY` → Bronze + Silver
# MAGIC ## Phase 4 Step 3b.5 — Industry context for the Grant Wizard
# MAGIC
# MAGIC **What this notebook does**
# MAGIC
# MAGIC Pulls the latest year of ABS `AUSTRALIAN_INDUSTRY` data (number of
# MAGIC businesses, employment, income, value added — broken down by ANZSIC code)
# MAGIC from the public ABS Data API as CSV, lands the raw shape in Bronze, and
# MAGIC pivots into an agent-friendly Silver table keyed by ANZSIC class.
# MAGIC
# MAGIC **Why now**
# MAGIC
# MAGIC The agent already verifies businesses, fetches weather/hazards/climate,
# MAGIC and retrieves DRFA grant rules. To complete the "Grant Wizard 92% match
# MAGIC score" beat from the spec, it needs **industry-typical numbers**:
# MAGIC *"a dairy operation in NSW typically has ~3 employees and ~$420K annual
# MAGIC revenue (ABS AUSTRALIAN_INDUSTRY 2022-23)"*. Without these, any
# MAGIC number in the pre-filled grant is hallucinated.
# MAGIC
# MAGIC **Format choice: CSV (not SDMX-JSON)**
# MAGIC
# MAGIC ABS supports both. CSV is flat, library-free, ~10 lines to parse.
# MAGIC SDMX-JSON is compact and metadata-rich but needs the `sdmx1` library
# MAGIC and a learning curve. For a focused, one-shot ingest, CSV wins.
# MAGIC If the CSV path fails (some older dataflows historically refused CSV),
# MAGIC the fallback is `%pip install sdmx1` + JSON parsing.
# MAGIC
# MAGIC **Outputs**
# MAGIC
# MAGIC - `bronze.abs_industry_raw` — direct landing of CSV rows (audit-pure)
# MAGIC - `silver.industry_context` — one row per ANZSIC class, pivoted, with computed averages
# MAGIC
# MAGIC **Compute:** Serverless. `requests` and `pandas` are in the image.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration

# COMMAND ----------

CATALOG       = "eco_resilience"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"

BRONZE_TABLE         = f"{CATALOG}.{BRONZE_SCHEMA}.abs_industry_raw"
BRONZE_CODELIST_TBL  = f"{CATALOG}.{BRONZE_SCHEMA}.abs_anzsic_codelist"
SILVER_TABLE         = f"{CATALOG}.{SILVER_SCHEMA}.industry_context"

# ABS Data API SDMX REST endpoint
# Version 1.1.0 confirmed via /rest/dataflow/ABS/AUSTRALIAN_INDUSTRY metadata
# (2026-05-13). If ABS publishes a newer version later, swap to "latest" or
# bump this number — re-run notebook 10 and the rest of the pipeline rebuilds.
ABS_AGENCY    = "ABS"
ABS_DATAFLOW  = "AUSTRALIAN_INDUSTRY"
ABS_VERSION   = "1.1.0"
API_URL       = f"https://data.api.abs.gov.au/rest/data/{ABS_AGENCY},{ABS_DATAFLOW},{ABS_VERSION}/all"

print(f"BRONZE_TABLE = {BRONZE_TABLE}")
print(f"SILVER_TABLE = {SILVER_TABLE}")
print(f"API_URL      = {API_URL}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Fetch CSV from the ABS Data API
# MAGIC
# MAGIC Single HTTP GET with `Accept: application/vnd.sdmx.data+csv`. ABS Data
# MAGIC API requires no auth for public dataflows. We add a User-Agent header
# MAGIC for politeness — some APIs reject anonymous requests.
# MAGIC
# MAGIC The response is typically a few MB across all ANZSIC industries × all
# MAGIC measures × all time periods. Acceptable on the driver.

# COMMAND ----------

import requests
import time
from io import StringIO

t0 = time.time()
response = requests.get(
    API_URL,
    headers={
        "Accept":     "application/vnd.sdmx.data+csv",
        "User-Agent": "eco_resilience_hackathon/1.0",
    },
    timeout=60,
)
elapsed = time.time() - t0

print(f"HTTP {response.status_code} | {len(response.content):,} bytes | {elapsed:.1f}s")
print(f"Content-Type: {response.headers.get('Content-Type')}")
response.raise_for_status()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Parse and explore — what does ABS actually return?
# MAGIC
# MAGIC ABS column names and measure codes vary across dataflows. We print the
# MAGIC shape, distinct measures, distinct industries, and time periods to
# MAGIC discover what we're working with — these inform the Silver pivot below.

# COMMAND ----------

import pandas as pd

# Read EVERY column as string — preserves the original ABS code forms (Division
# letters like 'A', 2-digit Subdivisions like '01', 4-digit Classes like '0160').
# If we let pandas infer types, "0160" would silently become integer 160 and the
# codelist join would break. OBS_VALUE is cast back to numeric in section 6.
df_pd = pd.read_csv(StringIO(response.text), dtype=str)
print(f"Rows:    {len(df_pd):,}")
print(f"Columns ({len(df_pd.columns)}): {df_pd.columns.tolist()}")
print()
df_pd.head(5)

# COMMAND ----------

# Identify the likely column roles defensively
columns_lower = {c.lower(): c for c in df_pd.columns}

def _pick(*candidates):
    """Find the first column whose lowercase name matches any candidate."""
    for cand in candidates:
        if cand in columns_lower:
            return columns_lower[cand]
    return None

INDUSTRY_COL = _pick("industry", "anzsic", "industry_code")
MEASURE_COL  = _pick("measure", "m")
TIME_COL     = _pick("time_period", "time", "period")
VALUE_COL    = _pick("obs_value", "value")

print(f"INDUSTRY_COL = {INDUSTRY_COL}")
print(f"MEASURE_COL  = {MEASURE_COL}")
print(f"TIME_COL     = {TIME_COL}")
print(f"VALUE_COL    = {VALUE_COL}")

assert all([INDUSTRY_COL, MEASURE_COL, TIME_COL, VALUE_COL]), (
    "Could not auto-detect column roles — inspect df_pd.columns above and "
    "edit the _pick() calls in this cell."
)

# COMMAND ----------

# Distinct measure codes — these are what we'll pivot into Silver columns
print("Distinct measures in the data:")
display(
    df_pd[MEASURE_COL]
    .value_counts()
    .reset_index()
    .rename(columns={"index": "measure_code", MEASURE_COL: "row_count"})
)

# COMMAND ----------

# Distinct time periods (we'll filter to the latest in Silver)
periods = sorted(df_pd[TIME_COL].dropna().unique().tolist())
print(f"Time periods available ({len(periods)}):")
for p in periods:
    print(f"  • {p}")
LATEST_PERIOD = periods[-1]
print(f"\nLatest period to use for Silver: {LATEST_PERIOD}")

# COMMAND ----------

# Distinct industries
industries = sorted(df_pd[INDUSTRY_COL].dropna().astype(str).unique().tolist())
print(f"Distinct industries: {len(industries)}")
print(f"First 20: {industries[:20]}")
print(f"Last 5:   {industries[-5:]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Bronze — land the raw CSV
# MAGIC
# MAGIC Lowercase column names and add audit fields. Bronze keeps every row from
# MAGIC the API response — all measures, all time periods, all industries.

# COMMAND ----------

from pyspark.sql import functions as F

# Normalise column names for SQL friendliness
df_pd_clean = df_pd.copy()
df_pd_clean.columns = [c.lower().replace(" ", "_").replace("-", "_") for c in df_pd_clean.columns]

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SILVER_SCHEMA}")

df_bronze = (
    spark.createDataFrame(df_pd_clean)
    .withColumn("_source_url",  F.lit(API_URL))
    .withColumn("_ingest_time", F.current_timestamp())
)

(
    df_bronze.write
    .format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .saveAsTable(BRONZE_TABLE)
)

print(f"✅ Wrote {df_bronze.count():,} rows to {BRONZE_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Fetch the full ANZSIC codelist from ABS
# MAGIC
# MAGIC ABS doesn't include the human-readable industry name in the data
# MAGIC response — only the code. Rather than hardcode a small lookup dict, we
# MAGIC pull the full ANZSIC codelist from ABS via the SDMX structure endpoint
# MAGIC and land it as `bronze.abs_anzsic_codelist` (~500 codes covering every
# MAGIC Division / Subdivision / Group / Class). The Silver build then joins
# MAGIC against this table to populate `anzsic_name` for every industry.
# MAGIC
# MAGIC Codelist ID is discovered defensively: we probe a handful of likely IDs
# MAGIC (`CL_ANZSIC_2006`, `ANZSIC_2006`, `CL_INDUSTRY`, `CL_ANZSIC`) and use the
# MAGIC first one that returns HTTP 200.

# COMMAND ----------

# 5a. Codelist URL — discovered via the dataflow `references=all` diagnostic
# on 2026-05-13. AUSTRALIAN_INDUSTRY uses its own curated codelist
# (CL_AUSTRALIAN_INDUSTRY, ~108 codes at a mix of Division/Subdivision/Group/Class
# levels), NOT the generic CL_ANZSIC_2006 (~500 codes).
#
# ABS structure endpoints require:
#   • GET, not HEAD
#   • Accept header for SDMX-JSON
#   • Version in the URL path (1.0.0 here)
#
# If ABS bumps the version, update CODELIST_VERSION below.
CODELIST_AGENCY  = ABS_AGENCY            # "ABS"
CODELIST_ID      = "CL_AUSTRALIAN_INDUSTRY"
CODELIST_VERSION = "1.0.0"
codelist_url     = f"https://data.api.abs.gov.au/rest/codelist/{CODELIST_AGENCY}/{CODELIST_ID}/{CODELIST_VERSION}"
print(f"Using codelist: {CODELIST_ID} v{CODELIST_VERSION}")
print(f"URL: {codelist_url}")

# COMMAND ----------

# 5b. Fetch the codelist as SDMX-JSON (structure responses don't reliably support CSV)
r = requests.get(
    codelist_url,
    headers={
        "Accept":     "application/vnd.sdmx.structure+json",
        "User-Agent": "eco_resilience_hackathon/1.0",
    },
    timeout=30,
)
print(f"HTTP {r.status_code} | {len(r.content):,} bytes | content-type: {r.headers.get('Content-Type')}")
r.raise_for_status()
codelist_json = r.json()

# COMMAND ----------

# 5c. Parse — handle both string and {language:text} name shapes,
#     and walk the JSON defensively in case the path differs across versions
def _find_codes_array(obj):
    """Recursively locate the `codes` array in any SDMX-JSON structure response."""
    if isinstance(obj, dict):
        if isinstance(obj.get("codes"), list):
            return obj["codes"]
        for v in obj.values():
            result = _find_codes_array(v)
            if result is not None:
                return result
    elif isinstance(obj, list):
        for item in obj:
            result = _find_codes_array(item)
            if result is not None:
                return result
    return None

def _extract_name(name_field):
    """Handle both string and {'en': '...'} dict forms."""
    if isinstance(name_field, str):
        return name_field
    if isinstance(name_field, dict):
        return name_field.get("en") or next(iter(name_field.values()), None)
    return None

codes_raw = _find_codes_array(codelist_json)
assert codes_raw, "Could not locate the `codes` array in the codelist response; inspect codelist_json."

codes_pd = pd.DataFrame([
    {
        "anzsic_code": str(c.get("id")),
        "anzsic_name": _extract_name(c.get("name")),
    }
    for c in codes_raw
    if c.get("id") is not None
])

print(f"Parsed {len(codes_pd)} code entries.")
print(f"Sample:")
codes_pd.head(15)

# COMMAND ----------

# 5d. Land as Bronze
(
    spark.createDataFrame(codes_pd)
    .withColumn("_source_url",  F.lit(codelist_url))
    .withColumn("_ingest_time", F.current_timestamp())
    .write.format("delta")
    .mode("overwrite")
    .saveAsTable(BRONZE_CODELIST_TBL)
)
print(f"✅ Wrote {len(codes_pd):,} codes to {BRONZE_CODELIST_TBL}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Silver — pivot measures into one row per industry
# MAGIC
# MAGIC We pivot in pandas first (more forgiving than Spark SQL `PIVOT` when
# MAGIC measure codes are unknown ahead of time), then write to Delta with a
# MAGIC LEFT JOIN against the codelist for human-readable names.
# MAGIC
# MAGIC ### Actual ABS measure codes for this dataflow
# MAGIC
# MAGIC Discovered via `CL_AUSTRALIAN_INDUSTRY_MEASURE` codelist + a focused
# MAGIC sample row for ANZSIC `01` (Agriculture) on 2026-05-13:
# MAGIC
# MAGIC | ABS code | Meaning | Maps to our column |
# MAGIC |---|---|---|
# MAGIC | `EMPTOTAL` | Employment at end of June (UNIT_MULT=3) | `num_employees_thousand` |
# MAGIC | `INCTOTAL` | Total income ($m, UNIT_MULT=6) | `total_income_aud_m` |
# MAGIC | `INCSALGDSSERV` | Sales and service income ($m) | `sales_service_income_aud_m` |
# MAGIC | `INDUSTRY` | Industry value added ($m) | `industry_value_added_aud_m` |
# MAGIC | `EBITDA` | EBITDA ($m) | `ebitda_aud_m` |
# MAGIC | `OPBT` | Operating profit before tax ($m) | `operating_profit_aud_m` |
# MAGIC | `LABCOSTTOT` | Total labour costs ($m) | `total_labour_costs_aud_m` |
# MAGIC | `EXPLCWAGESNA` | Wages and salaries ($m) | `wages_salaries_aud_m` |
# MAGIC | `EXPTOTAL` | Total expenses ($m) | `total_expenses_aud_m` |
# MAGIC | `PURCHASES` | Purchases of goods and materials ($m) | `purchases_aud_m` |
# MAGIC | `EXPLCSUP` | Employer super contributions ($m) | `super_contributions_aud_m` |
# MAGIC
# MAGIC ### Things we discovered AREN'T in this dataflow
# MAGIC
# MAGIC - **Number of businesses** — published in a different ABS dataflow
# MAGIC   (`CABEE`). Per-business averages can't be computed here. We expose the
# MAGIC   industry totals instead — still defensible "Australian agriculture
# MAGIC   industry generates $105B in income annually".
# MAGIC
# MAGIC ### Units convention
# MAGIC
# MAGIC ABS reports values pre-scaled by `UNIT_MULT`. We preserve ABS-native units:
# MAGIC - Employment: **thousands** (e.g. `337.1` means 337,100 people)
# MAGIC - Money: **millions of AUD** (e.g. `104830.0` means $104.83 billion)
# MAGIC
# MAGIC The agent's tool description (notebook 11) will spell this out so the LLM
# MAGIC formats numbers correctly for users.

# COMMAND ----------

MEASURE_RENAME = {
    "EMPTOTAL":        "num_employees_thousand",       # UNIT_MULT=3 → in thousands
    "INCTOTAL":        "total_income_aud_m",            # UNIT_MULT=6 → in $AUD millions
    "INCSALGDSSERV":   "sales_service_income_aud_m",
    "INDUSTRY":        "industry_value_added_aud_m",
    "EBITDA":          "ebitda_aud_m",
    "OPBT":            "operating_profit_aud_m",
    "LABCOSTTOT":      "total_labour_costs_aud_m",
    "EXPLCWAGESNA":    "wages_salaries_aud_m",
    "EXPTOTAL":        "total_expenses_aud_m",
    "PURCHASES":       "purchases_aud_m",
    "EXPLCSUP":        "super_contributions_aud_m",
}

# Filter Bronze (in pandas) to latest period, then pivot
latest_pd = df_pd_clean[df_pd_clean[TIME_COL.lower()] == LATEST_PERIOD].copy()

# Cast obs_value to numeric. We read the whole CSV as strings (section 2) to
# preserve code formats like '01' or 'A' — now convert just the values to float.
latest_pd[VALUE_COL.lower()] = pd.to_numeric(latest_pd[VALUE_COL.lower()], errors="coerce")
print(f"Latest-period rows: {len(latest_pd):,}")

pivoted_pd = (
    latest_pd
    .pivot_table(
        index=INDUSTRY_COL.lower(),
        columns=MEASURE_COL.lower(),
        values=VALUE_COL.lower(),
        aggfunc="first",
    )
    .reset_index()
)

# Rename the columns we recognise; drop the rest
keep_cols = {INDUSTRY_COL.lower(): "anzsic_code"}
for abs_code in pivoted_pd.columns:
    if abs_code in MEASURE_RENAME:
        keep_cols[abs_code] = MEASURE_RENAME[abs_code]

pivoted_pd = pivoted_pd.rename(columns=keep_cols)
pivoted_pd = pivoted_pd[[c for c in pivoted_pd.columns if c in keep_cols.values()]]
print(f"Recognised measure columns kept: {sorted([c for c in pivoted_pd.columns if c != 'anzsic_code'])}")

# Ensure every target Silver column exists even if not present in ABS response
TARGET_COLS = list(MEASURE_RENAME.values())
for target in TARGET_COLS:
    if target not in pivoted_pd.columns:
        pivoted_pd[target] = None

# Preserve ABS codes verbatim — they're mostly 2-digit Subdivisions (`01`-`99`)
# in this dataflow. Reading CSV as dtype=str (section 2) keeps leading zeros.
pivoted_pd["anzsic_code"]    = pivoted_pd["anzsic_code"].astype(str)
pivoted_pd["reference_year"] = LATEST_PERIOD

# Final column order (anzsic_name added by the Spark join below)
pivoted_pd = pivoted_pd[
    ["anzsic_code", "reference_year"] + TARGET_COLS
]

print(f"\nPivoted (pre-name-join) shape: {pivoted_pd.shape}")
pivoted_pd.head(10)

# COMMAND ----------

# Land pivoted data as a temp view, then build Silver with a LEFT JOIN against
# the codelist Bronze table (5d) for human-readable names.
spark.createDataFrame(pivoted_pd).select(
    F.col("anzsic_code").cast("string"),
    F.col("reference_year").cast("string"),
    F.col("num_employees_thousand").cast("double"),
    F.col("total_income_aud_m").cast("double"),
    F.col("sales_service_income_aud_m").cast("double"),
    F.col("industry_value_added_aud_m").cast("double"),
    F.col("ebitda_aud_m").cast("double"),
    F.col("operating_profit_aud_m").cast("double"),
    F.col("total_labour_costs_aud_m").cast("double"),
    F.col("wages_salaries_aud_m").cast("double"),
    F.col("total_expenses_aud_m").cast("double"),
    F.col("purchases_aud_m").cast("double"),
    F.col("super_contributions_aud_m").cast("double"),
).createOrReplaceTempView("_silver_industry_tmp")

spark.sql(f"""
    CREATE OR REPLACE TABLE {SILVER_TABLE}
    CLUSTER BY (anzsic_code) AS
    SELECT
        p.anzsic_code,
        cl.anzsic_name,
        p.reference_year,
        p.num_employees_thousand,
        p.total_income_aud_m,
        p.sales_service_income_aud_m,
        p.industry_value_added_aud_m,
        p.ebitda_aud_m,
        p.operating_profit_aud_m,
        p.total_labour_costs_aud_m,
        p.wages_salaries_aud_m,
        p.total_expenses_aud_m,
        p.purchases_aud_m,
        p.super_contributions_aud_m
    FROM _silver_industry_tmp p
    LEFT JOIN {BRONZE_CODELIST_TBL} cl
      ON cl.anzsic_code = p.anzsic_code
""")

print(f"✅ Wrote Silver to {SILVER_TABLE} (CLUSTER BY anzsic_code, joined to {BRONZE_CODELIST_TBL})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Smoke tests
# MAGIC
# MAGIC ### 7a — Counts and column coverage

# COMMAND ----------

display(spark.sql(f"""
    SELECT
        COUNT(*)                                                            AS total_rows,
        COUNT(DISTINCT anzsic_code)                                              AS distinct_codes,
        SUM(CASE WHEN anzsic_name IS NOT NULL THEN 1 ELSE 0 END)                  AS named_codes,
        SUM(CASE WHEN num_employees_thousand IS NOT NULL THEN 1 ELSE 0 END)       AS rows_with_employment,
        SUM(CASE WHEN total_income_aud_m IS NOT NULL THEN 1 ELSE 0 END)           AS rows_with_income,
        SUM(CASE WHEN industry_value_added_aud_m IS NOT NULL THEN 1 ELSE 0 END)   AS rows_with_iva
    FROM   {SILVER_TABLE}
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7b — Rows for our named industries (the demo set)

# COMMAND ----------

display(spark.sql(f"""
    SELECT *
    FROM   {SILVER_TABLE}
    WHERE  anzsic_name IS NOT NULL
    ORDER  BY anzsic_code
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7c — Spot-check Tom's industry: ANZSIC 01 (Agriculture)
# MAGIC
# MAGIC Tom is a dairy farmer. The `AUSTRALIAN_INDUSTRY` dataflow doesn't publish
# MAGIC at the 4-digit Class level (so `0160 Dairy Cattle Farming` isn't here) —
# MAGIC it aggregates to 2-digit Subdivision. Tom's row is `01 Agriculture`.
# MAGIC
# MAGIC Expected (FY2024): `num_employees_thousand` ≈ 337 (i.e. 337,100 people),
# MAGIC `total_income_aud_m` ≈ 104,830 ($104.8 billion), `industry_value_added_aud_m` ≈ 24,976.

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {SILVER_TABLE} WHERE anzsic_code = '01'"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7d — Spot-check ANZSIC 45 (Food and Beverage Services)
# MAGIC
# MAGIC The cafe equivalent — also Subdivision-level, not Class-level.

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {SILVER_TABLE} WHERE anzsic_code = '45'"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Done
# MAGIC
# MAGIC | Layer | Table | Purpose |
# MAGIC |---|---|---|
# MAGIC | Bronze | `bronze.abs_industry_raw` | Raw CSV from ABS Data API — all 11 measures × ~102 industries × years 2007–2024 |
# MAGIC | Bronze | `bronze.abs_anzsic_codelist` | `CL_AUSTRALIAN_INDUSTRY` codelist (~108 codes) — Subdivision-level industry names |
# MAGIC | Silver | `silver.industry_context` | One row per Subdivision (latest year), pivoted: employment + 9 financial metrics in ABS-native units (employment in thousands, money in $AUD millions), joined with codelist for human-readable names |
# MAGIC
# MAGIC **Important: ABS publishes at the Subdivision level (2-digit codes like `01` Agriculture, `45` Food and Beverage Services), NOT at the 4-digit Class level.** Tom's dairy farm aggregates up to ANZSIC `01`. Business counts (`num_businesses`) are NOT in this dataflow — they live in the separate `CABEE` ABS product.
# MAGIC
# MAGIC ### What's next (Step 3b.5 part 2)
# MAGIC
# MAGIC `notebooks/11_register_industry_tool.py` — registers
# MAGIC `silver.get_industry_context(code STRING)` as a UC SQL UDF that the
# MAGIC agent can call. Same pure-SQL pattern as the data tools in notebook 08.
# MAGIC
# MAGIC ### Troubleshooting
# MAGIC
# MAGIC - **CSV not supported (HTTP 406 / non-CSV response):** fall back to SDMX-JSON.
# MAGIC   `%pip install sdmx1` then use `sdmx.Client("ABS").data("AUSTRALIAN_INDUSTRY")`
# MAGIC   and convert with `sdmx.to_pandas(...)`. The rest of the pipeline (Bronze
# MAGIC   write, pivot, Silver build) stays the same.
# MAGIC - **All measure columns NULL in Silver:** the `MEASURE_RENAME` map in
# MAGIC   section 6 didn't recognise the ABS codes. Re-run section 3's measure
# MAGIC   inspection, copy the codes you see, and add them to the map keys.
# MAGIC - **Unknown ANZSIC codes show `NULL` for `anzsic_name`:** the data has a
# MAGIC   code that isn't in the codelist Bronze table. Usually means ABS uses a
# MAGIC   pseudo-code in the data (e.g. "TOT" for "Total of all industries") that
# MAGIC   isn't a real ANZSIC entry. Filter these out in downstream queries or
# MAGIC   manually add them to a supplemental lookup if needed.
# MAGIC - **Codelist fetch fails (none of the candidate IDs return 200):** browse
# MAGIC   https://data.api.abs.gov.au/rest/codelist/ABS to find the current ID
# MAGIC   ABS is using, and add it to `CODELIST_CANDIDATES` in section 5a.