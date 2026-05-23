# Databricks notebook source
# MAGIC %md
# MAGIC # 11 — Register `get_industry_context` (ABS Industry UC Tool)
# MAGIC ## Phase 4 Step 3b.5 — Part 2
# MAGIC
# MAGIC **What this notebook does**
# MAGIC
# MAGIC Registers `eco_resilience.silver.get_industry_context(code STRING)` as a
# MAGIC UC SQL function the Mosaic AI agent can discover and call. It exposes
# MAGIC sector-wide totals from `silver.industry_context` (built by notebook 10)
# MAGIC plus four computed ratios.
# MAGIC
# MAGIC **Why this is the sixth tool**
# MAGIC
# MAGIC The agent currently has: identity (`verify_abn`), forecast
# MAGIC (`get_weather_forecast`), live hazards (`get_active_hazards`), climate
# MAGIC (`get_climate_projection`), and DRFA rules (`query_nema_guidelines`). This
# MAGIC tool adds the ABS sector-context layer the spec calls out for the Grant
# MAGIC Wizard — so the agent can ground statements like *"Tom operates within
# MAGIC Australia's $105B agriculture sector"* in real ABS data rather than
# MAGIC invented numbers.
# MAGIC
# MAGIC **What's different from the original plan**
# MAGIC
# MAGIC The original plan signature included `num_businesses` and per-business
# MAGIC averages. The `AUSTRALIAN_INDUSTRY` dataflow doesn't publish business
# MAGIC counts (those live in the separate `CABEE` ABS product), so this
# MAGIC function exposes industry-wide totals plus four computed ratios:
# MAGIC
# MAGIC - `revenue_per_employee_aud` — sector productivity proxy
# MAGIC - `wages_share_of_income_pct` — labour intensity
# MAGIC - `ebitda_margin_pct` — sector profitability
# MAGIC - `value_added_intensity_pct` — GDP contribution per $ of revenue
# MAGIC
# MAGIC **Compute:** Serverless. Pure SQL — no Python sandbox concerns.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration

# COMMAND ----------

CATALOG       = "eco_resilience"
SILVER_SCHEMA = "silver"

FUNCTION_NAME = "get_industry_context"
FQN           = f"{CATALOG}.{SILVER_SCHEMA}.{FUNCTION_NAME}"

SOURCE_TABLE  = f"{CATALOG}.{SILVER_SCHEMA}.industry_context"

print(f"Will register: {FQN}")
print(f"Reading from:  {SOURCE_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Pre-flight — confirm the source Silver table exists with data

# COMMAND ----------

cnt = spark.table(SOURCE_TABLE).count()
print(f"  ✅ {SOURCE_TABLE}  ({cnt:,} rows)")

# Spot-check Tom's industry (Agriculture) and Food and Beverage Services
display(spark.sql(f"""
    SELECT anzsic_code, anzsic_name, reference_year,
           num_employees_thousand, total_income_aud_m, industry_value_added_aud_m
    FROM   {SOURCE_TABLE}
    WHERE  anzsic_code IN ('01', '45')
    ORDER  BY anzsic_code
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Tool description — the LLM-facing COMMENT
# MAGIC
# MAGIC The most important sentences to get right:
# MAGIC - **2-digit ANZSIC Subdivision codes** (NOT 4-digit Classes — this dataflow doesn't have those)
# MAGIC - **Sector totals, not per-business** (the data limitation matters for honest pitch language)
# MAGIC - **Units convention** — employment in thousands, money in $AUD millions
# MAGIC - **Common codes** the LLM can pull from training-data ANZSIC knowledge

# COMMAND ----------

TOOL_DESCRIPTION = (
    "Returns Australian Bureau of Statistics industry context for a 2-digit "
    "ANZSIC Subdivision code: industry-wide totals (employment, total income, "
    "industry value added, operating profit, wages) plus derived sector "
    "ratios (revenue per employee, wages share of income, EBITDA margin, "
    "value-added intensity). Use this whenever the user mentions an industry "
    "(farming, hospitality, retail, construction, manufacturing, etc.) or you "
    "need to ground grant estimates, loss calculations, or impact discussions "
    "in real ABS data. The argument is a 2-digit ANZSIC Subdivision code as a "
    "STRING with leading zeros preserved (e.g. 01 for Agriculture which "
    "covers all farming and primary production, 45 for Food and Beverage "
    "Services, 43 for Retail Trade, 11 for Food Product Manufacturing). "
    "IMPORTANT: this dataflow publishes industry-wide totals, NOT per-business "
    "norms — frame results as sector context (for example, the agriculture "
    "industry employs 337,000 people), not per-business specifics. Employment "
    "values are in thousands; monetary values are in AUD millions. Returns the "
    "error field populated when the code is not in our dataset."
)

ARG_COMMENT = (
    "2-digit ANZSIC Subdivision code as a STRING with leading zeros preserved. "
    "Examples: 01 Agriculture, 45 Food and Beverage Services, 43 Retail Trade. "
    "NOT a 4-digit Class code."
)

print(f"Tool description: {len(TOOL_DESCRIPTION)} chars")
print()
print(TOOL_DESCRIPTION)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Register the function
# MAGIC
# MAGIC One `CREATE OR REPLACE FUNCTION` with:
# MAGIC - **12 result fields**: identity (3) + raw totals (5) + derived ratios (4)
# MAGIC - **Trimmed input** so `'01'` and `' 01 '` both work
# MAGIC - **Error field** populated only when the code isn't found
# MAGIC - **Named arg `code`** (not `anzsic_code`) to avoid collision with the column

# COMMAND ----------

create_sql = f"""
CREATE OR REPLACE FUNCTION {FQN}(
  code STRING COMMENT '{ARG_COMMENT}'
)
RETURNS STRUCT<
  anzsic_code                 STRING,
  anzsic_name                 STRING,
  reference_year              STRING,
  num_employees_thousand      DOUBLE,
  total_income_aud_m          DOUBLE,
  industry_value_added_aud_m  DOUBLE,
  operating_profit_aud_m      DOUBLE,
  wages_salaries_aud_m        DOUBLE,
  revenue_per_employee_aud    INT,
  wages_share_of_income_pct   DOUBLE,
  ebitda_margin_pct           DOUBLE,
  value_added_intensity_pct   DOUBLE,
  error                       STRING
>
COMMENT '{TOOL_DESCRIPTION}'
RETURN (
  WITH
  cleaned AS (
    SELECT TRIM(code) AS code_clean
  ),
  ind AS (
    SELECT
      i.anzsic_code,
      i.anzsic_name,
      i.reference_year,
      i.num_employees_thousand,
      i.total_income_aud_m,
      i.industry_value_added_aud_m,
      i.operating_profit_aud_m,
      i.ebitda_aud_m,
      i.wages_salaries_aud_m
    FROM {SOURCE_TABLE} i
    JOIN cleaned c ON i.anzsic_code = c.code_clean
    LIMIT 1
  )
  SELECT named_struct(
    'anzsic_code',
        TRIM(code),                                 -- echo the (trimmed) input, even on error
    'anzsic_name',
        (SELECT anzsic_name FROM ind),
    'reference_year',
        (SELECT reference_year FROM ind),
    'num_employees_thousand',
        (SELECT num_employees_thousand FROM ind),
    'total_income_aud_m',
        (SELECT total_income_aud_m FROM ind),
    'industry_value_added_aud_m',
        (SELECT industry_value_added_aud_m FROM ind),
    'operating_profit_aud_m',
        (SELECT operating_profit_aud_m FROM ind),
    'wages_salaries_aud_m',
        (SELECT wages_salaries_aud_m FROM ind),
    'revenue_per_employee_aud',
        (SELECT
            CASE WHEN num_employees_thousand IS NOT NULL
                  AND num_employees_thousand > 0
                  AND total_income_aud_m IS NOT NULL
                 THEN CAST((total_income_aud_m * 1000.0) / num_employees_thousand AS INT)
                 ELSE NULL
            END
         FROM ind),
    'wages_share_of_income_pct',
        (SELECT
            CASE WHEN total_income_aud_m IS NOT NULL
                  AND total_income_aud_m > 0
                  AND wages_salaries_aud_m IS NOT NULL
                 THEN ROUND((wages_salaries_aud_m / total_income_aud_m) * 100.0, 2)
                 ELSE NULL
            END
         FROM ind),
    'ebitda_margin_pct',
        (SELECT
            CASE WHEN total_income_aud_m IS NOT NULL
                  AND total_income_aud_m > 0
                  AND ebitda_aud_m IS NOT NULL
                 THEN ROUND((ebitda_aud_m / total_income_aud_m) * 100.0, 2)
                 ELSE NULL
            END
         FROM ind),
    'value_added_intensity_pct',
        (SELECT
            CASE WHEN total_income_aud_m IS NOT NULL
                  AND total_income_aud_m > 0
                  AND industry_value_added_aud_m IS NOT NULL
                 THEN ROUND((industry_value_added_aud_m / total_income_aud_m) * 100.0, 2)
                 ELSE NULL
            END
         FROM ind),
    'error',
        CASE
          WHEN NOT EXISTS (SELECT 1 FROM ind)
          THEN concat('ANZSIC code ', TRIM(code), ' not found in our industry data. Use a 2-digit Subdivision code like 01, 45, 43.')
          ELSE NULL
        END
  )
)
"""

spark.sql(create_sql)
print(f"✅ Registered {FQN}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Smoke tests
# MAGIC
# MAGIC ### 5a — Tom's industry: ANZSIC `01` (Agriculture)
# MAGIC
# MAGIC Expected (FY2024): `anzsic_name="Agriculture"`, ~337 (thousand) employees,
# MAGIC `total_income_aud_m` ≈ 104,830, ratios populated.

# COMMAND ----------

display(spark.sql(f"SELECT r.* from (select {FQN}('01') as r)"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5b — Food and Beverage Services: ANZSIC `45`
# MAGIC
# MAGIC The hospitality industry — for Tom's "what if I ran a cafe instead?" line.

# COMMAND ----------

display(spark.sql(f"SELECT r.* from (select {FQN}('45') as r)"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5c — Whitespace handling
# MAGIC
# MAGIC The TRIM in the function body should make `' 01 '` behave the same as `'01'`.

# COMMAND ----------

display(spark.sql(f"SELECT r.* from (select {FQN}(' 45 ') as r)"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5d — Error path: unknown code
# MAGIC
# MAGIC Expected: `error` field populated with a helpful message, all other fields NULL.

# COMMAND ----------

display(spark.sql(f"SELECT r.* from (select {FQN}('99') as r)"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5e — Error path: 4-digit code (this dataflow doesn't have Classes)
# MAGIC
# MAGIC Expected: same error, since `'0160'` isn't a Subdivision code in our data.

# COMMAND ----------

display(spark.sql(f"SELECT r.* from (select {FQN}('0160') as r)"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Discoverability — what the agent sees

# COMMAND ----------

display(spark.sql(f"DESCRIBE FUNCTION EXTENDED {FQN}"))

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog eco_resilience;

# COMMAND ----------

display(spark.sql(f"SHOW USER FUNCTIONS IN {CATALOG}.{SILVER_SCHEMA}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Done
# MAGIC
# MAGIC | What we built | Why it matters |
# MAGIC |---|---|
# MAGIC | `silver.get_industry_context` UC SQL function | Sixth tool for the agent — sector context for grant grounding |
# MAGIC | Raw totals + computed ratios | Compensates for the missing `num_businesses` field — gives useful sector signals |
# MAGIC | Strong COMMENT with units convention | LLM gets the units right when formatting numbers for users |
# MAGIC | Error path with helpful message | Tells the agent (and user) what valid codes look like |
# MAGIC
# MAGIC ### What's next — Part 3 of Step 3b.5
# MAGIC
# MAGIC Update `notebooks/07_minimal_agent.py`:
# MAGIC
# MAGIC 1. Add `silver.get_industry_context` to `TOOL_FUNCTIONS` (now 6 tools)
# MAGIC 2. Extend the system prompt with the sixth tool and example codes
# MAGIC 3. Add a multi-tool smoke test that exercises industry context + DRFA grant
# MAGIC    rules + identity — the closest thing yet to the full Tom-the-farmer
# MAGIC    "Magic Moment" narrative.
# MAGIC
# MAGIC ### Then — Phase 4 Step 3c (`generate_grant_pdf`)
# MAGIC
# MAGIC The final agent tool: Jinja2 template that renders all the agent's
# MAGIC reasoning (identity + spatial + climate + DRFA citations + industry
# MAGIC context) into a NEMA-formatted PDF grant application.