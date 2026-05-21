# Databricks notebook source
# MAGIC %md
# MAGIC # 06 — Register Mosaic AI Agent Tools as Unity Catalog Functions
# MAGIC ## Step 1: `verify_abn`
# MAGIC
# MAGIC **What this notebook does**
# MAGIC
# MAGIC Promotes the plain-Python `verify_abn` helper from notebook 05 into a
# MAGIC registered Unity Catalog Function the Mosaic AI Agent Framework can
# MAGIC auto-discover as a tool. Same logic, different surface: now callable from
# MAGIC SQL, Genie, the agent — anywhere with UC EXECUTE permission.
# MAGIC
# MAGIC **Why we need this**
# MAGIC
# MAGIC The Agent Framework only discovers tools that live as UC functions. The
# MAGIC LLM reads `COMMENT ON FUNCTION ...` as the tool description and uses
# MAGIC argument types + comments to decide when and how to call it. Notebook 05's
# MAGIC plain Python helper is invisible to the agent.
# MAGIC
# MAGIC **What this notebook does NOT do**
# MAGIC
# MAGIC - Does not delete or modify notebook 05 — that stays as a manual test surface.
# MAGIC - Does not grant EXECUTE permissions (that's Phase 4 step 2, once the
# MAGIC   agent's service principal exists).
# MAGIC - Does not wire the function into the agent — that's a later step.
# MAGIC
# MAGIC **Pattern established here**
# MAGIC
# MAGIC Five more agent tools will follow this same pattern:
# MAGIC `get_weather_forecast`, `get_active_hazards`, `get_climate_projection`,
# MAGIC `query_nema_guidelines`, `generate_grant_pdf`.
# MAGIC
# MAGIC **Compute:** Serverless. No `%pip install` needed.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration

# COMMAND ----------

CATALOG       = "eco_resilience"
SILVER_SCHEMA = "silver"
FUNCTION_NAME = "verify_abn"
FQN           = f"{CATALOG}.{SILVER_SCHEMA}.{FUNCTION_NAME}"

SECRET_SCOPE  = "eco_resilience"
SECRET_KEY    = "abr_auth_guid"

print(f"Will register: {FQN}")
print(f"Secret used:   {SECRET_SCOPE}/{SECRET_KEY}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Pre-flight — confirm dependencies exist
# MAGIC
# MAGIC The UC function reads three Silver tables and one secret. If any are
# MAGIC missing, the function will register but fail at runtime — surface that now.

# COMMAND ----------

required_tables = [
    f"{CATALOG}.{SILVER_SCHEMA}.poa_h3_lookup",
    f"{CATALOG}.{SILVER_SCHEMA}.poa_to_weather_location",
    f"{CATALOG}.{SILVER_SCHEMA}.poa_to_csiro_station",
]
for t in required_tables:
    cnt = spark.table(t).count()
    print(f"  ✅ {t}  ({cnt:,} rows)")

guid_check = dbutils.secrets.get(scope=SECRET_SCOPE, key=SECRET_KEY)
assert guid_check, f"Secret not found at {SECRET_SCOPE}/{SECRET_KEY}"
print(f"  ✅ ABR GUID loaded ({len(guid_check)} chars)")
del guid_check   # keep it out of notebook memory

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Tool description — the LLM-facing COMMENT
# MAGIC
# MAGIC This text becomes the agent's tool description. The LLM reads it to
# MAGIC decide when to call the function and how to interpret its output.
# MAGIC Worth tuning carefully — quality directly affects agent behaviour.
# MAGIC
# MAGIC **Style rules I'm following:**
# MAGIC - Lead with what it does, not how.
# MAGIC - Be explicit about ordering ("ALWAYS call this FIRST...") — agents follow this.
# MAGIC - State what's in the return and when fields are null.
# MAGIC - No single quotes / apostrophes (would need escaping in SQL literals).

# COMMAND ----------

TOOL_DESCRIPTION = (
    "Verifies an Australian Business Number against the official Australian "
    "Business Register and returns business identity (name, status, type, "
    "state, postcode) plus spatial join keys (H3 cells, nearest seeded "
    "weather location, nearest CSIRO climate station) for downstream tools. "
    "ALWAYS call this FIRST in a conversation when the user provides an ABN "
    "because its output is the input to every other tool. Returns the error "
    "field populated and other fields null when the ABN is malformed or not "
    "registered."
)

ARG_COMMENT = "An Australian Business Number — 11 digits, spaces tolerated"

print(TOOL_DESCRIPTION)
print()
print(f"Length: {len(TOOL_DESCRIPTION)} chars")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Register the function(s)
# MAGIC
# MAGIC **Architecture note — discovered after first attempt.**
# MAGIC
# MAGIC UC Python scalar UDFs cannot call `spark.sql(...)` or `dbutils.secrets.get(...)`
# MAGIC from inside the function body — the body runs in an isolated sandbox without
# MAGIC SparkSession or `dbutils` access. (Pre-flight checks in section 2 run on the
# MAGIC driver, where both are available, so they don't predict UDF-runtime behaviour.)
# MAGIC The first version of this notebook tried to do both inside one Python UDF
# MAGIC and returned NULL for every call because the exceptions were silently swallowed
# MAGIC by the UC sandbox.
# MAGIC
# MAGIC We therefore split the work into **two functions that compose at the SQL level**:
# MAGIC
# MAGIC | Function | Language | What it does |
# MAGIC |---|---|---|
# MAGIC | `silver.abr_fetch_identity(abn, guid)` | Python | Pure HTTP call to ABR + JSONP parse. No `spark`, no `dbutils`. |
# MAGIC | `silver.verify_abn(abn)` | SQL | Calls `abr_fetch_identity` with `secret(...)`, joins to the three Silver lookups, returns the full STRUCT. |
# MAGIC
# MAGIC The agent only sees `silver.verify_abn` — same external contract as planned.
# MAGIC Internally it's now SQL-composed.
# MAGIC
# MAGIC The Databricks SQL `secret('scope', 'key')` function safely supplies the
# MAGIC GUID from Databricks Secrets into the Python UDF as a regular argument.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4a — Python primitive: `abr_fetch_identity`
# MAGIC
# MAGIC Pure HTTP. No Spark, no `dbutils`. The internal-only COMMENT discourages
# MAGIC the agent from calling it directly — agents should call `verify_abn`.

# COMMAND ----------

PRIMITIVE_COMMENT = (
    "Internal primitive — fetches business identity from the ABR JSON endpoint. "
    "Use silver.verify_abn() from the agent; this function does not include the "
    "spatial join keys the agent needs."
)

PRIMITIVE_BODY = """
import json
import requests

def _parse_abr_body(body):
    body = body.strip()
    if body.startswith("{"):
        return json.loads(body)
    first = body.find("{")
    last  = body.rfind("}")
    if first == -1 or last == -1 or last < first:
        raise ValueError(f"No JSON object in ABR response: {body[:120]!r}")
    return json.loads(body[first:last+1])

# Result template keyed to the declared STRUCT
empty = {
    "abn":         abn,
    "entity_name": None,
    "abn_status":  None,
    "entity_type": None,
    "state":       None,
    "postcode":    None,
    "error":       None,
}

abn_clean = (abn or "").strip().replace(" ", "")
if not (abn_clean.isdigit() and len(abn_clean) == 11):
    empty["error"] = f"Invalid ABN format: '{abn}' must be 11 digits"
    return empty

try:
    r = requests.get(
        "https://abr.business.gov.au/json/AbnDetails.aspx",
        params={"abn": abn_clean, "guid": guid, "callback": ""},
        timeout=15,
    )
    r.raise_for_status()
    record = _parse_abr_body(r.text)
except Exception as e:
    empty["abn"]   = abn_clean
    empty["error"] = f"ABR API call failed: {type(e).__name__}: {e}"
    return empty

if not record.get("Abn"):
    empty["abn"]   = abn_clean
    empty["error"] = record.get("Message", f"ABN {abn_clean} not found")
    return empty

return {
    "abn":         record.get("Abn"),
    "entity_name": record.get("EntityName"),
    "abn_status":  record.get("AbnStatus"),
    "entity_type": record.get("EntityTypeName"),
    "state":       record.get("AddressState"),
    "postcode":    record.get("AddressPostcode"),
    "error":       None,
}
"""

## create UC function (python)
create_primitive_sql = f"""
CREATE OR REPLACE FUNCTION {CATALOG}.{SILVER_SCHEMA}.abr_fetch_identity(
  abn  STRING COMMENT '{ARG_COMMENT}',
  guid STRING COMMENT 'ABR Authentication GUID — typically supplied via secret()'
)
RETURNS STRUCT<
  abn         STRING,
  entity_name STRING,
  abn_status  STRING,
  entity_type STRING,
  state       STRING,
  postcode    STRING,
  error       STRING
>
LANGUAGE PYTHON
COMMENT '{PRIMITIVE_COMMENT}'
AS $$
{PRIMITIVE_BODY}
$$
"""

spark.sql(create_primitive_sql)
print(f"✅ Registered {CATALOG}.{SILVER_SCHEMA}.abr_fetch_identity (Python primitive)")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP FUNCTION IF EXISTS eco_resilience.silver.verify_abn;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4b — SQL wrapper: `verify_abn`
# MAGIC
# MAGIC Composes the Python primitive's output (business identity) with three
# MAGIC scalar subqueries against our Silver lookup tables. This is the function
# MAGIC the agent calls. Its `COMMENT ON FUNCTION` is the LLM's tool description.

# COMMAND ----------

create_wrapper_sql = f"""
CREATE OR REPLACE FUNCTION {FQN}(
  abn STRING COMMENT '{ARG_COMMENT}'
)
RETURNS STRUCT<
  abn                       STRING,
  entity_name               STRING,
  abn_status                STRING,
  entity_type               STRING,
  state                     STRING,
  postcode                  STRING,
  in_nsw                    BOOLEAN,
  h3_cells                  ARRAY<BIGINT>,
  nearest_weather_location  STRING,
  nearest_csiro_station     STRING,
  error                     STRING
>
COMMENT '{TOOL_DESCRIPTION}'
RETURN (
  WITH
  identity AS (
    SELECT {CATALOG}.{SILVER_SCHEMA}.abr_fetch_identity(
             abn,
             secret('{SECRET_SCOPE}', '{SECRET_KEY}')
           ) AS r
  ),
  enriched AS (
    SELECT
      i.r AS r,
      (SELECT collect_list(h3_cell)
       FROM   {CATALOG}.{SILVER_SCHEMA}.poa_h3_lookup
       WHERE  poa_code = i.r.postcode) AS h3_cells,
      (SELECT MAX(nearest_weather_location)
       FROM   {CATALOG}.{SILVER_SCHEMA}.poa_to_weather_location
       WHERE  poa_code = i.r.postcode) AS nwl,
      (SELECT MAX(nearest_csiro_station)
       FROM   {CATALOG}.{SILVER_SCHEMA}.poa_to_csiro_station
       WHERE  poa_code = i.r.postcode) AS ncs
    FROM identity i
  )
  SELECT named_struct(
    'abn',                       r.abn,
    'entity_name',               r.entity_name,
    'abn_status',                r.abn_status,
    'entity_type',               r.entity_type,
    'state',                     r.state,
    'postcode',                  r.postcode,
    'in_nsw',                    h3_cells IS NOT NULL AND size(h3_cells) > 0,
    'h3_cells',                  COALESCE(h3_cells, array()),
    'nearest_weather_location',  nwl,
    'nearest_csiro_station',     ncs,
    'error',                     r.error
  )
  FROM enriched
)
"""

spark.sql(create_wrapper_sql)
print(f"✅ Registered {FQN} (SQL wrapper)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Discoverability check
# MAGIC
# MAGIC This is the surface the Mosaic AI Agent Framework introspects when it
# MAGIC discovers tools. The comment field shown below is what the LLM reads.

# COMMAND ----------

display(spark.sql(f"DESCRIBE FUNCTION EXTENDED {FQN}"))

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG eco_resilience

# COMMAND ----------

display(spark.sql(f"SHOW USER FUNCTIONS IN {CATALOG}.{SILVER_SCHEMA}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Smoke tests via SQL
# MAGIC
# MAGIC Same three personas as notebook 05, now called through the registered
# MAGIC UC function instead of the plain Python helper. The agent will call it
# MAGIC this way — through SQL, not through Python.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6a — Australia Post (VIC, non-NSW path)
# MAGIC
# MAGIC Expected: real `entity_name`, `state='VIC'`, `in_nsw=false`, empty `h3_cells`, null spatial fields, null error.

# COMMAND ----------

display(spark.sql(f"SELECT result.* FROM (SELECT {FQN}('51824753556') AS result)"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6b — Bathurst Regional Council (NSW, full spatial chain)
# MAGIC
# MAGIC Expected: `state='NSW'`, `postcode='2795'`, `in_nsw=true`, ~850 `h3_cells`,
# MAGIC `nearest_weather_location='Bathurst'`,
# MAGIC `nearest_csiro_station='BATHURST-AGRICULTURAL-STATION'`, null error.
# MAGIC
# MAGIC This is the green-light test that proves the full Bronze→Silver→UC-function
# MAGIC chain works inside the UC sandbox.

# COMMAND ----------

display(spark.sql(f"SELECT result.* from (select {FQN}('42173522302') as result)"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6c — Graceful error paths
# MAGIC
# MAGIC Three input shapes the agent will see in the wild:

# COMMAND ----------

display(spark.sql(f"SELECT result.* from (select {FQN}('not an abn') as result)"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Done
# MAGIC
# MAGIC | What we built | Why it matters |
# MAGIC |---|---|
# MAGIC | `eco_resilience.silver.abr_fetch_identity` Python UDF | Pure HTTP primitive — works inside the UC sandbox (no `spark`, no `dbutils`) |
# MAGIC | `eco_resilience.silver.verify_abn` SQL UDF | Composes the primitive with `secret()` + three Silver-lookup subqueries. This is what the agent sees. |
# MAGIC | Strong `COMMENT ON FUNCTION` on `verify_abn` | Becomes the LLM's tool description — drives correct tool selection |
# MAGIC | STRUCT return type | Agent gets type-safe outputs, can chain into other tools |
# MAGIC | Smoke tests via SQL | Validates the SQL wrapper produces exactly what the Python helper in notebook 05 produces |
# MAGIC
# MAGIC **Pattern lesson for future UC tools:** when an agent tool needs both an
# MAGIC external API call AND Delta-table joins, write a Python UDF for the API
# MAGIC part (no `spark`/`dbutils` inside) and a SQL UDF wrapper that composes it
# MAGIC with the table joins. Use `secret()` to pass credentials from Databricks
# MAGIC Secrets into the Python UDF as an argument.
# MAGIC
# MAGIC ### What's next (Phase 4)
# MAGIC
# MAGIC - **Step 2 — Build a minimal Mosaic AI agent** that uses just this one tool.
# MAGIC   First time you'll see the agent actually talk back. The agent's prompt
# MAGIC   will be something like: *"You verify ABNs and report business identity
# MAGIC   to the user."* Tool registration → endpoint → quick chat test.
# MAGIC - **Step 3 — Add the remaining UC tool functions** following this same
# MAGIC   pattern: `get_weather_forecast`, `get_active_hazards`,
# MAGIC   `get_climate_projection`, `query_nema_guidelines`, `generate_grant_pdf`.
# MAGIC - **Step 4 — MLflow tracing** to see the agent's reasoning trace per call.
# MAGIC - **Step 5 — AI Judge evaluation harness** with a small "golden dataset"
# MAGIC   of test conversations.