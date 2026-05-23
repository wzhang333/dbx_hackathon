# Databricks notebook source
# MAGIC %md
# MAGIC # 05 — ABR Business Runtime Tool
# MAGIC
# MAGIC **What this notebook does**
# MAGIC
# MAGIC Builds `verify_abn(abn)` — a Python function the agent calls per-query
# MAGIC at runtime to convert an Australian Business Number into a fully-spatially-
# MAGIC joined business record. This is the **bridge** between user identity and
# MAGIC the spatial backbone we built in notebooks 01–04.
# MAGIC
# MAGIC **Why this is fundamentally different from 01–04**
# MAGIC
# MAGIC 01–04 were ingestion notebooks. **05 is a runtime tool.** No Bronze, no
# MAGIC Silver — ABR data isn't pre-loaded because:
# MAGIC - 30M+ Australian businesses → bulk pre-load is impractical
# MAGIC - Data changes daily → caching would be stale
# MAGIC - License prohibits bulk re-publishing
# MAGIC - Per-query is the right pattern: pay per call, get fresh data
# MAGIC
# MAGIC **Output of `verify_abn(abn)`**
# MAGIC
# MAGIC ```
# MAGIC {
# MAGIC   abn:                       "42173522302",
# MAGIC   entity_name:               "BATHURST REGIONAL COUNCIL",
# MAGIC   abn_status:                "Active",
# MAGIC   entity_type:               "Local Government Entity",
# MAGIC   state:                     "NSW",
# MAGIC   postcode:                  "2795",
# MAGIC   in_nsw:                    True,
# MAGIC   h3_cells:                  [617707471230091263, ... ~850 cells ...],
# MAGIC   nearest_weather_location:  "Bathurst",
# MAGIC   nearest_csiro_station:     "BATHURST-AGRICULTURAL-STATION"
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC **Compute:** Serverless. `requests` is in the image; no `%pip install`.
# MAGIC
# MAGIC **Phase 4 link:** this stays as a plain Python function for now. We'll
# MAGIC register it as a Unity Catalog Function alongside the other agent tools
# MAGIC (`get_weather_forecast`, `get_active_hazards`, etc.) in Phase 4.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration

# COMMAND ----------

CATALOG       = "eco_resilience"
SILVER_SCHEMA = "silver"

# Secret scope + key holding the ABR Authentication GUID
SECRET_SCOPE = "eco_resilience"
SECRET_KEY   = "abr_auth_guid"

# ABR API
ABR_URL = "https://abr.business.gov.au/json/AbnDetails.aspx"

# Silver lookup tables (built by notebooks 01, 02, 04)
POA_H3_TABLE         = f"{CATALOG}.{SILVER_SCHEMA}.poa_h3_lookup"
POA_WEATHER_TABLE    = f"{CATALOG}.{SILVER_SCHEMA}.poa_to_weather_location"
POA_CSIRO_TABLE      = f"{CATALOG}.{SILVER_SCHEMA}.poa_to_csiro_station"

print(f"POA_H3_TABLE         = {POA_H3_TABLE}")
print(f"POA_WEATHER_TABLE    = {POA_WEATHER_TABLE}")
print(f"POA_CSIRO_TABLE      = {POA_CSIRO_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Read GUID from Databricks Secrets
# MAGIC
# MAGIC Same pattern as notebook 03's TfNSW key. Output is auto-masked by Databricks.

# COMMAND ----------

abr_guid = dbutils.secrets.get(scope=SECRET_SCOPE, key=SECRET_KEY)
assert abr_guid, f"GUID not found at {SECRET_SCOPE}/{SECRET_KEY}"
print(f"✅ GUID loaded (length: {len(abr_guid)} chars)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Quick API ping — does the GUID work?
# MAGIC
# MAGIC Hits Australia Post's well-known ABN as a smoke test. If this returns
# MAGIC `EntityName: AUSTRALIAN POSTAL CORPORATION`, the GUID is good and we
# MAGIC can build the full helpers below.

# COMMAND ----------

import requests, json, time

def _parse_abr_body(body: str) -> dict:
    """Parse an ABR response body, tolerating JSONP wrappers like `callback({...})`
    or `({...})`. Slices between the first `{` and last `}` rather than parsing
    the wrapper syntactically."""
    body = body.strip()
    if body.startswith("{"):
        return json.loads(body)               # plain JSON
    first = body.find("{")
    last  = body.rfind("}")
    if first == -1 or last == -1 or last < first:
        raise ValueError(f"No JSON object found in ABR response: {body[:120]!r}")
    return json.loads(body[first:last+1])

def _ping(abn: str) -> dict:
    """Bare-bones GET — just to verify auth works. Production version is fetch_abr_record below."""
    params = {"abn": abn, "guid": abr_guid, "callback": ""}
    r = requests.get(ABR_URL, params=params, timeout=15)
    r.raise_for_status()
    return _parse_abr_body(r.text)

t0 = time.time()
sample = _ping("51824753556")
elapsed_ms = (time.time() - t0) * 1000

print(f"HTTP 200 | {elapsed_ms:.0f}ms")
print(f"  Abn:        {sample.get('Abn')}")
print(f"  EntityName: {sample.get('EntityName')}")
print(f"  AbnStatus:  {sample.get('AbnStatus')}")
print(f"  State:      {sample.get('AddressState')}")
print(f"  Postcode:   {sample.get('AddressPostcode')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. `fetch_abr_record(abn)` — the API helper
# MAGIC
# MAGIC Defensive against:
# MAGIC - Whitespace / spaces in the ABN string ("42 173 522 302" is common formatting)
# MAGIC - Network timeouts → caller gets `{error: "..."}` not a crash
# MAGIC - Unregistered ABN → ABR returns `Abn=""` not a 404, so check explicitly
# MAGIC - Cancelled ABN → still returns full record but with `AbnStatus="Cancelled"`

# COMMAND ----------

def fetch_abr_record(abn: str) -> dict:
    """One ABR call. Returns the parsed JSON record, or a dict with `error`."""
    abn_clean = abn.strip().replace(" ", "")
    if not (abn_clean.isdigit() and len(abn_clean) == 11):
        return {"error": f"Invalid ABN format: '{abn}' must be 11 digits"}

    try:
        r = requests.get(
            ABR_URL,
            params={"abn": abn_clean, "guid": abr_guid, "callback": ""},
            timeout=15,
        )
        r.raise_for_status()
        record = _parse_abr_body(r.text)
    except requests.RequestException as e:
        return {"error": f"ABR API call failed: {e}"}
    except ValueError as e:
        return {"error": f"ABR response not JSON: {e}"}

    # ABR returns Abn="" + Message="..." for unknown ABNs (not a 404)
    if not record.get("Abn"):
        return {"error": record.get("Message", f"ABN {abn_clean} not found")}

    return record

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. `lookup_spatial_context(postcode)` — joins to our Silver tables
# MAGIC
# MAGIC One Spark SQL query, three left joins. Returns the H3 cells, nearest
# MAGIC seeded weather location, and nearest CSIRO station for any NSW postcode.
# MAGIC Non-NSW postcodes (or postcodes not present in our lookups) come back
# MAGIC as `in_nsw=false` with empty/null spatial fields — graceful, no crash.

# COMMAND ----------

def lookup_spatial_context(postcode: str | None) -> dict:
    """Translate a postcode into the agent's spatial join keys."""
    empty = {
        "in_nsw": False,
        "h3_cells": [],
        "nearest_weather_location": None,
        "nearest_csiro_station": None,
    }

    # Postcode missing or malformed → no spatial context
    if not postcode or not (postcode.isdigit() and len(postcode) == 4):
        return empty

    # Single combined query — returns one row with the array + two scalars
    rows = spark.sql(f"""
        WITH cells AS (
            SELECT collect_list(h3_cell) AS h3_cells
            FROM   {POA_H3_TABLE}
            WHERE  poa_code = '{postcode}'
        ),
        weather AS (
            SELECT nearest_weather_location
            FROM   {POA_WEATHER_TABLE}
            WHERE  poa_code = '{postcode}'
        ),
        csiro AS (
            SELECT nearest_csiro_station
            FROM   {POA_CSIRO_TABLE}
            WHERE  poa_code = '{postcode}'
        )
        SELECT  c.h3_cells,
                w.nearest_weather_location,
                s.nearest_csiro_station
        FROM    cells c
        LEFT JOIN weather w ON true
        LEFT JOIN csiro   s ON true
    """).collect()

    if not rows or not rows[0].h3_cells:
        return empty

    return {
        "in_nsw": True,
        "h3_cells": rows[0].h3_cells,
        "nearest_weather_location": rows[0].nearest_weather_location,
        "nearest_csiro_station":    rows[0].nearest_csiro_station,
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. `verify_abn(abn)` — the public function
# MAGIC
# MAGIC Composes (4) and (5) into the dict the agent will use as its first
# MAGIC tool call in every conversation.

# COMMAND ----------

def verify_abn(abn: str) -> dict:
    """The agent's first tool call. ABN → identity + spatial join keys (h3_cells, nearest_weather_location, nearest_csiro_station)."""
    record = fetch_abr_record(abn)
    if "error" in record:
        return {"abn": abn, **record}

    postcode = record.get("AddressPostcode")
    # input postcode, return a dict including keys of h3_cells, nearest_weather_location, nearest_csiro_station
    spatial  = lookup_spatial_context(postcode)

    return {
        "abn":          record.get("Abn"),
        "entity_name":  record.get("EntityName"),
        "abn_status":   record.get("AbnStatus"),
        "entity_type":  record.get("EntityTypeName"),
        "state":        record.get("AddressState"),
        "postcode":     postcode,
        **spatial,
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Smoke tests
# MAGIC
# MAGIC Three personas to exercise the full code path.

# COMMAND ----------

def pretty(result: dict) -> None:
    """Trim the h3_cells array for readable output."""
    out = dict(result)
    cells = out.get("h3_cells", [])
    if cells and len(cells) > 5:
        out["h3_cells"] = f"<{len(cells)} cells, first 3: {cells[:3]}>"
    print(json.dumps(out, indent=2, default=str))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7a — Australia Post (VIC) — non-NSW path
# MAGIC
# MAGIC Expected: real `entity_name`, `state="VIC"`, `in_nsw=false`, no spatial joins.

# COMMAND ----------

t0 = time.time()
result_aus_post = verify_abn("51824753556")
print(f"verify_abn() round-trip: {(time.time()-t0)*1000:.0f}ms\n")
pretty(result_aus_post)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7b — Bathurst Regional Council (NSW, postcode 2795) — full spatial chain
# MAGIC
# MAGIC Expected: `state="NSW"`, `in_nsw=true`, ~850 H3 cells,
# MAGIC `nearest_weather_location="Bathurst"`,
# MAGIC `nearest_csiro_station="BATHURST-AGRICULTURAL-STATION"`.
# MAGIC
# MAGIC This is the smoke test that proves the data layer is end-to-end correct.

# COMMAND ----------

t0 = time.time()
result_bathurst = verify_abn("42173522302")
print(f"verify_abn() round-trip: {(time.time()-t0)*1000:.0f}ms\n")
pretty(result_bathurst)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7c — Malformed / unknown ABN — graceful error path
# MAGIC
# MAGIC Expected: returns a dict with `error: ...`, doesn't crash. The agent will
# MAGIC see this and surface a friendly "we couldn't find that ABN" message.

# COMMAND ----------

print("--- Malformed ABN ---")
pretty(verify_abn("not-an-abn"))
print("\n--- 11-digit but not registered ---")
pretty(verify_abn("12345678901"))
print("\n--- Spaces in input (real-world copy/paste) ---")
pretty(verify_abn("42 173 522 302"))   # should still work — same as 7b

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Done
# MAGIC
# MAGIC | What we built | Pattern |
# MAGIC |---|---|
# MAGIC | `fetch_abr_record(abn)` | HTTP helper, defensive parsing |
# MAGIC | `lookup_spatial_context(postcode)` | Single Spark SQL with three left joins |
# MAGIC | `verify_abn(abn)` | Public function — the agent's first tool call |
# MAGIC
# MAGIC ### What's next
# MAGIC
# MAGIC - **Phase 3 — Gold layer + Genie prep:**
# MAGIC   - Build `gold.business_risk_profile` view that the agent can query in
# MAGIC     one SQL call after `verify_abn` returns.
# MAGIC   - Add `COMMENT ON COLUMN` everywhere so Genie's NL→SQL works well.
# MAGIC   - Curate a Genie Space with 5–10 example analyst questions.
# MAGIC - **Phase 4 — Mosaic AI Agent build:**
# MAGIC   - Promote `verify_abn` to a UC function (`CREATE OR REPLACE FUNCTION ... LANGUAGE PYTHON`).
# MAGIC   - Register the other tools alongside it: `get_weather_forecast`,
# MAGIC     `get_active_hazards`, `get_climate_projection`, `query_nema_guidelines`,
# MAGIC     `generate_grant_pdf`.
# MAGIC   - Wire all six into the Mosaic AI Agent Framework with multi-step reasoning.
# MAGIC - **Phase 5 — Streamlit Lakehouse App + Genie Space.**