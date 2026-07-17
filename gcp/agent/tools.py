"""EcoResilience agent tools — GCP-native port of the 7 Databricks UC functions.

Original (Databricks)                          →  This module (GCP)
─────────────────────────────────────────────────────────────────────────
verify_abn            (Python tool + secrets)  →  verify_abn            (Secret Manager)
silver.get_weather_forecast   (SQL UDF)        →  get_weather_forecast  (BigQuery)
silver.get_active_hazards     (SQL UDF)        →  get_active_hazards    (BigQuery)
silver.get_climate_projection (SQL UDF)        →  get_climate_projection(BigQuery)
silver.query_nema_guidelines  (vector_search)  →  query_nema_guidelines (BQ VECTOR_SEARCH
                                                   + Vertex AI embeddings)
silver.get_industry_context   (SQL UDF)        →  get_industry_context  (BigQuery)
silver.generate_grant_pdf     (SQL UDF)        →  generate_grant_pdf    (pure Python)

Each tool returns a JSON-serialisable dict with the SAME field names the
UC STRUCTs used, so the system prompt (unchanged from eco_agent.py) keeps
steering the model correctly.
"""

import json
import uuid
from datetime import datetime, timezone

import requests
from google.cloud import bigquery
from langchain_core.tools import tool

from . import config

_bq = None


def bq() -> bigquery.Client:
    global _bq
    if _bq is None:
        _bq = bigquery.Client(project=config.PROJECT_ID, location=config.BQ_LOCATION)
    return _bq


def _rows(sql: str, params: list) -> list[dict]:
    job = bq().query(sql, job_config=bigquery.QueryJobConfig(query_parameters=params))
    return [dict(r) for r in job.result()]


def _json_safe(obj):
    """Recursively convert datetimes so json.dumps never chokes on BQ rows."""
    if isinstance(obj, dict):
        return {k: _json_safe(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_json_safe(v) for v in obj]
    if isinstance(obj, datetime):
        return obj.isoformat()
    return obj


# ───────────────────────────────────────────────────────────────────────────
# Tool 1: verify_abn — live ABR API lookup (Secret Manager for the GUID)
# ───────────────────────────────────────────────────────────────────────────
ABR_URL = "https://abr.business.gov.au/json/AbnDetails.aspx"


def _parse_abr_body(body: str) -> dict:
    """Tolerates JSONP wrappers — slices between first '{' and last '}'."""
    body = body.strip()
    if body.startswith("{"):
        return json.loads(body)
    first, last = body.find("{"), body.rfind("}")
    if first == -1 or last == -1 or last < first:
        raise ValueError(f"No JSON object in ABR response: {body[:120]!r}")
    return json.loads(body[first : last + 1])


@tool
def verify_abn(abn: str) -> dict:
    """Verifies an Australian Business Number against the official Australian
    Business Register and returns business identity (name, status, type, state,
    postcode, in_nsw flag). ALWAYS call this FIRST in a conversation when the
    user provides an ABN because its postcode output is the input to tools 2-4.
    Returns the error field populated and other fields null when the ABN is
    malformed or not registered."""
    try:
        guid = config.get_secret(config.ABR_GUID_SECRET)
    except Exception as e:
        return {"abn": abn, "error": f"Could not load ABR credential: {e}"}

    abn_clean = (abn or "").strip().replace(" ", "")
    if not (abn_clean.isdigit() and len(abn_clean) == 11):
        return {"abn": abn, "error": "Invalid ABN format: must be 11 digits"}

    try:
        r = requests.get(
            ABR_URL,
            params={"abn": abn_clean, "guid": guid, "callback": ""},
            timeout=15,
        )
        r.raise_for_status()
        record = _parse_abr_body(r.text)
    except Exception as e:
        return {"abn": abn_clean, "error": f"ABR API call failed: {type(e).__name__}: {e}"}

    if not record.get("Abn"):
        return {"abn": abn_clean, "error": record.get("Message", f"ABN {abn_clean} not found")}

    state = record.get("AddressState")
    return {
        "abn": record.get("Abn"),
        "entity_name": record.get("EntityName"),
        "abn_status": record.get("AbnStatus"),
        "entity_type": record.get("EntityTypeName"),
        "state": state,
        "postcode": record.get("AddressPostcode"),
        "in_nsw": state == "NSW",
        "error": None,
    }


# ───────────────────────────────────────────────────────────────────────────
# Tool 2: get_weather_forecast — next 12h + 24h summary at nearest station
# ───────────────────────────────────────────────────────────────────────────
@tool
def get_weather_forecast(postcode: str) -> dict:
    """Returns weather forecast for the next 12 hours at the seeded station
    nearest the user NSW postcode, plus 24-hour summary statistics for
    rainfall and wind speed. Use this when the user asks about current
    weather, upcoming conditions, todays forecast, rain expected, wind,
    or temperature near their business. Argument is a 4-digit NSW postcode
    (typically the postcode field returned by verify_abn)."""
    postcode = (postcode or "").strip()
    sql = f"""
        WITH loc AS (
          SELECT nearest_weather_location
          FROM `{config.bq_table(config.SILVER, 'poa_to_weather_location')}`
          WHERE poa_code = @postcode
        ),
        hourly AS (
          SELECT w.forecast_time,
                 w.temperature_c    AS temp_c,
                 w.precipitation_mm AS rain_mm,
                 w.windspeed_kmh    AS wind_kmh
          FROM `{config.bq_table(config.SILVER, 'weather_current')}` w
          JOIN loc ON w.location_name = loc.nearest_weather_location
          WHERE w.forecast_time >= CURRENT_TIMESTAMP()
            AND w.forecast_time <  TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
        )
        SELECT (SELECT nearest_weather_location FROM loc) AS forecast_location,
               ARRAY(SELECT AS STRUCT forecast_time, temp_c, rain_mm, wind_kmh
                     FROM hourly ORDER BY forecast_time LIMIT 12) AS next_12h,
               (SELECT MAX(rain_mm) FROM hourly) AS max_rain_24h_mm,
               (SELECT MAX(wind_kmh) FROM hourly) AS max_wind_24h_kmh
    """
    rows = _rows(sql, [bigquery.ScalarQueryParameter("postcode", "STRING", postcode)])
    r = rows[0] if rows else {}
    if not r.get("forecast_location"):
        return {"poa_code": postcode, "next_12h": [], "error": f"Postcode {postcode} not found in NSW weather lookup"}
    return _json_safe({
        "poa_code": postcode,
        "forecast_location": r["forecast_location"],
        "next_12h": [dict(x) for x in r["next_12h"]],
        "max_rain_24h_mm": r["max_rain_24h_mm"],
        "max_wind_24h_kmh": r["max_wind_24h_kmh"],
        "error": None,
    })


# ───────────────────────────────────────────────────────────────────────────
# Tool 3: get_active_hazards — live TfNSW hazards inside the postcode boundary
# ───────────────────────────────────────────────────────────────────────────
@tool
def get_active_hazards(postcode: str) -> dict:
    """Returns currently-active TfNSW road hazards (incidents, floods, fires,
    roadworks) within the user NSW postcode boundary. Use this when the
    user asks about disruptions, road closures, fires, floods, blocked
    roads, active emergencies, or anything affecting transportation right
    now near their business. Argument is a 4-digit NSW postcode."""
    postcode = (postcode or "").strip()
    exists_sql = f"""
        SELECT COUNT(*) AS n FROM `{config.bq_table(config.SILVER, 'poa_h3_lookup')}`
        WHERE poa_code = @postcode
    """
    p = [bigquery.ScalarQueryParameter("postcode", "STRING", postcode)]
    if _rows(exists_sql, p)[0]["n"] == 0:
        return {"poa_code": postcode, "hazard_count": 0, "hazards": [],
                "error": f"Postcode {postcode} not found in NSW H3 lookup"}

    sql = f"""
        SELECT h.hazard_type, h.main_category, h.display_name, h.advice_a,
               h.expected_delay_min, h.impacting_network, h.last_updated_ts
        FROM `{config.bq_table(config.SILVER, 'hazards_current')}` h
        JOIN `{config.bq_table(config.SILVER, 'poa_h3_lookup')}` c
          ON h.h3_cell = c.h3_cell
        WHERE c.poa_code = @postcode
        ORDER BY h.impacting_network DESC, h.last_updated_ts DESC
        LIMIT 20
    """
    hazards = _rows(sql, p)
    count_sql = f"""
        SELECT COUNT(*) AS n
        FROM `{config.bq_table(config.SILVER, 'hazards_current')}` h
        JOIN `{config.bq_table(config.SILVER, 'poa_h3_lookup')}` c
          ON h.h3_cell = c.h3_cell
        WHERE c.poa_code = @postcode
    """
    total = _rows(count_sql, p)[0]["n"]
    return _json_safe({"poa_code": postcode, "hazard_count": int(total),
                       "hazards": hazards, "error": None})


# ───────────────────────────────────────────────────────────────────────────
# Tool 4: get_climate_projection — CSIRO 2020s vs 2080s medians
# ───────────────────────────────────────────────────────────────────────────
@tool
def get_climate_projection(postcode: str) -> dict:
    """Returns long-term climate projections for the user NSW postcode —
    median annual mean temperature in the 2020s vs the 2080s, computed
    across 8 global climate models under moderate (rcp45) and high (rcp85)
    emissions scenarios. Use this when the user asks about long-term
    climate trends, future warming, strategic planning for 2030 or 2050
    or 2080, or whether the climate is changing in their region. Argument
    is a 4-digit NSW postcode."""
    postcode = (postcode or "").strip()
    sql = f"""
        WITH st AS (
          SELECT nearest_csiro_station
          FROM `{config.bq_table(config.SILVER, 'poa_to_csiro_station')}`
          WHERE poa_code = @postcode
        ),
        filtered AS (
          SELECT c.rcp, c.period, c.value, c.model
          FROM `{config.bq_table(config.SILVER, 'csiro_projections')}` c
          JOIN st ON c.station_name = st.nearest_csiro_station
          WHERE c.variable = 'tas' AND c.time_aggregation = 'Annual'
            AND c.rcp IN ('rcp45', 'rcp85')
            AND c.period IN ('2020-2039', '2080-2099')
        ),
        medians AS (
          SELECT rcp, period,
                 APPROX_QUANTILES(value, 2)[OFFSET(1)] AS median_temp,
                 COUNT(DISTINCT model) AS n_models
          FROM filtered GROUP BY rcp, period
        )
        SELECT (SELECT nearest_csiro_station FROM st) AS station_name,
               MAX(IF(rcp='rcp45' AND period='2020-2039', median_temp, NULL)) AS current_2020s,
               MAX(IF(rcp='rcp45' AND period='2080-2099', median_temp, NULL)) AS future_2080s,
               MAX(IF(rcp='rcp85' AND period='2080-2099', median_temp, NULL)) AS worst_2080s,
               MAX(n_models) AS n_models
        FROM medians
    """
    rows = _rows(sql, [bigquery.ScalarQueryParameter("postcode", "STRING", postcode)])
    r = rows[0] if rows else {}
    if not r.get("station_name"):
        return {"poa_code": postcode, "error": f"Postcode {postcode} not found in NSW CSIRO lookup"}
    cur, fut = r.get("current_2020s"), r.get("future_2080s")
    return {
        "poa_code": postcode,
        "station_name": r["station_name"],
        "current_2020s_temp_c": round(cur, 2) if cur is not None else None,
        "future_2080s_temp_c": round(fut, 2) if fut is not None else None,
        "warming_delta_c": round(fut - cur, 2) if cur is not None and fut is not None else None,
        "worst_case_2080s_rcp85_temp_c": round(r["worst_2080s"], 2) if r.get("worst_2080s") is not None else None,
        "models_n": int(r["n_models"]) if r.get("n_models") is not None else None,
        "error": None,
    }


# ───────────────────────────────────────────────────────────────────────────
# Tool 5: query_nema_guidelines — RAG over DRFA PDFs
#   Databricks vector_search() → Vertex AI embeddings + BigQuery VECTOR_SEARCH
# ───────────────────────────────────────────────────────────────────────────
def _embed(texts: list[str]) -> list[list[float]]:
    import vertexai
    from vertexai.language_models import TextEmbeddingInput, TextEmbeddingModel

    vertexai.init(project=config.PROJECT_ID, location=config.VERTEX_LOCATION)
    model = TextEmbeddingModel.from_pretrained(config.EMBEDDING_MODEL)
    inputs = [TextEmbeddingInput(t, task_type="RETRIEVAL_QUERY") for t in texts]
    return [e.values for e in model.get_embeddings(inputs)]


@tool
def query_nema_guidelines(question: str) -> dict:
    """Searches the official NEMA Disaster Recovery Funding Arrangements (DRFA)
    documents using semantic similarity, and returns the top 5 most relevant
    text chunks for a natural-language question. Use this whenever the user
    asks about disaster recovery grant eligibility, application requirements,
    Category A/B/C/D assistance, what costs are claimable, evidence
    requirements, or any specific DRFA rule. The tool returns the chunk text,
    the source PDF filename, the page number it came from, and a similarity
    score. ALWAYS cite the source PDF filename and page number when
    summarising the results. NEVER invent rules — only state what the
    retrieved chunks explicitly say. The argument is a natural-language
    question; no postcode needed."""
    try:
        qvec = _embed([question])[0]
    except Exception as e:
        return {"question": question, "chunks_count": 0, "chunks": [],
                "error": f"Embedding failed: {type(e).__name__}: {e}"}

    sql = f"""
        SELECT base.chunk_to_retrieve AS text,
               base.source_pdf,
               base.page_number,
               1 - distance AS similarity_score
        FROM VECTOR_SEARCH(
          TABLE `{config.bq_table(config.BRONZE, 'drfa_chunks')}`,
          'embedding',
          (SELECT @qvec AS embedding),
          top_k => 5,
          distance_type => 'COSINE'
        )
        ORDER BY similarity_score DESC
    """
    try:
        chunks = _rows(sql, [bigquery.ArrayQueryParameter("qvec", "FLOAT64", qvec)])
    except Exception as e:
        return {"question": question, "chunks_count": 0, "chunks": [],
                "error": f"Vector search failed: {type(e).__name__}: {e}"}
    for c in chunks:
        if c.get("similarity_score") is not None:
            c["similarity_score"] = round(float(c["similarity_score"]), 4)
    return {"question": question, "chunks_count": len(chunks), "chunks": chunks, "error": None}


# ───────────────────────────────────────────────────────────────────────────
# Tool 6: get_industry_context — ABS sector totals + derived ratios
# ───────────────────────────────────────────────────────────────────────────
@tool
def get_industry_context(code: str) -> dict:
    """Returns Australian Bureau of Statistics industry context for a 2-digit
    ANZSIC Subdivision code: industry-wide totals (employment, total income,
    industry value added, operating profit, wages) plus derived sector
    ratios (revenue per employee, wages share of income, EBITDA margin,
    value-added intensity). Use this whenever the user mentions an industry
    (farming, hospitality, retail, construction, manufacturing, etc.).
    The argument is a 2-digit ANZSIC Subdivision code as a STRING with
    leading zeros preserved (e.g. 01 Agriculture, 45 Food and Beverage
    Services, 43 Retail Trade, 11 Food Product Manufacturing). This dataflow
    publishes industry-wide totals, NOT per-business norms. Employment
    values are in thousands; monetary values are in AUD millions."""
    code_clean = (code or "").strip()
    sql = f"""
        SELECT anzsic_code, anzsic_name, reference_year,
               num_employees_thousand, total_income_aud_m,
               industry_value_added_aud_m, operating_profit_aud_m,
               ebitda_aud_m, wages_salaries_aud_m
        FROM `{config.bq_table(config.SILVER, 'industry_context')}`
        WHERE anzsic_code = @code
        LIMIT 1
    """
    rows = _rows(sql, [bigquery.ScalarQueryParameter("code", "STRING", code_clean)])
    if not rows:
        return {"anzsic_code": code_clean,
                "error": f"ANZSIC code {code_clean} not found in our industry data. "
                         "Use a 2-digit Subdivision code like 01, 45, 43."}
    r = rows[0]
    emp, inc = r["num_employees_thousand"], r["total_income_aud_m"]
    wages, ebitda, iva = r["wages_salaries_aud_m"], r["ebitda_aud_m"], r["industry_value_added_aud_m"]
    return {
        "anzsic_code": r["anzsic_code"],
        "anzsic_name": r["anzsic_name"],
        "reference_year": r["reference_year"],
        "num_employees_thousand": emp,
        "total_income_aud_m": inc,
        "industry_value_added_aud_m": iva,
        "operating_profit_aud_m": r["operating_profit_aud_m"],
        "wages_salaries_aud_m": wages,
        "revenue_per_employee_aud": int(inc * 1000.0 / emp) if emp and inc else None,
        "wages_share_of_income_pct": round(wages / inc * 100.0, 2) if inc and wages is not None else None,
        "ebitda_margin_pct": round(ebitda / inc * 100.0, 2) if inc and ebitda is not None else None,
        "value_added_intensity_pct": round(iva / inc * 100.0, 2) if inc and iva is not None else None,
        "error": None,
    }


# ───────────────────────────────────────────────────────────────────────────
# Tool 7: generate_grant_pdf — structured DRAFT grant application
#   (pure Python port of the SQL UDF's validation + STRUCT assembly;
#    the Flask app renders the actual PDF with fpdf2, unchanged)
# ───────────────────────────────────────────────────────────────────────────
VALID_DISASTERS = {"flood", "fire", "storm", "earthquake", "drought", "cyclone"}
VALID_CATEGORIES = {"A", "B", "C", "D"}

NEXT_STEPS = [
    "Review the draft for accuracy",
    "Attach disaster damage evidence (photos, repair quotes, insurance records)",
    "Confirm the DRFA category cites the right authority",
    "Submit to NEMA via the official portal",
]


def generate_grant_struct(abn, entity_name, entity_state, entity_postcode,
                          disaster_type, disaster_date, drfa_category,
                          estimated_loss_aud, justification) -> dict:
    """Deterministic (non-LLM) grant-draft assembly, callable from Flask too."""
    def _invalid():
        try:
            datetime.strptime(str(disaster_date), "%Y-%m-%d")
            date_ok = True
        except (ValueError, TypeError):
            date_ok = False
        if not abn or not str(abn).strip():
            return True
        if not entity_name or not str(entity_name).strip():
            return True
        if disaster_type not in VALID_DISASTERS:
            return True
        if drfa_category not in VALID_CATEGORIES:
            return True
        if not date_ok:
            return True
        if estimated_loss_aud is None or float(estimated_loss_aud) < 0:
            return True
        if not justification or len(str(justification).strip()) < 10:
            return True
        return False

    status = "INVALID" if _invalid() else "DRAFT"
    return {
        "application_id": str(uuid.uuid4()),
        "draft_timestamp": datetime.now(timezone.utc).isoformat(),
        "applicant": {"abn": abn, "entity_name": entity_name,
                      "state": entity_state, "postcode": entity_postcode},
        "disaster": {"type": disaster_type, "date": disaster_date,
                     "in_nsw": entity_state == "NSW"},
        "grant_request": {"drfa_category": drfa_category,
                          "estimated_loss_aud": float(estimated_loss_aud or 0),
                          "justification": justification},
        "status": status,
        "next_steps": NEXT_STEPS if status == "DRAFT" else [],
        "error": (
            "One or more inputs invalid. Check abn, entity_name, disaster_type "
            "(flood/fire/storm/earthquake/drought/cyclone), disaster_date "
            "(YYYY-MM-DD), drfa_category (A/B/C/D), estimated_loss_aud "
            "(positive number), and justification (at least 10 characters)."
        ) if status == "INVALID" else None,
    }


@tool
def generate_grant_pdf(abn: str, entity_name: str, entity_state: str,
                       entity_postcode: str, disaster_type: str,
                       disaster_date: str, drfa_category: str,
                       estimated_loss_aud: float, justification: str) -> dict:
    """Composes agent reasoning into a structured DRAFT grant application under
    the Australian Disaster Recovery Funding Arrangements (DRFA). Takes the
    applicant identity fields (abn, entity_name, entity_state, entity_postcode)
    directly as args — pass them verbatim from your prior verify_abn tool
    call. Use this AS THE FINAL STEP after you have already called the
    relevant data tools. disaster_type is one of flood/fire/storm/earthquake/
    drought/cyclone; disaster_date is YYYY-MM-DD; drfa_category is A/B/C/D;
    estimated_loss_aud is a positive whole-dollar number; justification is a
    2-4 sentence plain-English narrative. Returns a STRUCT with
    application_id, applicant identity, disaster details, grant request,
    draft status (DRAFT or INVALID), and a next-steps checklist. It does NOT
    produce PDF bytes — the web app renders the PDF. Call at most ONCE per
    conversation."""
    return generate_grant_struct(abn, entity_name, entity_state, entity_postcode,
                                 disaster_type, disaster_date, drfa_category,
                                 estimated_loss_aud, justification)


ALL_TOOLS = [
    verify_abn,
    get_weather_forecast,
    get_active_hazards,
    get_climate_projection,
    query_nema_guidelines,
    get_industry_context,
    generate_grant_pdf,
]
