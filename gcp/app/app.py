"""
EcoResilience AI — Cloud Run backend (GCP-native port of the Databricks App).

Service mapping vs. the original app.py:
  WorkspaceClient + SQL Warehouse   → google.cloud.bigquery (parameterized queries)
  w.jobs.run_now / job polling      → synchronous ABR ingest (abn_ingest.py)
  ACE / agent serving endpoints     → in-process LangGraph agent (agent package)
  Lakebase (Databricks Postgres)    → Cloud SQL for PostgreSQL (IAM auth)
  h3_h3tostring/h3_boundaryaswkt    → `h3` Python library
  silver.generate_grant_pdf SQL UDF → agent.tools.generate_grant_struct (Python)

All API routes keep their original paths and response shapes so index.html
runs unmodified.
"""

import os
import re
import sys
import json
import logging
import time
from datetime import datetime, date
from collections import defaultdict

import requests
from flask import Flask, request, jsonify, send_from_directory, Response
from google.cloud import bigquery

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from agent import config
from agent.agent import build_agent, build_llm, invoke_agent, setup_tracing
from agent.tools import generate_grant_struct
import abn_ingest

# ========== Logging Setup ==========
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger("ecoresilience")

app = Flask(__name__, static_folder=".", static_url_path="")

bq = bigquery.Client(project=config.PROJECT_ID, location=config.BQ_LOCATION)

# Cloud SQL (grant history) — optional, mirrors the Lakebase "silently
# disabled" behaviour when unset.
CLOUD_SQL_CONNECTION = os.environ.get("CLOUD_SQL_CONNECTION_NAME", "").strip()
CLOUD_SQL_DB = os.environ.get("CLOUD_SQL_DB", "eco_resilience")
CLOUD_SQL_USER = os.environ.get("CLOUD_SQL_USER", "").strip()

setup_tracing()

# The LangGraph agent and a plain chat LLM (for ACE conversational replies).
# Built lazily so the container starts fast and health checks pass even if
# Vertex AI briefly hiccups.
_agent = None
_chat_llm = None


def get_agent():
    global _agent
    if _agent is None:
        _agent = build_agent()
    return _agent


def get_chat_llm():
    global _chat_llm
    if _chat_llm is None:
        _chat_llm = build_llm()
    return _chat_llm


logger.info(f"App initialized | project={config.PROJECT_ID} | provider={config.LLM_PROVIDER}")


def run_query(sql: str, params: list | None = None, timeout: float = 50.0) -> list[dict]:
    """Execute a parameterized BigQuery query, return rows as dicts."""
    job_config = bigquery.QueryJobConfig(query_parameters=params or [])
    job = bq.query(sql, job_config=job_config)
    return [dict(r) for r in job.result(timeout=timeout)]


def T(dataset: str, table: str) -> str:
    return f"`{config.bq_table(dataset, table)}`"


def _iso(v):
    return v.isoformat() if isinstance(v, datetime) else v


# ========== Performance Profiling (unchanged) ==========
_perf_log = []
_PERF_LOG_MAX = 200


def _record_perf(entry):
    _perf_log.append(entry)
    if len(_perf_log) > _PERF_LOG_MAX:
        _perf_log.pop(0)


@app.before_request
def _perf_start_timer():
    request._perf_start = time.perf_counter()
    request._perf_checkpoints = []


@app.after_request
def _perf_end_timer(response):
    start = getattr(request, "_perf_start", None)
    if start is None:
        return response
    elapsed_ms = (time.perf_counter() - start) * 1000
    checkpoints = getattr(request, "_perf_checkpoints", [])
    if request.path.startswith("/api/"):
        _record_perf({
            "ts": datetime.utcnow().isoformat() + "Z",
            "method": request.method,
            "endpoint": request.path,
            "status": response.status_code,
            "total_ms": round(elapsed_ms, 1),
            "checkpoints": checkpoints,
        })
        if elapsed_ms > 5000:
            logger.warning(f"[PERF] SLOW {request.method} {request.path} => {elapsed_ms:.0f}ms | checkpoints={checkpoints}")
        elif elapsed_ms > 1000:
            logger.info(f"[PERF] {request.method} {request.path} => {elapsed_ms:.0f}ms | checkpoints={checkpoints}")
    return response


def perf_checkpoint(label):
    start = getattr(request, "_perf_start", None)
    if start is not None and hasattr(request, "_perf_checkpoints"):
        request._perf_checkpoints.append(
            {"label": label, "at_ms": round((time.perf_counter() - start) * 1000, 1)})


# ========== Error handlers ==========
@app.errorhandler(Exception)
def handle_exception(e):
    logger.error(f"[unhandled] {type(e).__name__}: {e}")
    return jsonify({"error": f"Internal server error: {str(e)}"}), 500


@app.errorhandler(404)
def not_found(e):
    return jsonify({"error": "Endpoint not found"}), 404


# ========== Static Files ==========
@app.route("/")
def serve_index():
    response = send_from_directory(".", "index.html")
    response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    return response


@app.route("/api/health", methods=["GET"])
def health():
    return jsonify({"status": "healthy", "project": config.PROJECT_ID,
                    "llm_provider": config.LLM_PROVIDER})


# ========== ABN ingestion (was: Databricks job trigger + polling) ==========

@app.route("/api/ingest-only", methods=["POST"])
def ingest_only():
    """Ingest ABN details synchronously (was: run_now mode='ingest_details')."""
    data = request.get_json() or {}
    abn = str(data.get("abn", "")).replace(" ", "").replace("-", "")
    if not abn:
        return jsonify({"error": "ABN parameter is required"}), 400
    try:
        result = abn_ingest.ingest_abn(bq, abn)
        if result.get("error"):
            return jsonify({"error": result["error"]}), 404
        # run_id kept for frontend compatibility; the work is already done.
        return jsonify({"run_id": 0, "status": "triggered", "mode": "ingest_details"})
    except Exception as e:
        logger.error(f"[ingest-only] Error for ABN={abn}: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/trigger-job", methods=["POST"])
def trigger_job():
    """Full ETL (was: Databricks job). Ingestion + gold refresh run inline;
    'risk_only' mode skips the ABR call and just refreshes gold."""
    data = request.get_json() or {}
    abn = str(data.get("abn", "")).replace(" ", "").replace("-", "")
    mode = data.get("mode", "full")
    if not abn:
        return jsonify({"error": "ABN parameter is required"}), 400
    try:
        perf_checkpoint("pre_ingest")
        if mode != "risk_only":
            result = abn_ingest.ingest_abn(bq, abn)
            if result.get("error"):
                return jsonify({"error": result["error"]}), 404
        perf_checkpoint("post_ingest")
        return jsonify({"run_id": 0, "status": "triggered", "mode": mode})
    except Exception as e:
        logger.error(f"[trigger-job] Error for ABN={abn}: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/job-status/<int:run_id>", methods=["GET"])
def job_status(run_id):
    """Ingestion is synchronous now — always reports success so the existing
    frontend polling loop terminates on its first poll."""
    return jsonify({"life_cycle_state": "TERMINATED", "result_state": "SUCCESS",
                    "state_message": ""})


# ========== Business queries ==========

@app.route("/api/query", methods=["POST"])
def query_table():
    data = request.get_json() or {}
    search_term = data.get("search_term", "")
    search_type = data.get("search_type", "abn")
    if not search_term:
        return jsonify({"error": "search_term is required"}), 400

    if search_type == "abn":
        clean = search_term.replace(" ", "").replace("-", "")
        if not clean.isdigit():
            return jsonify({"error": "ABN must be numeric"}), 400
        sql = f"""
            SELECT s.abn, s.organisation_name, s.status, s.entity_type, s.state, s.postcode
            FROM {T(config.SILVER, 'abn_lookup_structured')} s
            WHERE s.abn = @abn LIMIT 20
        """
        params = [bigquery.ScalarQueryParameter("abn", "INT64", int(clean))]
    else:
        sql = f"""
            SELECT s.abn, s.organisation_name, s.status, s.entity_type, s.state, s.postcode
            FROM {T(config.SILVER, 'abn_lookup_structured')} s
            WHERE LOWER(s.organisation_name) LIKE LOWER(@term) LIMIT 20
        """
        params = [bigquery.ScalarQueryParameter("term", "STRING", f"%{search_term}%")]

    rows = run_query(sql, params)
    data_array = [[str(r["abn"]), r["organisation_name"], r["status"],
                   r["entity_type"], r["state"], str(r["postcode"] or "")] for r in rows]
    return jsonify({"status": "success", "rows": data_array, "row_count": len(data_array)})


@app.route("/api/business-details", methods=["POST"])
def business_details():
    data = request.get_json() or {}
    abn = str(data.get("abn", "")).replace(" ", "").replace("-", "")
    if not abn or not abn.isdigit():
        return jsonify({"error": "Valid ABN required"}), 400

    sql = f"""
        SELECT d.abn, s.organisation_name, s.status, d.entity_type, s.state,
               d.postcode, d.location_name, d.ingested_at
        FROM {T(config.GOLD, 'business_details')} d
        LEFT JOIN {T(config.SILVER, 'abn_lookup_structured')} s ON d.abn = s.abn
        WHERE d.abn = @abn LIMIT 1
    """
    rows = run_query(sql, [bigquery.ScalarQueryParameter("abn", "INT64", int(abn))])
    data_array = [[str(r["abn"]), r["organisation_name"], r["status"], r["entity_type"],
                   r["state"], str(r["postcode"] or ""), r["location_name"],
                   _iso(r["ingested_at"])] for r in rows]
    return jsonify({"status": "success", "rows": data_array, "row_count": len(data_array)})


@app.route("/api/check-freshness", methods=["POST"])
def check_freshness():
    data = request.get_json() or {}
    abn = str(data.get("abn", "")).replace(" ", "").replace("-", "")
    if not abn or not abn.isdigit():
        return jsonify({"error": "Valid ABN required"}), 400

    sql = f"""
        SELECT abn, ingested_at,
               ingested_at > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR) AS is_fresh
        FROM {T(config.GOLD, 'business_details')}
        WHERE abn = @abn LIMIT 1
    """
    rows = run_query(sql, [bigquery.ScalarQueryParameter("abn", "INT64", int(abn))])
    if rows:
        return jsonify({"status": "success", "exists": True,
                        "is_fresh": bool(rows[0]["is_fresh"]),
                        "ingested_at": _iso(rows[0]["ingested_at"])})
    return jsonify({"status": "success", "exists": False, "is_fresh": False, "ingested_at": None})


# ========== Supplier Management ==========

@app.route("/api/supplier/lookup", methods=["POST"])
def supplier_lookup():
    data = request.get_json() or {}
    abn = str(data.get("abn", "")).replace(" ", "").replace("-", "")
    if not abn.isdigit() or len(abn) != 11:
        return jsonify({"error": "A valid 11-digit ABN is required"}), 400

    sql = f"""
        SELECT s.abn, s.organisation_name, s.status, s.entity_type, s.state, s.postcode
        FROM {T(config.SILVER, 'abn_lookup_structured')} s
        WHERE s.abn = @abn LIMIT 1
    """
    rows = run_query(sql, [bigquery.ScalarQueryParameter("abn", "INT64", int(abn))])
    data_array = [[str(r["abn"]), r["organisation_name"], r["status"], r["entity_type"],
                   r["state"], str(r["postcode"] or "")] for r in rows]
    return jsonify({"status": "success", "rows": data_array, "row_count": len(data_array)})


@app.route("/api/supplier/bulk-lookup", methods=["POST"])
def supplier_bulk_lookup():
    data = request.get_json() or {}
    raw_abns = data.get("abns", "")
    abn_list = [a.strip().replace(" ", "").replace("-", "") for a in raw_abns.split(",") if a.strip()]
    valid = [int(a) for a in abn_list if a.isdigit() and len(a) == 11]
    invalid = [a for a in abn_list if not (a.isdigit() and len(a) == 11)]
    if not valid:
        return jsonify({"error": "No valid 11-digit ABNs provided"}), 400

    sql = f"""
        SELECT s.abn, s.organisation_name, s.status, s.entity_type, s.state, s.postcode
        FROM {T(config.SILVER, 'abn_lookup_structured')} s
        WHERE s.abn IN UNNEST(@abns)
    """
    rows = run_query(sql, [bigquery.ArrayQueryParameter("abns", "INT64", valid)])
    data_array = [[str(r["abn"]), r["organisation_name"], r["status"], r["entity_type"],
                   r["state"], str(r["postcode"] or "")] for r in rows]
    return jsonify({"status": "success", "rows": data_array,
                    "row_count": len(data_array), "invalid_abns": invalid})


@app.route("/api/supplier/add", methods=["POST"])
def add_supplier():
    data = request.get_json() or {}
    user_abn = str(data.get("user_abn", "")).replace(" ", "").replace("-", "")
    suppliers = data.get("suppliers", [])
    if not user_abn.isdigit() or len(user_abn) != 11:
        return jsonify({"error": "Valid user_abn is required"}), 400
    if not suppliers:
        return jsonify({"error": "At least one supplier is required"}), 400

    table = config.bq_table(config.GOLD, "supplier_relationships_history")
    rows_to_insert = []
    for s in suppliers:
        s_abn = str(s.get("abn", "")).replace(" ", "").replace("-", "")
        pc = s.get("postcode")
        rows_to_insert.append({
            "user_abn": int(user_abn),
            "supplier_abn": int(s_abn) if s_abn.isdigit() else None,
            "supplier_name": str(s.get("name", "")),
            "supplier_status": str(s.get("status", "")),
            "supplier_entity_type": str(s.get("entity_type", "")),
            "supplier_state": str(s.get("state", "")),
            "supplier_postcode": int(pc) if pc and str(pc).isdigit() else None,
            "added_at": datetime.utcnow().isoformat(),
            "action": "ADD",
        })
    errors = bq.insert_rows_json(table, rows_to_insert)
    if errors:
        return jsonify({"error": f"Insert failed: {errors}"}), 500
    return jsonify({"status": "success", "added_count": len(suppliers)})


@app.route("/api/suppliers/list", methods=["POST"])
def list_suppliers():
    data = request.get_json() or {}
    user_abn = str(data.get("user_abn", "")).replace(" ", "").replace("-", "")
    if not user_abn.isdigit() or len(user_abn) != 11:
        return jsonify({"error": "Valid user_abn is required"}), 400

    # supplier_relationships is a view over ..._history (latest ADD not
    # superseded by a REMOVE) — see transforms/supplier_relationships_view.sql
    sql = f"""
        SELECT sr.supplier_abn, sr.supplier_name, sr.supplier_status,
               sr.supplier_entity_type, sr.supplier_state, sr.supplier_postcode,
               sr.added_at, bd.location_name
        FROM {T(config.GOLD, 'supplier_relationships')} sr
        LEFT JOIN {T(config.GOLD, 'business_details')} bd ON sr.supplier_abn = bd.abn
        WHERE sr.user_abn = @user_abn
        ORDER BY sr.added_at DESC
    """
    rows = run_query(sql, [bigquery.ScalarQueryParameter("user_abn", "INT64", int(user_abn))])
    data_array = [[str(r["supplier_abn"]), r["supplier_name"], r["supplier_status"],
                   r["supplier_entity_type"], r["supplier_state"],
                   str(r["supplier_postcode"] or ""), _iso(r["added_at"]),
                   r["location_name"]] for r in rows]
    return jsonify({"status": "success", "rows": data_array, "row_count": len(data_array)})


@app.route("/api/supplier/remove", methods=["POST"])
def remove_supplier():
    data = request.get_json() or {}
    user_abn = str(data.get("user_abn", "")).replace(" ", "").replace("-", "")
    supplier_abn = str(data.get("supplier_abn", "")).replace(" ", "").replace("-", "")
    if not user_abn.isdigit() or len(user_abn) != 11:
        return jsonify({"error": "Valid user_abn is required"}), 400
    if not supplier_abn.isdigit() or len(supplier_abn) != 11:
        return jsonify({"error": "Valid supplier_abn is required"}), 400

    table = config.bq_table(config.GOLD, "supplier_relationships_history")
    errors = bq.insert_rows_json(table, [{
        "user_abn": int(user_abn), "supplier_abn": int(supplier_abn),
        "supplier_name": "", "supplier_status": "", "supplier_entity_type": "",
        "supplier_state": "", "supplier_postcode": None,
        "added_at": datetime.utcnow().isoformat(), "action": "REMOVE",
    }])
    if errors:
        return jsonify({"error": f"Delete failed: {errors}"}), 500
    return jsonify({"status": "success", "removed_abn": supplier_abn})


# ========== Nearby Hazards ==========

@app.route("/api/nearby-hazards", methods=["POST"])
def nearby_hazards():
    data = request.get_json() or {}
    postcode = str(data.get("postcode", "")).strip()
    if not postcode.isdigit():
        return jsonify({"error": "Valid postcode required"}), 400

    sql = f"""
        SELECT DISTINCT h.hazard_type, h.display_name, h.headline, h.is_major,
               h.impacting_network, ROUND(h.latitude, 4) AS lat,
               ROUND(h.longitude, 4) AS lng, h.roads_json
        FROM {T(config.SILVER, 'poa_h3_lookup')} p
        INNER JOIN {T(config.SILVER, 'hazards_current')} h
            ON p.h3_cell = h.h3_cell AND h.ended = FALSE
        WHERE p.poa_code = @postcode
        LIMIT 20
    """
    rows = run_query(sql, [bigquery.ScalarQueryParameter("postcode", "STRING", postcode)])
    hazards = []
    for r in rows:
        road_name = ""
        if r.get("roads_json"):
            try:
                roads = json.loads(r["roads_json"])
                if roads:
                    rd = roads[0]
                    main, suburb, cross = rd.get("mainStreet", ""), rd.get("suburb", ""), rd.get("crossStreet", "")
                    road_name = f"{main}, {suburb}" if main and suburb else main
                    if cross:
                        road_name += f" (near {cross})"
            except Exception:
                pass
        hazards.append({
            "type": r["hazard_type"] or "", "name": r["display_name"] or "",
            "headline": r["headline"] or "", "is_major": bool(r["is_major"]),
            "impacting_network": bool(r["impacting_network"]),
            "lat": float(r["lat"]) if r["lat"] is not None else None,
            "lng": float(r["lng"]) if r["lng"] is not None else None,
            "road": road_name,
        })
    return jsonify({"status": "success", "hazards": hazards, "postcode": postcode})


# ========== ACE Recovery Assistant (in-process LLM, was: serving endpoint) ==========

def get_business_risk_context(postcode):
    """Concise risk snapshot for a postcode — same three queries as the
    Databricks version, ported to BigQuery."""
    context = {"postcode": postcode, "risk_level": "Unknown", "total_cells": 0,
               "at_risk_cells": 0, "critical_cells": 0, "risk_pct": 0,
               "hazards": [], "weather": None}
    if not postcode or not str(postcode).isdigit():
        return context
    postcode = str(postcode).strip()
    p = [bigquery.ScalarQueryParameter("postcode", "STRING", postcode)]

    try:
        risk_sql = f"""
            WITH cell_risk AS (
                SELECT p.h3_cell,
                       MAX(CASE
                             WHEN h.hazard_type IN ('flood', 'fire') THEN 3
                             WHEN h.impacting_network = TRUE THEN 2
                             WHEN h.hazard_type IS NOT NULL THEN 1
                             ELSE 0 END) AS risk
                FROM {T(config.SILVER, 'poa_h3_lookup')} p
                LEFT JOIN {T(config.SILVER, 'hazards_current')} h
                    ON p.h3_cell = h.h3_cell AND h.ended = FALSE
                WHERE p.poa_code = @postcode
                GROUP BY p.h3_cell
            )
            SELECT COUNT(*) AS total_cells,
                   SUM(IF(risk > 0, 1, 0)) AS at_risk_cells,
                   SUM(IF(risk >= 3, 1, 0)) AS critical_cells
            FROM cell_risk
        """
        rows = run_query(risk_sql, p)
        if rows:
            total = int(rows[0]["total_cells"] or 0)
            at_risk = int(rows[0]["at_risk_cells"] or 0)
            critical = int(rows[0]["critical_cells"] or 0)
            risk_pct = round(at_risk / total * 100) if total else 0
            risk_level = ("Critical" if critical > 0 else
                          "High" if risk_pct > 20 else
                          "Moderate" if at_risk > 0 else "Low")
            context.update({"risk_level": risk_level, "total_cells": total,
                            "at_risk_cells": at_risk, "critical_cells": critical,
                            "risk_pct": risk_pct})
    except Exception as e:
        logger.warning(f"[ace-risk-context] Risk summary unavailable for {postcode}: {e}")

    try:
        hazards_sql = f"""
            SELECT DISTINCT h.hazard_type,
                   COALESCE(h.display_name, h.headline) AS hazard_label,
                   h.is_major, h.impacting_network
            FROM {T(config.SILVER, 'poa_h3_lookup')} p
            INNER JOIN {T(config.SILVER, 'hazards_current')} h
                ON p.h3_cell = h.h3_cell AND h.ended = FALSE
            WHERE p.poa_code = @postcode
            LIMIT 5
        """
        hazards = []
        for row in run_query(hazards_sql, p):
            severity = ("major" if row["is_major"] else
                        "network" if row["impacting_network"] else "active")
            label = (row["hazard_label"] or "").strip() or str(row["hazard_type"]).replace("_", " ")
            hazards.append(f"{row['hazard_type']}: {label} ({severity})")
        context["hazards"] = hazards
    except Exception as e:
        logger.warning(f"[ace-risk-context] Hazards unavailable for {postcode}: {e}")

    try:
        weather_sql = f"""
            SELECT m.nearest_weather_location, w.precipitation_mm, w.temperature_c,
                   w.windspeed_kmh, w.humidity_pct, w.weather_code
            FROM {T(config.SILVER, 'poa_to_weather_location')} m
            INNER JOIN {T(config.SILVER, 'weather_current')} w
                ON w.location_name = m.nearest_weather_location
            WHERE m.poa_code = @postcode
            ORDER BY w.forecast_time ASC
            LIMIT 1
        """
        rows = run_query(weather_sql, p)
        if rows:
            r = rows[0]
            context["weather"] = {
                "station": r["nearest_weather_location"] or "",
                "precipitation_mm": float(r["precipitation_mm"]) if r["precipitation_mm"] is not None else None,
                "temperature_c": float(r["temperature_c"]) if r["temperature_c"] is not None else None,
                "windspeed_kmh": float(r["windspeed_kmh"]) if r["windspeed_kmh"] is not None else None,
                "humidity_pct": float(r["humidity_pct"]) if r["humidity_pct"] is not None else None,
                "weather_code": r["weather_code"],
            }
    except Exception as e:
        logger.warning(f"[ace-risk-context] Weather unavailable for {postcode}: {e}")

    return context


def format_risk_context(context):
    hazards = context.get("hazards") or []
    weather = context.get("weather") or {}
    weather_parts = []
    if weather.get("station"):
        weather_parts.append(f"station={weather['station']}")
    for k in ("precipitation_mm", "temperature_c", "windspeed_kmh"):
        if weather.get(k) is not None:
            weather_parts.append(f"{k}={weather[k]}")
    return (f"overall_risk={context.get('risk_level', 'Unknown')}; "
            f"total_cells={context.get('total_cells', 0)}; "
            f"at_risk_cells={context.get('at_risk_cells', 0)}; "
            f"critical_cells={context.get('critical_cells', 0)}; "
            f"risk_pct={context.get('risk_pct', 0)}; "
            f"active_hazards={'; '.join(hazards) if hazards else 'no active hazards reported'}; "
            f"weather={', '.join(weather_parts) if weather_parts else 'unavailable'}")


def build_ace_prompt_messages(abn, business_name, postcode, history, user_message):
    risk_context = get_business_risk_context(postcode)
    history = history if isinstance(history, list) else []
    messages = [{
        "role": "system",
        "content": (
            "You are ACE, the EcoResilience recovery assistant. Provide concise, practical disaster recovery guidance tailored to the supplied risk context. "
            "Reference the current risk situation directly when it is available, prioritise immediate actions, and avoid generic placeholder introductions. "
            f"The user has completed a risk assessment. Business context: name={business_name or 'Unknown'}, ABN={abn}, postcode={postcode or 'Unknown'}. "
            f"Current risk context: {format_risk_context(risk_context)}"
        ),
    }]
    for item in history[-8:]:
        role = item.get("role") if isinstance(item, dict) else None
        content = str(item.get("content", "")).strip() if isinstance(item, dict) else ""
        if role in {"user", "assistant"} and content:
            messages.append({"role": role, "content": content})
    messages.append({"role": "user", "content": user_message})
    return messages, risk_context


def _chat(messages: list[dict]) -> str:
    """One LLM chat turn on Vertex AI (was: OpenAI-compatible serving client)."""
    resp = get_chat_llm().invoke(messages)
    return (resp.content or "").strip() if isinstance(resp.content, str) else str(resp.content)


@app.route("/api/ace-opening", methods=["POST"])
def ace_opening():
    data = request.get_json() or {}
    abn = str(data.get("abn", "")).replace(" ", "").replace("-", "")
    business_name = str(data.get("business_name", "")).strip()
    postcode = str(data.get("postcode", "")).strip()
    if not abn.isdigit() or len(abn) != 11:
        return jsonify({"error": "Valid ABN required"}), 400

    opening_instruction = (
        "Open the conversation with a concise risk briefing for this business. "
        "Describe the current risk situation using the supplied context, mention the most important immediate recovery priorities, "
        "and invite follow-up questions. Keep it practical and under 120 words."
    )
    messages, risk_context = build_ace_prompt_messages(abn, business_name, postcode, [], opening_instruction)
    return jsonify({"status": "success", "message": _chat(messages), "risk_context": risk_context})


@app.route("/api/ace-chat", methods=["POST"])
def ace_chat():
    data = request.get_json() or {}
    abn = str(data.get("abn", "")).replace(" ", "").replace("-", "")
    message = str(data.get("message", "")).strip()
    history = data.get("history", []) or []
    business_name = str(data.get("business_name", "")).strip()
    postcode = str(data.get("postcode", "")).strip()
    if not abn.isdigit() or len(abn) != 11:
        return jsonify({"error": "Valid ABN required"}), 400
    if not message:
        return jsonify({"error": "message is required"}), 400

    messages, risk_context = build_ace_prompt_messages(abn, business_name, postcode, history, message)
    return jsonify({"status": "success", "message": _chat(messages), "risk_context": risk_context})


# ========== H3 Map (h3 Python lib replaces Databricks h3_* SQL) ==========

@app.route("/api/h3-cells", methods=["POST"])
def get_h3_cells():
    import h3

    data = request.get_json() or {}
    postcode = str(data.get("postcode", "")).strip()
    if not postcode.isdigit():
        return jsonify({"error": "Valid postcode required"}), 400

    sql = f"""
        SELECT p.h3_cell,
               COALESCE(CASE
                   WHEN h.is_major = TRUE THEN 3
                   WHEN h.hazard_type IN ('flood', 'fire') THEN 3
                   WHEN h.impacting_network = TRUE THEN 2
                   WHEN h.h3_cell IS NOT NULL THEN 1
               END, 0) AS risk
        FROM {T(config.SILVER, 'poa_h3_lookup')} p
        LEFT JOIN {T(config.SILVER, 'hazards_current')} h
            ON p.h3_cell = h.h3_cell AND h.ended = FALSE
        WHERE p.poa_code = @postcode
        LIMIT 1500
    """
    rows = run_query(sql, [bigquery.ScalarQueryParameter("postcode", "STRING", postcode)])
    cells = []
    for r in rows:
        try:
            boundary = [[lat, lng] for lat, lng in h3.cell_to_boundary(r["h3_cell"])]
        except Exception:
            continue
        cells.append({"h3": r["h3_cell"], "risk": int(r["risk"] or 0), "boundary": boundary})

    weather = None
    try:
        weather_sql = f"""
            SELECT w.precipitation_mm, w.temperature_c, w.windspeed_kmh,
                   w.location_name, w.forecast_time
            FROM {T(config.SILVER, 'poa_to_weather_location')} m
            INNER JOIN {T(config.SILVER, 'weather_current')} w
                ON w.location_name = m.nearest_weather_location
            WHERE m.poa_code = @postcode AND w.forecast_time <= CURRENT_TIMESTAMP()
            ORDER BY w.forecast_time DESC
            LIMIT 1
        """
        w_rows = run_query(weather_sql, [bigquery.ScalarQueryParameter("postcode", "STRING", postcode)])
        if w_rows:
            wr = w_rows[0]
            weather = {"precipitation_mm": float(wr["precipitation_mm"] or 0),
                       "temperature_c": float(wr["temperature_c"]) if wr["temperature_c"] is not None else None,
                       "windspeed_kmh": float(wr["windspeed_kmh"]) if wr["windspeed_kmh"] is not None else None,
                       "station": wr["location_name"] or "",
                       "forecast_time": _iso(wr["forecast_time"]) or ""}
    except Exception as we:
        logger.warning(f"[h3-cells] Weather fetch failed: {we}")

    return jsonify({"status": "success", "cells": cells, "postcode": postcode, "weather": weather})


# ========== Performance Profiling Endpoints (unchanged) ==========

@app.route("/api/perf-log", methods=["POST"])
def perf_log_client():
    data = request.get_json() or {}
    _record_perf({"ts": datetime.utcnow().isoformat() + "Z", "source": "client",
                  "session_id": data.get("session_id", "unknown"),
                  "total_ms": round(data.get("total_ms", 0), 1),
                  "stages": data.get("stages", [])})
    return jsonify({"status": "logged"})


@app.route("/api/perf-report", methods=["GET"])
def perf_report():
    source_filter = request.args.get("source", "")
    limit = int(request.args.get("limit", "50"))
    entries = _perf_log[-limit:]
    if source_filter == "client":
        entries = [e for e in entries if e.get("source") == "client"]
    elif source_filter == "server":
        entries = [e for e in entries if e.get("source") != "client"]
    server_entries = [e for e in _perf_log if e.get("method")]
    endpoint_stats = defaultdict(list)
    for e in server_entries[-100:]:
        endpoint_stats[e["endpoint"]].append(e["total_ms"])
    summary = {ep: {"count": len(ts), "avg_ms": round(sum(ts) / len(ts), 1),
                    "max_ms": round(max(ts), 1), "min_ms": round(min(ts), 1),
                    "p95_ms": round(sorted(ts)[int(len(ts) * 0.95)] if len(ts) >= 2 else ts[0], 1)}
               for ep, ts in endpoint_stats.items()}
    return jsonify({"entries": entries, "summary": summary,
                    "total_entries_in_buffer": len(_perf_log)})


# ========== Grant Draft Generation (Magic Moment) ==========

DISASTER_TYPES = {"flood", "fire", "storm", "earthquake", "drought", "cyclone"}


def _compose_grant_prompt(business, risk, chat_history, user_prompt):
    parts = [
        f"Business identity (verified): {business.get('name', 'Unknown')}, "
        f"ABN {business.get('abn', '')}, "
        f"{business.get('state', '')} {business.get('postcode', '')}. "
        f"Entity type: {business.get('entity_type', '')}.",
    ]
    if risk:
        hazards = risk.get("hazards") or []
        weather = risk.get("weather") or {}
        weather_parts = []
        if weather.get("precipitation_mm") is not None:
            weather_parts.append(f"{weather['precipitation_mm']}mm precipitation")
        if weather.get("temperature_c") is not None:
            weather_parts.append(f"{weather['temperature_c']}°C")
        if weather.get("windspeed_kmh") is not None:
            weather_parts.append(f"{weather['windspeed_kmh']}kmh wind")
        parts.append(
            f"Risk context: {risk.get('risk_level', 'Unknown')} risk level. "
            f"{risk.get('at_risk_cells', 0)} of {risk.get('total_cells', 0)} H3 cells affected. "
            f"Active hazards: {'; '.join(hazards) if hazards else 'none reported'}. "
            f"Weather: {', '.join(weather_parts) or 'unavailable'}."
        )
    if chat_history:
        history_lines = ["User's prior conversation with the recovery assistant (recent turns):"]
        for turn in chat_history[-8:]:
            content = (turn.get("content") or "").strip()
            if content:
                history_lines.append(f"  {turn.get('role', 'user')}: {content[:300]}")
        parts.append("\n".join(history_lines))
    parts.append(f"User request: {user_prompt}")
    return "\n\n".join(parts)


def _call_eco_resilience_agent(full_user_message):
    """In-process LangGraph agent call (was: POST to a serving endpoint)."""
    return invoke_agent(get_agent(), [{"role": "user", "content": full_user_message}])


def _extract_estimated_loss(sources_user_first):
    money_re = r"\$\s*([\d,]+(?:\.\d{1,2})?)"
    near_damage_re = r"(?i)(?:damage|loss|estimate|claim|cost)[^\d$\n]{0,60}\$\s*([\d,]+(?:\.\d{1,2})?)"
    MIN_REASONABLE = 1000.0

    def _parse(raw):
        try:
            return float(raw.replace(",", ""))
        except ValueError:
            return None

    for src in sources_user_first:
        if not src:
            continue
        m = re.search(near_damage_re, src)
        if m:
            v = _parse(m.group(1))
            if v is not None and v >= MIN_REASONABLE:
                return v
    for src in sources_user_first[:-1]:
        if not src:
            continue
        for raw in re.findall(money_re, src):
            v = _parse(raw)
            if v is not None and v >= MIN_REASONABLE:
                return v
    for src in sources_user_first:
        if not src:
            continue
        for raw in re.findall(money_re, src):
            v = _parse(raw)
            if v is not None and v >= MIN_REASONABLE:
                return v
    return 0.0


def _extract_grant_args(agent_text, chat_history, business, user_prompt):
    user_chat_text = "\n".join((t.get("content") or "") for t in (chat_history or [])
                               if t.get("role") == "user")
    full_chat_text = "\n".join((t.get("content") or "") for t in (chat_history or []))
    sources_user_first = [user_prompt or "", user_chat_text, full_chat_text, agent_text or ""]
    blob = "\n".join(s for s in sources_user_first if s).lower()

    disaster_type = next((d for d in DISASTER_TYPES if d in blob), "flood")

    disaster_date = date.today().isoformat()
    for src in sources_user_first:
        m = re.search(r"\b(20\d{2}-\d{2}-\d{2})\b", src)
        if m:
            disaster_date = m.group(1)
            break

    drfa_category = "C"
    for src in sources_user_first:
        m = re.search(r"(?i)category\s+([abcd])\b", src)
        if m:
            drfa_category = m.group(1).upper()
            break

    estimated_loss_aud = _extract_estimated_loss(sources_user_first)

    narrative = (agent_text or "").strip()
    justification = narrative
    if len(narrative) > 800:
        para_match = re.search(
            r"(?is)([^\n]{40,800}(?:justif|recommend|claim|apply|seek|qualif|eligib)[^\n]{0,800})",
            narrative)
        justification = para_match.group(1).strip() if para_match else narrative[:800]
    justification = re.sub(
        r"\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b",
        "[ref redacted]", justification)

    return {
        "abn": business.get("abn", ""),
        "entity_name": business.get("name", ""),
        "entity_state": business.get("state", ""),
        "entity_postcode": business.get("postcode", ""),
        "disaster_type": disaster_type,
        "disaster_date": disaster_date,
        "drfa_category": drfa_category,
        "estimated_loss_aud": estimated_loss_aud,
        "justification": justification,
    }


def _render_grant_pdf(grant_struct, agent_narrative):
    """Render the grant STRUCT as a PDF using fpdf2 — unchanged from the
    Databricks version (pure Python, no platform dependency)."""
    from fpdf import FPDF

    NAVY = (15, 23, 42)
    ORANGE = (251, 146, 60)
    SLATE_500 = (100, 116, 139)
    SLATE_700 = (51, 65, 85)
    SLATE_900 = (15, 23, 42)
    SLATE_50 = (248, 250, 252)
    SLATE_100 = (241, 245, 249)
    LIGHT_GRY = (226, 232, 240)
    BANNER_BG = (255, 247, 237)
    BANNER_TX = (154, 52, 18)

    applicant = grant_struct.get("applicant") or {}
    disaster = grant_struct.get("disaster") or {}
    grant_request = grant_struct.get("grant_request") or {}
    next_steps = grant_struct.get("next_steps") or []
    application_id = grant_struct.get("application_id", "unknown")
    draft_ts = grant_struct.get("draft_timestamp", "")
    status = grant_struct.get("status", "DRAFT")
    error_msg = grant_struct.get("error")

    def _latin1(s):
        return (s or "").encode("latin-1", "replace").decode("latin-1")

    pdf = FPDF(format="A4", orientation="P", unit="mm")
    pdf.set_auto_page_break(auto=True, margin=20)
    pdf.add_page()
    pdf.set_margins(20, 20, 20)

    pdf.set_fill_color(*NAVY)
    pdf.rect(0, 0, 210, 18, style="F")
    pdf.set_fill_color(*ORANGE)
    pdf.rect(0, 18, 210, 1.5, style="F")

    pdf.set_xy(15, 4)
    pdf.set_fill_color(*ORANGE)
    pdf.set_text_color(255, 255, 255)
    pdf.set_font("Helvetica", "B", 9)
    pdf.cell(10, 7, "ER", fill=True, align="C")

    pdf.set_xy(28, 4)
    pdf.set_font("Helvetica", "B", 13)
    pdf.cell(170, 7, "EcoResilience AI - DRFA Grant Application")

    pdf.set_xy(28, 11)
    pdf.set_font("Helvetica", "", 7)
    pdf.set_text_color(203, 213, 225)
    pdf.cell(170, 4, _latin1(f"Application ID: {application_id}    |    Generated: {draft_ts}    |    Status: {status}"))

    pdf.set_y(25)
    pdf.set_text_color(*SLATE_900)

    pdf.set_fill_color(*BANNER_BG)
    pdf.set_draw_color(*ORANGE)
    pdf.set_line_width(0.5)
    pdf.set_text_color(*BANNER_TX)
    pdf.set_font("Helvetica", "B", 9)
    pdf.cell(0, 5, " DRAFT - Not for Submission", fill=True, border="L",
             new_x="LMARGIN", new_y="NEXT")
    pdf.set_font("Helvetica", "", 8)
    pdf.set_text_color(*SLATE_700)
    pdf.multi_cell(0, 4, _latin1(
        " This document was prepared by EcoResilience AI based on verified business identity, "
        "real-time hazard data, and the Disaster Recovery Funding Arrangements (DRFA) guidelines. "
        "Please review every field with your records and supporting evidence before submitting "
        "to the National Emergency Management Agency (NEMA)."), border="L", fill=True)
    pdf.ln(3)

    def section_header(num, title):
        pdf.ln(2)
        pdf.set_fill_color(*NAVY)
        pdf.set_text_color(255, 255, 255)
        pdf.set_font("Helvetica", "B", 8)
        pdf.cell(7, 6, str(num), fill=True, align="C")
        pdf.set_text_color(*SLATE_700)
        pdf.set_font("Helvetica", "B", 11)
        pdf.cell(0, 6, "  " + title.upper(), new_x="LMARGIN", new_y="NEXT")
        pdf.set_draw_color(*NAVY)
        pdf.set_line_width(0.6)
        pdf.line(20, pdf.get_y(), 190, pdf.get_y())
        pdf.ln(2.5)

    def field_row(label, value, value_style="normal", value_color=None):
        pdf.set_font("Helvetica", "B", 7)
        pdf.set_text_color(*SLATE_500)
        pdf.cell(55, 5, label.upper())
        if value_style == "bold":
            pdf.set_font("Helvetica", "B", 10)
        elif value_style == "category":
            pdf.set_font("Helvetica", "B", 12)
        else:
            pdf.set_font("Helvetica", "", 10)
        pdf.set_text_color(*(value_color or SLATE_900))
        pdf.cell(0, 5, _latin1(str(value if value not in (None, "") else "-")),
                 new_x="LMARGIN", new_y="NEXT")
        pdf.ln(0.5)

    section_header(1, "Applicant Details")
    field_row("Business Name", applicant.get("entity_name"))
    field_row("Australian Business Number", applicant.get("abn"))
    field_row("State", applicant.get("state"))
    field_row("Postcode", applicant.get("postcode"))

    section_header(2, "Disaster Event")
    field_row("Disaster Type", (disaster.get("type") or "").capitalize())
    field_row("Disaster Date", disaster.get("date"))
    field_row("Located in NSW (DRFA-eligible)",
              "YES" if disaster.get("in_nsw") else "NO",
              value_color=(21, 128, 61) if disaster.get("in_nsw") else (180, 83, 9))

    section_header(3, "Grant Request")
    field_row("DRFA Category", f"Category {grant_request.get('drfa_category', '-')}",
              value_style="category", value_color=ORANGE)
    loss_amt = grant_request.get("estimated_loss_aud") or 0
    field_row("Estimated Loss (AUD)", f"${loss_amt:,.0f}", value_style="bold")

    pdf.ln(2)
    pdf.set_font("Helvetica", "B", 7)
    pdf.set_text_color(*SLATE_500)
    pdf.cell(0, 4, "JUSTIFICATION", new_x="LMARGIN", new_y="NEXT")
    pdf.set_font("Helvetica", "I", 9)
    pdf.set_text_color(*SLATE_900)
    pdf.set_fill_color(*SLATE_50)
    pdf.set_draw_color(*ORANGE)
    pdf.set_line_width(1.0)
    pdf.multi_cell(0, 5, _latin1(grant_request.get("justification") or
                                 "(No justification narrative captured.)"),
                   border="L", fill=True)

    section_header(4, "Next Steps for the Applicant")
    if next_steps:
        pdf.set_text_color(*SLATE_900)
        for i, step in enumerate(next_steps, 1):
            pdf.set_font("Helvetica", "B", 9)
            pdf.cell(7, 5, f"{i}.")
            pdf.set_font("Helvetica", "", 9)
            pdf.multi_cell(0, 5, _latin1(step))
            pdf.ln(0.5)
    else:
        pdf.set_font("Helvetica", "I", 9)
        pdf.set_text_color(*SLATE_500)
        pdf.cell(0, 5, _latin1(f"No next-steps checklist (status: {status})"),
                 new_x="LMARGIN", new_y="NEXT")

    if agent_narrative:
        section_header(5, "Supporting Analysis")
        pdf.set_font("Helvetica", "B", 7)
        pdf.set_text_color(*SLATE_500)
        pdf.cell(0, 4, "AGENT REASONING & DRFA-GROUNDED CONTEXT",
                 new_x="LMARGIN", new_y="NEXT")
        pdf.ln(0.5)
        pdf.set_font("Helvetica", "", 9)
        pdf.set_text_color(*SLATE_900)
        pdf.set_fill_color(*SLATE_100)
        pdf.set_draw_color(*NAVY)
        pdf.set_line_width(1.0)
        pdf.multi_cell(0, 4.5, _latin1(agent_narrative), border="L", fill=True)

    if error_msg and status != "DRAFT":
        section_header("!", "Validation Notes")
        pdf.set_font("Helvetica", "", 9)
        pdf.set_text_color(127, 29, 29)
        pdf.set_fill_color(254, 242, 242)
        pdf.set_draw_color(185, 28, 28)
        pdf.set_line_width(1.0)
        pdf.multi_cell(0, 5, _latin1(error_msg), border="L", fill=True)

    pdf.set_y(-22)
    pdf.set_draw_color(*LIGHT_GRY)
    pdf.set_line_width(0.3)
    pdf.line(20, pdf.get_y(), 190, pdf.get_y())
    pdf.ln(1.5)
    pdf.set_font("Helvetica", "B", 7)
    pdf.set_text_color(*ORANGE)
    pdf.cell(0, 3, "GENERATED BY ECORESILIENCE AI", new_x="LMARGIN", new_y="NEXT")
    pdf.set_font("Helvetica", "", 7)
    pdf.set_text_color(*SLATE_500)
    pdf.multi_cell(0, 3.2, _latin1(
        f"Application ID {application_id}  |  {draft_ts}  |  "
        "This document reflects information available at generation time. "
        "It is a DRAFT intended for human review. Submit only after verifying "
        "every field and attaching supporting evidence (damage photos, repair "
        "quotes, insurance correspondence, ATO primary-producer registration) "
        "per the cited DRFA category requirements."))

    return bytes(pdf.output())


# ============================================================
# Cloud SQL — OLTP store for grant submission history
# (replaces Lakebase; same table shape, IAM DB auth instead of minted JWTs)
# ============================================================

_sql_connector = None


def _cloudsql_conn():
    """IAM-authenticated Postgres connection via the Cloud SQL Python Connector."""
    global _sql_connector
    from google.cloud.sql.connector import Connector

    if _sql_connector is None:
        _sql_connector = Connector()
    return _sql_connector.connect(
        CLOUD_SQL_CONNECTION,
        "pg8000",
        user=CLOUD_SQL_USER,
        db=CLOUD_SQL_DB,
        enable_iam_auth=True,
    )


def _log_grant_to_cloudsql(business: dict, grant_struct: dict, user_query: str) -> None:
    """Best-effort INSERT of one grant submission. Swallows errors."""
    if not CLOUD_SQL_CONNECTION:
        return
    try:
        conn = _cloudsql_conn()
        try:
            cur = conn.cursor()
            cur.execute(
                """
                INSERT INTO grant_submissions
                  (abn, business_name, postcode, state, application_id, grant_status, user_query)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (business.get("abn"), business.get("name"), business.get("postcode"),
                 business.get("state"), grant_struct.get("application_id"),
                 grant_struct.get("status"), user_query or ""))
            conn.commit()
        finally:
            conn.close()
        logger.info(f"[cloudsql] logged grant app_id={grant_struct.get('application_id')}")
    except Exception as e:
        logger.warning(f"[cloudsql] grant log failed (non-fatal): {type(e).__name__}: {e}")


@app.route("/api/grant-history", methods=["GET"])
def grant_history():
    if not CLOUD_SQL_CONNECTION:
        return jsonify({"submissions": [], "lakebase_configured": False})
    abn = request.args.get("abn", "").strip()
    if not abn:
        return jsonify({"submissions": [], "lakebase_configured": True})
    try:
        conn = _cloudsql_conn()
        try:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT id, abn, business_name, postcode, state,
                       application_id, grant_status, user_query, generated_at
                FROM grant_submissions
                WHERE abn = %s
                ORDER BY generated_at DESC
                LIMIT 20
                """, (abn,))
            cols = [d[0] for d in cur.description]
            rows = []
            for r in cur.fetchall():
                row = dict(zip(cols, r))
                if row.get("generated_at"):
                    row["generated_at"] = row["generated_at"].isoformat()
                rows.append(row)
        finally:
            conn.close()
        return jsonify({"submissions": rows, "lakebase_configured": True})
    except Exception as e:
        logger.error(f"[cloudsql] grant-history read failed: {type(e).__name__}: {e}")
        return jsonify({"submissions": [], "lakebase_configured": True, "error": str(e)}), 500


@app.route("/api/grant-draft", methods=["POST"])
def grant_draft():
    """Generate a downloadable DRFA grant application PDF."""
    try:
        data = request.get_json() or {}
        business = data.get("business", {}) or {}
        risk = data.get("risk", {}) or {}
        chat_history = data.get("chat_history", []) or []
        user_prompt = (data.get("user_prompt") or "").strip()

        abn = str(business.get("abn", "")).replace(" ", "").replace("-", "")
        if not (abn.isdigit() and len(abn) == 11):
            return jsonify({"error": "Valid 11-digit ABN required in business.abn"}), 400
        if not user_prompt:
            return jsonify({"error": "user_prompt is required"}), 400
        business["abn"] = abn

        logger.info(f"[grant-draft] start | ABN={abn} | chat_turns={len(chat_history)}")

        perf_checkpoint("pre_agent_call")
        full_user_message = _compose_grant_prompt(business, risk, chat_history, user_prompt)
        agent_text = _call_eco_resilience_agent(full_user_message)
        perf_checkpoint("post_agent_call")

        grant_args = _extract_grant_args(agent_text, chat_history, business, user_prompt)
        logger.info(f"[grant-draft] extracted | disaster={grant_args['disaster_type']} | "
                    f"cat={grant_args['drfa_category']} | loss=${grant_args['estimated_loss_aud']:.0f}")

        # Deterministic (non-LLM) struct assembly — was a SQL UDF call
        grant_struct = generate_grant_struct(**grant_args)
        perf_checkpoint("post_struct")

        pdf_bytes = _render_grant_pdf(grant_struct, agent_text)
        perf_checkpoint("post_pdf_render")

        application_id = grant_struct.get("application_id", "unknown")
        status = grant_struct.get("status", "DRAFT")
        logger.info(f"[grant-draft] done | app_id={application_id} | status={status}")

        _log_grant_to_cloudsql(business, grant_struct, user_prompt)

        return Response(
            pdf_bytes,
            mimetype="application/pdf",
            headers={
                "Content-Disposition": f'attachment; filename="DRFA-{application_id[:8]}.pdf"',
                "X-Application-Id": application_id,
                "X-Grant-Status": status,
                "Cache-Control": "no-store",
            })
    except requests.RequestException as e:
        logger.error(f"[grant-draft] HTTP error: {type(e).__name__}: {e}")
        return jsonify({"error": f"Upstream service unreachable: {e}"}), 502
    except Exception as e:
        logger.error(f"[grant-draft] Unexpected error: {type(e).__name__}: {e}")
        return jsonify({"error": f"{type(e).__name__}: {e}"}), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)), debug=True)
