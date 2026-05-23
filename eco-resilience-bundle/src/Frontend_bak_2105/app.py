"""
EcoResilience AI - Databricks App Backend
Serves the frontend and proxies Databricks API calls using the app's service principal.
"""

import os
import re
import sys
import json
import logging
import time
from datetime import datetime, date
from dotenv import load_dotenv
load_dotenv()
import requests
from flask import Flask, request, jsonify, send_from_directory, Response
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState

# ========== Logging Setup ==========
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout
)
logger = logging.getLogger("ecoresilience")

app = Flask(__name__, static_folder=".", static_url_path="")

# Initialize Databricks SDK (uses app service principal credentials automatically)
w = WorkspaceClient()

# Configuration from environment variables
WAREHOUSE_ID = os.environ.get("DATABRICKS_WAREHOUSE_ID", "dc189fe4fd0f924b")
JOB_ID = int(os.environ.get("ECORESILIENCE_JOB_ID", "1009568797220506"))
ACE_ENDPOINT_NAME = os.environ.get("ECORESILIENCE_ACE_ENDPOINT", "agents_eco_resilience-gold-ace_disaster_agent")
ECORESILIENCE_AGENT_ENDPOINT = os.environ.get("ECORESILIENCE_AGENT_ENDPOINT", "eco_resilience_agent")

# Lakebase (OLTP grant history). Optional — if LAKEBASE_HOST is unset, the
# history feature is silently disabled and /api/grant-history returns an empty list.
LAKEBASE_HOST     = os.environ.get("LAKEBASE_HOST", "").strip()
LAKEBASE_DB       = os.environ.get("LAKEBASE_DB",   "databricks_postgres")
LAKEBASE_USER     = os.environ.get("LAKEBASE_USER", "").strip()
LAKEBASE_INSTANCE = os.environ.get("LAKEBASE_INSTANCE_NAME", "grant-history-db").strip()

logger.info(
    f"App initialized | Warehouse: {WAREHOUSE_ID} | Job: {JOB_ID} | "
    f"ACE: {ACE_ENDPOINT_NAME} | EcoResilience Agent: {ECORESILIENCE_AGENT_ENDPOINT}"
)

# ========== Performance Profiling ==========
import functools
from collections import defaultdict

# In-memory ring buffer for recent API timings (last 100 calls)
_perf_log = []
_PERF_LOG_MAX = 200

def _record_perf(entry):
    """Record a performance entry to the in-memory log."""
    _perf_log.append(entry)
    if len(_perf_log) > _PERF_LOG_MAX:
        _perf_log.pop(0)


@app.before_request
def _perf_start_timer():
    """Attach a start timestamp to every request."""
    request._perf_start = time.perf_counter()
    request._perf_checkpoints = []


@app.after_request
def _perf_end_timer(response):
    """Log total request duration and any sub-checkpoints."""
    start = getattr(request, '_perf_start', None)
    if start is None:
        return response

    elapsed_ms = (time.perf_counter() - start) * 1000
    endpoint = request.endpoint or request.path
    checkpoints = getattr(request, '_perf_checkpoints', [])

    # Only log API calls (skip static files)
    if request.path.startswith('/api/'):
        entry = {
            "ts": datetime.utcnow().isoformat() + "Z",
            "method": request.method,
            "endpoint": request.path,
            "status": response.status_code,
            "total_ms": round(elapsed_ms, 1),
            "checkpoints": checkpoints,
        }
        _record_perf(entry)

        # Log with detail level based on duration
        if elapsed_ms > 5000:
            logger.warning(f"[PERF] SLOW {request.method} {request.path} => {elapsed_ms:.0f}ms | checkpoints={checkpoints}")
        elif elapsed_ms > 1000:
            logger.info(f"[PERF] {request.method} {request.path} => {elapsed_ms:.0f}ms | checkpoints={checkpoints}")
        else:
            logger.debug(f"[PERF] {request.method} {request.path} => {elapsed_ms:.0f}ms")

    return response


def perf_checkpoint(label):
    """Record a named checkpoint within a request for sub-operation timing."""
    start = getattr(request, '_perf_start', None)
    if start is not None:
        elapsed_ms = (time.perf_counter() - start) * 1000
        if hasattr(request, '_perf_checkpoints'):
            request._perf_checkpoints.append({"label": label, "at_ms": round(elapsed_ms, 1)})



# Global error handlers - always return JSON, never HTML
@app.errorhandler(Exception)
def handle_exception(e):
    logger.error(f"[unhandled] {type(e).__name__}: {e}")
    return jsonify({"error": f"Internal server error: {str(e)}"}), 500

@app.errorhandler(404)
def not_found(e):
    return jsonify({"error": "Endpoint not found"}), 404

@app.errorhandler(500)
def internal_error(e):
    return jsonify({"error": f"Internal server error: {str(e)}"}), 500


# ========== Static Files ==========

@app.route("/")
def serve_index():
    response = send_from_directory(".", "index.html")
    response.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate'
    response.headers['Pragma'] = 'no-cache'
    response.headers['Expires'] = '0'
    return response




@app.route("/api/debug", methods=["GET"])
def debug_info():
    """Debug endpoint to verify deployment state."""
    import time
    index_path = os.path.join(os.path.dirname(__file__), "index.html")
    index_size = os.path.getsize(index_path) if os.path.exists(index_path) else -1
    index_mtime = os.path.getmtime(index_path) if os.path.exists(index_path) else -1
    
    # Check if spinner code is in the served file
    has_spinner = False
    if os.path.exists(index_path):
        with open(index_path, "r") as f:
            content = f.read()
            has_spinner = "ace-loading-spinner" in content
    
    return jsonify({
        "status": "ok",
        "deploy_check": "2026-05-12-v5-spinner-fix",
        "index_html_size": index_size,
        "index_html_mtime": index_mtime,
        "has_spinner_code": has_spinner,
        "working_dir": os.getcwd(),
        "files_in_dir": os.listdir(os.path.dirname(__file__))
    })



@app.route("/api/test-ace", methods=["GET"])
def test_ace():
    """Test endpoint - visit in browser to check ACE flow."""
    errors = []
    results = {}
    
    # Test 1: SQL execution
    try:
        test_sql = "SELECT 1 AS test_value"
        resp = execute_sql_statement(test_sql, wait_timeout="10s")
        if resp.status.state == StatementState.SUCCEEDED:
            results["sql_test"] = "OK"
        else:
            errors.append(f"SQL test failed: {resp.status.error.message if resp.status.error else 'unknown'}")
    except Exception as e:
        errors.append(f"SQL test error: {type(e).__name__}: {e}")
    
    # Test 2: ACE endpoint call
    try:
        client = get_ace_client()
        response = client.chat.completions.create(
            model=ACE_ENDPOINT_NAME,
            messages=[
                {"role": "system", "content": "You are ACE. Reply briefly."},
                {"role": "user", "content": "Say hello in 10 words or less."}
            ],
        )
        msg = extract_chat_content(response)
        results["ace_endpoint"] = f"OK - response: {msg[:100]}"
    except Exception as e:
        errors.append(f"ACE endpoint error: {type(e).__name__}: {e}")
    
    # Test 3: Risk context for Bathurst
    try:
        ctx = get_business_risk_context("2795")
        results["risk_context"] = f"OK - risk_level={ctx.get('risk_level')}, cells={ctx.get('total_cells')}"
    except Exception as e:
        errors.append(f"Risk context error: {type(e).__name__}: {e}")
    
    return jsonify({
        "status": "errors" if errors else "all_ok",
        "errors": errors,
        "results": results,
        "endpoint_name": ACE_ENDPOINT_NAME,
        "warehouse_id": WAREHOUSE_ID,
    })


# ========== API Endpoints ==========

@app.route("/api/ingest-only", methods=["POST"])
def ingest_only():
    """Trigger ABN ingestion only (no risk calculation).
    Used by 'Verify My Business' and 'Look Up' supplier buttons.
    Ingests ABN details and populates gold.business_details.
    """
    try:
        data = request.get_json()
        abn = data.get("abn", "")

        logger.info(f"[ingest-only] ABN={abn}")

        if not abn:
            logger.warning("[ingest-only] Missing ABN parameter")
            return jsonify({"error": "ABN parameter is required"}), 400

        # Run the full job but with mode='ingest_details' - this will:
        # 1. Ingest raw ABN from ABR API (task 1)
        # 2. Parse bronze -> silver + write to gold.business_details_history (task 2 in ingest_details mode)
        run = w.jobs.run_now(
            job_id=JOB_ID,
            job_parameters={"abn": abn, "mode": "ingest_details"}
        )

        logger.info(f"[ingest-only] Job triggered | run_id={run.run_id} | ABN={abn}")
        return jsonify({
            "run_id": run.run_id,
            "status": "triggered",
            "mode": "ingest_details"
        })

    except Exception as e:
        logger.error(f"[ingest-only] Error for ABN={abn}: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/trigger-job", methods=["POST"])
def trigger_job():
    """Trigger the full ETL job (ingest + risk calculation).
    Used by the 'Assess Risk' button.
    
    Accepts 'mode' parameter:
      - 'full': Run both ingest + transform + risk calculation (default)
      - 'risk_only': Skip ABR ingestion, only recalculate risk scores
    """
    try:
        data = request.get_json()
        abn = data.get("abn", "")
        mode = data.get("mode", "full")

        logger.info(f"[trigger-job] ABN={abn} mode={mode}")

        if not abn:
            logger.warning("[trigger-job] Missing ABN parameter")
            return jsonify({"error": "ABN parameter is required"}), 400

        perf_checkpoint("pre_sdk_call")
        if mode == "risk_only":
            # Only run the transformation task (skip ingestion)
            run = w.jobs.run_now(
                job_id=JOB_ID,
                job_parameters={"abn": abn, "mode": "risk_only"},
                only=["Process_ABN_Silver_Gold"]
            )
        else:
            # Full ETL: ingest from ABR + transform + risk calc
            run = w.jobs.run_now(
                job_id=JOB_ID,
                job_parameters={"abn": abn, "mode": "full"}
            )
        perf_checkpoint("post_sdk_call")

        logger.info(f"[trigger-job] Job triggered | run_id={run.run_id} | mode={mode} | ABN={abn}")
        return jsonify({
            "run_id": run.run_id,
            "status": "triggered",
            "mode": mode
        })

    except Exception as e:
        logger.error(f"[trigger-job] Error for ABN={abn}: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/job-status/<int:run_id>", methods=["GET"])
def job_status(run_id):
    """Poll the status of a job run."""
    try:
        perf_checkpoint("pre_get_run")
        run = w.jobs.get_run(run_id=run_id)
        perf_checkpoint("post_get_run")
        state = run.state
        logger.debug(f"[job-status] run_id={run_id} state={state.life_cycle_state}")

        return jsonify({
            "life_cycle_state": state.life_cycle_state.value if state.life_cycle_state else None,
            "result_state": state.result_state.value if state.result_state else None,
            "state_message": state.state_message or ""
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/query", methods=["POST"])
def query_table():
    """Query business details - reads from gold.business_details joined with silver.abn_lookup_structured."""
    try:
        data = request.get_json()
        search_term = data.get("search_term", "")
        search_type = data.get("search_type", "abn")

        logger.info(f"[query] search_term={search_term} search_type={search_type}")

        if not search_term:
            return jsonify({"error": "search_term is required"}), 400

        # Build the SQL query - read from silver for basic info, join gold for location
        if search_type == "abn":
            clean_abn = search_term.replace(" ", "").replace("-", "")
            where_clause = f"s.abn = {clean_abn}"
        else:
            safe_term = search_term.replace("'", "''")
            where_clause = f"LOWER(s.organisation_name) LIKE LOWER('%{safe_term}%')"

        sql = f"""
            SELECT s.abn, s.organisation_name, s.status, s.entity_type, s.state, s.postcode
            FROM eco_resilience.silver.abn_lookup_structured s
            WHERE {where_clause}
            LIMIT 20
        """

        response = w.statement_execution.execute_statement(
            statement=sql,
            warehouse_id=WAREHOUSE_ID,
            wait_timeout="30s",
        )

        if response.status.state == StatementState.SUCCEEDED:
            rows = []
            if response.result and response.result.data_array:
                rows = response.result.data_array

            return jsonify({
                "status": "success",
                "rows": rows,
                "row_count": len(rows)
            })
        elif response.status.state == StatementState.FAILED:
            error_msg = response.status.error.message if response.status.error else "Unknown error"
            return jsonify({"error": f"Query failed: {error_msg}"}), 500
        else:
            return jsonify({"error": f"Unexpected state: {response.status.state}"}), 500

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/business-details", methods=["POST"])
def business_details():
    """Get business details from gold.business_details (includes location_name from spatial join)."""
    try:
        data = request.get_json()
        abn = data.get("abn", "").replace(" ", "").replace("-", "")

        logger.info(f"[business-details] ABN={abn}")

        if not abn or not abn.isdigit():
            return jsonify({"error": "Valid ABN required"}), 400

        sql = f"""
            SELECT d.abn, s.organisation_name, s.status, d.entity_type, s.state, d.postcode, d.location_name, d.ingested_at
            FROM eco_resilience.gold.business_details d
            LEFT JOIN eco_resilience.silver.abn_lookup_structured s ON d.abn = s.abn
            WHERE d.abn = {abn}
            LIMIT 1
        """

        response = w.statement_execution.execute_statement(
            statement=sql,
            warehouse_id=WAREHOUSE_ID,
            wait_timeout="30s",
        )

        if response.status.state == StatementState.SUCCEEDED:
            rows = []
            if response.result and response.result.data_array:
                rows = response.result.data_array
            logger.info(f"[business-details] ABN={abn} returned {len(rows)} rows")
            return jsonify({"status": "success", "rows": rows, "row_count": len(rows)})
        elif response.status.state == StatementState.FAILED:
            error_msg = response.status.error.message if response.status.error else "Unknown error"
            logger.error(f"[business-details] Query FAILED for ABN={abn}: {error_msg}")
            return jsonify({"error": f"Query failed: {error_msg}"}), 500
        else:
            logger.error(f"[business-details] Unexpected state for ABN={abn}: {response.status.state}")
            return jsonify({"error": f"Unexpected state: {response.status.state}"}), 500

    except Exception as e:
        logger.error(f"[business-details] Exception for ABN={abn}: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/check-freshness", methods=["POST"])
def check_freshness():
    """Check if an ABN's business details are less than 24 hours old.
    Now checks gold.business_details instead of risk scores.
    """
    try:
        data = request.get_json()
        abn = data.get("abn", "").replace(" ", "").replace("-", "")

        logger.info(f"[check-freshness] ABN={abn}")

        if not abn or not abn.isdigit():
            return jsonify({"error": "Valid ABN required"}), 400

        sql = f"""
            SELECT abn, ingested_at,
                   CASE WHEN ingested_at > current_timestamp() - INTERVAL 24 HOURS 
                        THEN true ELSE false END AS is_fresh
            FROM eco_resilience.gold.business_details
            WHERE abn = {abn}
            LIMIT 1
        """

        response = w.statement_execution.execute_statement(
            statement=sql,
            warehouse_id=WAREHOUSE_ID,
            wait_timeout="30s",
        )

        if response.status.state == StatementState.SUCCEEDED:
            rows = []
            if response.result and response.result.data_array:
                rows = response.result.data_array

            if rows and len(rows) > 0:
                is_fresh = rows[0][2] == "true"
                logger.info(f"[check-freshness] ABN={abn} exists=True is_fresh={is_fresh} ingested_at={rows[0][1]}")
                return jsonify({
                    "status": "success",
                    "exists": True,
                    "is_fresh": is_fresh,
                    "ingested_at": rows[0][1]
                })
            else:
                logger.info(f"[check-freshness] ABN={abn} exists=False")
                return jsonify({
                    "status": "success",
                    "exists": False,
                    "is_fresh": False,
                    "ingested_at": None
                })
        else:
            return jsonify({"status": "success", "exists": False, "is_fresh": False, "ingested_at": None})

    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ========== Supplier Management Endpoints ==========

@app.route("/api/supplier/lookup", methods=["POST"])
def supplier_lookup():
    """Look up a single supplier ABN - reads from gold.business_details joined with silver."""
    try:
        data = request.get_json()
        abn = data.get("abn", "").replace(" ", "").replace("-", "")

        logger.info(f"[supplier/lookup] ABN={abn}")

        if not abn or not abn.isdigit() or len(abn) != 11:
            logger.warning(f"[supplier/lookup] Invalid ABN: {abn}")
            return jsonify({"error": "A valid 11-digit ABN is required"}), 400

        sql = f"""
            SELECT s.abn, s.organisation_name, s.status, s.entity_type, s.state, s.postcode
            FROM eco_resilience.silver.abn_lookup_structured s
            WHERE s.abn = {abn}
            LIMIT 1
        """

        response = w.statement_execution.execute_statement(
            statement=sql,
            warehouse_id=WAREHOUSE_ID,
            wait_timeout="30s",
        )

        if response.status.state == StatementState.SUCCEEDED:
            rows = []
            if response.result and response.result.data_array:
                rows = response.result.data_array
            logger.info(f"[supplier/lookup] ABN={abn} returned {len(rows)} rows")
            return jsonify({"status": "success", "rows": rows, "row_count": len(rows)})
        elif response.status.state == StatementState.FAILED:
            error_msg = response.status.error.message if response.status.error else "Unknown error"
            logger.error(f"[supplier/lookup] Query FAILED for ABN={abn}: {error_msg}")
            return jsonify({"error": f"Query failed: {error_msg}"}), 500
        else:
            logger.error(f"[supplier/lookup] Unexpected state for ABN={abn}: {response.status.state}")
            return jsonify({"error": f"Unexpected state: {response.status.state}"}), 500

    except Exception as e:
        logger.error(f"[supplier/lookup] Exception for ABN={abn}: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/supplier/bulk-lookup", methods=["POST"])
def supplier_bulk_lookup():
    """Look up multiple supplier ABNs from a comma-separated list."""
    try:
        data = request.get_json()
        raw_abns = data.get("abns", "")

        # Parse and validate
        abn_list = [a.strip().replace(" ", "").replace("-", "") for a in raw_abns.split(",") if a.strip()]
        valid_abns = [a for a in abn_list if a.isdigit() and len(a) == 11]
        invalid_abns = [a for a in abn_list if not (a.isdigit() and len(a) == 11)]

        if not valid_abns:
            return jsonify({"error": "No valid 11-digit ABNs provided"}), 400

        in_clause = ", ".join(valid_abns)
        sql = f"""
            SELECT s.abn, s.organisation_name, s.status, s.entity_type, s.state, s.postcode
            FROM eco_resilience.silver.abn_lookup_structured s
            WHERE s.abn IN ({in_clause})
        """

        response = w.statement_execution.execute_statement(
            statement=sql,
            warehouse_id=WAREHOUSE_ID,
            wait_timeout="30s",
        )

        if response.status.state == StatementState.SUCCEEDED:
            rows = []
            if response.result and response.result.data_array:
                rows = response.result.data_array
            return jsonify({
                "status": "success",
                "rows": rows,
                "row_count": len(rows),
                "invalid_abns": invalid_abns
            })
        elif response.status.state == StatementState.FAILED:
            error_msg = response.status.error.message if response.status.error else "Unknown error"
            return jsonify({"error": f"Query failed: {error_msg}"}), 500
        else:
            return jsonify({"error": f"Unexpected state: {response.status.state}"}), 500

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/supplier/add", methods=["POST"])
def add_supplier():
    """Add one or more suppliers to the user's supplier list in supplier_relationships table."""
    try:
        data = request.get_json()
        user_abn = str(data.get("user_abn", "")).replace(" ", "").replace("-", "")
        suppliers = data.get("suppliers", [])

        logger.info(f"[supplier/add] user_abn={user_abn} suppliers_count={len(suppliers)}")

        if not user_abn or not user_abn.isdigit() or len(user_abn) != 11:
            return jsonify({"error": "Valid user_abn is required"}), 400

        if not suppliers:
            return jsonify({"error": "At least one supplier is required"}), 400

        # Build VALUES clause for batch insert
        value_rows = []
        for s in suppliers:
            s_abn = str(s.get("abn", "")).replace(" ", "").replace("-", "")
            s_name = str(s.get("name", "")).replace("'", "''")
            s_status = str(s.get("status", "")).replace("'", "''")
            s_entity_type = str(s.get("entity_type", "")).replace("'", "''")
            s_state = str(s.get("state", "")).replace("'", "''")
            s_postcode = s.get("postcode")
            postcode_val = str(s_postcode) if s_postcode and str(s_postcode).isdigit() else "NULL"

            value_rows.append(
                f"({user_abn}, {s_abn}, '{s_name}', '{s_status}', "
                f"'{s_entity_type}', '{s_state}', {postcode_val}, current_timestamp(), 'ADD')"
            )

        values_sql = ", ".join(value_rows)
        sql = f"""
            INSERT INTO eco_resilience.gold.supplier_relationships_history
            (user_abn, supplier_abn, supplier_name, supplier_status, supplier_entity_type, supplier_state, supplier_postcode, added_at, action)
            VALUES {values_sql}
        """

        response = w.statement_execution.execute_statement(
            statement=sql,
            warehouse_id=WAREHOUSE_ID,
            wait_timeout="30s",
        )

        if response.status.state == StatementState.SUCCEEDED:
            return jsonify({"status": "success", "added_count": len(suppliers)})
        elif response.status.state == StatementState.FAILED:
            error_msg = response.status.error.message if response.status.error else "Unknown error"
            return jsonify({"error": f"Insert failed: {error_msg}"}), 500
        else:
            return jsonify({"error": f"Unexpected state: {response.status.state}"}), 500

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/suppliers/list", methods=["POST"])
def list_suppliers():
    """List all suppliers for a given user ABN - reads from supplier_relationships joined with gold.business_details."""
    try:
        data = request.get_json()
        user_abn = str(data.get("user_abn", "")).replace(" ", "").replace("-", "")

        logger.info(f"[suppliers/list] user_abn={user_abn}")

        logger.info(f"[suppliers/list] user_abn={user_abn}")

        if not user_abn or not user_abn.isdigit() or len(user_abn) != 11:
            logger.warning(f"[suppliers/list] Invalid user_abn: {user_abn}")
            return jsonify({"error": "Valid user_abn is required"}), 400

        sql = f"""
            SELECT sr.supplier_abn, sr.supplier_name, sr.supplier_status, sr.supplier_entity_type,
                   sr.supplier_state, sr.supplier_postcode, sr.added_at,
                   bd.location_name
            FROM eco_resilience.gold.supplier_relationships sr
            LEFT JOIN eco_resilience.gold.business_details bd ON sr.supplier_abn = bd.abn
            WHERE sr.user_abn = {user_abn}
            ORDER BY sr.added_at DESC
        """

        perf_checkpoint("pre_sql_exec")
        response = w.statement_execution.execute_statement(
            statement=sql,
            warehouse_id=WAREHOUSE_ID,
            wait_timeout="30s",
        )
        perf_checkpoint("post_sql_exec")

        if response.status.state == StatementState.SUCCEEDED:
            rows = []
            if response.result and response.result.data_array:
                rows = response.result.data_array
            logger.info(f"[suppliers/list] user_abn={user_abn} returned {len(rows)} rows")
            return jsonify({"status": "success", "rows": rows, "row_count": len(rows)})
        elif response.status.state == StatementState.FAILED:
            error_msg = response.status.error.message if response.status.error else "Unknown error"
            logger.error(f"[suppliers/list] Query FAILED for user_abn={user_abn}: {error_msg}")
            return jsonify({"error": f"Query failed: {error_msg}"}), 500
        else:
            logger.error(f"[suppliers/list] Unexpected state for user_abn={user_abn}: {response.status.state}")
            return jsonify({"error": f"Unexpected state: {response.status.state}"}), 500

    except Exception as e:
        logger.error(f"[suppliers/list] Exception for user_abn={user_abn}: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/supplier/remove", methods=["POST"])
def remove_supplier():
    """Remove a supplier from the user's supplier list."""
    try:
        data = request.get_json()
        user_abn = str(data.get("user_abn", "")).replace(" ", "").replace("-", "")
        supplier_abn = str(data.get("supplier_abn", "")).replace(" ", "").replace("-", "")

        logger.info(f"[supplier/remove] user_abn={user_abn} supplier_abn={supplier_abn}")

        if not user_abn or not user_abn.isdigit() or len(user_abn) != 11:
            return jsonify({"error": "Valid user_abn is required"}), 400

        if not supplier_abn or not supplier_abn.isdigit() or len(supplier_abn) != 11:
            return jsonify({"error": "Valid supplier_abn is required"}), 400

        sql = f"""
            INSERT INTO eco_resilience.gold.supplier_relationships_history
            (user_abn, supplier_abn, supplier_name, supplier_status, supplier_entity_type, supplier_state, supplier_postcode, added_at, action)
            VALUES ({user_abn}, {supplier_abn}, '', '', '', '', NULL, current_timestamp(), 'REMOVE')
        """

        response = w.statement_execution.execute_statement(
            statement=sql,
            warehouse_id=WAREHOUSE_ID,
            wait_timeout="30s",
        )

        if response.status.state == StatementState.SUCCEEDED:
            return jsonify({"status": "success", "removed_abn": supplier_abn})
        elif response.status.state == StatementState.FAILED:
            error_msg = response.status.error.message if response.status.error else "Unknown error"
            return jsonify({"error": f"Delete failed: {error_msg}"}), 500
        else:
            return jsonify({"error": f"Unexpected state: {response.status.state}"}), 500

    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/health", methods=["GET"])
def health():
    """Health check endpoint."""
    return jsonify({
        "status": "healthy",
        "warehouse_id": WAREHOUSE_ID,
        "job_id": JOB_ID
    })




# ========== Nearby Hazards Endpoint ==========

@app.route("/api/nearby-hazards", methods=["POST"])
def nearby_hazards():
    """Return active hazards for a given postcode."""
    try:
        data = request.get_json()
        postcode = str(data.get("postcode", "")).strip()

        if not postcode or not postcode.isdigit():
            return jsonify({"error": "Valid postcode required"}), 400

        logger.info(f"[nearby-hazards] postcode={postcode}")

        sql = f"""
            SELECT DISTINCT
                h.hazard_type,
                h.display_name,
                h.headline,
                h.is_major,
                h.impacting_network,
                ROUND(h.latitude, 4) as lat,
                ROUND(h.longitude, 4) as lng,
                h.roads_json
            FROM eco_resilience.silver.poa_h3_lookup p
            INNER JOIN eco_resilience.silver.hazards_current h
                ON p.h3_cell = h.h3_cell AND h.ended = false
            WHERE p.poa_code = '{postcode}'
            LIMIT 20
        """

        response = w.statement_execution.execute_statement(
            statement=sql,
            warehouse_id=WAREHOUSE_ID,
            wait_timeout="50s",
        )

        if response.status.state == StatementState.SUCCEEDED:
            rows = response.result.data_array if response.result and response.result.data_array else []
            hazards = []
            for r in rows:
                # Extract road name from roads_json
                road_name = ""
                if r[7]:
                    try:
                        import json
                        roads = json.loads(r[7])
                        if roads and len(roads) > 0:
                            rd = roads[0]
                            main = rd.get("mainStreet", "")
                            suburb = rd.get("suburb", "")
                            cross = rd.get("crossStreet", "")
                            if main and suburb:
                                road_name = f"{main}, {suburb}"
                            elif main:
                                road_name = main
                            if cross:
                                road_name += f" (near {cross})"
                    except Exception:
                        pass
                hazards.append({
                    "type": r[0] or "",
                    "name": r[1] or "",
                    "headline": r[2] or "",
                    "is_major": r[3] == "true",
                    "impacting_network": r[4] == "true",
                    "lat": float(r[5]) if r[5] else None,
                    "lng": float(r[6]) if r[6] else None,
                    "road": road_name
                })
            logger.info(f"[nearby-hazards] postcode={postcode} returned {len(hazards)} hazards")
            return jsonify({"status": "success", "hazards": hazards, "postcode": postcode})
        else:
            error_msg = ""
            if response.status.state == StatementState.FAILED:
                error_msg = response.status.error.message if response.status.error else "Unknown"
            return jsonify({"error": f"Query failed: {error_msg}"}), 500

    except Exception as e:
        logger.error(f"[nearby-hazards] Error: {type(e).__name__}: {e}")
        return jsonify({"error": str(e)}), 500


# ========== ACE Recovery Assistant ==========

def get_ace_client():
    """Return an OpenAI-compatible client for the ACE agent endpoint."""
    return w.serving_endpoints.get_open_ai_client()


def extract_chat_content(response):
    """Extract assistant text from Databricks agent or OpenAI-style responses."""
    if hasattr(response, "messages") and response.messages:
        parts = []
        for msg in response.messages:
            if isinstance(msg, dict):
                content = msg.get("content")
            else:
                content = getattr(msg, "content", None)
            if isinstance(content, list):
                for item in content:
                    if isinstance(item, dict) and item.get("type") == "text":
                        parts.append(item.get("text", ""))
            elif content:
                parts.append(str(content))
        joined = "\n".join(part for part in parts if part).strip()
        if joined:
            return joined

    choices = getattr(response, "choices", None)
    if choices:
        message = getattr(choices[0], "message", None)
        if message and getattr(message, "content", None):
            return str(message.content).strip()

    return "I've reviewed your recovery context, but I couldn't format a reply just now. Please try again."


def execute_sql_statement(statement, wait_timeout="50s"):
    """Run a SQL statement through the configured warehouse."""
    return w.statement_execution.execute_statement(
        statement=statement,
        warehouse_id=WAREHOUSE_ID,
        wait_timeout=wait_timeout,
    )


def get_business_risk_context(postcode):
    """Collect a concise risk snapshot for a postcode from the same data used by the app."""
    context = {
        "postcode": postcode,
        "risk_level": "Unknown",
        "total_cells": 0,
        "at_risk_cells": 0,
        "critical_cells": 0,
        "risk_pct": 0,
        "hazards": [],
        "weather": None,
    }

    if not postcode or not str(postcode).isdigit():
        return context

    postcode = str(postcode).strip()

    try:
        risk_sql = f"""
            WITH cell_risk AS (
                SELECT
                    p.h3_cell,
                    MAX(
                        CASE
                            WHEN h.hazard_type IN ('flood', 'fire') THEN 3
                            WHEN h.impacting_network = true THEN 2
                            WHEN h.hazard_type IS NOT NULL THEN 1
                            ELSE 0
                        END
                    ) AS risk
                FROM eco_resilience.silver.poa_h3_lookup p
                LEFT JOIN eco_resilience.silver.hazards_current h
                    ON p.h3_cell = h.h3_cell AND h.ended = false
                WHERE p.poa_code = '{postcode}'
                GROUP BY p.h3_cell
            )
            SELECT
                COUNT(*) AS total_cells,
                SUM(CASE WHEN risk > 0 THEN 1 ELSE 0 END) AS at_risk_cells,
                SUM(CASE WHEN risk >= 3 THEN 1 ELSE 0 END) AS critical_cells
            FROM cell_risk
        """
        risk_response = execute_sql_statement(risk_sql)
        if risk_response.status.state == StatementState.SUCCEEDED:
            rows = risk_response.result.data_array if risk_response.result and risk_response.result.data_array else []
            if rows:
                total_cells = int(rows[0][0] or 0)
                at_risk_cells = int(rows[0][1] or 0)
                critical_cells = int(rows[0][2] or 0)
                risk_pct = round((at_risk_cells / total_cells) * 100) if total_cells else 0

                risk_level = "Low"
                if critical_cells > 0:
                    risk_level = "Critical"
                elif risk_pct > 20:
                    risk_level = "High"
                elif at_risk_cells > 0:
                    risk_level = "Moderate"

                context.update({
                    "risk_level": risk_level,
                    "total_cells": total_cells,
                    "at_risk_cells": at_risk_cells,
                    "critical_cells": critical_cells,
                    "risk_pct": risk_pct,
                })
    except Exception as e:
        logger.warning(f"[ace-risk-context] Risk summary unavailable for postcode={postcode}: {e}")

    try:
        hazards_sql = f"""
            SELECT DISTINCT
                h.hazard_type,
                COALESCE(h.display_name, h.headline) AS hazard_label,
                h.is_major,
                h.impacting_network
            FROM eco_resilience.silver.poa_h3_lookup p
            INNER JOIN eco_resilience.silver.hazards_current h
                ON p.h3_cell = h.h3_cell AND h.ended = false
            WHERE p.poa_code = '{postcode}'
            LIMIT 5
        """
        hazard_response = execute_sql_statement(hazards_sql)
        if hazard_response.status.state == StatementState.SUCCEEDED:
            rows = hazard_response.result.data_array if hazard_response.result and hazard_response.result.data_array else []
            hazards = []
            for row in rows:
                hazard_type = str(row[0] or "hazard").strip()
                hazard_label = str(row[1] or "").strip()
                severity = "major" if str(row[2]).lower() == "true" else "network" if str(row[3]).lower() == "true" else "active"
                label = hazard_label or hazard_type.replace('_', ' ')
                hazards.append(f"{hazard_type}: {label} ({severity})")
            context["hazards"] = hazards
    except Exception as e:
        logger.warning(f"[ace-risk-context] Hazards unavailable for postcode={postcode}: {e}")

    try:
        weather_sql = f"""
            SELECT
                m.nearest_weather_location,
                w.precipitation_mm,
                w.temperature_c,
                w.windspeed_kmh,
                w.humidity_pct,
                w.weather_code
            FROM eco_resilience.silver.poa_to_weather_location m
            INNER JOIN eco_resilience.silver.weather_current w
                ON w.location_name = m.nearest_weather_location
            WHERE m.poa_code = '{postcode}'
            ORDER BY w.forecast_time ASC
            LIMIT 1
        """
        weather_response = execute_sql_statement(weather_sql)
        if weather_response.status.state == StatementState.SUCCEEDED:
            rows = weather_response.result.data_array if weather_response.result and weather_response.result.data_array else []
            if rows:
                row = rows[0]
                context["weather"] = {
                    "station": row[0] or "",
                    "precipitation_mm": float(row[1]) if row[1] is not None else None,
                    "temperature_c": float(row[2]) if row[2] is not None else None,
                    "windspeed_kmh": float(row[3]) if row[3] is not None else None,
                    "humidity_pct": float(row[4]) if row[4] is not None else None,
                    "weather_code": row[5] if row[5] is not None else None,
                }
    except Exception as e:
        logger.warning(f"[ace-risk-context] Weather unavailable for postcode={postcode}: {e}")

    return context


def format_risk_context(context):
    """Format the risk snapshot into a compact prompt-friendly string."""
    hazards = context.get("hazards") or []
    weather = context.get("weather") or {}

    weather_parts = []
    if weather.get("station"):
        weather_parts.append(f"station={weather['station']}")
    if weather.get("precipitation_mm") is not None:
        weather_parts.append(f"precipitation_mm={weather['precipitation_mm']}")
    if weather.get("temperature_c") is not None:
        weather_parts.append(f"temperature_c={weather['temperature_c']}")
    if weather.get("windspeed_kmh") is not None:
        weather_parts.append(f"windspeed_kmh={weather['windspeed_kmh']}")

    weather_text = ", ".join(weather_parts) if weather_parts else "unavailable"
    hazards_text = "; ".join(hazards) if hazards else "no active hazards reported"

    return (
        f"overall_risk={context.get('risk_level', 'Unknown')}; "
        f"total_cells={context.get('total_cells', 0)}; "
        f"at_risk_cells={context.get('at_risk_cells', 0)}; "
        f"critical_cells={context.get('critical_cells', 0)}; "
        f"risk_pct={context.get('risk_pct', 0)}; "
        f"active_hazards={hazards_text}; "
        f"weather={weather_text}"
    )


def build_ace_prompt_messages(abn, business_name, postcode, history, user_message):
    """Construct the chat payload for ACE with actual risk context."""
    risk_context = get_business_risk_context(postcode)
    history = history if isinstance(history, list) else []

    prompt_messages = [
        {
            "role": "system",
            "content": (
                "You are ACE, the EcoResilience recovery assistant. Provide concise, practical disaster recovery guidance tailored to the supplied risk context. "
                "Reference the current risk situation directly when it is available, prioritise immediate actions, and avoid generic placeholder introductions. "
                f"The user has completed a risk assessment. Business context: name={business_name or 'Unknown'}, ABN={abn}, postcode={postcode or 'Unknown'}. "
                f"Current risk context: {format_risk_context(risk_context)}"
            )
        }
    ]

    for item in history[-8:]:
        role = item.get("role") if isinstance(item, dict) else None
        content = str(item.get("content", "")).strip() if isinstance(item, dict) else ""
        if role in {"user", "assistant"} and content:
            prompt_messages.append({"role": role, "content": content})

    prompt_messages.append({"role": "user", "content": user_message})
    return prompt_messages, risk_context


@app.route("/api/ace-opening", methods=["POST"])
def ace_opening():
    """Generate the opening ACE message from real business risk context."""
    try:
        data = request.get_json() or {}
        abn = str(data.get("abn", "")).replace(" ", "").replace("-", "")
        business_name = str(data.get("business_name", "")).strip()
        postcode = str(data.get("postcode", "")).strip()

        if not abn or not abn.isdigit() or len(abn) != 11:
            return jsonify({"error": "Valid ABN required"}), 400

        opening_instruction = (
            "Open the conversation with a concise risk briefing for this business. "
            "Describe the current risk situation using the supplied context, mention the most important immediate recovery priorities, "
            "and invite follow-up questions. Keep it practical and under 120 words."
        )

        prompt_messages, risk_context = build_ace_prompt_messages(
            abn=abn,
            business_name=business_name,
            postcode=postcode,
            history=[],
            user_message=opening_instruction,
        )

        client = get_ace_client()
        response = client.chat.completions.create(
            model=ACE_ENDPOINT_NAME,
            messages=prompt_messages,
        )

        assistant_message = extract_chat_content(response)
        logger.info(f"[ace-opening] ABN={abn} postcode={postcode or 'unknown'}")
        return jsonify({"status": "success", "message": assistant_message, "risk_context": risk_context})

    except Exception as e:
        logger.error(f"[ace-opening] Error: {type(e).__name__}: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/ace-chat", methods=["POST"])
def ace_chat():
    """Send a Recovery Assistant chat message to the ACE agent endpoint."""
    try:
        data = request.get_json() or {}
        abn = str(data.get("abn", "")).replace(" ", "").replace("-", "")
        message = str(data.get("message", "")).strip()
        history = data.get("history", []) or []
        business_name = str(data.get("business_name", "")).strip()
        postcode = str(data.get("postcode", "")).strip()

        if not abn or not abn.isdigit() or len(abn) != 11:
            return jsonify({"error": "Valid ABN required"}), 400

        if not message:
            return jsonify({"error": "message is required"}), 400

        prompt_messages, risk_context = build_ace_prompt_messages(
            abn=abn,
            business_name=business_name,
            postcode=postcode,
            history=history,
            user_message=message,
        )

        client = get_ace_client()
        response = client.chat.completions.create(
            model=ACE_ENDPOINT_NAME,
            messages=prompt_messages,
        )

        assistant_message = extract_chat_content(response)
        logger.info(f"[ace-chat] ABN={abn} prompt_len={len(message)} history_items={len(history)}")
        return jsonify({"status": "success", "message": assistant_message, "risk_context": risk_context})

    except Exception as e:
        logger.error(f"[ace-chat] Error: {type(e).__name__}: {e}")
        return jsonify({"error": str(e)}), 500


# ========== H3 Map Endpoint ==========

def parse_wkt_polygon(wkt):
    """Parse WKT POLYGON into [[lat, lng], ...] for Leaflet (swap lon/lat -> lat/lng)."""
    match = wkt.replace("POLYGON((", "").replace("))", "")
    coords = []
    for pair in match.split(","):
        parts = pair.strip().split(" ")
        if len(parts) == 2:
            lng, lat = float(parts[0]), float(parts[1])
            coords.append([lat, lng])
    return coords


@app.route("/api/h3-cells", methods=["POST"])
def get_h3_cells():
    """Return H3 cells with pre-computed boundaries, risk levels, and area weather."""
    try:
        data = request.get_json()
        postcode = str(data.get("postcode", "")).strip()

        if not postcode or not postcode.isdigit():
            return jsonify({"error": "Valid postcode required"}), 400

        logger.info(f"[h3-cells] postcode={postcode}")

        sql = f"""
            SELECT
                h3_h3tostring(p.h3_cell) AS h3_index,
                h3_boundaryaswkt(p.h3_cell) AS boundary_wkt,
                COALESCE(
                    CASE
                        WHEN h.is_major = true THEN 3
                        WHEN h.hazard_type IN ('flood', 'fire') THEN 3
                        WHEN h.impacting_network = true THEN 2
                        WHEN h.h3_cell IS NOT NULL THEN 1
                    END, 0
                ) AS risk
            FROM eco_resilience.silver.poa_h3_lookup p
            LEFT JOIN eco_resilience.silver.hazards_current h
                ON p.h3_cell = h.h3_cell AND h.ended = false
            WHERE p.poa_code = '{postcode}'
            LIMIT 1500
        """

        response = w.statement_execution.execute_statement(
            statement=sql,
            warehouse_id=WAREHOUSE_ID,
            wait_timeout="50s",
        )

        if response.status.state == StatementState.SUCCEEDED:
            rows = response.result.data_array if response.result and response.result.data_array else []
            cells = []
            for r in rows:
                if r[0] and r[1]:
                    coords = parse_wkt_polygon(r[1])
                    if coords:
                        risk = int(r[2]) if r[2] else 0
                        cells.append({"h3": r[0], "risk": risk, "boundary": coords})

            # Fetch area weather via nearest weather station
            weather = None
            try:
                weather_sql = f"""
                    SELECT w.precipitation_mm, w.temperature_c, w.windspeed_kmh,
                           w.location_name, w.forecast_time
                    FROM eco_resilience.silver.poa_to_weather_location m
                    INNER JOIN eco_resilience.silver.weather_current w
                        ON w.location_name = m.nearest_weather_location
                    WHERE m.poa_code = '{postcode}'
                      AND w.forecast_time <= current_timestamp()
                    ORDER BY w.forecast_time DESC
                    LIMIT 1
                """
                w_resp = w.statement_execution.execute_statement(
                    statement=weather_sql,
                    warehouse_id=WAREHOUSE_ID,
                    wait_timeout="50s",
                )
                if w_resp.status.state == StatementState.SUCCEEDED:
                    w_rows = w_resp.result.data_array if w_resp.result and w_resp.result.data_array else []
                    if w_rows:
                        weather = {
                            "precipitation_mm": float(w_rows[0][0]) if w_rows[0][0] else 0,
                            "temperature_c": float(w_rows[0][1]) if w_rows[0][1] else None,
                            "windspeed_kmh": float(w_rows[0][2]) if w_rows[0][2] else None,
                            "station": w_rows[0][3] or "",
                            "forecast_time": w_rows[0][4] or ""
                        }
            except Exception as we:
                logger.warning(f"[h3-cells] Weather fetch failed: {we}")

            logger.info(f"[h3-cells] postcode={postcode} returned {len(cells)} cells, weather={weather is not None}")
            return jsonify({"status": "success", "cells": cells, "postcode": postcode, "weather": weather})
        else:
            error_msg = ""
            if response.status.state == StatementState.FAILED:
                error_msg = response.status.error.message if response.status.error else "Unknown"
            logger.error(f"[h3-cells] SQL failed: {error_msg}")
            return jsonify({"error": f"Query failed: {error_msg}"}), 500

    except Exception as e:
        logger.error(f"[h3-cells] Error: {type(e).__name__}: {e}")
        return jsonify({"error": f"{type(e).__name__}: {str(e)}"}), 500




# ========== Performance Profiling Endpoints ==========

@app.route("/api/perf-log", methods=["POST"])
def perf_log_client():
    """Receive client-side performance timing data from the frontend."""
    try:
        data = request.get_json() or {}
        session_id = data.get("session_id", "unknown")
        stages = data.get("stages", [])
        total_ms = data.get("total_ms", 0)
        
        logger.info(f"[PERF-CLIENT] session={session_id} total={total_ms:.0f}ms stages={len(stages)}")
        for stage in stages:
            duration = stage.get("duration_ms", 0)
            name = stage.get("name", "?")
            detail = stage.get("detail", "")
            if duration > 3000:
                logger.warning(f"[PERF-CLIENT]   SLOW >> {name}: {duration:.0f}ms {detail}")
            else:
                logger.info(f"[PERF-CLIENT]   {name}: {duration:.0f}ms {detail}")
        
        # Store in perf log
        _record_perf({
            "ts": datetime.utcnow().isoformat() + "Z",
            "source": "client",
            "session_id": session_id,
            "total_ms": round(total_ms, 1),
            "stages": stages,
        })
        
        return jsonify({"status": "logged"})
    except Exception as e:
        logger.error(f"[perf-log] Error: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/perf-report", methods=["GET"])
def perf_report():
    """Return recent performance data for analysis."""
    source_filter = request.args.get("source", "")  # "client", "server", or "" for all
    limit = int(request.args.get("limit", "50"))
    
    entries = _perf_log[-limit:]
    if source_filter == "client":
        entries = [e for e in entries if e.get("source") == "client"]
    elif source_filter == "server":
        entries = [e for e in entries if e.get("source") != "client"]
    
    # Compute summary stats for server-side entries
    server_entries = [e for e in _perf_log if e.get("method")]
    endpoint_stats = defaultdict(list)
    for e in server_entries[-100:]:
        endpoint_stats[e["endpoint"]].append(e["total_ms"])
    
    summary = {}
    for ep, timings in endpoint_stats.items():
        summary[ep] = {
            "count": len(timings),
            "avg_ms": round(sum(timings) / len(timings), 1),
            "max_ms": round(max(timings), 1),
            "min_ms": round(min(timings), 1),
            "p95_ms": round(sorted(timings)[int(len(timings) * 0.95)] if len(timings) >= 2 else timings[0], 1),
        }
    
    return jsonify({
        "entries": entries,
        "summary": summary,
        "total_entries_in_buffer": len(_perf_log),
    })


# ========== Grant Draft Generation (Magic Moment) ==========
#
# Orchestrates eco_resilience_agent (Mosaic AI Serving) + silver.generate_grant_pdf
# (Unity Catalog SQL function) + Jinja2 + WeasyPrint to produce a downloadable
# DRFA grant application PDF. Consumes browser state from sections 1, 2, 3.

DISASTER_TYPES = {"flood", "fire", "storm", "earthquake", "drought", "cyclone"}


def _compose_grant_prompt(business, risk, chat_history, user_prompt):
    """Assemble all upstream context (sections 1, 2, 3) into ONE user message
    for eco_resilience_agent. The agent's system prompt handles tool selection;
    we just hand it a rich, single-turn prompt."""
    parts = [
        f"Business identity (verified): {business.get('name', 'Unknown')}, "
        f"ABN {business.get('abn', '')}, "
        f"{business.get('state', '')} {business.get('postcode', '')}. "
        f"Entity type: {business.get('entity_type', '')}.",
    ]

    if risk:
        hazards = risk.get("hazards") or []
        hazards_str = "; ".join(hazards) if hazards else "none reported"
        weather = risk.get("weather") or {}
        weather_parts = []
        if weather.get("precipitation_mm") is not None:
            weather_parts.append(f"{weather['precipitation_mm']}mm precipitation")
        if weather.get("temperature_c") is not None:
            weather_parts.append(f"{weather['temperature_c']}°C")
        if weather.get("windspeed_kmh") is not None:
            weather_parts.append(f"{weather['windspeed_kmh']}kmh wind")
        weather_str = ", ".join(weather_parts) or "unavailable"

        parts.append(
            f"Risk context: {risk.get('risk_level', 'Unknown')} risk level. "
            f"{risk.get('at_risk_cells', 0)} of {risk.get('total_cells', 0)} H3 cells affected. "
            f"Active hazards: {hazards_str}. "
            f"Weather: {weather_str}."
        )

    if chat_history:
        history_lines = ["User's prior conversation with the recovery assistant (recent turns):"]
        for turn in chat_history[-8:]:
            role = turn.get("role", "user")
            content = (turn.get("content") or "").strip()
            if content:
                history_lines.append(f"  {role}: {content[:300]}")
        parts.append("\n".join(history_lines))

    parts.append(f"User request: {user_prompt}")
    return "\n\n".join(parts)


def _call_eco_resilience_agent(full_user_message):
    """POST to the eco_resilience_agent endpoint, return assistant text.

    Uses `w.api_client.do()` — the SDK's authenticated HTTP client. This
    handles auth uniformly for:
    - Local dev: PAT via DATABRICKS_TOKEN env var
    - Lakehouse Apps: OAuth M2M via the app service principal
      (DATABRICKS_CLIENT_ID / DATABRICKS_CLIENT_SECRET auto-injected)
    - Any other auth method the SDK supports

    No need to manually construct headers; SDK injects the right Authorization
    header (and refreshes OAuth tokens) for every call.
    """
    payload = {"input": [{"role": "user", "content": full_user_message}]}
    path    = f"/serving-endpoints/{ECORESILIENCE_AGENT_ENDPOINT}/invocations"

    # Public attr in databricks-sdk >= 0.18; fall back to underscore-prefixed for older.
    api_client = getattr(w, "api_client", None) or getattr(w, "_api_client", None)
    if api_client is None:
        raise RuntimeError(
            "Cannot find SDK api_client (neither w.api_client nor w._api_client). "
            "databricks-sdk may be too old; upgrade in requirements.txt."
        )

    try:
        body = api_client.do(method="POST", path=path, body=payload)
    except Exception as e:
        raise RuntimeError(
            f"Agent endpoint call failed: {type(e).__name__}: {e}. "
            f"endpoint={ECORESILIENCE_AGENT_ENDPOINT!r}, "
            f"host={w.config.host!r}, "
            f"auth_type={getattr(w.config, 'auth_type', '?')!r}"
        )

    # ResponsesAgentResponse wire format: { output: [ { content: [ { text: "..." } ] } ] }
    try:
        return body["output"][0]["content"][0]["text"]
    except (KeyError, IndexError, TypeError) as e:
        raise RuntimeError(
            f"Unexpected response shape from agent endpoint: {type(e).__name__}: {e}. "
            f"Body type={type(body).__name__}, "
            f"keys={list(body.keys()) if isinstance(body, dict) else 'N/A'}"
        )


def _extract_estimated_loss(sources_user_first):
    """Find the user's claimed estimated loss amount.

    Strategy (in priority order, first hit wins):
      1. Look for a $-amount within 60 chars of a damage/loss/estimate keyword
         in a user-authored source. Most reliable — exactly what the claimant said.
      2. Fall back to any $-amount >= $1,000 in user-authored sources. Filters
         out per-unit figures and industry citations like "$104.8 billion".
      3. Fall back to any $-amount >= $1,000 anywhere (including agent text).
      4. Return 0.0 if nothing found.
    """
    money_re        = r"\$\s*([\d,]+(?:\.\d{1,2})?)"
    near_damage_re  = r"(?i)(?:damage|loss|estimate|claim|cost)[^\d$\n]{0,60}\$\s*([\d,]+(?:\.\d{1,2})?)"
    MIN_REASONABLE  = 1000.0   # ignore per-unit / industry-scale figures

    def _parse(raw):
        try:
            return float(raw.replace(",", ""))
        except ValueError:
            return None

    # Pass 1: user-authored sources, $-amount near a damage keyword
    for src in sources_user_first:
        if not src:
            continue
        m = re.search(near_damage_re, src)
        if m:
            v = _parse(m.group(1))
            if v is not None and v >= MIN_REASONABLE:
                return v

    # Pass 2: user-authored sources, any reasonable $-amount
    # (skip the agent_text which is the LAST element in sources_user_first)
    for src in sources_user_first[:-1]:
        if not src:
            continue
        for raw in re.findall(money_re, src):
            v = _parse(raw)
            if v is not None and v >= MIN_REASONABLE:
                return v

    # Pass 3: anywhere, any reasonable $-amount
    for src in sources_user_first:
        if not src:
            continue
        for raw in re.findall(money_re, src):
            v = _parse(raw)
            if v is not None and v >= MIN_REASONABLE:
                return v

    return 0.0


def _extract_grant_args(agent_text, chat_history, business, user_prompt):
    """Parse disaster_type, disaster_date, drfa_category, estimated_loss_aud,
    justification from the agent's response + chat history + section-4 prompt.

    Source priority: USER input first (most authoritative — they're the
    claimant), then agent narrative as fallback. Without this, the regex
    picks up agent-cited industry numbers like "$104.8 billion" and uses
    that as the estimated loss.
    """
    # Sources ordered by authority — user prompt (section 4) wins, then user
    # chat turns (section 3), then any chat content, then agent narrative.
    user_chat_text = "\n".join(
        (t.get("content") or "") for t in (chat_history or [])
        if t.get("role") == "user"
    )
    full_chat_text = "\n".join((t.get("content") or "") for t in (chat_history or []))
    sources_user_first = [
        user_prompt or "",
        user_chat_text,
        full_chat_text,
        agent_text or "",
    ]
    combined = "\n".join(s for s in sources_user_first if s)
    blob = combined.lower()

    # disaster_type — first known disaster keyword wins
    disaster_type = next((d for d in DISASTER_TYPES if d in blob), "flood")

    # disaster_date — search user-first sources for ISO YYYY-MM-DD
    disaster_date = date.today().isoformat()
    for src in sources_user_first:
        m = re.search(r"\b(20\d{2}-\d{2}-\d{2})\b", src)
        if m:
            disaster_date = m.group(1)
            break

    # drfa_category — find "Category X" pattern, X ∈ {A,B,C,D}; user-first
    drfa_category = "C"
    for src in sources_user_first:
        m = re.search(r"(?i)category\s+([abcd])\b", src)
        if m:
            drfa_category = m.group(1).upper()
            break

    # estimated_loss_aud — search user-first, prefer values near damage/loss
    # keywords, ignore amounts under $1,000 (these are usually per-unit figures
    # or industry-scale citations like "$104.8 billion").
    estimated_loss_aud = _extract_estimated_loss(sources_user_first)

    # justification — prefer a paragraph from the agent that talks about why
    # the applicant qualifies; otherwise take the first 800 chars of agent_text.
    narrative = (agent_text or "").strip()
    justification = narrative
    if len(narrative) > 800:
        para_match = re.search(
            r"(?is)([^\n]{40,800}(?:justif|recommend|claim|apply|seek|qualif|eligib)[^\n]{0,800})",
            narrative,
        )
        justification = para_match.group(1).strip() if para_match else narrative[:800]
    # Strip any UUID-like strings from the justification to avoid the agent's
    # internal application_id appearing in the PDF (we use Flask's UUID below).
    justification = re.sub(
        r"\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b",
        "[ref redacted]",
        justification,
    )

    return {
        "abn":                business.get("abn", ""),
        "entity_name":        business.get("name", ""),
        "entity_state":       business.get("state", ""),
        "entity_postcode":    business.get("postcode", ""),
        "disaster_type":      disaster_type,
        "disaster_date":      disaster_date,
        "drfa_category":      drfa_category,
        "estimated_loss_aud": estimated_loss_aud,
        "justification":      justification,
    }


def _sql_quote(s):
    """Single-quote escape for SQL string literals. Use only for trusted args."""
    return (s or "").replace("'", "''")


def _call_generate_grant_pdf_sql(abn, entity_name, entity_state, entity_postcode,
                                 disaster_type, disaster_date, drfa_category,
                                 estimated_loss_aud, justification):
    """Call eco_resilience.silver.generate_grant_pdf with deterministic args
    and return the STRUCT result as a flat dict suitable for the Jinja2 template.

    Returns dict with keys:
      application_id, draft_timestamp, applicant (dict), disaster (dict),
      grant_request (dict), status, next_steps (list), error (str|None)
    """
    sql = (
        "SELECT to_json(eco_resilience.silver.generate_grant_pdf("
        f"'{_sql_quote(abn)}', "
        f"'{_sql_quote(entity_name)}', "
        f"'{_sql_quote(entity_state)}', "
        f"'{_sql_quote(entity_postcode)}', "
        f"'{_sql_quote(disaster_type)}', "
        f"'{_sql_quote(disaster_date)}', "
        f"'{_sql_quote(drfa_category)}', "
        f"CAST({float(estimated_loss_aud)} AS DOUBLE), "
        f"'{_sql_quote(justification)}'"
        ")) AS result_json"
    )

    resp = w.statement_execution.execute_statement(
        statement=sql, warehouse_id=WAREHOUSE_ID, wait_timeout="50s"
    )
    if resp.status.state != StatementState.SUCCEEDED:
        error_msg = resp.status.error.message if resp.status.error else "unknown"
        raise RuntimeError(f"silver.generate_grant_pdf failed: {error_msg}")

    rows = resp.result.data_array if resp.result and resp.result.data_array else []
    if not rows:
        raise RuntimeError("silver.generate_grant_pdf returned no rows")

    grant_struct = json.loads(rows[0][0])

    # next_steps comes back as a list (good). Ensure it's never None for the template.
    grant_struct.setdefault("next_steps", [])
    grant_struct.setdefault("applicant", {})
    grant_struct.setdefault("disaster", {})
    grant_struct.setdefault("grant_request", {})
    return grant_struct


def _render_grant_pdf(grant_struct, agent_narrative):
    """Render the grant STRUCT as a PDF using fpdf2 (pure Python, no C deps).

    Replaced weasyprint+Jinja2 with fpdf2 because weasyprint needs libpango/
    libcairo/libgobject system libraries that aren't guaranteed in Lakehouse
    Apps containers. fpdf2 is pure Python and runs identically everywhere.

    Layout: A4, navy header, numbered sections, draft banner, footer.
    Visually clean rather than Tailwind-fancy — works reliably in any env.
    """
    from fpdf import FPDF

    # Color palette — keep navy/orange identity from the Tailwind UI mockup
    NAVY      = (15, 23, 42)       # #0f172a
    ORANGE    = (251, 146, 60)     # #fb923c
    SLATE_500 = (100, 116, 139)
    SLATE_700 = (51, 65, 85)
    SLATE_900 = (15, 23, 42)
    SLATE_50  = (248, 250, 252)
    SLATE_100 = (241, 245, 249)
    LIGHT_GRY = (226, 232, 240)
    BANNER_BG = (255, 247, 237)    # orange-50
    BANNER_TX = (154, 52, 18)      # orange-800

    applicant      = grant_struct.get("applicant") or {}
    disaster       = grant_struct.get("disaster") or {}
    grant_request  = grant_struct.get("grant_request") or {}
    next_steps     = grant_struct.get("next_steps") or []
    application_id = grant_struct.get("application_id", "unknown")
    draft_ts       = grant_struct.get("draft_timestamp", "")
    status         = grant_struct.get("status", "DRAFT")
    error_msg      = grant_struct.get("error")

    def _latin1(s):
        """fpdf2 with built-in Helvetica uses Latin-1; replace non-Latin-1 chars."""
        return (s or "").encode("latin-1", "replace").decode("latin-1")

    pdf = FPDF(format="A4", orientation="P", unit="mm")
    pdf.set_auto_page_break(auto=True, margin=20)
    pdf.add_page()
    pdf.set_margins(20, 20, 20)

    # ── Header bar (full-width navy strip) ──
    pdf.set_fill_color(*NAVY)
    pdf.rect(0, 0, 210, 18, style="F")
    pdf.set_fill_color(*ORANGE)
    pdf.rect(0, 18, 210, 1.5, style="F")  # orange accent line

    pdf.set_xy(15, 4)
    pdf.set_fill_color(*ORANGE)
    pdf.set_text_color(255, 255, 255)
    pdf.set_font("Helvetica", "B", 9)
    pdf.cell(10, 7, "ER", fill=True, align="C")

    pdf.set_xy(28, 4)
    pdf.set_font("Helvetica", "B", 13)
    pdf.set_text_color(255, 255, 255)
    pdf.cell(170, 7, "EcoResilience AI - DRFA Grant Application")

    pdf.set_xy(28, 11)
    pdf.set_font("Helvetica", "", 7)
    pdf.set_text_color(203, 213, 225)  # slate-300
    meta = f"Application ID: {application_id}    |    Generated: {draft_ts}    |    Status: {status}"
    pdf.cell(170, 4, _latin1(meta))

    # Reset position below header
    pdf.set_y(25)
    pdf.set_text_color(*SLATE_900)

    # ── DRAFT banner ──
    pdf.set_fill_color(*BANNER_BG)
    pdf.set_draw_color(*ORANGE)
    pdf.set_line_width(0.5)
    pdf.set_text_color(*BANNER_TX)
    pdf.set_font("Helvetica", "B", 9)
    pdf.cell(0, 5, " DRAFT - Not for Submission",
             fill=True, border="L", new_x="LMARGIN", new_y="NEXT")
    pdf.set_font("Helvetica", "", 8)
    pdf.set_text_color(*SLATE_700)
    pdf.multi_cell(0, 4, _latin1(
        " This document was prepared by EcoResilience AI based on verified business identity, "
        "real-time hazard data, and the Disaster Recovery Funding Arrangements (DRFA) guidelines. "
        "Please review every field with your records and supporting evidence before submitting "
        "to the National Emergency Management Agency (NEMA)."
    ), border="L", fill=True)
    pdf.ln(3)

    def section_header(num, title):
        pdf.ln(2)
        y0 = pdf.get_y()
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

    # ── Section 1: Applicant ──
    section_header(1, "Applicant Details")
    field_row("Business Name", applicant.get("entity_name"))
    field_row("Australian Business Number", applicant.get("abn"))
    field_row("State", applicant.get("state"))
    field_row("Postcode", applicant.get("postcode"))

    # ── Section 2: Disaster ──
    section_header(2, "Disaster Event")
    field_row("Disaster Type", (disaster.get("type") or "").capitalize())
    field_row("Disaster Date", disaster.get("date"))
    field_row("Located in NSW (DRFA-eligible)",
              "YES" if disaster.get("in_nsw") else "NO",
              value_color=(21, 128, 61) if disaster.get("in_nsw") else (180, 83, 9))

    # ── Section 3: Grant Request ──
    section_header(3, "Grant Request")
    field_row("DRFA Category",
              f"Category {grant_request.get('drfa_category', '-')}",
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
    just_text = grant_request.get("justification") or "(No justification narrative captured.)"
    pdf.multi_cell(0, 5, _latin1(just_text), border="L", fill=True)

    # ── Section 4: Next Steps ──
    section_header(4, "Next Steps for the Applicant")
    if next_steps:
        pdf.set_font("Helvetica", "", 9)
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

    # ── Section 5: Agent Narrative ──
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

    # ── Validation notes (only if status != DRAFT) ──
    if error_msg and status != "DRAFT":
        section_header("!", "Validation Notes")
        pdf.set_font("Helvetica", "", 9)
        pdf.set_text_color(127, 29, 29)   # red-900
        pdf.set_fill_color(254, 242, 242)  # red-50
        pdf.set_draw_color(185, 28, 28)    # red-700
        pdf.set_line_width(1.0)
        pdf.multi_cell(0, 5, _latin1(error_msg), border="L", fill=True)

    # ── Footer ──
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
    footer = (
        f"Application ID {application_id}  |  {draft_ts}  |  "
        "This document reflects information available at generation time. "
        "It is a DRAFT intended for human review. Submit only after verifying "
        "every field and attaching supporting evidence (damage photos, repair "
        "quotes, insurance correspondence, ATO primary-producer registration) "
        "per the cited DRFA category requirements."
    )
    pdf.multi_cell(0, 3.2, _latin1(footer))

    # fpdf2 2.7+ returns bytearray from output(); cast to bytes for Flask Response
    return bytes(pdf.output())


# ============================================================
# Lakebase — OLTP store for grant submission history
# ============================================================
# Persists each generated grant to a managed Postgres (Lakebase) table so the
# frontend can show "Your recent applications". This is the OLTP side of the
# lakehouse — same Unity Catalog governance, sub-100ms point reads.
#
# Auth: short-lived JWT minted by w.database.generate_database_credential().
# Lakebase rejects standard PATs/OAuth tokens — only its purpose-minted JWTs work.
# Write semantics: best-effort. If Lakebase is unavailable, the grant PDF still
# returns to the user; only the history row is missed.

def _lakebase_conn():
    """JWT-authenticated Postgres connection to Lakebase. Caller closes via with-statement."""
    # Late import so the rest of the app works even if psycopg2 isn't installed yet
    import psycopg2
    import uuid

    cred = w.database.generate_database_credential(
        request_id=str(uuid.uuid4()),
        instance_names=[LAKEBASE_INSTANCE],
    )
    return psycopg2.connect(
        host=LAKEBASE_HOST,
        port=5432,
        dbname=LAKEBASE_DB,
        user=LAKEBASE_USER,
        password=cred.token,
        sslmode="require",
    )


def _log_grant_to_lakebase(business: dict, grant_struct: dict, user_query: str) -> None:
    """Best-effort INSERT of one grant submission into Lakebase. Swallows errors."""
    if not LAKEBASE_HOST:
        return   # Lakebase not configured — silently skip
    try:
        with _lakebase_conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO grant_submissions
                  (abn, business_name, postcode, state, application_id, grant_status, user_query)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    business.get("abn"),
                    business.get("name"),
                    business.get("postcode"),
                    business.get("state"),
                    grant_struct.get("application_id"),
                    grant_struct.get("status"),
                    (user_query or "")[:500],   # truncate to keep row size sane
                ),
            )
        logger.info(f"[lakebase] logged grant app_id={grant_struct.get('application_id')}")
    except Exception as e:
        logger.warning(f"[lakebase] grant log failed (non-fatal): {type(e).__name__}: {e}")


@app.route("/api/grant-history", methods=["GET"])
def grant_history():
    """Return the last 20 grant submissions from Lakebase (newest first).

    Returns {submissions: []} if Lakebase isn't configured or the read fails,
    so the frontend can render gracefully either way.
    """
    if not LAKEBASE_HOST:
        return jsonify({"submissions": [], "lakebase_configured": False})

    try:
        with _lakebase_conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, abn, business_name, postcode, state,
                       application_id, grant_status, user_query,
                       generated_at
                FROM grant_submissions
                ORDER BY generated_at DESC
                LIMIT 20
                """
            )
            cols = [d.name for d in cur.description]
            rows = []
            for r in cur.fetchall():
                row = dict(zip(cols, r))
                # psycopg2 returns datetime — convert to ISO for JSON
                if row.get("generated_at"):
                    row["generated_at"] = row["generated_at"].isoformat()
                rows.append(row)
        return jsonify({"submissions": rows, "lakebase_configured": True})
    except Exception as e:
        logger.error(f"[lakebase] grant-history read failed: {type(e).__name__}: {e}")
        return jsonify({"submissions": [], "lakebase_configured": True, "error": str(e)}), 500


@app.route("/api/grant-draft", methods=["POST"])
def grant_draft():
    """Generate a downloadable DRFA grant application PDF.

    Request body:
      {
        "business":     { abn, name, state, postcode, entity_type },     // section 1
        "risk":         { risk_level, total_cells, at_risk_cells, ... }, // section 2
        "chat_history": [ {role, content}, ... ],                        // section 3
        "user_prompt":  "Describe what happened..."                      // section 4
      }
    Response: application/pdf bytes, with X-Application-Id + X-Grant-Status headers.
    """
    try:
        data = request.get_json() or {}
        business     = data.get("business", {}) or {}
        risk         = data.get("risk", {}) or {}
        chat_history = data.get("chat_history", []) or []
        user_prompt  = (data.get("user_prompt") or "").strip()

        abn = str(business.get("abn", "")).replace(" ", "").replace("-", "")
        if not (abn.isdigit() and len(abn) == 11):
            return jsonify({"error": "Valid 11-digit ABN required in business.abn"}), 400
        if not user_prompt:
            return jsonify({"error": "user_prompt is required"}), 400
        # Make sure business.abn is the cleaned form before we pass it down
        business["abn"] = abn

        logger.info(
            f"[grant-draft] start | ABN={abn} | "
            f"chat_turns={len(chat_history)} | prompt_len={len(user_prompt)}"
        )

        perf_checkpoint("pre_compose_prompt")

        # 1. Build the single context message for the agent
        full_user_message = _compose_grant_prompt(
            business=business, risk=risk,
            chat_history=chat_history, user_prompt=user_prompt,
        )
        perf_checkpoint("post_compose_prompt")

        # 2. Call our agent — does all the multi-tool reasoning
        agent_text = _call_eco_resilience_agent(full_user_message)
        perf_checkpoint("post_agent_call")

        # 3. Extract structured grant args from agent response + chat context
        grant_args = _extract_grant_args(
            agent_text=agent_text,
            chat_history=chat_history,
            business=business,
            user_prompt=user_prompt,
        )
        logger.info(
            f"[grant-draft] extracted | "
            f"disaster={grant_args['disaster_type']} | "
            f"date={grant_args['disaster_date']} | "
            f"cat={grant_args['drfa_category']} | "
            f"loss=${grant_args['estimated_loss_aud']:.0f}"
        )
        perf_checkpoint("post_extract_args")

        # 4. Deterministic SQL call — guaranteed STRUCT back
        grant_struct = _call_generate_grant_pdf_sql(**grant_args)
        perf_checkpoint("post_sql_call")

        # 5. Render PDF
        pdf_bytes = _render_grant_pdf(grant_struct, agent_text)
        perf_checkpoint("post_pdf_render")

        application_id = grant_struct.get("application_id", "unknown")
        status         = grant_struct.get("status", "DRAFT")
        logger.info(f"[grant-draft] done | app_id={application_id} | status={status} | bytes={len(pdf_bytes)}")

        # Best-effort persist to Lakebase OLTP store for the "Your recent applications" panel.
        # Doesn't raise — failures only log a warning; the PDF still returns to the user.
        _log_grant_to_lakebase(business=business, grant_struct=grant_struct, user_query=user_prompt)

        return Response(
            pdf_bytes,
            mimetype="application/pdf",
            headers={
                "Content-Disposition": f'attachment; filename="DRFA-{application_id[:8]}.pdf"',
                "X-Application-Id":    application_id,
                "X-Grant-Status":      status,
                "Cache-Control":       "no-store",
            },
        )
    except requests.RequestException as e:
        logger.error(f"[grant-draft] Agent HTTP error: {type(e).__name__}: {e}")
        return jsonify({"error": f"Agent endpoint unreachable: {e}"}), 502
    except RuntimeError as e:
        logger.error(f"[grant-draft] Runtime error: {e}")
        return jsonify({"error": str(e)}), 500
    except Exception as e:
        logger.error(f"[grant-draft] Unexpected error: {type(e).__name__}: {e}")
        return jsonify({"error": f"{type(e).__name__}: {e}"}), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True)
