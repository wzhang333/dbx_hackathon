"""
EcoResilience AI - Databricks App Backend
Serves the frontend and proxies Databricks API calls using the app's service principal.
"""

import os
import sys
import json
import logging
import time
from datetime import datetime
from flask import Flask, request, jsonify, send_from_directory
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

logger.info(f"App initialized | Warehouse: {WAREHOUSE_ID} | Job: {JOB_ID} | ACE Endpoint: {ACE_ENDPOINT_NAME}")

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


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True)
