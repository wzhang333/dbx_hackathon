# Databricks notebook source
# MAGIC %md
# MAGIC # 15 — AI Judge / Agent Evaluation Harness (Phase 4 Step 5)
# MAGIC
# MAGIC Quantitative evaluation of the deployed `eco_resilience_agent` v2 using
# MAGIC **Databricks Mosaic AI Agent Evaluation** (`mlflow.evaluate` with
# MAGIC `model_type="databricks-agent"`).
# MAGIC
# MAGIC ### Why this exists
# MAGIC
# MAGIC The deployed agent works end-to-end and produces the Magic Moment. But how
# MAGIC well does it really work? Does it cite the right DRFA page? Does it skip
# MAGIC NSW-only tools for non-NSW businesses? Does it hallucinate when the user
# MAGIC asks something ambiguous?
# MAGIC
# MAGIC Without numbers, the pitch is *"look at this cool thing"*.
# MAGIC With numbers, the pitch is *"this cool thing scores 88% on groundedness
# MAGIC and 92% on citation accuracy across 12 disaster-recovery scenarios — all
# MAGIC reproducible in MLflow"*.
# MAGIC
# MAGIC ### What this notebook produces
# MAGIC
# MAGIC 1. A versioned **golden dataset** of 12 test scenarios (saved as MLflow artifact)
# MAGIC 2. An MLflow run with **aggregate quantitative metrics** (correctness, relevance, safety, guideline adherence)
# MAGIC 3. **Per-case scores** + the judge LLM's rationale for each judgment
# MAGIC 4. A defensible **evaluation methodology** beat for the pitch
# MAGIC
# MAGIC ### Learning leverage
# MAGIC
# MAGIC This is the most transferable LLM-engineering skill in 2026. Every team
# MAGIC running LLMs in production needs to:
# MAGIC - Design a golden dataset for an agent
# MAGIC - Write custom guidelines as natural-language rules
# MAGIC - Interpret LLM-as-judge scores + read the judge's rationale
# MAGIC - Iterate: failing case → diagnose → fix prompt/tool → re-evaluate
# MAGIC
# MAGIC Skills you build here transfer to any future LLM project, any framework.
# MAGIC
# MAGIC ### Pre-requisites
# MAGIC
# MAGIC - The agent endpoint `eco_resilience_agent` is **deployed and warm**
# MAGIC - The secret `eco_resilience / abr_auth_guid` is set (so verify_abn works)
# MAGIC - You have CAN_QUERY on the agent endpoint
# MAGIC - You have EXECUTE on `eco_resilience.silver.generate_grant_pdf`
# MAGIC
# MAGIC ### Estimated wall time
# MAGIC
# MAGIC ~15-30 min total. Most of that is the agent calls running in §5.

# COMMAND ----------

# MAGIC %md
# MAGIC ## §1 — Install dependencies

# COMMAND ----------

# MAGIC %pip install -q --upgrade "mlflow[databricks]>=3.0" "databricks-agents>=0.20"

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

# MAGIC %md
# MAGIC ## §2 — Configuration
# MAGIC
# MAGIC Single place to tweak: catalog/schema names, agent endpoint, experiment to write to.

# COMMAND ----------

import os
import mlflow
import pandas as pd
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

CATALOG       = "eco_resilience"
SILVER_SCHEMA = "silver"
ENDPOINT_NAME = "eco_resilience_agent"
UC_MODEL_NAME = f"{CATALOG}.{SILVER_SCHEMA}.eco_resilience_agent"

# Point MLflow at Unity Catalog — same registry where the deployed agent lives.
mlflow.set_registry_uri("databricks-uc")

# Eval runs land in a DEDICATED experiment, separate from the agent's production
# inference traces. Why separate:
#   1. Eval runs don't get drowned by inference traces (prod fires hundreds per day)
#   2. v1 / v2 / vN eval campaigns compare cleanly side-by-side in one place
#   3. Self-documenting name vs opaque numeric ID
#   4. Production-realistic pattern: <model>_eval is its own experiment
#
# `set_experiment(experiment_name=...)` auto-creates the experiment if missing,
# idempotent on re-run. Path must be a workspace folder you can write to.
EVAL_EXPERIMENT_PATH = "/Shared/eco_resilience/eco_resilience_agent_eval"
experiment = mlflow.set_experiment(experiment_name=EVAL_EXPERIMENT_PATH)
EXPERIMENT_ID = experiment.experiment_id
print(f"Eval experiment: {EVAL_EXPERIMENT_PATH}")
print(f"            ID:  {EXPERIMENT_ID}")

# Set ABR_AUTH_GUID so any local-context invocation works. The DEPLOYED endpoint
# gets this via environment_vars at deploy time — we mirror it here for any
# fallback path that hits verify_abn outside the serving container.
os.environ["ABR_AUTH_GUID"] = dbutils.secrets.get(
    scope="eco_resilience", key="abr_auth_guid"
)

print(f"ENDPOINT_NAME = {ENDPOINT_NAME}")
print(f"EXPERIMENT_ID = {EXPERIMENT_ID}")
print(f"Registry URI  = {mlflow.get_registry_uri()}")
print(f"Workspace     = {w.config.host}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## §3 — Define the golden dataset (12 cases)
# MAGIC
# MAGIC Each case has three fields the Agent Evaluation framework consumes:
# MAGIC
# MAGIC | Field | Purpose |
# MAGIC |---|---|
# MAGIC | `request` | The user query the agent must respond to |
# MAGIC | `expected_facts` | Factual claims the response should contain (LLM judge does fuzzy semantic match) |
# MAGIC | `guidelines` | Natural-language behaviour rules the judge LLM verifies |
# MAGIC
# MAGIC The 12 cases below cover **6 dimensions** of agent behaviour:
# MAGIC
# MAGIC 1. **Happy path** (Magic Moment full flow)
# MAGIC 2. **Single-tool calls** (RAG only, industry only, weather, hazards, climate)
# MAGIC 3. **Multi-tool reasoning** (identity + industry + DRFA chained)
# MAGIC 4. **Non-NSW handling** (graceful degradation for VIC business)
# MAGIC 5. **Malformed input** (invalid ABN — must not crash or hallucinate)
# MAGIC 6. **Missing input** (no ABN provided — must ask, not fabricate)
# MAGIC
# MAGIC ### Design principles
# MAGIC
# MAGIC - `expected_facts` are **claims, not exact strings**. The judge does semantic
# MAGIC   matching, so "Bathurst Regional Council is verified" matches both
# MAGIC   "BATHURST REGIONAL COUNCIL" and "Bathurst Council was verified".
# MAGIC - `guidelines` are **unambiguous rules**. The judge follows them literally,
# MAGIC   so "must cite at least one DRFA PDF filename AND page number" works;
# MAGIC   "should look professional" doesn't.

# COMMAND ----------

EVAL_CASES = [
    # ─────────────────────────────────────────────────────────────────────
    # Case 1: Magic Moment happy path — the headline demo scenario
    # ─────────────────────────────────────────────────────────────────────
    {
        "request": (
            "Verify ABN 42173522302. The business is a dairy operation that has been "
            "affected by a flood on 2026-05-10 and estimates damages of around $48,500. "
            "Reason carefully across what you can verify, what NEMA rules apply, what "
            "evidence the applicant should attach, and prepare a draft grant "
            "application under the appropriate DRFA category."
        ),
        "expected_facts": [
            "Bathurst Regional Council is the verified entity",
            "The business is located in NSW, postcode 2795",
            "DRFA Category C or D is appropriate for the disaster",
            "The agent produces a structured DRAFT grant application",
        ],
        "guidelines": [
            "Must cite at least one DRFA PDF filename AND a page number",
            "Must include a draft application_id (UUID format)",
            "Must NOT include raw H3 cell IDs (long numeric strings starting with 6...)",
            "Must include an agriculture employment or revenue number (sector-wide figure)",
        ],
    },
    # ─────────────────────────────────────────────────────────────────────
    # Case 2: Pure DRFA RAG — no ABN required
    # ─────────────────────────────────────────────────────────────────────
    {
        "request": (
            "What is the difference between Category C and Category D disaster "
            "recovery assistance under the DRFA?"
        ),
        "expected_facts": [
            "Category C provides recovery grants for affected entities",
            "Category D is exceptional circumstances assistance with broader discretion",
            "Category D activation requires Australian Government agreement",
        ],
        "guidelines": [
            "Must cite at least one DRFA PDF filename AND a page number",
            "Should not require or ask for an ABN — the question is general policy",
        ],
    },
    # ─────────────────────────────────────────────────────────────────────
    # Case 3: Pure industry context — no ABN required
    # ─────────────────────────────────────────────────────────────────────
    {
        "request": "How big is Australia's agriculture industry in employment and revenue terms?",
        "expected_facts": [
            "Australian agriculture employment figure is in the hundreds of thousands",
            "Australian agriculture revenue or income figure is in the tens of billions or more",
        ],
        "guidelines": [
            "Numbers must be framed as SECTOR-WIDE TOTALS, not per-business figures",
            "Must NOT fabricate business count or per-business statistics (we don't have that data)",
        ],
    },
    # ─────────────────────────────────────────────────────────────────────
    # Case 4: Bathurst weather check — identity → postcode → weather
    # ─────────────────────────────────────────────────────────────────────
    {
        "request": (
            "Verify ABN 42173522302 and tell me the current weather conditions "
            "for the business location."
        ),
        "expected_facts": [
            "Business is located in NSW postcode 2795",
            "Response includes current weather data for Bathurst area",
            "Response mentions at least one of: temperature, precipitation, wind",
        ],
        "guidelines": [
            "Must NOT include raw H3 cell IDs",
        ],
    },
    # ─────────────────────────────────────────────────────────────────────
    # Case 5: Active hazards at Bathurst
    # ─────────────────────────────────────────────────────────────────────
    {
        "request": (
            "Verify ABN 42173522302. What active hazards are currently affecting "
            "this business location?"
        ),
        "expected_facts": [
            "Business is verified as Bathurst Regional Council in NSW 2795",
            "Response addresses whether hazards are active or not in the area",
        ],
        "guidelines": [
            "If no hazards are active, must say so explicitly — do not fabricate hazards",
            "Must cite real hazard data (not generic 'be careful' advice)",
        ],
    },
    # ─────────────────────────────────────────────────────────────────────
    # Case 6: Climate projection — long-term outlook
    # ─────────────────────────────────────────────────────────────────────
    {
        "request": (
            "Verify ABN 42173522302. What's the long-term climate outlook for this "
            "business — what should they plan for in the next 30-60 years?"
        ),
        "expected_facts": [
            "Business is in NSW postcode 2795",
            "Response references CSIRO 2020s vs 2080s climate projections",
            "Response mentions temperature change",
        ],
        "guidelines": [
            "Must reference CSIRO projection data, not generic climate change claims",
            "Must mention both moderate and high emissions scenarios (or note both exist)",
        ],
    },
    # ─────────────────────────────────────────────────────────────────────
    # Case 7: Invalid ABN — graceful error handling
    # ─────────────────────────────────────────────────────────────────────
    {
        "request": "Can you verify ABN 12345 for me?",
        "expected_facts": [
            "12345 is not a valid 11-digit ABN",
            "Response asks for a correct ABN or explains the format requirement",
        ],
        "guidelines": [
            "Must NOT crash or surface a Python traceback",
            "Must NOT fabricate a business identity for the invalid ABN",
        ],
    },
    # ─────────────────────────────────────────────────────────────────────
    # Case 8: Non-NSW ABN (Australia Post, VIC) — must skip NSW-only tools
    # ─────────────────────────────────────────────────────────────────────
    {
        "request": (
            "Verify ABN 28864970579 and tell me what weather and hazards I should be watching."
        ),
        "expected_facts": [
            "Australia Post is verified as the business",
            "Business is located in VIC (not NSW)",
            "Weather and hazard data is unavailable or limited because the data layer is NSW-only",
        ],
        "guidelines": [
            "Must explicitly say that weather/hazard tools are NSW-only when handling a non-NSW ABN",
            "Must NOT fabricate VIC weather or hazard data",
        ],
    },
    # ─────────────────────────────────────────────────────────────────────
    # Case 9: Multi-tool reasoning — identity + industry + DRFA chained
    # ─────────────────────────────────────────────────────────────────────
    {
        "request": (
            "Verify ABN 42173522302. The business is a dairy operation in flood-prone "
            "country. Give me both Australian agriculture industry context AND what "
            "disaster recovery assistance might be available under the DRFA."
        ),
        "expected_facts": [
            "Bathurst Regional Council is the verified entity",
            "Australian agriculture industry totals are quoted (employment and/or revenue)",
            "DRFA Category C or primary-producer assistance is described",
        ],
        "guidelines": [
            "Response must surface evidence of multi-source reasoning: the verified business entity name AND a sector-wide agriculture industry figure AND a DRFA citation with page number",
            "Industry numbers framed as sector-wide TOTALS, not per-business",
        ],
    },
    # ─────────────────────────────────────────────────────────────────────
    # Case 10: No-ABN request requiring an ABN — must ask, not fabricate
    # ─────────────────────────────────────────────────────────────────────
    {
        "request": "Help me apply for disaster recovery assistance.",
        "expected_facts": [
            "Response asks the user to provide an ABN before drafting an application",
        ],
        "guidelines": [
            "Must NOT proceed with a placeholder or fictional ABN",
            "Must explain why the ABN is needed (identity verification)",
        ],
    },
    # ─────────────────────────────────────────────────────────────────────
    # Case 11: Explicit grant request with structured details
    # ─────────────────────────────────────────────────────────────────────
    {
        "request": (
            "Verify ABN 42173522302 and prepare a DRFA Category C grant application "
            "for a flood on 2026-05-10 with estimated damages of $50,000 to a dairy operation."
        ),
        "expected_facts": [
            "Bathurst Regional Council identity is verified",
            "A draft grant application is produced under Category C",
            "Estimated loss reflected in the draft is approximately $50,000",
        ],
        "guidelines": [
            "Must surface an application_id (UUID format)",
            "Must explicitly say the application is a DRAFT for review, not submitted",
            "Must cite DRFA rules supporting the Category C choice",
        ],
    },
    # ─────────────────────────────────────────────────────────────────────
    # Case 12: Cross-domain general question — no specific business
    # ─────────────────────────────────────────────────────────────────────
  {
      "request": "I'm a dairy farmer in northern NSW worried about climate change. What should I plan for?",
      "expected_facts": [
          "Response asks the user for a specific postcode or location before giving climate-specific guidance",
      ],
      "guidelines": [
          "Must NOT pick an arbitrary postcode (e.g. Lismore, Coffs Harbour) as a proxy for 'northern NSW'",
          "Must NOT fabricate CSIRO projections without first calling get_climate_projection on a user-supplied postcode",
      ],
  },
]

# Convert to the pandas DataFrame shape mlflow.evaluate expects
eval_df = pd.DataFrame(EVAL_CASES)
print(f"Loaded {len(eval_df)} evaluation cases")
display(eval_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## §3b — Trajectory expectations + helpers
# MAGIC
# MAGIC The built-in `mlflow.evaluate(model_type="databricks-agent")` judges only the
# MAGIC FINAL response. It misses a critical class of failures: **the agent says
# MAGIC the right things but called the wrong tools (or no tools at all)**.
# MAGIC Trajectory evaluation closes that gap.
# MAGIC
# MAGIC ### What we score
# MAGIC
# MAGIC | Metric | Question it answers |
# MAGIC |---|---|
# MAGIC | `trajectory_recall` | Did the agent call all the tools we expected? |
# MAGIC | `trajectory_precision` | Of the tools it called, how many were expected? |
# MAGIC | `forbidden_called` | Did it call any tools it should have skipped? (e.g. NSW-only tools for a VIC business) |
# MAGIC
# MAGIC ### Why programmatic (not LLM-judged)
# MAGIC
# MAGIC - **Deterministic** — same trace → same score, no LLM randomness
# MAGIC - **Free** — no judge API calls
# MAGIC - **Fast** — set comparison, microseconds
# MAGIC - **Educational** — you can read the source and see exactly what was measured
# MAGIC
# MAGIC ### How we get the trace
# MAGIC
# MAGIC Our agent was deployed with `ENABLE_MLFLOW_TRACING=true` (set by
# MAGIC `databricks.agents.deploy()` automatically). That makes the endpoint
# MAGIC return the full MLflow trace **inline** in the response body under
# MAGIC `databricks_output.trace`. We parse `trace.data.spans` to extract
# MAGIC every tool that was invoked.

# COMMAND ----------

# The 7 tools the agent can call. Used for span filtering.
KNOWN_TOOLS = {
    "verify_abn",
    "get_weather_forecast",
    "get_active_hazards",
    "get_climate_projection",
    "query_nema_guidelines",
    "get_industry_context",
    "generate_grant_pdf",
}

# Per-case expected tools (set) — what the agent SHOULD call
# Keys are 1-based case indices matching EVAL_CASES.
# Use None to skip trajectory scoring (case where agent has discretion).
EXPECTED_TOOLS_BY_CASE = {
    # 1: Magic Moment — full multi-tool flow ideal path
    1:  {"verify_abn", "query_nema_guidelines", "get_industry_context",
         "get_active_hazards", "generate_grant_pdf"},
    # 2: Pure DRFA RAG — only needs the RAG tool
    2:  {"query_nema_guidelines"},
    # 3: Pure industry context — only needs the ABS tool
    3:  {"get_industry_context"},
    # 4: Weather check — verify first, then weather
    4:  {"verify_abn", "get_weather_forecast"},
    # 5: Hazards check
    5:  {"verify_abn", "get_active_hazards"},
    # 6: Climate projection
    6:  {"verify_abn", "get_climate_projection"},
    # 7: Invalid ABN — should still ATTEMPT verify_abn (which returns error)
    7:  {"verify_abn"},
    # 8: Non-NSW (VIC) — verify_abn yes, postcode-tools NO (see forbidden below)
    8:  {"verify_abn"},
    # 9: Multi-tool chained reasoning
    9:  {"verify_abn", "get_industry_context", "query_nema_guidelines"},
    # 10: No ABN provided — agent should ask, NOT call any tools
    10: set(),
    # 11: Explicit grant request
    11: {"verify_abn", "query_nema_guidelines", "generate_grant_pdf"},
    # 12: Cross-domain general question — agent has discretion, skip trajectory
    12: None,
}


EXPECTED_TOOLS_BY_CASE_LS = {
    # 1: Magic Moment — full multi-tool flow ideal path
    1:  ["verify_abn", "query_nema_guidelines", "get_industry_context",
         "get_active_hazards", "generate_grant_pdf"],
    # 2: Pure DRFA RAG — only needs the RAG tool
    2:  ["query_nema_guidelines"],
    # 3: Pure industry context — only needs the ABS tool
    3:  ["get_industry_context"],
    # 4: Weather check — verify first, then weather
    4:  ["verify_abn", "get_weather_forecast"],
    # 5: Hazards check
    5:  ["verify_abn", "get_active_hazards"],
    # 6: Climate projection
    6:  ["verify_abn", "get_climate_projection"],
    # 7: Invalid ABN — should still ATTEMPT verify_abn (which returns error)
    7:  ["verify_abn"],
    # 8: Non-NSW (VIC) — verify_abn yes, postcode-tools NO (see forbidden below)
    8:  ["verify_abn"],
    # 9: Multi-tool chained reasoning
    9:  ["verify_abn", "get_industry_context", "query_nema_guidelines"],
    # 10: No ABN provided — agent should ask, NOT call any tools
    10: set(),
    # 11: Explicit grant request
    11: ["verify_abn", "query_nema_guidelines", "generate_grant_pdf"],
    # 12: Cross-domain general question — agent has discretion, skip trajectory
    12: None,
}

# Per-case FORBIDDEN tools — must NOT be called for that case
FORBIDDEN_TOOLS_BY_CASE = {
    # Case 8: Non-NSW business — must NOT use NSW-only postcode tools
    8:  {"get_weather_forecast", "get_active_hazards", "get_climate_projection"},
    # Case 10: No ABN provided — must NOT fabricate tool calls
    10: {"verify_abn", "get_weather_forecast", "get_active_hazards",
         "get_climate_projection", "generate_grant_pdf"},
}

# COMMAND ----------

def _extract_tools_from_response(response_body):
    """Pull the ordered list of tools called from the inline MLflow trace.

    DEPRECATED PATH — kept for backwards compat. The current Mosaic AI Serving
    deployment doesn't include traces inline in responses (only the
    `databricks_request_id`), so this function returns [] for current
    deployments. Use _extract_tools_from_spans() with traces fetched from MLflow
    via search_traces() — see refresh_actual_tools_from_mlflow_traces() below.

    Returns a list (preserves order). Returns [] if no trace is attached.
    """
    spans = (response_body
             .get("databricks_output", {})
             .get("trace", {})
             .get("data", {})
             .get("spans", []))
    if not spans:
        return []
    spans_sorted = sorted(spans, key=lambda s: s.get("start_time_unix_nano", 0))
    return _extract_tool_names_from_span_objects(spans_sorted)


def _extract_tool_names_from_span_objects(spans):
    """Filter a list of spans down to just the tool-call names in order.

    Handles dict-style spans from mlflow.search_traces(). Identifies tool spans
    using TWO strategies (to handle both inline and fetched trace formats):
      1. attributes['mlflow.spanType'] == 'TOOL' (reliable for fetched traces)
      2. Name matching against KNOWN_TOOLS after stripping UC prefixes

    Tool names from the deployed agent use UC function naming convention:
      eco_resilience__silver__get_industry_context → get_industry_context
    """
    tools = []
    for span in spans:
        if isinstance(span, dict):
            name = span.get("name", "")
            attrs = span.get("attributes", {})
            span_type = attrs.get("mlflow.spanType", "")
        else:
            name = getattr(span, "name", "")
            span_type = getattr(span, "span_type", "")

        # Strategy 1: Use mlflow.spanType == 'TOOL' (most reliable)
        if span_type == "TOOL":
            # Strip UC function prefix: catalog__schema__func → func
            bare = name.rsplit("__", 1)[-1] if "__" in name else name
            if bare in KNOWN_TOOLS:
                tools.append(bare)
                continue

        # Strategy 2: Fallback — direct name match (for inline traces)
        bare = name.split(".")[-1] if "." in name else name
        bare = bare.rsplit("__", 1)[-1] if "__" in bare else bare
        if bare in KNOWN_TOOLS:
            tools.append(bare)
    return tools


# ─────────────────────────────────────────────────────────────────────────
# MLflow-trace path — required because current deployment doesn't return
# traces inline in the response body.
# ─────────────────────────────────────────────────────────────────────────

# Experiment ID where the deployed agent endpoint logs its traces.
# CHANGE this if the agent is redeployed and lands in a different experiment.
# To find: w.serving_endpoints.get(name=ENDPOINT_NAME).config.served_entities[*].environment_vars['MLFLOW_EXPERIMENT_ID']
AGENT_TRACE_EXPERIMENT_ID = "1186845106167464"


def fetch_recent_agent_traces(n=50):
    """Batch-fetch the N most recent traces from the agent's experiment.

    One MLflow call retrieves all traces; we then match per-case in pandas
    by client_request_id (much faster than N individual lookups).
    """
    return mlflow.search_traces(
        locations=[AGENT_TRACE_EXPERIMENT_ID],
        max_results=n,
    )


def _extract_tools_from_mlflow_trace_row(trace_row):
    """Given one row from mlflow.search_traces(), extract tool names from its spans."""
    spans = trace_row.get("spans") if isinstance(trace_row, dict) else trace_row["spans"]
    if spans is None:
        return []
    # Sort spans by start_time to preserve execution order
    if spans and isinstance(spans[0], dict):
        spans = sorted(spans, key=lambda s: s.get("start_time_unix_nano", 0))
    return _extract_tool_names_from_span_objects(spans)


def refresh_actual_tools_from_mlflow_traces(wait_seconds=10, max_traces=100):
    """Re-extract `actual_tools` for every case in eval_df_with_responses by
    looking up the corresponding trace in MLflow.

    This is the CORRECT path for the current Mosaic AI Serving deployment
    (traces are logged async, not returned inline). For each case:
      1. Read `databricks_request_id` from the stored response body
      2. Batch-fetch recent traces from the agent's experiment
      3. Match by client_request_id, extract tool names from spans
      4. Update the `actual_tools` column in place

    Args:
        wait_seconds: pause before fetching, gives async traces time to land
        max_traces: how many recent traces to pull in the batch fetch
    """
    import json, time   # explicit imports — function may be called from a fresh cell

    # Wait briefly for async trace logging to catch up
    if wait_seconds > 0:
        print(f"Waiting {wait_seconds}s for trace logging to settle...")
        time.sleep(wait_seconds)

    # 1. Pull request_ids from stored bodies
    request_ids = []
    for raw in eval_df_with_responses["raw_response_body"]:
        if raw is None:
            request_ids.append(None)
            continue
        body = json.loads(raw)
        rid = body.get("databricks_output", {}).get("databricks_request_id")
        request_ids.append(rid)
    print(f"Found {sum(1 for r in request_ids if r)} request_ids in stored bodies.")

    # 2. Batch-fetch recent traces (one MLflow call)
    print(f"Fetching {max_traces} most recent traces from experiment {AGENT_TRACE_EXPERIMENT_ID}...")
    recent = fetch_recent_agent_traces(n=max_traces)
    print(f"Got {len(recent)} traces.")

    # 3. Match by client_request_id (pandas filter — sidesteps filter_string syntax quirks)
    refreshed = []
    matched_count = 0
    for rid in request_ids:
        if not rid:
            refreshed.append([])
            continue
        match = recent[recent["client_request_id"] == rid]
        if len(match) == 0:
            refreshed.append([])
            continue
        matched_count += 1
        tools = _extract_tools_from_mlflow_trace_row(match.iloc[0])
        refreshed.append(tools)

    # 4. Update the DataFrame
    eval_df_with_responses["actual_tools"] = refreshed
    non_empty = sum(1 for t in refreshed if t)
    print(
        f"Refreshed actual_tools for {len(refreshed)} cases. "
        f"Matched to MLflow trace: {matched_count}/{len(refreshed)}. "
        f"Non-empty extraction: {non_empty}."
    )
    if matched_count < sum(1 for r in request_ids if r):
        unmatched = sum(1 for r in request_ids if r) - matched_count
        print(
            f"⚠️  {unmatched} request_id(s) have no matching MLflow trace. Possible causes:\n"
            f"    1. The traces aged out of the recent window — increase max_traces\n"
            f"    2. The stored bodies are from a PRE-v3 deployment (wrong experiment)\n"
            f"       → Re-run §5a to make fresh calls under v3\n"
            f"    3. The deployment changed experiment_id — update AGENT_TRACE_EXPERIMENT_ID"
        )
    return refreshed


def score_trajectory(actual, expected, forbidden=None):
    """Score one case's tool trajectory.

    Args:
        actual:    ordered list of tools the agent called
        expected:  set of tools the agent SHOULD have called (None = skip)
        forbidden: set of tools the agent must NOT have called (default empty)

    Returns dict with:
        - recall:            len(matched) / len(expected)   [None if skipped]
        - precision:         len(matched) / len(actual)     [None if skipped]
        - missed:            sorted list of expected tools NOT called
        - extra:             sorted list of called tools not in expected
        - forbidden_called:  sorted list of forbidden tools that were called
        - score:             composite 0-1 score for the case
        - rationale:         human-readable explanation
    """
    if expected is None:
        return {
            "recall": None, "precision": None,
            "missed": [], "extra": [], "forbidden_called": [],
            "score": None,
            "rationale": "skipped (agent has discretion for this case)",
        }

    actual_set    = set(actual)
    forbidden_set = forbidden or set()
    matched           = actual_set & expected
    missed            = expected - actual_set
    extra             = actual_set - expected - forbidden_set
    forbidden_called  = actual_set & forbidden_set

    recall    = len(matched) / len(expected) if expected else 1.0
    precision = len(matched) / len(actual_set) if actual_set else (1.0 if not expected else 0.0)

    # Composite: full recall = 1.0, lose 0.5 if any forbidden tools were called.
    forbidden_penalty = 0.5 if forbidden_called else 0.0
    score = max(0.0, recall - forbidden_penalty)

    parts = [f"called={sorted(actual_set) or '∅'}"]
    if missed:           parts.append(f"MISSED={sorted(missed)}")
    if forbidden_called: parts.append(f"FORBIDDEN_CALLED={sorted(forbidden_called)}")
    if extra:            parts.append(f"extra={sorted(extra)}")

    return {
        "recall": recall, "precision": precision,
        "missed": sorted(missed), "extra": sorted(extra),
        "forbidden_called": sorted(forbidden_called),
        "score": score,
        "rationale": " | ".join(parts),
    }

print("✅ Trajectory helpers loaded.")
print(f"   {len(EXPECTED_TOOLS_BY_CASE)} cases have expected_tools defined")
print(f"   {sum(1 for v in EXPECTED_TOOLS_BY_CASE.values() if v is None)} cases skipped (agent discretion)")
print(f"   {len(FORBIDDEN_TOOLS_BY_CASE)} cases have forbidden_tools defined")

# COMMAND ----------

# MAGIC %md
# MAGIC ## §4 — Define the predict function + smoke test
# MAGIC
# MAGIC `mlflow.evaluate` calls a `predict_fn` once per case. The function takes the
# MAGIC input (a single row of the DataFrame as a dict) and returns the agent's
# MAGIC text response. We delegate auth to `w.api_client.do()` — same SDK pattern
# MAGIC used by the deployed Lakehouse App.
# MAGIC
# MAGIC The **smoke test** at the end of this cell catches the most common failure
# MAGIC modes (agent cold-start, permission errors, wire format mismatches) BEFORE
# MAGIC we spend 15 min running the full evaluation.

# COMMAND ----------

def _extract_request(model_input):
    """Pull the user query out of whatever shape mlflow passes in."""
    if isinstance(model_input, dict):
        return model_input.get("request", "")
    if isinstance(model_input, pd.DataFrame):
        return model_input["request"].iloc[0] if "request" in model_input.columns else ""
    if isinstance(model_input, pd.Series):
        return model_input.get("request", "")
    if isinstance(model_input, list):
        return _extract_request(model_input[0]) if model_input else ""
    return str(model_input)


def _invoke_agent(model_input):
    """Low-level: POST to eco_resilience_agent, return the parsed response body.

    Centralizes the HTTP call so both predict_fn (text-only) and
    predict_with_trace (text + trace) reuse the same logic.
    """
    request = _extract_request(model_input)
    payload = {"input": [{"role": "user", "content": request}]}
    return w.api_client.do(
        method="POST",
        path=f"/serving-endpoints/{ENDPOINT_NAME}/invocations",
        body=payload,
    )


def predict_fn(model_input):
    """Return just the assistant text. Used by the §4 smoke test."""
    body = _invoke_agent(model_input)
    # ResponsesAgent wire format: { output: [ { content: [ { text: "..." } ] } ] }
    return body["output"][0]["content"][0]["text"]


def predict_with_trace(model_input):
    """Return (response_text, tools_called, raw_body) for §5a's manual loop.

    `tools_called` is an ordered list extracted from the inline MLflow trace.
    The raw body is returned for caller-side debugging.
    """
    body = _invoke_agent(model_input)
    text  = body["output"][0]["content"][0]["text"]
    tools = _extract_tools_from_response(body)
    return text, tools, body


# ─────────────────────────────────────────────────────────────────────
# Smoke test — fail fast if anything is wrong with the auth/wire setup
# ─────────────────────────────────────────────────────────────────────
print("Smoke test: calling predict_with_trace on EVAL_CASES[0] (Magic Moment)...\n")
try:
    sample_text, sample_tools, sample_body = predict_with_trace(EVAL_CASES[0])
except Exception as e:
    raise RuntimeError(
        f"Smoke test FAILED — agent invocation errored. "
        f"Diagnose this before running the full eval.\n"
        f"  Error: {type(e).__name__}: {e}\n"
        f"  Most likely causes:\n"
        f"    1. Agent endpoint not deployed or not READY (check Serving UI)\n"
        f"    2. Caller lacks CAN_QUERY on {ENDPOINT_NAME}\n"
        f"    3. SDK version mismatch (re-run §1 install + restart)\n"
    )

print(f"Response (first 500 chars):\n{sample_text[:500]}\n")
print(f"Tools called (extracted from inline trace): {sample_tools}\n")

assert "BATHURST" in sample_text.upper(), (
    f"Smoke test FAILED — agent did not return Bathurst identity. "
    f"Check ABR_AUTH_GUID is set in the deployed endpoint's environment_vars.\n"
    f"Response was:\n{sample_text}"
)
if not sample_tools:
    print("⚠️  Trace extraction returned [] — trajectory metrics will be empty.\n"
          "   Verify the agent endpoint has ENABLE_MLFLOW_TRACING=true.\n"
          "   `databricks.agents.deploy()` sets this automatically; if you used a\n"
          "   custom deploy path, you may need to enable it manually.")
else:
    print(f"✅ Trace extraction works — found {len(sample_tools)} tool call(s).")

print("\n✅ Smoke test passed. Endpoint reachable + verify_abn works + trace parsing alive.")
print("Proceed to §5 to run the full evaluation.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## §5a — Pre-compute agent responses for all 12 cases
# MAGIC
# MAGIC **Why this two-stage pattern (pre-compute → evaluate) instead of a single
# MAGIC `mlflow.evaluate(model=predict_fn, ...)` call?**
# MAGIC
# MAGIC `mlflow.evaluate(model_type="databricks-agent")` auto-invokes models using
# MAGIC the **OpenAI ChatCompletion wire format** (`{"messages": [...]}`). Our
# MAGIC `eco_resilience_agent` is a `ResponsesAgent` that uses a different wire
# MAGIC format (`{"input": [...]}`). Passing a callable to `model=` doesn't help —
# MAGIC the framework still sends ChatCompletion-shaped requests under the hood
# MAGIC and bypasses our `predict_fn`.
# MAGIC
# MAGIC The clean fix: **decouple model invocation from evaluation**.
# MAGIC
# MAGIC 1. WE call the agent for each case using our `predict_fn` (this cell)
# MAGIC 2. Store every response in a `response` column of the DataFrame
# MAGIC 3. Pass the resulting DataFrame to `mlflow.evaluate(predictions="response", ...)` (next cell)
# MAGIC 4. Framework runs the judges on the precomputed data — no auto-invocation
# MAGIC
# MAGIC Side benefit: we can debug per-case failures inline, see exactly how long
# MAGIC each agent call takes, and re-run the judges later without re-calling the
# MAGIC agent (saves cost).

# COMMAND ----------

import json
import time

responses    = []
tools_called = []   # ordered list of tool names per case (from trace)
raw_bodies   = []   # NEW — full raw response body per case (for trace debugging)
durations_ms = []
errors       = []

print(f"Calling eco_resilience_agent for {len(EVAL_CASES)} cases...\n")

for i, case in enumerate(EVAL_CASES, start=1):
    print(f"[{i:2d}/{len(EVAL_CASES)}] {case['request'][:80]}...")
    t0 = time.perf_counter()
    try:
        response_text, tools, body = predict_with_trace(case)
        elapsed_ms = (time.perf_counter() - t0) * 1000
        responses.append(response_text)
        tools_called.append(tools)
        raw_bodies.append(body)
        durations_ms.append(elapsed_ms)
        errors.append(None)
        preview = response_text[:80].replace(chr(10), ' ')
        print(f"           ✅ {elapsed_ms:.0f}ms — tools={tools} — {preview}...")
    except Exception as e:
        elapsed_ms = (time.perf_counter() - t0) * 1000
        err = f"{type(e).__name__}: {e}"
        # Even on error, store a placeholder string so the judges have something to score
        responses.append(f"[AGENT ERROR] {err}")
        tools_called.append([])
        raw_bodies.append(None)
        durations_ms.append(elapsed_ms)
        errors.append(err)
        print(f"           ❌ {elapsed_ms:.0f}ms — ERROR: {err}")
    print()

# Build the evaluated DataFrame — agent responses + trajectories precomputed
eval_df_with_responses = eval_df.copy()
eval_df_with_responses["response"]      = responses
eval_df_with_responses["actual_tools"]  = tools_called
eval_df_with_responses["latency_ms"]    = durations_ms
# Serialise raw bodies as JSON strings so the DataFrame is uniform shape +
# survives display() and to_json() artifact logging. Lets us re-extract tools
# WITHOUT re-calling the agent — see refresh_actual_tools_from_stored_bodies()
# and the §5a.5 inspector cell below.
eval_df_with_responses["raw_response_body"] = [
    json.dumps(b, default=str) if b else None for b in raw_bodies
]

succeeded = sum(1 for e in errors if e is None)
failed    = sum(1 for e in errors if e is not None)
avg_ms    = sum(durations_ms) / len(durations_ms) if durations_ms else 0
total_tool_calls = sum(len(t) for t in tools_called)

print("─" * 60)
print(f"Agent invocations complete: {succeeded}/{len(EVAL_CASES)} succeeded, {failed} errored")
print(f"Avg latency:          {avg_ms:.0f}ms per call")
print(f"Total agent-call time: {sum(durations_ms)/1000:.1f}s")
print(f"Total tool calls:      {total_tool_calls} (across all cases)")
print()
if failed > 0:
    print("⚠️  Some cases errored. The eval will still run on the placeholder responses,")
    print("    but expect those cases to score 0 on most judges. Fix the underlying")
    print("    agent issue before iterating.")

# Don't display raw_response_body — it's huge and would slow the cell render.
display(eval_df_with_responses[["request", "response", "actual_tools", "latency_ms"]])

# COMMAND ----------

# MAGIC %md
# MAGIC ## §5a.5 — Trace inspector (debug `_extract_tools_from_response`)
# MAGIC
# MAGIC Use the raw bodies stored in §5a to inspect actual span structure WITHOUT
# MAGIC re-calling the agent. The first time you see `actual_tools=[]` for a case
# MAGIC that should have called multiple tools, this is the diagnostic cell.
# MAGIC
# MAGIC The pattern is:
# MAGIC 1. Run `inspect_trace(N)` to see how the spans are actually shaped
# MAGIC 2. Update `_extract_tools_from_response()` in §3b based on what you see
# MAGIC 3. Call `refresh_actual_tools_from_stored_bodies()` to rescore — fast, no HTTP

# COMMAND ----------

def inspect_trace(case_idx_1based: int):
    """Print structure of the stored response body for one case.

    Uses the JSON column from §5a — no HTTP calls. Re-runnable as you iterate
    on the extractor.
    """
    row = eval_df_with_responses.iloc[case_idx_1based - 1]
    if not row.get("raw_response_body"):
        print(f"Case {case_idx_1based}: no raw_response_body (case probably errored).")
        return
    body = json.loads(row["raw_response_body"])

    print(f"═══ Case {case_idx_1based}: {row['request'][:80]}... ═══")
    print(f"Top-level keys:           {list(body.keys())}")

    db_out = body.get("databricks_output", {})
    print(f"databricks_output keys:   {list(db_out.keys())}")

    trace = db_out.get("trace", {})
    print(f"trace keys:               {list(trace.keys())}")

    spans = trace.get("data", {}).get("spans", [])
    print(f"\nNumber of spans:          {len(spans)}")
    print("Span names (first 25):")
    for i, span in enumerate(spans[:25], start=1):
        name      = span.get("name", "<no name>")
        parent_id = span.get("parent_span_id")
        depth     = "ROOT " if not parent_id else "child"
        print(f"  {i:2d}. [{depth}] {name}")

    extracted = _extract_tools_from_response(body)
    print(f"\n_extract_tools_from_response() returns: {extracted}")
    print(f"KNOWN_TOOLS for reference:               {sorted(KNOWN_TOOLS)}")


def refresh_actual_tools_from_stored_bodies():
    """Re-run _extract_tools_from_response over the stored raw bodies.

    Useful after you tune _extract_tools_from_response (e.g. add new span-name
    patterns or change the normalization rule) — call this to update the
    `actual_tools` column without re-calling the agent. Then re-run §5c to
    rescore trajectory.
    """
    refreshed = []
    for raw in eval_df_with_responses["raw_response_body"]:
        if not raw:
            refreshed.append([])
            continue
        body = json.loads(raw)
        refreshed.append(_extract_tools_from_response(body))
    eval_df_with_responses["actual_tools"] = refreshed
    print(f"Refreshed actual_tools for {len(refreshed)} cases. "
          f"Non-empty: {sum(1 for t in refreshed if t)}")
    return refreshed


# Inspect Case 1 (Magic Moment) — should show ~5 tool calls if extraction works.
# Use inspect_trace(N) for any other case to compare patterns.
inspect_trace(1)

# COMMAND ----------

# DBTITLE 1,Refresh actual_tools from MLflow traces
# ── Refresh actual_tools from MLflow traces (async path) ──────────────
# The deployed agent logs traces ASYNCHRONOUSLY to experiment 1186845106167464.
# They are NOT returned inline in the response body. This function:
#   1. Batch-fetches recent traces from the agent's experiment
#   2. Matches each case's databricks_request_id to its trace
#   3. Extracts tool names from spans where mlflow.spanType == 'TOOL'
#   4. Updates eval_df_with_responses['actual_tools'] in place

refresh_actual_tools_from_mlflow_traces(wait_seconds=5, max_traces=100)

# Show results
print("\n── Updated actual_tools per case ──")
for i, row in eval_df_with_responses.iterrows():
    tools = row["actual_tools"]
    query = row["request"][:60]
    tools_str = str(tools) if tools else "[]"
    print(f"  Case {i+1:2d}: {tools_str:<60} | {query}...")

# COMMAND ----------

# MAGIC %md
# MAGIC ## §5b — Run `mlflow.evaluate()` on the precomputed responses
# MAGIC
# MAGIC Now that every case has a `response` column, we tell the evaluator:
# MAGIC *"don't invoke any model — score these precomputed responses against the
# MAGIC expected_facts + guidelines"*. This is the `predictions="response"` knob.
# MAGIC
# MAGIC ### Built-in judges that fire automatically
# MAGIC
# MAGIC | Judge | What it scores |
# MAGIC |---|---|
# MAGIC | `correctness` | Does the response contain the expected_facts? |
# MAGIC | `relevance_to_query` | Does it actually answer what the user asked? |
# MAGIC | `safety` | No harmful, biased, or inappropriate content |
# MAGIC | `guideline_adherence` | Does it follow each `guidelines` rule? |
# MAGIC
# MAGIC ### Wall-time expectation
# MAGIC
# MAGIC ~3-5 min — judges only (no agent calls). Each judge is ~3-5s per case;
# MAGIC 12 cases × ~4 judges ≈ 4 minutes.

# COMMAND ----------

with mlflow.start_run(run_name="eco_agent_eval_v1") as run:
    # Log the eval dataset (with responses) as an artifact for reproducibility
    eval_df_with_responses.to_json("/tmp/eval_dataset_with_responses.json", orient="records", indent=2)
    mlflow.log_artifact("/tmp/eval_dataset_with_responses.json")
    mlflow.log_param("agent_endpoint",    ENDPOINT_NAME)
    mlflow.log_param("num_cases",         len(eval_df_with_responses))
    mlflow.log_param("num_succeeded",     succeeded)
    mlflow.log_param("num_errored",       failed)
    mlflow.log_param("eval_framework",    "databricks-agent")
    mlflow.log_metric("avg_agent_latency_ms", avg_ms)

    # KEY: `predictions="response"` tells the evaluator to use the precomputed
    # responses column — no model invocation, just judging.
    results = mlflow.evaluate(
        data=eval_df_with_responses,
        predictions="response",
        model_type="databricks-agent",
    )

    EVAL_RUN_ID = run.info.run_id

# Build the experiment URL for the pitch
host = w.config.host.rstrip("/")
EXPERIMENT_URL = f"{host}/ml/experiments/{EXPERIMENT_ID}/runs/{EVAL_RUN_ID}"

print(f"\n✅ Evaluation complete.")
print(f"   Run ID: {EVAL_RUN_ID}")
print(f"   View in MLflow:")
print(f"   {EXPERIMENT_URL}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## §5c — Trajectory scoring (deterministic, no LLM judge)
# MAGIC
# MAGIC Score each case's actual tool-call list against `EXPECTED_TOOLS_BY_CASE`
# MAGIC and `FORBIDDEN_TOOLS_BY_CASE` from §3b. Aggregate metrics are **logged to
# MAGIC the same MLflow run** as the §5b LLM-judge metrics — so the run shows
# MAGIC both response quality AND reasoning correctness side by side.
# MAGIC
# MAGIC ### Why these metrics matter
# MAGIC
# MAGIC | Metric | Question it answers |
# MAGIC |---|---|
# MAGIC | `trajectory_avg_recall` | What % of expected tools did the agent actually invoke? |
# MAGIC | `trajectory_avg_precision` | What % of invoked tools were the right ones? |
# MAGIC | `trajectory_avg_score` | Composite — penalizes forbidden-tool calls heavily |
# MAGIC | `trajectory_perfect_cases` | Count of cases with a 100% trajectory match |
# MAGIC | `trajectory_forbidden_calls` | Number of cases where a forbidden tool was called |

# COMMAND ----------

eval_df_with_responses.display()

# COMMAND ----------

# ── Part 1: Score every case ──────────────────────────────────────────
trajectory_rows = []
for i, row in eval_df_with_responses.iterrows():
    case_idx  = i + 1  # 1-based to match EXPECTED_TOOLS_BY_CASE keys
    actual    = list(row["actual_tools"]) if isinstance(row["actual_tools"], (list, tuple)) else []
    expected  = EXPECTED_TOOLS_BY_CASE.get(case_idx)
    expected2 = EXPECTED_TOOLS_BY_CASE_LS.get(case_idx)
    forbidden = FORBIDDEN_TOOLS_BY_CASE.get(case_idx)
    result    = score_trajectory(actual, expected, forbidden)
    trajectory_rows.append({
        "case":             case_idx,
        "request":          row["request"][:80],
        "expected_tools": expected2,
        "actual_tools":     actual,
        # "expected_tools":   sorted(expected) if expected is not None else None,
        "forbidden_tools":  sorted(forbidden) if forbidden else None,
        **result,
    })

trajectory_df = pd.DataFrame(trajectory_rows)
print(f"Scored {len(trajectory_df)} cases for trajectory metrics.")

# COMMAND ----------

trajectory_df.display()

# COMMAND ----------

# ── Part 2: Aggregate + log to MLflow ─────────────────────────────────
scored = trajectory_df.dropna(subset=["score"])  # excludes any case with expected_tools=None

aggregate = {
    "trajectory_avg_recall":      scored["recall"].mean()    if len(scored) else 0.0,
    "trajectory_avg_precision":   scored["precision"].mean() if len(scored) else 0.0,
    "trajectory_avg_score":       scored["score"].mean()     if len(scored) else 0.0,
    "trajectory_perfect_cases":   int((scored["score"] == 1.0).sum()),
    "trajectory_scored_cases":    len(scored),
    "trajectory_skipped_cases":   len(trajectory_df) - len(scored),
    "trajectory_forbidden_calls": int(trajectory_df["forbidden_called"].apply(bool).sum()),
}

# Log into the SAME MLflow run that §5b created — metrics appear alongside
# correctness / guideline_adherence / safety in the run's Metrics tab.
with mlflow.start_run(run_id=EVAL_RUN_ID):
    for k, v in aggregate.items():
        mlflow.log_metric(k, v)
    # Save the per-case trajectory table as an artifact for drill-down
    trajectory_df.to_json("/tmp/trajectory_results.json", orient="records", indent=2)
    mlflow.log_artifact("/tmp/trajectory_results.json")

print("═" * 70)
print("  TRAJECTORY METRICS (logged to MLflow run alongside judge scores)")
print("═" * 70)
for k, v in aggregate.items():
    if isinstance(v, float):
        print(f"  {k:<40} {v:>7.1%}")
    else:
        print(f"  {k:<40} {v:>7}")

# COMMAND ----------

# ── Part 3: Per-case leaderboard, sorted by score ─────────────────────
# Worst-performing cases at the top — that's where to focus iteration.
display(
    trajectory_df[
        ["case", "request", "expected_tools", "actual_tools",
         "missed", "forbidden_called", "extra", "score", "rationale"]
    ].sort_values("score", ascending=True, na_position="last")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## §6 — Inspect aggregate metrics + per-case results
# MAGIC
# MAGIC `results.metrics` gives the **pitch numbers**. `results.tables["eval_results"]`
# MAGIC gives the **per-case detail** — request, response, score-per-judge, and the
# MAGIC judge's natural-language rationale for each judgment.
# MAGIC
# MAGIC Failing cases are the gold here — they tell you exactly what to fix in
# MAGIC `eco_agent.py`'s SYSTEM_PROMPT or which tool needs better descriptions.

# COMMAND ----------

# Aggregate metrics — the numbers you'll quote in the pitch
print("═" * 64)
print("  AGGREGATE METRICS")
print("═" * 64)
for metric_name in sorted(results.metrics.keys()):
    value = results.metrics[metric_name]
    if isinstance(value, (int, float)):
        if 0 <= value <= 1.0:
            print(f"  {metric_name:.<50} {value:>7.1%}")
        else:
            print(f"  {metric_name:.<50} {value:>7.2f}")
    else:
        print(f"  {metric_name:.<50} {value}")

# COMMAND ----------

# Per-case results — drill into each case's score and the judge's rationale
print("Per-case detail:")
per_case = results.tables.get("eval_results")
if per_case is not None:
    display(per_case)
else:
    print("(No 'eval_results' table found — check results.tables keys below)")
    print(f"Available tables: {list(results.tables.keys())}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Failure analysis — what to fix next
# MAGIC
# MAGIC Cases where the judge scored less than perfect tell us exactly where the
# MAGIC agent's behaviour can improve. Each row shows:
# MAGIC
# MAGIC - `request` — the user query
# MAGIC - `response` — what the agent actually said
# MAGIC - `correctness/score` — 0 or 1 per fact, averaged
# MAGIC - `correctness/rationale` — the judge LLM's explanation
# MAGIC - `guideline_adherence/score` — 0 or 1 per guideline, averaged
# MAGIC - `guideline_adherence/rationale` — which guideline was violated and how
# MAGIC
# MAGIC Use these to iterate on either:
# MAGIC 1. **The agent** — edit `eco_agent.py`'s SYSTEM_PROMPT, redeploy, re-evaluate
# MAGIC 2. **The test case** — if the case was too strict or genuinely ambiguous, fix the expected_facts/guidelines

# COMMAND ----------

if per_case is not None:
    # Detect failures across whichever judge columns exist
    score_cols = [c for c in per_case.columns if c.endswith("/score")]
    if score_cols:
        # A case is "failing" if any judge gave it less than perfect (< 1.0)
        per_case["min_score"] = per_case[score_cols].min(axis=1)
        failures = per_case[per_case["min_score"] < 1.0]

        print(f"\n=== {len(failures)} case(s) had at least one judge score < 1.0 ===\n")
        if len(failures):
            # Show the request + the lowest-scoring rationale for each failure
            for idx, row in failures.iterrows():
                print(f"━━━ Case {idx+1}: min_score={row['min_score']:.0%} ━━━")
                print(f"Request: {row.get('request', 'N/A')[:200]}")
                # Find the failing judge(s)
                for col in score_cols:
                    if row[col] < 1.0:
                        rationale_col = col.replace("/score", "/rationale")
                        print(f"\n  ❌ {col} = {row[col]:.0%}")
                        if rationale_col in per_case.columns:
                            rationale = str(row[rationale_col])[:400]
                            print(f"     Why: {rationale}")
                print()
        else:
            print("🎉 All cases passed all judges.")
    else:
        print("No /score columns found. Available columns:")
        print(list(per_case.columns))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Trajectory failures (deterministic, from §5c)
# MAGIC
# MAGIC LLM-judged failures above tell you the agent's *output* was off.
# MAGIC Trajectory failures below tell you the agent's *reasoning path* was off
# MAGIC (called the wrong tools, missed required tools, or invoked forbidden ones).
# MAGIC Different failure mode, different fix.

# COMMAND ----------

print("\n=== Trajectory failures (score < 1.0) ===\n")
traj_failures = trajectory_df[trajectory_df["score"].fillna(1.0) < 1.0]
if len(traj_failures):
    for _, row in traj_failures.iterrows():
        score = row["score"]
        score_pct = f"{score:.0%}" if score is not None else "N/A"
        print(f"━━━ Case {row['case']}: score={score_pct} ━━━")
        print(f"Request:  {row['request']}")
        print(f"Expected: {row['expected_tools']}")
        print(f"Called:   {row['actual_tools']}")
        if row["missed"]:
            print(f"  ❌ MISSED:    {row['missed']}")
        if row["forbidden_called"]:
            print(f"  🚫 FORBIDDEN: {row['forbidden_called']}")
        if row["extra"]:
            print(f"  ⚠️  EXTRA:     {row['extra']}")
        print()
else:
    print("🎉 All cases (with expected_tools defined) hit perfect trajectory.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## §7 — Pitch-friendly summary box
# MAGIC
# MAGIC Screenshot this cell's output for the slide deck.

# COMMAND ----------

# Build the summary lines first
header_lines = [
    "╔" + "═" * 62 + "╗",
    "║" + "  eco_resilience_agent — Evaluation Results v1".ljust(62) + "║",
    "╠" + "═" * 62 + "╣",
    "║" + f"  Cases evaluated:    {len(eval_df)}".ljust(62) + "║",
    "║" + f"  Endpoint:           {ENDPOINT_NAME}".ljust(62) + "║",
    "║" + f"  Run ID:             {EVAL_RUN_ID[:32]}...".ljust(62) + "║",
    "║" + "".ljust(62) + "║",
    "║" + "  Aggregate metrics (quotable in pitch):".ljust(62) + "║",
    "║" + "".ljust(62) + "║",
]

metric_lines = []
for metric_name in sorted(results.metrics.keys()):
    value = results.metrics[metric_name]
    if isinstance(value, (int, float)) and 0 <= value <= 1.0:
        line = f"  {metric_name:.<42} {value:>7.1%}"
        metric_lines.append("║" + line.ljust(62) + "║")

footer_lines = [
    "║" + "".ljust(62) + "║",
    "║" + "  Drill-down in MLflow:".ljust(62) + "║",
    "║" + f"  {EXPERIMENT_URL[:58]}".ljust(62) + "║",
    "║" + "".ljust(62) + "║",
    "╚" + "═" * 62 + "╝",
]

for line in header_lines + metric_lines + footer_lines:
    print(line)

print()
print("─" * 64)
print("Full MLflow run link (clickable):")
print(f"  {EXPERIMENT_URL}")
print("─" * 64)

# COMMAND ----------

# MAGIC %md
# MAGIC ## §8 — Done
# MAGIC
# MAGIC | Artifact | Where it lives | How to use in pitch |
# MAGIC |---|---|---|
# MAGIC | MLflow run `eco_agent_eval_v1` | Experiment `/Shared/eco_resilience/eco_resilience_agent_eval` | Open during demo, walk through the metrics tab |
# MAGIC | Golden dataset JSON | MLflow run artifact `eval_dataset.json` | Download from the run UI, attach to pitch deck appendix |
# MAGIC | Aggregate scores | `results.metrics` (printed in §6) | Quote in spoken pitch ("our agent scores 88% on guideline adherence") |
# MAGIC | Per-case rationales | `results.tables["eval_results"]` | Drill into one passing case + one failing-then-iterated case during Q&A |
# MAGIC | Pitch summary box | §7 output | Screenshot for the slide deck |
# MAGIC
# MAGIC ### Iteration loop (if you have time)
# MAGIC
# MAGIC When a case fails:
# MAGIC 1. Click into the MLflow run → find the failing case
# MAGIC 2. Read the judge's rationale (the most valuable thing in this whole notebook)
# MAGIC 3. Diagnose: is the SYSTEM_PROMPT unclear? Is a tool description vague? Is the data missing?
# MAGIC 4. Fix in `eco_agent.py`, redeploy via `notebooks/13_deploy_agent.py`, then re-run this notebook
# MAGIC 5. Compare metrics across runs — MLflow's "Compare runs" view is great here
# MAGIC
# MAGIC The "iterated v1 → v2 improvement" story is a strong pitch beat: *"we found
# MAGIC the agent didn't cite page numbers in 3 out of 12 cases, refined the system
# MAGIC prompt to mandate citations, and improved guideline adherence from 78% to 92%."*
# MAGIC
# MAGIC ### Cost note
# MAGIC
# MAGIC Each eval run costs ~$2-5 in LLM API calls (agent + 4 judges × 12 cases).
# MAGIC Negligible for hackathon; budget item for production.
# MAGIC
# MAGIC ### What's next
# MAGIC
# MAGIC - **Phase 5 — Genie Space + Gold view** (your call)
# MAGIC - **Phase 6 — Demo recording** (the closing item before deadline)