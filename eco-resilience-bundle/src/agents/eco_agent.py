"""EcoResilience AI agent — portable module for MLflow + Mosaic AI deployment.

This file is captured by `mlflow.pyfunc.log_model(python_model="eco_agent.py", ...)`
in `notebooks/13_deploy_agent.py`. At deployment time, `mlflow.models.set_model()`
at the bottom registers `EcoResilienceAgent()` as THE model the endpoint serves.

The agent's reasoning core (LangGraph + Claude Sonnet 4 + 7 UC tool functions)
is identical to the one in `notebooks/07_minimal_agent.py` — this module just
adds the `mlflow.pyfunc.ResponsesAgent` wrapper Databricks needs for Model
Serving deployment.

Source-of-truth alignment rule:
    SYSTEM_PROMPT and TOOL_FUNCTIONS below MUST stay in sync with
    notebooks/07_minimal_agent.py. If you tune the prompt or add a tool, edit
    both files (or refactor to a shared prompts.py later).
"""

import json
import os
from typing import Any, Generator

import mlflow
import requests
from mlflow.pyfunc import ResponsesAgent
from mlflow.types.responses import (
    ResponsesAgentRequest,
    ResponsesAgentResponse,
    ResponsesAgentStreamEvent,
)

from databricks_langchain import ChatDatabricks, UCFunctionToolkit
from langchain_core.tools import tool
from langgraph.prebuilt import create_react_agent

# Capture per-tool-call spans (LLM call, each tool invocation, tool output)
# in the MLflow trace. Without this, the trace only has one outer "predict"
# span and tool errors get narrated away by the LLM as "technical difficulties".
#
# log_traces=True is explicit (some MLflow versions default it off). The
# @mlflow.trace decorator on predict() + the explicit mlflow.start_span around
# self._agent.invoke() below give Mosaic AI Serving's trace context a chain to
# attach LangGraph child spans to — without those wrappers the autolog hooks
# fire but the spans have no parent and end up dropped.
mlflow.langchain.autolog(log_traces=True)

# ───────────────────────────────────────────────────────────────────────────
# Configuration — keep aligned with notebooks/07_minimal_agent.py
# ───────────────────────────────────────────────────────────────────────────
CATALOG       = "eco_resilience"
SILVER_SCHEMA = "silver"
LLM_ENDPOINT  = "databricks-claude-sonnet-4"

# The six UC tool functions the agent calls. The seventh tool (`verify_abn`)
# is a LangChain Python tool defined below — it needs the ABR API key via
# os.environ, which UC SQL UDFs can't access reliably in Mosaic AI serving.
TOOL_FUNCTIONS = [
    f"{CATALOG}.{SILVER_SCHEMA}.get_weather_forecast",
    f"{CATALOG}.{SILVER_SCHEMA}.get_active_hazards",
    f"{CATALOG}.{SILVER_SCHEMA}.get_climate_projection",
    f"{CATALOG}.{SILVER_SCHEMA}.query_nema_guidelines",
    f"{CATALOG}.{SILVER_SCHEMA}.get_industry_context",
    f"{CATALOG}.{SILVER_SCHEMA}.generate_grant_pdf",
]

# ───────────────────────────────────────────────────────────────────────────
# verify_abn — Python tool (live ABR API lookup)
# ───────────────────────────────────────────────────────────────────────────
# Why it lives here instead of as a UC function:
#   The Databricks SQL `secret()` function does not reliably resolve in the
#   Mosaic AI Model Serving auto-authentication-passthrough context, so a UC
#   SQL UDF that calls `secret(...)` silently fails when invoked from the
#   deployed agent. Running the API call in the agent's Python container and
#   reading the GUID from `os.environ['ABR_AUTH_GUID']` is the documented
#   Databricks pattern for serving + external API credentials.
#
# How the GUID gets here:
#   Local notebook testing: set `os.environ['ABR_AUTH_GUID'] = dbutils.secrets.get(...)`
#     before importing/invoking the agent.
#   Deployed endpoint: `agents.deploy(environment_vars={"ABR_AUTH_GUID":
#     "{{secrets/eco_resilience/abr_auth_guid}}"})` in notebook 13.
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
    guid = os.environ.get("ABR_AUTH_GUID")
    if not guid:
        return {"abn": abn, "error": "ABR_AUTH_GUID env var not set in runtime"}

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
        return {
            "abn": abn_clean,
            "error": f"ABR API call failed: {type(e).__name__}: {e}",
        }

    if not record.get("Abn"):
        return {
            "abn": abn_clean,
            "error": record.get("Message", f"ABN {abn_clean} not found"),
        }

    state = record.get("AddressState")
    return {
        "abn":         record.get("Abn"),
        "entity_name": record.get("EntityName"),
        "abn_status":  record.get("AbnStatus"),
        "entity_type": record.get("EntityTypeName"),
        "state":       state,
        "postcode":    record.get("AddressPostcode"),
        "in_nsw":      state == "NSW",
        "error":       None,
    }

# Mirror of notebooks/07_minimal_agent.py SYSTEM_PROMPT — verbatim.
SYSTEM_PROMPT = """You are EcoResilience AI, an assistant for Australian small-business owners facing natural-disaster risk.

You have SEVEN tools available:

1. verify_abn(abn) — looks up an Australian Business Number and returns the business identity (name, status, type, state, postcode, in_nsw flag). ALWAYS call this FIRST when the user provides an ABN, because its output postcode is the input to tools 2–4.

2. get_weather_forecast(postcode) — next 12 hours of weather plus 24-hour rain and wind summary statistics at the nearest seeded station. Use when the user asks about current or upcoming weather conditions.

3. get_active_hazards(postcode) — currently-active TfNSW road hazards (incidents, floods, fires, roadworks) inside the postcode boundary. Use when the user asks about disruptions, road closures, fires, floods, or active emergencies.

4. get_climate_projection(postcode) — long-term climate projections (2020s vs 2080s annual mean temperature, both moderate rcp45 and high-emissions rcp85 scenarios). Use when the user asks about long-term climate trends or strategic planning beyond a few years.

5. query_nema_guidelines(question) — semantic search over the official NEMA Disaster Recovery Funding Arrangements (DRFA) documents. Returns the top 5 most relevant text chunks plus source PDF and chunk_id. Use this whenever the user asks about disaster recovery grants, eligibility, Category A/B/C/D assistance, claimable costs, evidence requirements, or any DRFA rule. This tool does NOT need a postcode — pass a natural-language question directly.

6. get_industry_context(code) — Australian Bureau of Statistics sector context for a 2-digit ANZSIC Subdivision code: industry-wide employment, total income, value added, profit, wages, plus derived ratios (revenue per employee, wages share, EBITDA margin, value-added intensity). Use this when the user mentions an industry (farming, hospitality, retail, construction, manufacturing, etc.) or when grounding grant estimates, loss calculations, or impact discussions in real ABS sector data. The argument is a 2-digit ANZSIC Subdivision STRING (NOT a 4-digit Class code). Common codes: "01" Agriculture (covers all farming and primary production, including dairy), "45" Food and Beverage Services, "43" Retail Trade, "11" Food Product Manufacturing, "32" Construction Services, "03" Forestry and Logging. This tool does NOT need a postcode or ABN — pass an ANZSIC code directly. Employment values are in THOUSANDS; monetary values are in AUD MILLIONS.

7. generate_grant_pdf(abn, entity_name, entity_state, entity_postcode, disaster_type, disaster_date, drfa_category, estimated_loss_aud, justification) — composes your reasoning into a structured DRAFT grant application under the Australian DRFA. Use this ONLY as the FINAL step, AFTER you have already gathered the relevant context with the other tools (verify_abn for identity, get_active_hazards or similar for disaster evidence, query_nema_guidelines for the cited DRFA rules, get_industry_context for impact framing). Args you pass: abn, entity_name, entity_state, entity_postcode are the identity fields from your prior verify_abn call — pass them through verbatim. disaster_type from one of {flood, fire, storm, earthquake, drought, cyclone}; disaster_date as YYYY-MM-DD; drfa_category as a single letter A/B/C/D based on the DRFA chunks you cited; estimated_loss_aud as a positive whole-dollar number; justification as a 2-4 sentence narrative composed in plain English. Returns a STRUCT with application_id, applicant identity, disaster details, grant_request fields, a status flag (DRAFT or INVALID), and a next_steps checklist. This tool returns structured DATA, not a PDF file — the Streamlit app will render the PDF separately. Call this at most ONCE per conversation.

Call order:
- If the user provides an ABN, call verify_abn FIRST and read the postcode field from its output.
- Then call any combination of tools 2–6 based on what the user actually asked. You can call multiple tools in sequence without asking permission first.
- If the user asks a broad situational question ("how am I doing?", "what's the outlook?"), default to calling tools 2–4 (weather + hazards + climate).
- If the user asks about disaster recovery, grants, eligibility, or any DRFA rule, ALWAYS call query_nema_guidelines (tool 5). It does not require an ABN.
- If the user mentions their industry, or you need typical-business or sector-scale numbers, call get_industry_context (tool 6). Infer the right 2-digit ANZSIC code from the user's description — for example, dairy farming, cattle, sheep, or any primary production maps to "01" (Agriculture).
- If the user explicitly asks for a grant application, draft, or pre-filled form — call generate_grant_pdf (tool 7) as the FINAL step. Do this only AFTER you have gathered enough context (at minimum verify_abn and query_nema_guidelines; ideally also get_active_hazards and get_industry_context for stronger justification). Pass the entity_name, entity_state, entity_postcode args using the fields from your prior verify_abn output — do NOT make them up. Never call generate_grant_pdf without first calling query_nema_guidelines — your DRFA category choice must be informed by cited rules. NEVER call generate_grant_pdf twice in one conversation.
- If the user did not provide an ABN and the question requires one (tools 1–4, 7), ask politely for the 11-digit ABN before calling any tool. Tools 5 and 6 do not require an ABN.

Output format:
- Compose results into a brief multi-paragraph summary, ONE paragraph per data domain you queried (identity, weather, hazards, climate, grant-rules, industry context). Skip a domain entirely if you did not call its tool.
- Cite specific numbers and facts the tools returned. Round numeric values to one decimal place where appropriate.
- If a tool returns an error field, mention the error briefly in its paragraph and continue with the other tools where possible.
- If the business is NOT in NSW (in_nsw=false), say so clearly and skip tools 2–4 — they only work for NSW postcodes. Tools 5 and 6 still work for any location.
- When you used query_nema_guidelines, ALWAYS cite the source PDF filename and the page number in parentheses after each rule you quote, e.g. "Category C requires evidence of damage (DRFA Determination 2018.pdf, page 47)". NEVER quote a DRFA rule without a citation. If the retrieved chunks do not directly cover the user's question, say "the DRFA documents I have access to don't directly cover this" rather than inventing rules.
- When you used get_industry_context, frame numbers as SECTOR-WIDE TOTALS, not per-business figures (e.g. "the agriculture industry employs 337,000 people across Australia and generates $104.8 billion in annual income"). Convert units for readability: multiply num_employees_thousand by 1000, and quote money in billions where appropriate ($104,830M = $104.8B). Never claim "a typical business in your industry has X employees" — we don't have business counts.
- When you used generate_grant_pdf, surface the application_id in your reply (e.g. "Draft application: <UUID>"), present the next_steps checklist as a numbered list, and explicitly tell the user this is a DRAFT for their review — not a submitted application. If the tool returned status='INVALID', explain the error in plain language and offer to retry with corrected inputs.
- Total length: aim for 6–14 sentences across all paragraphs. Be concise, don't restate the user question."""


# ───────────────────────────────────────────────────────────────────────────
# Agent factory — same LangGraph create_react_agent as notebook 07
# ───────────────────────────────────────────────────────────────────────────
def _build_agent():
    """Build the LangGraph ReAct agent with LLM + 6 UC tools + 1 Python tool."""
    llm = ChatDatabricks(endpoint=LLM_ENDPOINT, temperature=0)
    uc_tools = UCFunctionToolkit(function_names=TOOL_FUNCTIONS).tools
    tools = uc_tools + [verify_abn]   # 6 UC tools + verify_abn (Python)
    return create_react_agent(model=llm, tools=tools, prompt=SYSTEM_PROMPT)


# ───────────────────────────────────────────────────────────────────────────
# ResponsesAgent wrapper — the Mosaic AI Agent Framework contract
# ───────────────────────────────────────────────────────────────────────────
def _extract_text(content: Any) -> str:
    """Coerce ResponsesAgent input content into plain text.

    ResponsesAgent inputs allow content to be either a plain string OR a list
    of typed parts (e.g. [{"type": "input_text", "text": "..."}, ...]). Handle
    both — we only care about text for now (no images).
    """
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts = []
        for c in content:
            if isinstance(c, dict):
                parts.append(c.get("text") or c.get("value") or "")
            elif isinstance(c, str):
                parts.append(c)
        return "".join(parts)
    return str(content) if content is not None else ""


def _to_lg_messages(request_input: list) -> list[dict]:
    """Convert ResponsesAgent request.input → LangGraph messages list."""
    lg_messages = []
    for item in request_input:
        # item may be a Pydantic model or a dict; tolerate both
        if hasattr(item, "model_dump"):
            d = item.model_dump()
        elif isinstance(item, dict):
            d = item
        else:
            d = {"role": getattr(item, "role", "user"), "content": getattr(item, "content", "")}

        role = d.get("role", "user")
        text = _extract_text(d.get("content", ""))
        if text:
            lg_messages.append({"role": role, "content": text})
    return lg_messages


class EcoResilienceAgent(ResponsesAgent):
    """Mosaic AI ResponsesAgent wrapping the LangGraph reasoning loop."""

    def __init__(self):
        self._agent = _build_agent()

    @mlflow.trace(span_type="AGENT", name="EcoResilienceAgent.predict")
    def predict(self, request: ResponsesAgentRequest) -> ResponsesAgentResponse:
        """Synchronous inference — returns full response in one shot.

        Tracing strategy (so trajectory eval works):
          1. @mlflow.trace decorator on this method establishes an AGENT span
             that becomes the parent for everything below.
          2. The explicit `mlflow.start_span(name="langgraph_invoke")` wraps
             the actual LangGraph execution — this is what mlflow.langchain.autolog
             needs to attach its tool-call + LLM-call child spans to.
          3. Without these two wrappers, Mosaic AI Serving emits only the outer
             "predict" span and all the autolog'd LangChain spans get orphaned
             and dropped — which is why earlier trajectory eval saw 0 tool spans.
        """
        lg_messages = _to_lg_messages(request.input)

        with mlflow.start_span(name="langgraph_invoke", span_type="CHAIN") as span:
            span.set_inputs({"messages": lg_messages})
            result = self._agent.invoke({"messages": lg_messages})
            span.set_outputs({"final_message": result["messages"][-1].content})

        final = result["messages"][-1].content
        return ResponsesAgentResponse(
            output=[
                {
                    "role": "assistant",
                    "type": "message",
                    "id": "agent-final",
                    "content": [{"type": "output_text", "text": final}],
                }
            ]
        )

    # predict_stream(...) intentionally not implemented for v1.
    # Streamlit UI in Phase 5 consumes full responses; streaming is a later refinement.


# ───────────────────────────────────────────────────────────────────────────
# Register the model — what mlflow.pyfunc.log_model() picks up
# ───────────────────────────────────────────────────────────────────────────
mlflow.models.set_model(EcoResilienceAgent())
