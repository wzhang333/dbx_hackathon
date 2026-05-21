# Databricks notebook source
# MAGIC %md
# MAGIC # 13 — Deploy EcoResilience AI agent to Mosaic AI Model Serving (v2)
# MAGIC
# MAGIC This notebook takes the agent defined in `eco_agent.py` and:
# MAGIC
# MAGIC 1. Installs deployment dependencies
# MAGIC 2. Sets the ABR API GUID as an env var so the local sanity check works
# MAGIC 3. Sanity-checks the agent locally (imports + one `.predict()` call)
# MAGIC 4. Logs it with `mlflow.pyfunc.log_model(...)` declaring the 6 UC tools + LLM endpoint + VS index as resources
# MAGIC 5. Registers it to Unity Catalog at `eco_resilience.silver.eco_resilience_agent`
# MAGIC 6. Deploys it via `databricks.agents.deploy(..., environment_vars=...)` — **the critical line** that injects the ABR GUID into the serving container
# MAGIC 7. Polls for endpoint READY and runs an HTTP smoke test
# MAGIC
# MAGIC ### Why v2 vs v1
# MAGIC
# MAGIC v1 used a UC SQL UDF `silver.verify_abn` that called `secret()` to get the
# MAGIC ABR API key. That `secret()` lookup did not resolve reliably in Mosaic AI
# MAGIC Model Serving auto-authentication-passthrough context — `verify_abn` and
# MAGIC therefore `generate_grant_pdf` failed silently in production. v2 moves
# MAGIC `verify_abn` out of UC into `eco_agent.py` as a LangChain Python tool that
# MAGIC reads `os.environ['ABR_AUTH_GUID']`. The env var is injected via
# MAGIC `agents.deploy(environment_vars={...})` using Databricks' `{{secrets/...}}`
# MAGIC syntax — the secret is resolved at endpoint startup, so plaintext only
# MAGIC ever lives inside the serving container.
# MAGIC
# MAGIC ### Pre-requisites
# MAGIC
# MAGIC - The 6 UC functions in `eco_resilience.silver` registered (notebooks 08, 09, 11, 12).
# MAGIC - Vector Search index `eco_resilience.bronze.drfa_chunks_index` ONLINE (notebook 00).
# MAGIC - LLM serving endpoint `databricks-claude-sonnet-4` available.
# MAGIC - Secret `abr_auth_guid` stored in scope `eco_resilience`.
# MAGIC - `eco_agent.py` and `requirements.txt` uploaded as Workspace Files next to this notebook.

# COMMAND ----------

# MAGIC %md
# MAGIC ## §1 — Install dependencies

# COMMAND ----------

# MAGIC %pip install -q -r requirements.txt

# COMMAND ----------

# MAGIC %md
# MAGIC ## §2 — Restart Python so freshly-installed packages are picked up

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

# MAGIC %md
# MAGIC ## §3 — Configuration

# COMMAND ----------

import os
import sys

# Option B layout — eco_agent.py + requirements.txt sit NEXT TO this notebook
# in the Workspace, so the project root is just the current working directory.
PROJECT_ROOT = os.getcwd()
AGENT_FILE   = os.path.join(PROJECT_ROOT, "eco_agent.py")
REQS_FILE    = os.path.join(PROJECT_ROOT, "requirements.txt")

# Make eco_agent.py importable for the §4 sanity check
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

CATALOG          = "eco_resilience"
SILVER_SCHEMA    = "silver"
MODEL_NAME       = "eco_resilience_agent"
UC_MODEL_NAME    = f"{CATALOG}.{SILVER_SCHEMA}.{MODEL_NAME}"
ENDPOINT_NAME    = "eco_resilience_agent"

LLM_ENDPOINT     = "databricks-claude-sonnet-4"
VS_INDEX_NAME    = f"{CATALOG}.bronze.drfa_chunks_index"

# Six UC tool functions. verify_abn is NOT here — it's a Python tool in
# eco_agent.py that reads os.environ['ABR_AUTH_GUID'].
TOOL_FUNCTIONS = [
    f"{CATALOG}.{SILVER_SCHEMA}.get_weather_forecast",
    f"{CATALOG}.{SILVER_SCHEMA}.get_active_hazards",
    f"{CATALOG}.{SILVER_SCHEMA}.get_climate_projection",
    f"{CATALOG}.{SILVER_SCHEMA}.query_nema_guidelines",
    f"{CATALOG}.{SILVER_SCHEMA}.get_industry_context",
    f"{CATALOG}.{SILVER_SCHEMA}.generate_grant_pdf",
]

# Databricks secret-reference syntax — Model Serving resolves this at endpoint
# startup and exposes the plaintext value via os.environ['ABR_AUTH_GUID'].
# The plaintext never appears in code, MLflow artifacts, git, or logs.
ABR_SECRET_REF = "{{secrets/eco_resilience/abr_auth_guid}}"

print(f"PROJECT_ROOT     = {PROJECT_ROOT}")
print(f"AGENT_FILE       = {AGENT_FILE}")
print(f"UC_MODEL_NAME    = {UC_MODEL_NAME}")
print(f"ENDPOINT_NAME    = {ENDPOINT_NAME}")
print(f"UC tools         = {len(TOOL_FUNCTIONS)} functions")
print(f"Python tools     = 1 (verify_abn — defined in eco_agent.py)")
print(f"Secret reference = {ABR_SECRET_REF}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## §4 — Local sanity check
# MAGIC
# MAGIC Set the env var (so verify_abn works locally), import `eco_agent.py`,
# MAGIC instantiate the agent, and call `predict()` once. If this errors, fix it
# MAGIC BEFORE logging — debugging a model-serving endpoint is much slower than
# MAGIC debugging a notebook.

# COMMAND ----------

# Replicate locally what the deployed endpoint will get from environment_vars.
os.environ["ABR_AUTH_GUID"] = dbutils.secrets.get(
    scope="eco_resilience", key="abr_auth_guid"
)
print(f"ABR_AUTH_GUID set ({len(os.environ['ABR_AUTH_GUID'])} chars)")

# COMMAND ----------

from mlflow.types.responses import ResponsesAgentRequest

import importlib
import eco_agent as agent_module
importlib.reload(agent_module)

local_agent = agent_module.EcoResilienceAgent()

sanity_request = ResponsesAgentRequest(
    input=[{"role": "user", "content": "Hello — please verify ABN 42173522302."}]
)

print("Calling local_agent.predict() ...")
sanity_response = local_agent.predict(sanity_request)

# COMMAND ----------


print("\n--- Output text ---")

# OutputItem uses attribute access, not dict subscripting
output_item = sanity_response.output[0]
output_text = output_item.content[0]['text']
print(output_text[:800])

assert "BATHURST REGIONAL COUNCIL" in output_text.upper(), \
    "Local sanity check FAILED — verify_abn didn't return Bathurst identity. " \
    "Don't deploy until this passes."
print("\n✅ Local sanity check passed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## §5 — Point MLflow at Unity Catalog as the model registry

# COMMAND ----------

import mlflow
mlflow.set_registry_uri("databricks-uc")
print("Registry URI:", mlflow.get_registry_uri())

# COMMAND ----------

# MAGIC %md
# MAGIC ## §6 — Log the agent with `mlflow.pyfunc.log_model(...)`
# MAGIC
# MAGIC RESOURCES declares the 6 UC tools + LLM endpoint + VS index. `verify_abn`
# MAGIC is NOT a UC resource (it runs in-process in the serving container), so it
# MAGIC does NOT appear here. The secret reference in §8's `environment_vars`
# MAGIC handles the GUID injection at deploy time — no `DatabricksSecret`
# MAGIC resource needed.

# COMMAND ----------

from mlflow.models.resources import (
    DatabricksServingEndpoint,
    DatabricksFunction,
    DatabricksVectorSearchIndex,
)

RESOURCES = [
    DatabricksServingEndpoint(endpoint_name=LLM_ENDPOINT),
    *[DatabricksFunction(function_name=fn) for fn in TOOL_FUNCTIONS],
    DatabricksVectorSearchIndex(index_name=VS_INDEX_NAME),
]
print(f"Declared {len(RESOURCES)} resources (1 LLM + {len(TOOL_FUNCTIONS)} functions + 1 VS index).")

# COMMAND ----------

SAMPLE_INPUT = {
    "input": [
        {"role": "user", "content": "Verify ABN 42173522302 and tell me about the business."}
    ]
}

with mlflow.start_run(run_name="eco_resilience_agent_v2") as run:
    logged = mlflow.pyfunc.log_model(
        python_model=AGENT_FILE,
        artifact_path="agent",
        pip_requirements=REQS_FILE,
        resources=RESOURCES,
        input_example=SAMPLE_INPUT,
    )
    run_id    = run.info.run_id
    model_uri = logged.model_uri

print(f"\nrun_id    = {run_id}")
print(f"model_uri = {model_uri}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## §7 — Register the logged model into Unity Catalog

# COMMAND ----------

registered = mlflow.register_model(model_uri=model_uri, name=UC_MODEL_NAME)
print(f"Registered model name    = {registered.name}")
print(f"Registered model version = {registered.version}")
MODEL_VERSION = registered.version

# COMMAND ----------

# MAGIC %md
# MAGIC ## §8 — Deploy via `databricks.agents.deploy(...)` with env-var injection
# MAGIC
# MAGIC **The critical line is `environment_vars=...`** — it tells the Mosaic AI
# MAGIC Agent Framework to resolve the `{{secrets/...}}` reference at endpoint
# MAGIC startup and expose the plaintext value via `os.environ['ABR_AUTH_GUID']`
# MAGIC inside the serving container. That env var is what `eco_agent.py`'s
# MAGIC `verify_abn` reads.
# MAGIC
# MAGIC `scale_to_zero_enabled=False` keeps the endpoint warm during demo week.
# MAGIC Flip to True after the recording to save cost.

# COMMAND ----------

from databricks import agents

deployment = agents.deploy(
    model_name=UC_MODEL_NAME,
    model_version=MODEL_VERSION,
    scale_to_zero_enabled=False,
    endpoint_name=ENDPOINT_NAME,
    environment_vars={"ABR_AUTH_GUID": ABR_SECRET_REF},
)
print("Deployment kicked off:")
print(f"  endpoint_name   = {deployment.endpoint_name}")
print(f"  query_endpoint  = {deployment.query_endpoint}")
print(f"  review_app_url  = {getattr(deployment, 'review_app_url', '(n/a)')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## §9 — Wait for the endpoint to be READY
# MAGIC
# MAGIC First-time provisioning takes ~10 min. Poll until READY or fail after 20 min.

# COMMAND ----------



# COMMAND ----------

import time
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
deadline = time.time() + 20 * 60

while True:
    ep = w.serving_endpoints.get(name=ENDPOINT_NAME)
    state        = ep.state.ready if ep.state else None
    config_state = ep.state.config_update if ep.state else None
    print(f"[{int(time.time())}] ready={state} config_update={config_state}")

    if state and str(state).upper().endswith("READY"):
        print("Endpoint READY.")
        break
    if time.time() > deadline:
        raise TimeoutError("Endpoint did not become READY within 20 minutes — check Serving UI.")
    time.sleep(30)

# COMMAND ----------

# MAGIC %md
# MAGIC ## §10 — HTTP smoke test (the Tom-the-farmer Magic Moment)
# MAGIC
# MAGIC Hit the deployed endpoint with the full Magic-Moment prompt. We're
# MAGIC confirming all 7 tools work in the serving context — verify_abn (Python
# MAGIC tool with env-var GUID) + 6 UC tools.

# COMMAND ----------

import json
import requests

ctx       = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
host      = ctx.apiUrl().get()
token     = ctx.apiToken().get()

query_url = f"{host}/serving-endpoints/{ENDPOINT_NAME}/invocations"
headers   = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


""" test case for generate_grant_pdf:

I run a dairy farm in Bathurst NSW. My ABN is 42173522302. There was a flood here on 2026-05-10 with estimated damages of $48,500. Please prepare a DRFA grant application. 

"""


payload = {
    "input": [
        {
            "role": "user",
            "content": (
                "I run a dairy farm in Bathurst NSW. My ABN is 42173522302. "
                "There was a flood here on 2026-05-10 with estimated damages of $48,500. "
                "Please verify my business, check active hazards, search the DRFA rules, "
                "and prepare a draft grant application for me."
            ),
        }
    ]
}

print(f"POST {query_url}")
r = requests.post(query_url, headers=headers, data=json.dumps(payload), timeout=180)
print(f"HTTP {r.status_code}")
response_text = json.dumps(r.json(), indent=2)
print(response_text[:3000])

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### §10b — Verify the response contains the expected content

# COMMAND ----------

response_body = r.json()
final_text = response_body["output"][0]["content"][0]["text"]

checks = [
    ("Bathurst Regional Council", "BATHURST" in final_text.upper()),
    ("Draft application UUID surfaced", "Draft application" in final_text or "application_id" in final_text.lower()),
    ("DRFA citation with page number", "page" in final_text.lower() and (".pdf" in final_text.lower() or "DRFA" in final_text)),
]
print("Verification checks:")
for name, passed in checks:
    icon = "✅" if passed else "❌"
    print(f"  {icon}  {name}")

if not all(passed for _, passed in checks):
    print("\n⚠️  One or more checks failed — open the MLflow trace from this invocation to inspect each tool span.")
else:
    print("\n🎉  All checks passed. Endpoint is demo-ready.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## §11 — Done

# COMMAND ----------

print("=== EcoResilience AI agent — deployed v2 ===")
print(f"UC model                = {UC_MODEL_NAME} (v{MODEL_VERSION})")
print(f"Endpoint name           = {ENDPOINT_NAME}")
print(f"Query URL               = {query_url}")
print(f"MLflow run_id           = {run_id}")
print(f"Inference Table (auto)  = {CATALOG}.{SILVER_SCHEMA}.{MODEL_NAME}_payload")
print()
print("Phase 5 (Streamlit UI) can now POST to the query URL above.")
print()
print("To inspect per-tool-call spans: open the MLflow Experiments UI for the")
print("experiment linked above, find the trace for the §10 request, and expand")
print("the spans for verify_abn / generate_grant_pdf / each UC tool.")