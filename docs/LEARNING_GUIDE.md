# Learning Guide — Databricks → GCP Migration

This document turns the migration into a study resource. It explains **what
each Databricks concept maps to on GCP, why the mapping works, where the
abstractions differ**, and gives exercises against your own deployed stack.
Read it next to the code: every section points at the exact file that
implements it.

---

## 1. The mental model shift

Databricks is a **vertically integrated lakehouse**: one platform owns
storage format (Delta), catalog (Unity Catalog), compute (Spark), pipelines
(DLT), serving (Mosaic AI), secrets, and apps. GCP gives you the same
capabilities as **independent primitives** you compose yourself:

```
Databricks:  one workspace, everything inside it
GCP:         BigQuery + Cloud Run + Scheduler + Vertex AI + Secret Manager
             glued together by IAM service accounts
```

Two consequences you'll feel everywhere:

1. **IAM replaces implicit trust.** On Databricks, a notebook could read any
   table its user could. On GCP, *each workload runs as a service account*
   and gets only the roles you bind (`infra/00_bootstrap.sh`). When something
   fails with 403, your first question is always "which SA is this running
   as, and does it have the role?"
2. **Containers replace notebooks.** Every pipeline is now a Docker image
   with a `main.py` that exits 0 or 1. No attached cluster, no `%pip install`
   at the top of a notebook — dependencies are baked into the image.

---

## 2. BigQuery vs Delta Lake / Unity Catalog

**Code:** `gcp/infra/01_bigquery_schema.py`, all `jobs/*/main.py`

| Delta/UC concept | BigQuery equivalent | Notes |
|---|---|---|
| `catalog.schema.table` | `project.dataset.table` | Datasets are flat — `eco_resilience.bronze` became `eco_bronze` |
| `CLUSTER BY` (liquid clustering) | `CLUSTER BY` | Nearly identical syntax and purpose |
| Z-ordering | Clustering + **day partitioning** | Bronze tables partition on `DATE(_ingest_time)`; partitions also cap query cost |
| Delta time travel | Partitioned append-only history + `FOR SYSTEM_TIME AS OF` (7-day window) | We keep every ingest batch in bronze, so "time travel" is just a WHERE clause |
| `MERGE INTO` | `MERGE` | Same SQL — see `app/abn_ingest.py` |
| Spark DataFrame writes | `load_table_from_json` / `load_table_from_dataframe` | **Batch loads are free**; avoid `insert_rows_json` (streaming) unless you need sub-second availability — streamed rows can't be UPDATE/DELETEd for ~30 min |
| SQL Warehouse | nothing! | BigQuery is serverless; the Flask app just calls `bq.query()` |

Key habit change: **BigQuery bills by bytes scanned** (on-demand pricing).
`SELECT *` on a wide table costs real money at scale. Partition filters and
selecting only needed columns are the optimisation levers (not cluster
sizing, which no longer exists).

**Exercise:** run the same query on `eco_bronze.open_meteo_forecast` with and
without a `_ingest_time` filter and compare "bytes processed" in the query
plan (BigQuery console → Execution details).

## 3. Cloud Run Jobs vs Databricks Jobs

**Code:** `gcp/jobs/*/main.py`, `infra/03_build_and_deploy_jobs.sh`, `infra/04_scheduler.sh`

- A **Cloud Run Job** is a container that runs to completion. Exit 0 =
  success; non-zero = retry (we set `--max-retries 2`). This replaces a
  Databricks job task; the Dockerfile replaces the cluster spec.
- **Cloud Scheduler** is bare cron-as-a-service. It doesn't know about job
  dependencies — it just POSTs to the job's `:run` URL with an OAuth token.
  Compare `infra/04_scheduler.sh` with the old `refresh_weather.job.yml`.
- There is **no built-in DAG** (no `depends_on` between tasks). Our pipeline
  avoided needing one by folding silver rebuilds into the same job as the
  bronze ingest. When you outgrow that, the GCP-native ladder is:
  Cloud Workflows (simple step chains) → Cloud Composer / Airflow (real DAGs).

**Notable simplification in this migration:** the on-demand ABN pipeline
(Databricks job with 2 tasks + frontend polling) became a synchronous
function call (`app/abn_ingest.py`). A 1-second API call never needed a
distributed scheduler — recognising that is itself a cloud-architecture
lesson: *match the tool to the latency class of the work.*

**Exercise:** add a third scheduled job that snapshots
`eco_silver.hazards_current` counts into a `eco_gold.hazard_daily_stats`
table once a day. You'll touch: Dockerfile, job deploy, scheduler trigger.

## 4. The agent: Mosaic AI → in-process LangGraph on Vertex AI

**Code:** `gcp/agent/agent.py`, `gcp/agent/tools.py`

What stayed identical (because LangGraph is vendor-neutral):
- `create_react_agent(model, tools, prompt)` — the exact same call
- The SYSTEM_PROMPT — copied verbatim from `eco_agent.py`
- The ReAct loop: LLM decides → tool executes → result fed back → repeat

What changed:

| Databricks | GCP | Why it matters |
|---|---|---|
| `ChatDatabricks(endpoint=...)` | `ChatVertexAI` / `ChatAnthropicVertex` | LangChain's provider classes are drop-in swaps; auth flows from Application Default Credentials, no API key |
| `UCFunctionToolkit(function_names=[...])` | plain `@tool` Python functions | UC discovered tools from SQL COMMENTs; here the **docstring is the tool description** the LLM sees. Same prompt-engineering rules apply: say when to call it, what units, what the error field means |
| `mlflow.pyfunc.ResponsesAgent` wrapper + `agents.deploy()` | none — agent runs inside Flask | A serving wrapper only earns its complexity when the agent scales independently of the app |
| MLflow autolog traces | OpenTelemetry → Cloud Trace | `setup_tracing()` in agent.py |

The deepest lesson from the original codebase carries over: the UC SQL UDFs
returned **typed STRUCTs with an `error` field** instead of raising. The
Python ports keep that contract (`tools.py` returns
`{"...": ..., "error": None}`) because agents handle *data* about failures
far better than exceptions — an exception kills the reasoning loop; an error
field lets the LLM explain and continue.

**Exercise:** flip `LLM_PROVIDER=claude` and compare tool-calling behaviour
between Gemini and Claude on the same "verify ABN then summarise weather +
hazards" prompt. Look at how many tool calls each makes and in what order.

## 5. RAG: Databricks Vector Search → BigQuery VECTOR_SEARCH

**Code:** `gcp/jobs/ingest_drfa_rag/main.py`, `query_nema_guidelines` in `gcp/agent/tools.py`

The Databricks pipeline was: `ai_parse_document` → `ai_prep_search` →
Delta-Sync index on an always-on Vector Search endpoint, queried by the SQL
`vector_search()` function. The GCP replacement does each stage explicitly:

1. **Parse**: PyMuPDF extracts per-page text (page numbers preserved because
   the agent's citation contract — "(DRFA Determination 2018.pdf, page 47)" —
   depends on them).
2. **Chunk**: fixed-size 1200 chars with 200 overlap. Cruder than
   `ai_prep_search`'s semantic chunking, but transparent and tunable.
3. **Embed**: Vertex AI `text-embedding-005` (768-dim), with
   `task_type=RETRIEVAL_DOCUMENT` for corpus and `RETRIEVAL_QUERY` for
   questions — an asymmetric-embedding detail that measurably improves recall.
4. **Store + search**: embeddings live in a plain BigQuery column
   (`ARRAY<FLOAT64>`); `VECTOR_SEARCH(TABLE ..., 'embedding', (SELECT @qvec), top_k => 5)`
   does brute-force cosine search.

Why not Vertex AI Vector Search (the "obvious" mapping)? **Scale honesty.**
The corpus is ~1–2k chunks. Brute force over that is milliseconds in
BigQuery and costs fractions of a cent, while a Vector Search endpoint is
always-on (~$70+/month) and adds index-sync operational surface. The
graduation path when a corpus hits millions of vectors: `CREATE VECTOR
INDEX` in BigQuery (IVF/TreeAH), then Vertex AI Vector Search when you need
<10 ms latency at high QPS.

**Exercise:** ask the deployed agent a DRFA question, then re-run
`ingest_drfa_rag` with `CHUNK_SIZE=600` and compare answer citations. You've
just done a chunking-strategy evaluation — a core RAG skill.

## 6. Geospatial: Databricks H3 SQL → `h3` Python

**Code:** `jobs/seed_reference/main.py` (polyfill), `app/app.py` `/api/h3-cells` (boundaries)

Databricks has H3 built into SQL (`h3_polyfillash3`, `h3_longlatash3`,
`h3_boundaryaswkt`). BigQuery doesn't, so the H3 work moved to Python at
ingestion time:

- `h3.geo_to_cells(geojson, 8)` — polygon → cells (was `h3_polyfillash3`)
- `h3.latlng_to_cell(lat, lon, 8)` — point → cell (was `h3_longlatash3`)
- `h3.cell_to_boundary(cell)` — cell → polygon for the Leaflet map (was `h3_boundaryaswkt`)

The join strategy is unchanged: hazards and postcodes both carry resolution-8
H3 cell IDs (stored as STRINGs), so "hazards in my postcode" is a plain
equality join in BigQuery. Alternative worth knowing: BigQuery has native
`ST_*` geography functions — you could store POA polygons as `GEOGRAPHY` and
use `ST_CONTAINS`, trading H3's fixed-resolution simplicity for exact
boundaries.

## 7. Secrets & identity

**Code:** `agent/config.py::get_secret`, `infra/02_secrets.sh`, `infra/05_cloudsql.sh`

- `dbutils.secrets.get(scope, key)` → `SecretManagerServiceClient().access_secret_version()`.
  Same idea; the GCP version is versioned and IAM-gated per secret.
- The interesting one is **Cloud SQL IAM auth**: Lakebase minted short-lived
  JWTs (`generate_database_credential`); Cloud SQL's equivalent is
  `enable_iam_auth=True` in the Python Connector — the service account *is*
  the database user, no password exists at all. This "identity, not
  credentials" pattern is the direction all clouds are moving.
- Local dev fallback: `get_secret()` reads an env var first, so you can run
  everything locally without touching Secret Manager.

## 8. What we deliberately did NOT rebuild

Part of migration skill is knowing what to drop:

| Databricks piece | Decision | Rationale |
|---|---|---|
| Genie (self-serve BI) | Point Looker Studio at `eco_gold.*` if needed | BI tool, not app logic |
| MLflow model registry + agent evaluation notebooks | Dropped; use Vertex AI Experiments if/when you rebuild evals | No model artifacts left — the agent is code, not a registered model |
| DLT declarative pipeline | Folded into jobs + one view | 3 tables don't need a framework |
| Streaming/Auto Loader | Not present in the original either | Batch every 1–6 h fits the data's real cadence |
| The `archieve/` frontends | Ignored | Dead code |

## 9. Suggested learning path (with your own stack as the lab)

1. **Week 1 — BigQuery**: re-run the seed jobs, explore partitioning/cost in
   the console. Course: *Google Cloud Skills Boost — BigQuery for Data
   Analysts*. Docs: partitioned tables, MERGE.
2. **Week 2 — Cloud Run**: read both Dockerfiles, then deliberately break and
   fix a deploy (wrong port, missing IAM role) to learn the failure modes.
   Docs: Cloud Run Jobs, service identity.
3. **Week 3 — Vertex AI + LangGraph**: swap Gemini↔Claude, add an 8th tool
   (e.g. Open-Meteo flood API), watch traces in Cloud Trace.
   Docs: LangGraph quickstart, Vertex AI Model Garden.
4. **Week 4 — RAG engineering**: chunking experiments (§5 exercise), then add
   `CREATE VECTOR INDEX` and compare query plans.
5. **Later — productionisation**: Terraform-ify `infra/*.sh` (each script is
   ~1:1 with a `google_*` resource), add a Cloud Build trigger for CI/CD,
   add dbt for the gold layer.

### Certifications this maps to

| Cert | Covered by this project |
|---|---|
| Associate Cloud Engineer | IAM, Cloud Run, Scheduler, gcloud — ~60 % of the blueprint |
| Professional Data Engineer | BigQuery modelling, pipelines, partitioning — ~50 % |
| Professional ML Engineer | Vertex AI serving, embeddings, agent patterns — ~35 % |

### Reference links

- BigQuery VECTOR_SEARCH: https://cloud.google.com/bigquery/docs/vector-search
- Cloud Run Jobs: https://cloud.google.com/run/docs/create-jobs
- Cloud SQL IAM auth: https://cloud.google.com/sql/docs/postgres/iam-authentication
- Vertex AI text embeddings: https://cloud.google.com/vertex-ai/generative-ai/docs/embeddings/get-text-embeddings
- Claude on Vertex AI: https://cloud.google.com/vertex-ai/generative-ai/docs/partner-models/use-claude
- LangGraph: https://langchain-ai.github.io/langgraph/
- h3-py v4 API: https://uber.github.io/h3-py/
- OpenTelemetry → Cloud Trace: https://cloud.google.com/trace/docs/setup/python-ot
