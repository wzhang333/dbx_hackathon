# EcoResilience AI — GCP-Native Codebase

Complete refactor of the Databricks `eco-resilience-bundle` onto Google Cloud.
Zero Databricks dependency. Same application behaviour, same frontend
(`app/index.html` unchanged), same agent reasoning contract.

```
gcp/
├── agent/                  # Shared agent package (was eco_agent.py + 6 UC SQL UDFs)
│   ├── config.py           #   env-driven project/dataset/model config
│   ├── tools.py            #   the 7 tools as Python functions (BigQuery/Vertex AI)
│   └── agent.py            #   LangGraph ReAct agent, Gemini or Claude-on-Vertex
├── app/                    # Cloud Run service (was Databricks Apps)
│   ├── app.py              #   Flask backend — all /api/* routes preserved
│   ├── abn_ingest.py       #   synchronous ABR→BigQuery ingest (was a Jobs pipeline)
│   └── index.html          #   unchanged frontend
├── jobs/                   # Cloud Run Jobs (were Databricks Jobs/notebooks)
│   ├── ingest_weather/     #   Open-Meteo → bronze+silver, every 6h
│   ├── ingest_hazards/     #   TfNSW live hazards → bronze+silver, hourly
│   ├── seed_reference/     #   one-time: POA H3 lookup, CSIRO, ABS industry
│   └── ingest_drfa_rag/    #   one-time: DRFA PDFs → chunks+embeddings in BQ
├── transforms/             # SQL views (was DLT pipeline)
└── infra/                  # numbered gcloud scripts (was databricks.yml bundle)
```

## Service mapping

| Databricks | GCP replacement |
|---|---|
| Delta Lake + Unity Catalog | BigQuery (`eco_bronze` / `eco_silver` / `eco_gold` datasets) |
| DLT / Lakeflow pipelines | SQL inside Cloud Run Jobs + a BQ view |
| Databricks Jobs + cron | Cloud Scheduler → Cloud Run Jobs |
| Databricks Apps (Flask) | Cloud Run service |
| Model Serving agent endpoint | LangGraph agent **in-process** in the Cloud Run app |
| `databricks-claude-sonnet-4` | Gemini 2.5 Pro (default) or Claude on Vertex AI (`LLM_PROVIDER=claude`) |
| UC SQL UDF tools | Python `@tool` functions (`agent/tools.py`) |
| Vector Search index + endpoint | BigQuery `VECTOR_SEARCH` + Vertex AI `text-embedding-005` (serverless, no always-on endpoint) |
| Databricks Secrets | Secret Manager |
| Lakebase (Postgres) | Cloud SQL for PostgreSQL (IAM auth) |
| UC Volumes | Cloud Storage bucket |
| MLflow tracing | OpenTelemetry → Cloud Trace (optional) |
| H3 SQL functions (`h3_polyfillash3`, …) | `h3` Python library |
| `ai_parse_document` / `ai_prep_search` | PyMuPDF + fixed-size chunking |

## Deploy

Full walkthrough: [`../docs/DEPLOYMENT_GUIDE.md`](../docs/DEPLOYMENT_GUIDE.md). Short version:

```bash
export PROJECT_ID=<your-project> REGION=australia-southeast1 BUCKET=<your-bucket>
bash gcp/infra/00_bootstrap.sh
GOOGLE_CLOUD_PROJECT=$PROJECT_ID python gcp/infra/01_bigquery_schema.py
bash gcp/infra/02_secrets.sh
# upload reference data to gs://$BUCKET/reference/ (POA zip, CSIRO CSVs, DRFA PDFs)
bash gcp/infra/03_build_and_deploy_jobs.sh      # + run the one-time seeds it prints
bash gcp/infra/04_scheduler.sh
bash gcp/infra/05_cloudsql.sh                   # optional (grant history)
bash gcp/infra/06_deploy_app.sh
```

## Run locally

```bash
gcloud auth application-default login
cd gcp && pip install -r app/requirements.txt
GOOGLE_CLOUD_PROJECT=<your-project> python app/app.py   # http://localhost:8080
```
