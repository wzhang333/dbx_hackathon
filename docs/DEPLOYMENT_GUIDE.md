# EcoResilience AI — GCP Deployment Guide

Step-by-step instructions to deploy the refactored codebase in `gcp/` to your
personal GCP project. Every command is copy-pasteable; expected time for a
first full deploy is **half a day**, most of it waiting for builds and seeds.

> Prerequisites: `gcloud` CLI ≥ 480, `bq` CLI, Python 3.11+, a GCP project
> with billing enabled, and Owner (or equivalent) on that project.

---

## 0. One-time shell setup

```bash
export PROJECT_ID=<your-project-id>          # e.g. eco-resilience-ai
export REGION=australia-southeast1
export BUCKET=<globally-unique-bucket-name>  # e.g. ${PROJECT_ID}-landing

gcloud auth login
gcloud config set project $PROJECT_ID
gcloud auth application-default login        # for running Python scripts locally
```

If your bucket name differs from the default `eco-resilience-landing`, the
jobs pick it up via the `LANDING_BUCKET` env var (already wired in the
deploy scripts through `$BUCKET`).

---

## 1. Bootstrap the project (≈10 min)

```bash
bash gcp/infra/00_bootstrap.sh
```

What it does — and what each piece replaces:

| Step | Creates | Replaces |
|---|---|---|
| Enable APIs | BigQuery, Cloud Run, Scheduler, Secret Manager, Vertex AI, Cloud SQL, Artifact Registry, Cloud Build, Trace | Databricks workspace features |
| GCS bucket | `gs://$BUCKET` | Unity Catalog Volumes |
| BQ datasets | `eco_bronze`, `eco_silver`, `eco_gold` | UC schemas `bronze/silver/gold` |
| Artifact Registry | `eco-resilience` docker repo | (no equivalent — DBX ran notebooks directly) |
| Service accounts | `eco-app`, `eco-pipeline` + IAM roles | App service principal + job run-as identity |

Verify:

```bash
bq ls                                   # should list eco_bronze, eco_silver, eco_gold
gcloud iam service-accounts list        # eco-app, eco-pipeline
```

## 2. Create BigQuery tables (≈1 min)

```bash
pip install google-cloud-bigquery
GOOGLE_CLOUD_PROJECT=$PROJECT_ID python gcp/infra/01_bigquery_schema.py
```

Creates the append-only bronze tables (day-partitioned on `_ingest_time`,
clustered — the BigQuery idiom that replaces Delta time-travel + liquid
clustering), the MERGE targets (`abn_lookup_structured`, `business_details`,
`supplier_relationships_history`), and the `supplier_relationships` view.
Tables built by `CREATE OR REPLACE` inside the jobs (weather/hazards silver,
lookups, `drfa_chunks`) are intentionally *not* pre-created.

## 3. Store secrets (≈2 min)

```bash
bash gcp/infra/02_secrets.sh
```

Prompts for the **ABR auth GUID** (register free at
https://abr.business.gov.au/Tools/WebServices) and the **TfNSW API key**
(https://opendata.transport.nsw.gov.au). These were previously in the
Databricks secret scope `eco_resilience`.

## 4. Upload reference data (≈15 min)

Export these from the old Databricks Volumes (or re-download from source),
then:

```bash
# ABS POA 2021 boundaries (https://www.abs.gov.au/statistics/standards/
#   australian-statistical-geography-standard-asgs-edition-3/jul2021-jun2026/access-and-downloads/digital-boundary-files)
gcloud storage cp POA_2021_AUST_GDA2020_SHP.zip gs://$BUCKET/reference/poa/

# CSIRO Climate Change in Australia station CSVs (6 files)
gcloud storage cp csiro/*.csv gs://$BUCKET/reference/csiro/

# NEMA DRFA policy PDFs (16 files, https://www.nema.gov.au)
gcloud storage cp drfa_pdfs/*.pdf gs://$BUCKET/reference/drfa_pdfs/
```

## 5. Build + deploy the pipeline jobs (≈20 min)

```bash
bash gcp/infra/03_build_and_deploy_jobs.sh
```

Builds four images with Cloud Build and creates four Cloud Run **Jobs**
(containers that run to completion — the direct analogue of a Databricks
job task). Then run the one-time seeds **in this order** (weather depends on
`poa_centroids` from the seed job):

```bash
gcloud run jobs execute seed-reference  --region $REGION --wait   # ~10–20 min (H3 polyfill)
gcloud run jobs execute ingest-weather  --region $REGION --wait   # ~1 min
gcloud run jobs execute ingest-hazards  --region $REGION --wait   # ~1 min
gcloud run jobs execute ingest-drfa-rag --region $REGION --wait   # ~5–10 min (embeddings)
```

Verify the data landed:

```bash
bq query --use_legacy_sql=false 'SELECT COUNT(*) FROM eco_silver.poa_h3_lookup'
bq query --use_legacy_sql=false 'SELECT COUNT(*) FROM eco_silver.weather_current'
bq query --use_legacy_sql=false 'SELECT COUNT(*) FROM eco_bronze.drfa_chunks'
```

## 6. Schedule the recurring refreshes (≈2 min)

```bash
bash gcp/infra/04_scheduler.sh
```

Weather every 6 h, hazards hourly — Cloud Scheduler fires an authenticated
HTTP POST at the Cloud Run Jobs `:run` endpoint (replaces the cron blocks in
`refresh_weather.job.yml` / `refresh_hazards.job.yml`).

## 7. (Optional) Cloud SQL for grant history (≈15 min)

```bash
bash gcp/infra/05_cloudsql.sh
```

Then open **Cloud SQL Studio** in the console and run the `CREATE TABLE
grant_submissions` + `GRANT` statements the script prints. If you skip this
step the app still works; `/api/grant-history` just returns an empty list
(same graceful degradation the Lakebase integration had).

## 8. Deploy the app + agent (≈10 min)

```bash
# defaults to Gemini 2.5 Pro in australia-southeast1
bash gcp/infra/06_deploy_app.sh

# — or with Claude + grant history —
# First enable Claude in Vertex AI Model Garden (console → Model Garden →
# search "Claude" → Enable), then:
LLM_PROVIDER=claude \
CLOUD_SQL_CONNECTION_NAME=$PROJECT_ID:$REGION:eco-resilience-oltp \
CLOUD_SQL_USER=eco-app@$PROJECT_ID.iam \
bash gcp/infra/06_deploy_app.sh
```

Architectural note: the LangGraph agent runs **inside** the Flask container.
On Databricks it was a separate Model Serving endpoint; on GCP a separate
hop buys nothing at this scale and doubles the debugging surface. If you
later want the managed option, deploy `gcp/agent/` to **Vertex AI Agent
Engine** and swap `_call_eco_resilience_agent()` in `app.py` for a
`reasoning_engines` client call.

## 9. Smoke test

```bash
URL=$(gcloud run services describe eco-resilience-app --region $REGION --format='value(status.url)')

curl "$URL/api/health"
curl -X POST "$URL/api/ingest-only"    -H 'Content-Type: application/json' -d '{"abn":"42173522302"}'
curl -X POST "$URL/api/business-details" -H 'Content-Type: application/json' -d '{"abn":"42173522302"}'
curl -X POST "$URL/api/nearby-hazards" -H 'Content-Type: application/json' -d '{"postcode":"2795"}'
curl -X POST "$URL/api/ace-opening"    -H 'Content-Type: application/json' \
     -d '{"abn":"42173522302","business_name":"BATHURST REGIONAL COUNCIL","postcode":"2795"}'
```

Then open `$URL` in a browser and run the full flow: Verify ABN → Assess
Risk → chat with ACE → Generate Grant Draft (downloads the PDF).

## 10. Observability (optional, ≈15 min)

- **Logs**: Cloud Run → service/job → Logs (the app's `[PERF]` lines land here).
- **Traces**: install `opentelemetry-exporter-gcp-trace` in `app/requirements.txt`;
  `setup_tracing()` in `agent/agent.py` already wires the exporter.
- **Dashboards**: Cloud Monitoring → create a dashboard with Cloud Run request
  latency (p50/p95/p99), BigQuery bytes billed, Scheduler job success ratio.
- **Budget alert**: Billing → Budgets → alert at e.g. AUD 50/month. Vertex AI
  LLM calls are the dominant cost driver.

---

## Frontend behaviour changes to be aware of

The `index.html` frontend is unchanged, but two backend semantics shifted:

1. **ABN ingestion is synchronous now.** `/api/trigger-job` does the ABR
   lookup + BigQuery MERGE inline (~2 s) and `/api/job-status/<id>` always
   reports `TERMINATED/SUCCESS`, so the frontend's polling loop exits on its
   first poll. The old flow triggered a Databricks job and polled for
   30–60 s while a cluster spun up.
2. **`/api/debug` and `/api/test-ace`** (Databricks-specific diagnostics)
   were dropped; `/api/health` remains.

## Teardown

```bash
gcloud run services delete eco-resilience-app --region $REGION -q
for j in ingest-weather ingest-hazards seed-reference ingest-drfa-rag; do
  gcloud run jobs delete $j --region $REGION -q; done
for s in refresh-weather refresh-hazards; do
  gcloud scheduler jobs delete $s --location $REGION -q; done
gcloud sql instances delete eco-resilience-oltp -q          # if created
for ds in eco_bronze eco_silver eco_gold; do bq rm -r -f -d $PROJECT_ID:$ds; done
gcloud storage rm -r gs://$BUCKET
```

## Estimated monthly cost (light personal use)

| Service | Estimate |
|---|---|
| BigQuery (storage + queries, small data) | ~$1–5 |
| Cloud Run app (scale-to-zero) | ~$0–5 |
| Cloud Run Jobs (5 runs/day × <1 min) | <$1 |
| Vertex AI Gemini 2.5 Pro (~50 agent calls/day) | ~$10–30 |
| Vertex AI embeddings (one-time + queries) | <$1 |
| Cloud SQL db-f1-micro (optional) | ~$10 |
| Secret Manager, Scheduler, GCS | ~$1 |
| **Total** | **~$15–50/month** (vs $800+ for the always-on Databricks stack) |

Biggest saver vs the original plan: using **BigQuery VECTOR_SEARCH** instead
of a Vertex AI Vector Search endpoint removes a ~$70/month always-on cost.
