# EcoResilience AI — GCP Migration Master Plan

> **Goal**: Migrate every Databricks service in this agentic application to GCP-native services.  
> **Outcome**: A fully GCP-hosted, production-ready agentic AI app — zero Databricks dependency.

> **STATUS: IMPLEMENTED.** The refactored codebase lives in [`gcp/`](gcp/README.md).
> Follow [`docs/DEPLOYMENT_GUIDE.md`](docs/DEPLOYMENT_GUIDE.md) to deploy and
> [`docs/LEARNING_GUIDE.md`](docs/LEARNING_GUIDE.md) to study the migration.
> One deviation from this plan: RAG uses **BigQuery VECTOR_SEARCH** (serverless)
> instead of a Vertex AI Vector Search endpoint — saves ~$70/month at this corpus size.

---

## Table of Contents

1. [Current Architecture Summary](#1-current-architecture-summary)
2. [Service Mapping: Databricks → GCP](#2-service-mapping-databricks--gcp)
3. [Target GCP Architecture](#3-target-gcp-architecture)
4. [Migration Phases](#4-migration-phases)
   - [Phase 0: GCP Project Bootstrap](#phase-0-gcp-project-bootstrap)
   - [Phase 1: Data Storage & Lakehouse](#phase-1-data-storage--lakehouse)
   - [Phase 2: Data Pipelines & Ingestion](#phase-2-data-pipelines--ingestion)
   - [Phase 3: Vector Search & RAG](#phase-3-vector-search--rag)
   - [Phase 4: AI Agent](#phase-4-ai-agent)
   - [Phase 5: Frontend & API](#phase-5-frontend--api)
   - [Phase 6: Secrets & IAM](#phase-6-secrets--iam)
   - [Phase 7: Observability](#phase-7-observability)
5. [Detailed Deployment Steps](#5-detailed-deployment-steps)
6. [Code Refactoring Guide](#6-code-refactoring-guide)
7. [Cost Estimate](#7-cost-estimate)
8. [Learning Resources](#8-learning-resources)

---

## 1. Current Architecture Summary

EcoResilience AI is an autonomous disaster recovery assistant for Australian small businesses. A user enters their ABN; the AI agent:

1. Verifies the ABN via the Australian Business Register (ABR) API
2. Retrieves their NSW postcode
3. Fetches real-time weather (Open-Meteo), live hazards (Transport NSW), climate projections (CSIRO), and industry context (ABS)
4. Searches a vector index of NEMA disaster recovery policy PDFs (RAG)
5. Generates a pre-filled grant application

**Current stack**: Azure Databricks (Delta Lake, DLT, Unity Catalog, MLflow, Model Serving, Mosaic AI, Databricks Apps, Genie, Lakebase).

---

## 2. Service Mapping: Databricks → GCP

| # | Databricks Component | GCP Replacement | Rationale |
|---|---------------------|-----------------|-----------|
| 1 | **Delta Lake / Unity Catalog** (Bronze/Silver/Gold tables) | **BigQuery** (datasets as schemas, tables with partitioning) | Fully managed, serverless, native JSON/ARRAY support, INFORMATION_SCHEMA |
| 2 | **Lakeflow / DLT Pipelines** (streaming + materialized views) | **Dataflow** (Apache Beam for streaming) + **BigQuery Scheduled Queries / dbt** (batch transforms) | Dataflow for real-time; dbt-on-BQ for medallion MV pattern |
| 3 | **Databricks Jobs / Workflows** (cron scheduling) | **Cloud Scheduler** + **Cloud Run Jobs** | Lightweight cron triggers → containerised Python jobs |
| 4 | **MLflow Model Registry** | **Vertex AI Model Registry** | Native GCP model lineage, versioning, metadata |
| 5 | **Databricks Model Serving** (agent endpoint) | **Vertex AI Agent Engine** (managed LangGraph runtime) | Fully managed LangGraph with auto-scaling, tool binding |
| 6 | **Mosaic AI / UC Functions** (SQL UDFs as agent tools) | **Vertex AI Extensions / BigQuery Remote Functions** | BQ Remote Functions wrap Cloud Run; Extensions wrap REST APIs |
| 7 | **Databricks Vector Search** (drfa_chunks_index) | **Vertex AI Vector Search** (Matching Engine) | Managed ANN index, native text embedding via `text-embedding-004` |
| 8 | **Databricks Secrets Manager** | **Secret Manager** | 1:1 equivalent; accessed via Python SDK |
| 9 | **Databricks Apps** (Flask web app) | **Cloud Run** (containerised Flask app) | Serverless, scale-to-zero, HTTPS endpoint |
| 10 | **SQL Warehouse** (ad-hoc queries from Flask) | **BigQuery Python client** (direct queries) | No warehouse needed; BQ is serverless |
| 11 | **Lakebase** (PostgreSQL OLTP for grant history) | **Cloud SQL (PostgreSQL)** or **AlloyDB** | Drop-in PostgreSQL replacement |
| 12 | **Databricks Auto Loader** (cloud storage → bronze) | **Cloud Storage + Pub/Sub + Dataflow** | GCS event notifications → Pub/Sub → Dataflow streaming |
| 13 | **DBFS / Volumes** (raw files, PDFs, CSVs) | **Cloud Storage (GCS)** buckets | Object storage for landing, raw, and reference data |
| 14 | **Genie** (self-serve analytics) | **Looker Studio** or **Vertex AI Analytics Hub** | Embedded BI on BigQuery data |
| 15 | **Cluster compute** (Spark jobs) | **Dataproc Serverless** (for any Spark notebooks that must remain Spark) | Pay-per-job Spark; most jobs can be pure Python → Cloud Run |

---

## 3. Target GCP Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  USERS                                                                       │
│  Browser → Cloud Run (Flask app + index.html)                               │
│              │                                                               │
│              ├── POST /api/ace-chat                                         │
│              │       └── Vertex AI Agent Engine (LangGraph agent)           │
│              │               ├── Tool: verify_abn()  ────────────── ABR API │
│              │               ├── Tool: get_weather_forecast() ──── BigQuery │
│              │               ├── Tool: get_active_hazards() ────── BigQuery │
│              │               ├── Tool: get_climate_projection() ── BigQuery │
│              │               ├── Tool: query_nema_guidelines() ─── Vertex AI│
│              │               │                                  Vector Search│
│              │               └── Tool: get_industry_context() ─── BigQuery │
│              │                                                               │
│              └── Other /api/* endpoints → BigQuery Python client            │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│  DATA PIPELINE (refresh every 6h)                                           │
│                                                                              │
│  Cloud Scheduler ─── cron ─────────────────────────────────────────────────┤
│        │                                                                     │
│        ├── Cloud Run Job: ingest_weather.py                                 │
│        │       └── Open-Meteo API → GCS (landing) → BigQuery (bronze/silver)│
│        │                                                                     │
│        ├── Cloud Run Job: ingest_hazards.py                                 │
│        │       └── TfNSW API → GCS (landing) → BigQuery (bronze/silver)    │
│        │                                                                     │
│        ├── Cloud Run Job: ingest_abn.py (on-demand via Pub/Sub trigger)     │
│        │       └── ABR API → BigQuery (silver.abn_lookup_structured)        │
│        │                                                                     │
│        └── BigQuery Scheduled Query / dbt: transform to gold                │
│                └── gold.business_details, gold.nsw_postcode_resilience      │
│                                                                              │
│  One-time setup jobs (Cloud Run Jobs, run once):                            │
│        ├── ingest_csiro.py    → GCS → BigQuery                              │
│        ├── ingest_abs.py      → BigQuery                                    │
│        └── ingest_drfa_rag.py → PDF parse → Vertex AI Vector Search index  │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│  STORAGE LAYER                                                               │
│                                                                              │
│  GCS Bucket: eco-resilience-landing/                                        │
│    ├── weather/YYYY-MM-DD_HH/     (raw JSON from Open-Meteo)                │
│    ├── hazards/YYYY-MM-DD_HH/     (raw JSON from TfNSW)                     │
│    ├── reference/csiro/           (CSIRO climate CSVs)                      │
│    ├── reference/drfa_pdfs/       (NEMA DRFA PDFs × 16)                     │
│    └── reference/abs/             (ABS industry CSV)                        │
│                                                                              │
│  BigQuery Dataset: eco_resilience                                           │
│    ├── bronze.*   (raw append-only tables)                                  │
│    ├── silver.*   (cleaned, agent-query-ready)                              │
│    └── gold.*     (denormalised, BI-ready)                                  │
│                                                                              │
│  Cloud SQL (PostgreSQL): eco_resilience_oltp                                │
│    └── grant_history table  (Lakebase replacement)                          │
│                                                                              │
│  Vertex AI Vector Search Index                                              │
│    └── drfa_chunks_index    (DRFA policy PDF chunks)                        │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│  IAM & SECRETS                                                               │
│                                                                              │
│  Secret Manager:                                                             │
│    ├── abr-auth-guid                                                         │
│    ├── tfnsw-api-key                                                         │
│    └── cloud-sql-password (optional — prefer IAM auth)                      │
│                                                                              │
│  Service Accounts:                                                           │
│    ├── eco-resilience-app@...     (Cloud Run app SA)                        │
│    ├── eco-resilience-pipeline@.. (Cloud Run Jobs SA)                       │
│    └── eco-resilience-agent@...   (Vertex AI Agent Engine SA)               │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 4. Migration Phases

---

### Phase 0: GCP Project Bootstrap

**Duration**: ~1 day  
**Goal**: Provision all GCP infrastructure via Terraform or gcloud CLI.

**Steps**:

```bash
# 1. Create / select GCP project
gcloud projects create eco-resilience-ai --name="EcoResilience AI"
gcloud config set project eco-resilience-ai

# 2. Enable required APIs
gcloud services enable \
  bigquery.googleapis.com \
  run.googleapis.com \
  cloudscheduler.googleapis.com \
  secretmanager.googleapis.com \
  aiplatform.googleapis.com \
  storage.googleapis.com \
  sqladmin.googleapis.com \
  pubsub.googleapis.com \
  artifactregistry.googleapis.com \
  cloudbuild.googleapis.com \
  logging.googleapis.com \
  monitoring.googleapis.com

# 3. Create GCS bucket
gsutil mb -l australia-southeast1 gs://eco-resilience-landing

# 4. Create BigQuery datasets
bq mk --location=australia-southeast1 eco_resilience:bronze
bq mk --location=australia-southeast1 eco_resilience:silver
bq mk --location=australia-southeast1 eco_resilience:gold

# 5. Create service accounts
gcloud iam service-accounts create eco-resilience-app \
  --display-name="EcoResilience App (Cloud Run)"

gcloud iam service-accounts create eco-resilience-pipeline \
  --display-name="EcoResilience Pipeline (Cloud Run Jobs)"

gcloud iam service-accounts create eco-resilience-agent \
  --display-name="EcoResilience Agent (Vertex AI)"
```

**IAM Bindings**:

```bash
PROJECT=eco-resilience-ai

# App SA: read BigQuery, call Vertex AI, read secrets, read/write Cloud SQL
gcloud projects add-iam-policy-binding $PROJECT \
  --member="serviceAccount:eco-resilience-app@$PROJECT.iam.gserviceaccount.com" \
  --role="roles/bigquery.dataViewer"
gcloud projects add-iam-policy-binding $PROJECT \
  --member="serviceAccount:eco-resilience-app@$PROJECT.iam.gserviceaccount.com" \
  --role="roles/bigquery.jobUser"
gcloud projects add-iam-policy-binding $PROJECT \
  --member="serviceAccount:eco-resilience-app@$PROJECT.iam.gserviceaccount.com" \
  --role="roles/aiplatform.user"
gcloud projects add-iam-policy-binding $PROJECT \
  --member="serviceAccount:eco-resilience-app@$PROJECT.iam.gserviceaccount.com" \
  --role="roles/secretmanager.secretAccessor"

# Pipeline SA: write BigQuery, read/write GCS, read secrets
gcloud projects add-iam-policy-binding $PROJECT \
  --member="serviceAccount:eco-resilience-pipeline@$PROJECT.iam.gserviceaccount.com" \
  --role="roles/bigquery.dataEditor"
gcloud projects add-iam-policy-binding $PROJECT \
  --member="serviceAccount:eco-resilience-pipeline@$PROJECT.iam.gserviceaccount.com" \
  --role="roles/storage.objectAdmin"
gcloud projects add-iam-policy-binding $PROJECT \
  --member="serviceAccount:eco-resilience-pipeline@$PROJECT.iam.gserviceaccount.com" \
  --role="roles/secretmanager.secretAccessor"
```

---

### Phase 1: Data Storage & Lakehouse

**Duration**: ~2 days  
**Goal**: Recreate Bronze/Silver/Gold schemas in BigQuery. Upload reference data to GCS.

#### 1A. Upload reference data to GCS

```bash
# CSIRO climate CSVs (copy from your local machine / old Databricks Volume export)
gsutil -m cp -r ./reference_data/csiro/ gs://eco-resilience-landing/reference/csiro/

# NEMA DRFA PDFs (16 policy documents)
gsutil -m cp -r ./reference_data/drfa_pdfs/ gs://eco-resilience-landing/reference/drfa_pdfs/

# ABS industry CSV
gsutil cp ./reference_data/abs_industry.csv gs://eco-resilience-landing/reference/abs/
```

#### 1B. Create BigQuery table schemas

Create a file `infra/bigquery_schema.py` and run it once:

```python
# infra/bigquery_schema.py
from google.cloud import bigquery

client = bigquery.Client(project="eco-resilience-ai")

schemas = {
    "eco_resilience.bronze.open_meteo_forecast": [
        bigquery.SchemaField("_ingest_time", "TIMESTAMP"),
        bigquery.SchemaField("location_name", "STRING"),
        bigquery.SchemaField("latitude", "FLOAT64"),
        bigquery.SchemaField("longitude", "FLOAT64"),
        bigquery.SchemaField("forecast_json", "JSON"),   # raw API response
    ],
    "eco_resilience.bronze.tfnsw_hazards": [
        bigquery.SchemaField("_ingest_time", "TIMESTAMP"),
        bigquery.SchemaField("feed_type", "STRING"),     # incident/flood/fire/roadwork
        bigquery.SchemaField("hazard_id", "STRING"),
        bigquery.SchemaField("hazard_json", "JSON"),
    ],
    "eco_resilience.silver.weather_current": [
        bigquery.SchemaField("location_name", "STRING"),
        bigquery.SchemaField("postcode", "STRING"),
        bigquery.SchemaField("latitude", "FLOAT64"),
        bigquery.SchemaField("longitude", "FLOAT64"),
        bigquery.SchemaField("snapshot_time", "TIMESTAMP"),
        bigquery.SchemaField("temperature_2m", "FLOAT64"),
        bigquery.SchemaField("precipitation_sum_24h", "FLOAT64"),
        bigquery.SchemaField("wind_speed_max_24h", "FLOAT64"),
        bigquery.SchemaField("forecast_7d_json", "JSON"),
    ],
    # ... (add all remaining tables from the schema overview in Section 9)
}
```

> **Key design decision**: BigQuery uses partitioning instead of Delta Lake's Z-ordering.  
> Partition bronze tables on `DATE(_ingest_time)`. Partition silver/gold on `DATE(snapshot_time)`.

---

### Phase 2: Data Pipelines & Ingestion

**Duration**: ~3–4 days  
**Goal**: Convert all Databricks notebooks to standalone Python modules deployed as Cloud Run Jobs.

#### 2A. Convert ingestion notebooks to Python modules

Each Databricks notebook becomes a self-contained Python script. Replace all `spark.read/write` with `google.cloud.bigquery` and `google.cloud.storage`.

**Transformation pattern (weather example)**:

```
BEFORE (Databricks):
  spark.read.json("/Volumes/eco_resilience/landing/weather/*.json")
    .write.format("delta")
    .mode("append")
    .saveAsTable("eco_resilience.bronze.open_meteo_forecast")

AFTER (GCP):
  from google.cloud import bigquery, storage
  client = bigquery.Client()
  rows = [{"_ingest_time": ..., "location_name": ..., "forecast_json": json.dumps(resp)}]
  client.insert_rows_json("eco-resilience-ai.eco_resilience_bronze.open_meteo_forecast", rows)
```

**File structure for refactored codebase**:

```
gcp-eco-resilience/
├── app/                          # Cloud Run (web app)
│   ├── Dockerfile
│   ├── app.py                    # Flask — replace WorkspaceClient with BQ client
│   ├── index.html                # No changes needed
│   └── requirements.txt
├── jobs/                         # Cloud Run Jobs (pipelines)
│   ├── ingest_weather/
│   │   ├── Dockerfile
│   │   ├── main.py               # Refactored ingest_open_meteo.py
│   │   └── requirements.txt
│   ├── ingest_hazards/
│   │   ├── Dockerfile
│   │   ├── main.py               # Refactored ingest_tfnsw_hazards.py
│   │   └── requirements.txt
│   ├── ingest_abn/
│   │   ├── Dockerfile
│   │   ├── main.py               # Refactored ingest_abn_details.py
│   │   └── requirements.txt
│   ├── ingest_csiro/             # One-time setup
│   ├── ingest_abs/               # One-time setup
│   └── ingest_drfa_rag/          # One-time setup (PDF → Vector Search)
├── agent/                        # Vertex AI Agent Engine
│   ├── agent.py                  # Refactored eco_agent.py (LangGraph + tools)
│   ├── tools.py                  # Tool implementations (BQ queries, Vector Search)
│   ├── deploy_agent.py           # Deploy to Vertex AI Agent Engine
│   └── requirements.txt
├── transforms/                   # dbt or BigQuery Scheduled Queries
│   ├── dbt_project.yml
│   └── models/
│       ├── silver/
│       └── gold/
└── infra/
    ├── main.tf                   # Terraform (optional)
    ├── bigquery_schema.py        # One-time BQ table creation
    └── setup_secrets.sh          # Create Secret Manager entries
```

#### 2B. Deploy ingestion jobs to Cloud Run

```bash
# Example: weather ingestion job
cd jobs/ingest_weather

# Build and push image
gcloud builds submit --tag australia-southeast1-docker.pkg.dev/eco-resilience-ai/jobs/ingest-weather:latest

# Create Cloud Run Job
gcloud run jobs create ingest-weather \
  --image australia-southeast1-docker.pkg.dev/eco-resilience-ai/jobs/ingest-weather:latest \
  --region australia-southeast1 \
  --service-account eco-resilience-pipeline@eco-resilience-ai.iam.gserviceaccount.com \
  --max-retries 3 \
  --task-timeout 600

# Schedule it (every 6 hours)
gcloud scheduler jobs create http refresh-weather \
  --schedule "0 */6 * * *" \
  --uri "https://australia-southeast1-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/eco-resilience-ai/jobs/ingest-weather:run" \
  --http-method POST \
  --oauth-service-account-email eco-resilience-pipeline@eco-resilience-ai.iam.gserviceaccount.com \
  --location australia-southeast1
```

Repeat for `ingest-hazards` (offset by 1 hour: `0 1,7,13,19 * * *`).

#### 2C. Gold layer transforms with dbt (optional) or BigQuery Scheduled Queries

If you prefer no extra tooling, use BigQuery Scheduled Queries (native UI or API):

```sql
-- gold.business_details (scheduled daily)
CREATE OR REPLACE TABLE `eco-resilience-ai.eco_resilience_gold.business_details` AS
SELECT
  a.abn,
  a.business_name,
  a.entity_type,
  a.state,
  a.postcode,
  w.location_name AS nearest_weather_location,
  w.latitude,
  w.longitude
FROM `eco-resilience-ai.eco_resilience_silver.abn_lookup_structured` a
LEFT JOIN `eco-resilience-ai.eco_resilience_silver.poa_to_weather_location` p
  ON a.postcode = p.postcode
LEFT JOIN `eco-resilience-ai.eco_resilience_silver.weather_current` w
  ON p.location_name = w.location_name;
```

---

### Phase 3: Vector Search & RAG

**Duration**: ~2 days  
**Goal**: Rebuild the DRFA PDF vector index on Vertex AI Vector Search.

#### 3A. Parse PDFs and generate embeddings

```python
# jobs/ingest_drfa_rag/main.py
import os
import json
from google.cloud import storage, aiplatform
from vertexai.language_models import TextEmbeddingModel
import fitz  # PyMuPDF — replaces Databricks ai_parse_document

def chunk_pdf(pdf_bytes: bytes, source_name: str) -> list[dict]:
    """Extract text chunks from a PDF."""
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    chunks = []
    for page_num, page in enumerate(doc):
        text = page.get_text()
        # Simple fixed-size chunking (500 chars, 50 char overlap)
        for i in range(0, len(text), 450):
            chunk = text[i:i+500].strip()
            if len(chunk) > 100:  # skip tiny chunks
                chunks.append({
                    "chunk_id": f"{source_name}_p{page_num}_c{i}",
                    "chunk_to_retrieve": chunk,
                    "source_uri": source_name,
                    "page_number": page_num + 1,
                })
    return chunks

def embed_chunks(chunks: list[dict]) -> list[dict]:
    """Generate embeddings using Vertex AI text-embedding-004."""
    model = TextEmbeddingModel.from_pretrained("text-embedding-004")
    texts = [c["chunk_to_retrieve"] for c in chunks]
    embeddings = model.get_embeddings(texts)
    for chunk, emb in zip(chunks, embeddings):
        chunk["embedding"] = emb.values
    return chunks
```

#### 3B. Create and populate Vertex AI Vector Search index

```python
# jobs/ingest_drfa_rag/deploy_index.py
from google.cloud import aiplatform

aiplatform.init(project="eco-resilience-ai", location="australia-southeast1")

# Upload embeddings as JSONL to GCS first
# gs://eco-resilience-landing/vector-index/drfa_chunks.jsonl

# Create index
index = aiplatform.MatchingEngineIndex.create_tree_ah_index(
    display_name="drfa-chunks-index",
    contents_delta_uri="gs://eco-resilience-landing/vector-index/",
    dimensions=768,           # text-embedding-004 dimension
    approximate_neighbors_count=10,
    distance_measure_type="DOT_PRODUCT_DISTANCE",
)

# Deploy index to an endpoint
index_endpoint = aiplatform.MatchingEngineIndexEndpoint.create(
    display_name="drfa-chunks-endpoint",
    public_endpoint_enabled=True,
)

index_endpoint.deploy_index(
    index=index,
    deployed_index_id="drfa_chunks_deployed",
)
```

#### 3C. Update RAG tool to query Vertex AI Vector Search

```python
# agent/tools.py  — replaces register_rag_tool.py SQL UDF

from google.cloud import aiplatform
from vertexai.language_models import TextEmbeddingModel

def query_nema_guidelines(question: str) -> str:
    """Search NEMA DRFA policy documents for relevant guidance."""
    # 1. Embed the question
    model = TextEmbeddingModel.from_pretrained("text-embedding-004")
    embedding = model.get_embeddings([question])[0].values

    # 2. Query Vector Search
    index_endpoint = aiplatform.MatchingEngineIndexEndpoint(
        index_endpoint_name="projects/eco-resilience-ai/locations/australia-southeast1/indexEndpoints/ENDPOINT_ID"
    )
    results = index_endpoint.find_neighbors(
        deployed_index_id="drfa_chunks_deployed",
        queries=[embedding],
        num_neighbors=5,
    )

    # 3. Fetch chunk text from BigQuery by chunk_id
    from google.cloud import bigquery
    bq = bigquery.Client()
    ids = [r.id for r in results[0]]
    query = f"""
        SELECT chunk_id, chunk_to_retrieve, source_uri, page_number
        FROM `eco-resilience-ai.eco_resilience_bronze.drfa_chunks`
        WHERE chunk_id IN UNNEST(@ids)
    """
    job = bq.query(query, job_config=bigquery.QueryJobConfig(
        query_parameters=[bigquery.ArrayQueryParameter("ids", "STRING", ids)]
    ))
    rows = list(job.result())
    return "\n\n".join(
        f"[{r.source_uri}, page {r.page_number}]: {r.chunk_to_retrieve}"
        for r in rows
    )
```

---

### Phase 4: AI Agent

**Duration**: ~2–3 days  
**Goal**: Port the LangGraph agent to Vertex AI Agent Engine, replacing UC functions with Python tool functions backed by BigQuery.

#### 4A. Refactor `eco_agent.py` → `agent/agent.py`

**Key changes**:
- Replace `ChatDatabricks(endpoint="databricks-claude-sonnet-4")` → `ChatVertexAI(model_name="claude-sonnet-4@20250514")` (Claude on Vertex AI via Anthropic partnership) OR `ChatVertexAI(model_name="gemini-2.0-flash")` (native GCP)
- Replace `DatabricksFunctionClient` (UC tool binding) → direct Python function tools
- Replace `os.environ['ABR_AUTH_GUID']` injection pattern → Secret Manager

```python
# agent/agent.py  — refactored from eco_agent.py

import os
from langchain_google_vertexai import ChatVertexAI
from langgraph.prebuilt import create_react_agent
from agent.tools import (
    verify_abn,
    get_weather_forecast,
    get_active_hazards,
    get_climate_projection,
    query_nema_guidelines,
    get_industry_context,
    generate_grant_pdf,
)

# Load LLM — use Claude on Vertex (if Anthropic model garden enabled)
# or swap to gemini-2.5-pro for a fully GCP-native option
llm = ChatVertexAI(
    model_name="claude-sonnet-4@20250514",
    project="eco-resilience-ai",
    location="us-east5",           # Claude on Vertex available in us-east5
    max_tokens=4096,
)

SYSTEM_PROMPT = """You are ACE, the EcoResilience AI disaster recovery assistant...
[same system prompt as original eco_agent.py]
"""

tools = [
    verify_abn,
    get_weather_forecast,
    get_active_hazards,
    get_climate_projection,
    query_nema_guidelines,
    get_industry_context,
    generate_grant_pdf,
]

agent = create_react_agent(llm, tools, state_modifier=SYSTEM_PROMPT)
```

> **Note on Claude availability**: Claude Sonnet 4 is available on Vertex AI Model Garden in `us-east5` region. If you want everything in `australia-southeast1`, use `gemini-2.5-pro` which is globally available with no regional restriction.

#### 4B. Implement all tools as Python functions backed by BigQuery

```python
# agent/tools.py  — replaces all UC function SQL UDFs

from langchain_core.tools import tool
from google.cloud import bigquery, secretmanager
import requests

bq = bigquery.Client(project="eco-resilience-ai")

def _get_secret(name: str) -> str:
    client = secretmanager.SecretManagerServiceClient()
    name_path = f"projects/eco-resilience-ai/secrets/{name}/versions/latest"
    return client.access_secret_version(name=name_path).payload.data.decode()

@tool
def verify_abn(abn: str) -> dict:
    """Verify an Australian Business Number via the ABR API."""
    guid = _get_secret("abr-auth-guid")
    url = "https://abr.business.gov.au/json/AbnDetails.aspx"
    resp = requests.get(url, params={"abn": abn, "guid": guid}, timeout=10)
    resp.raise_for_status()
    data = resp.json()
    return {
        "abn": data.get("Abn"),
        "business_name": data.get("EntityName"),
        "status": data.get("AbnStatus"),
        "entity_type": data.get("EntityTypeName"),
        "state": data.get("AddressState"),
        "postcode": data.get("AddressPostcode"),
        "in_nsw": data.get("AddressState") == "NSW",
    }

@tool
def get_weather_forecast(postcode: str) -> str:
    """Get the current weather forecast for a NSW postcode."""
    query = """
        SELECT w.location_name, w.temperature_2m, w.precipitation_sum_24h,
               w.wind_speed_max_24h, w.forecast_7d_json
        FROM `eco-resilience-ai.eco_resilience_silver.weather_current` w
        JOIN `eco-resilience-ai.eco_resilience_silver.poa_to_weather_location` p
          ON w.location_name = p.location_name
        WHERE p.postcode = @postcode
        ORDER BY w.snapshot_time DESC
        LIMIT 1
    """
    job = bq.query(query, job_config=bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("postcode", "STRING", postcode)]
    ))
    rows = list(job.result())
    if not rows:
        return f"No weather data found for postcode {postcode}"
    r = rows[0]
    return (f"Weather at {r.location_name}: {r.temperature_2m}°C, "
            f"24h rain {r.precipitation_sum_24h}mm, wind {r.wind_speed_max_24h}km/h")

# ... (implement get_active_hazards, get_climate_projection, get_industry_context similarly)
```

#### 4C. Deploy agent to Vertex AI Agent Engine

```python
# agent/deploy_agent.py

import vertexai
from vertexai.preview import reasoning_engines

vertexai.init(project="eco-resilience-ai", location="us-east5")

# Option A: Deploy as a Reasoning Engine (Vertex AI managed LangGraph)
app = reasoning_engines.LangchainAgent(
    model="claude-sonnet-4@20250514",
    tools=[
        verify_abn, get_weather_forecast, get_active_hazards,
        get_climate_projection, query_nema_guidelines,
        get_industry_context, generate_grant_pdf,
    ],
    model_kwargs={"temperature": 0},
    agent_executor_kwargs={"max_iterations": 10},
)

remote_app = reasoning_engines.ReasoningEngine.create(
    app,
    requirements=["google-cloud-bigquery", "google-cloud-aiplatform",
                  "google-cloud-secret-manager", "requests", "langgraph",
                  "langchain-google-vertexai"],
    display_name="eco-resilience-agent",
    description="EcoResilience AI — Disaster recovery assistant for NSW businesses",
)

print(f"Agent deployed: {remote_app.resource_name}")
# Save resource_name — needed by Flask app
```

> **Alternative (simpler)**: Skip Vertex AI Agent Engine and just run the LangGraph agent directly inside the Cloud Run Flask app. This removes a service hop and is easier to debug.

---

### Phase 5: Frontend & API

**Duration**: ~2 days  
**Goal**: Refactor Flask `app.py` to use BigQuery/Vertex AI instead of Databricks SDK. Deploy to Cloud Run.

#### 5A. Key code changes in `app.py`

| Old (Databricks) | New (GCP) |
|-----------------|-----------|
| `from databricks.sdk import WorkspaceClient` | `from google.cloud import bigquery` |
| `w = WorkspaceClient()` | `bq = bigquery.Client(project="eco-resilience-ai")` |
| `w.serving_endpoints.get_openai_client(endpoint_name)` | Direct LangGraph agent call or Vertex AI endpoint |
| `w.jobs.run_now(job_id=JOB_ID, notebook_params=...)` | `requests.post(CLOUD_RUN_JOB_URL)` or Pub/Sub publish |
| `w.sql.execute_statement(warehouse_id=..., statement=...)` | `bq.query(sql).result()` |
| `LAKEBASE_HOST` (PostgreSQL via Databricks Lakebase) | Cloud SQL (PostgreSQL) connection string |

#### 5B. Environment variables in Cloud Run

```bash
# New environment variables (replacing Databricks-specific ones)
GOOGLE_CLOUD_PROJECT=eco-resilience-ai
BIGQUERY_DATASET=eco_resilience
VERTEX_AI_LOCATION=us-east5
AGENT_RESOURCE_NAME=projects/.../locations/us-east5/reasoningEngines/...
CLOUD_SQL_CONNECTION_NAME=eco-resilience-ai:australia-southeast1:eco-resilience-oltp
```

#### 5C. Build and deploy Flask app to Cloud Run

```bash
cd app/

# Build image
gcloud builds submit \
  --tag australia-southeast1-docker.pkg.dev/eco-resilience-ai/app/eco-resilience-app:latest

# Deploy to Cloud Run
gcloud run deploy eco-resilience-app \
  --image australia-southeast1-docker.pkg.dev/eco-resilience-ai/app/eco-resilience-app:latest \
  --region australia-southeast1 \
  --service-account eco-resilience-app@eco-resilience-ai.iam.gserviceaccount.com \
  --allow-unauthenticated \
  --set-env-vars GOOGLE_CLOUD_PROJECT=eco-resilience-ai \
  --min-instances 0 \
  --max-instances 10 \
  --memory 512Mi \
  --cpu 1
```

---

### Phase 6: Secrets & IAM

**Duration**: ~0.5 day

```bash
# Store secrets in Secret Manager
echo -n "YOUR_ABR_GUID_HERE" | \
  gcloud secrets create abr-auth-guid --data-file=- --replication-policy="automatic"

echo -n "YOUR_TFNSW_KEY_HERE" | \
  gcloud secrets create tfnsw-api-key --data-file=- --replication-policy="automatic"

# Grant Cloud Run Jobs SA access to secrets
gcloud secrets add-iam-policy-binding abr-auth-guid \
  --member="serviceAccount:eco-resilience-pipeline@eco-resilience-ai.iam.gserviceaccount.com" \
  --role="roles/secretmanager.secretAccessor"

gcloud secrets add-iam-policy-binding tfnsw-api-key \
  --member="serviceAccount:eco-resilience-pipeline@eco-resilience-ai.iam.gserviceaccount.com" \
  --role="roles/secretmanager.secretAccessor"

# Grant Flask app SA access to the ABR secret (needed for verify_abn tool)
gcloud secrets add-iam-policy-binding abr-auth-guid \
  --member="serviceAccount:eco-resilience-app@eco-resilience-ai.iam.gserviceaccount.com" \
  --role="roles/secretmanager.secretAccessor"
```

---

### Phase 7: Observability

**Duration**: ~0.5 day  
**Goal**: Replace Databricks MLflow tracing with Cloud Logging + Cloud Trace.

```python
# In agent/agent.py — add GCP-native tracing
from opentelemetry import trace
from opentelemetry.exporter.cloud_trace import CloudTraceSpanExporter
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

provider = TracerProvider()
provider.add_span_processor(BatchSpanProcessor(CloudTraceSpanExporter()))
trace.set_tracer_provider(provider)
tracer = trace.get_tracer("eco-resilience-agent")
```

**Dashboards**: Cloud Monitoring → create a dashboard for:
- Cloud Run request latency (P50, P95, P99)
- BigQuery bytes billed per job
- Vertex AI prediction latency
- Cloud Scheduler job success/failure

---

## 5. Detailed Deployment Steps

End-to-end ordered checklist for a fresh deploy:

```
SETUP (one-time, ~half day)
  [ ] 0.1  Create GCP project, link billing
  [ ] 0.2  Enable APIs (see Phase 0)
  [ ] 0.3  Create GCS bucket + BigQuery datasets
  [ ] 0.4  Create service accounts + IAM bindings
  [ ] 0.5  Create Artifact Registry repo: gcloud artifacts repositories create eco-resilience ...

REFERENCE DATA (one-time, ~1 day)
  [ ] 1.1  Export CSIRO CSVs from Databricks Volume → local → upload to GCS
  [ ] 1.2  Export DRFA PDFs from Databricks Volume → local → upload to GCS
  [ ] 1.3  Export ABS industry CSV → GCS
  [ ] 1.4  Run BigQuery schema creation script (infra/bigquery_schema.py)

PIPELINES (one-time initial run + recurring)
  [ ] 2.1  Deploy ingest_csiro Cloud Run Job → run once
  [ ] 2.2  Deploy ingest_abs Cloud Run Job → run once
  [ ] 2.3  Deploy ingest_weather Cloud Run Job → run once, then schedule
  [ ] 2.4  Deploy ingest_hazards Cloud Run Job → run once, then schedule
  [ ] 2.5  Create Cloud Scheduler triggers for weather + hazards

RAG (one-time)
  [ ] 3.1  Deploy ingest_drfa_rag Cloud Run Job → run once
  [ ] 3.2  Create Vertex AI Vector Search index (deploy_index.py)
  [ ] 3.3  Note index endpoint ID for agent tools

SECRETS
  [ ] 6.1  Create abr-auth-guid in Secret Manager
  [ ] 6.2  Create tfnsw-api-key in Secret Manager
  [ ] 6.3  Bind SA permissions

AGENT
  [ ] 4.1  Test agent locally (python agent/agent.py)
  [ ] 4.2  Deploy to Vertex AI Agent Engine (or skip and embed in Flask)
  [ ] 4.3  Smoke-test via curl

FRONTEND/API
  [ ] 5.1  Build Docker image for Flask app
  [ ] 5.2  Deploy to Cloud Run
  [ ] 5.3  Set all environment variables
  [ ] 5.4  Test all 20 API endpoints

OPTIONAL
  [ ] 7.1  Set up Cloud Monitoring dashboard
  [ ] 7.2  Set up Cloud SQL for grant history (replaces Lakebase)
  [ ] 7.3  Set up Looker Studio dashboard on BigQuery gold tables (replaces Genie)
```

---

## 6. Code Refactoring Guide

### Files to refactor

| Original file | Refactored to | Key changes |
|--------------|--------------|-------------|
| `src/app/app.py` | `app/app.py` | Replace `databricks.sdk` with `google.cloud.bigquery`; replace job triggers with Cloud Run Jobs HTTP calls |
| `src/agents/eco_agent.py` | `agent/agent.py` | Replace `ChatDatabricks` → `ChatVertexAI`; replace UC tool binding → Python `@tool` functions |
| `src/agents/deploy_agent.py` | `agent/deploy_agent.py` | Replace `mlflow` + `agents.deploy()` → Vertex AI Reasoning Engine |
| `src/ingestion/ingest_open_meteo.py` | `jobs/ingest_weather/main.py` | Replace `spark.write.saveAsTable()` → `bq.insert_rows_json()` |
| `src/ingestion/ingest_tfnsw_hazards.py` | `jobs/ingest_hazards/main.py` | Same pattern |
| `src/ingestion/ingest_abn_details.py` | `jobs/ingest_abn/main.py` | Same pattern |
| `src/ingestion/ingest_csiro_stations.py` | `jobs/ingest_csiro/main.py` | Read from GCS → write to BQ |
| `src/ingestion/ingest_abs_industry.py` | `jobs/ingest_abs/main.py` | Same pattern |
| `src/rag/ingest_drfa_rag.py` | `jobs/ingest_drfa_rag/main.py` | Replace `ai_parse_document` → PyMuPDF; replace `vector_search` → Vertex AI Vector Search |
| `src/uc_functions/register_*.py` | `agent/tools.py` | All UC SQL UDFs become Python `@tool` functions |
| `src/transformation/*.py` | `transforms/models/` | Convert to dbt SQL models or BQ Scheduled Queries |
| `src/dlt/dlt_ingestion_pipeline.py` | **Delete** | Replaced by Cloud Run Jobs + BQ scheduled queries |
| `eco-resilience-bundle/databricks.yml` | `infra/` | Replaced by Terraform or shell scripts |
| `resources/*.yml` | `infra/cloud_scheduler.sh` | Cloud Scheduler configs |

### Python dependency changes

```
REMOVE:
  databricks-sdk
  databricks-connect
  mlflow[databricks]
  pyspark
  delta-spark

ADD:
  google-cloud-bigquery>=3.0.0
  google-cloud-storage>=2.0.0
  google-cloud-aiplatform>=1.60.0
  google-cloud-secret-manager>=2.0.0
  langchain-google-vertexai>=1.0.0
  vertexai>=1.60.0
  pymupdf>=1.23.0          # PDF parsing (replaces ai_parse_document)
  opentelemetry-sdk
  opentelemetry-exporter-gcp-trace
```

---

## 7. Cost Estimate

| Service | Usage | Estimated Monthly Cost |
|---------|-------|----------------------|
| BigQuery | ~50GB storage + ~10GB queries/day | ~$30–60 |
| Cloud Run (app) | ~10K requests/day, scale-to-zero | ~$5–15 |
| Cloud Run Jobs (pipelines) | 4 jobs × 4/day × 30s each | ~$2–5 |
| Vertex AI Vector Search | 1 index endpoint, ~1K queries/day | ~$50–100 |
| Vertex AI Claude Sonnet 4 | ~500 agent calls/day, ~5K tokens each | ~$100–200 |
| Cloud SQL (PostgreSQL) | db-f1-micro, 10GB | ~$10 |
| Secret Manager | ~6 secrets, ~10K access/month | ~$1 |
| GCS | ~5GB storage | ~$1 |
| Cloud Scheduler | 4 jobs | ~$1 |
| **TOTAL** | | **~$200–400/month** |

> Databricks equivalent would cost $800–2000+/month for similar workloads.

---

## 8. Learning Resources

This migration exposes you to most of the core GCP data/AI stack. Below is a curated learning path.

### 8A. BigQuery (replaces Delta Lake + Unity Catalog)

- **Concept**: BigQuery is a serverless, columnar data warehouse. Unlike Spark/Delta, there's no "cluster" — you write SQL, it auto-scales.
- **Key differences from Delta Lake**:
  - No Z-ordering → use **partitioning** (`PARTITION BY DATE(col)`) and **clustering** (`CLUSTER BY col1, col2`)
  - No MERGE optimisation needed — BigQuery MERGE is native SQL
  - No streaming tables → use `insertAll` API for real-time, or BigQuery Storage Write API for high-throughput
- **Learning**: [BigQuery Fundamentals (Google Skillsboost)](https://www.cloudskillsboost.google/paths/8)
- **Key docs**: [BigQuery best practices](https://cloud.google.com/bigquery/docs/best-practices-performance-overview), [Partitioning guide](https://cloud.google.com/bigquery/docs/partitioned-tables)

### 8B. Cloud Run & Cloud Run Jobs (replaces Databricks Jobs)

- **Cloud Run**: Stateless HTTP containers. Your Flask app runs here.
- **Cloud Run Jobs**: For batch/pipeline workloads that run to completion. No HTTP endpoint needed.
- **Key concept**: Everything is a Docker container. Write a `Dockerfile`, push to Artifact Registry, deploy.
- **Learning**: [Cloud Run Quickstart](https://cloud.google.com/run/docs/quickstarts/build-and-deploy/deploy-python-service)
- **Pattern for jobs**: `CMD ["python", "main.py"]` — job exits 0 on success, non-zero on failure (Cloud Run retries).

### 8C. Vertex AI Agent Engine (replaces Databricks Model Serving)

- **What it is**: A managed runtime for LangGraph/LangChain agents. Similar to Databricks Model Serving but designed for agentic workloads.
- **Two deployment options**:
  1. **Reasoning Engine**: Managed LangGraph runtime. You submit your agent class; GCP handles scaling.
  2. **Inline in Cloud Run**: Just import your LangGraph agent inside Flask and call it directly. Simpler, less operational overhead.
- **Key concept**: Tool functions must be serialisable (pure Python, no open sockets at class-definition time).
- **Learning**: [Vertex AI Agent Engine overview](https://cloud.google.com/vertex-ai/generative-ai/docs/agent-engine/overview), [LangGraph on Vertex AI](https://cloud.google.com/vertex-ai/generative-ai/docs/agent-engine/use-langgraph)

### 8D. Vertex AI Vector Search (replaces Databricks Vector Search)

- **What it is**: Managed approximate nearest-neighbour (ANN) search using Google's ScaNN algorithm.
- **Two-step process**:
  1. Build an **Index** (offline, from your embeddings JSONL in GCS)
  2. Deploy to an **Index Endpoint** (always-on endpoint for real-time queries)
- **Key difference from Databricks**: You manage embeddings yourself (no auto-sync from a Delta table). You must re-upload + rebuild the index when documents change.
- **Learning**: [Vector Search quickstart](https://cloud.google.com/vertex-ai/docs/vector-search/quickstart), [Matching Engine concepts](https://cloud.google.com/vertex-ai/docs/vector-search/overview)

### 8E. Secret Manager (replaces Databricks Secrets)

- **1:1 equivalent**. Just different API.
- Python: `client.access_secret_version(name=f"projects/{project}/secrets/{name}/versions/latest")`
- **Best practice**: Grant the minimum SA the `secretmanager.secretAccessor` role on the *specific secret*, not the whole project.
- **Learning**: [Secret Manager quickstart](https://cloud.google.com/secret-manager/docs/quickstart)

### 8F. LangGraph + LangChain Google VertexAI (agent framework — no change needed)

- LangGraph is vendor-neutral. You only change the **LLM provider** (`ChatVertexAI` instead of `ChatDatabricks`) and how **tools** are discovered (Python functions instead of UC SQL UDFs).
- **Claude on Vertex AI**: Anthropic models (Claude Sonnet 4) are available in Vertex AI Model Garden. Region: `us-east5`. Enable via Model Garden console → "Enable" on Claude model.
- **Alternative**: Use `gemini-2.5-pro` for a fully `australia-southeast1`-native option (lower latency for Australian users).
- **Learning**: [LangGraph docs](https://langchain-ai.github.io/langgraph/), [ChatVertexAI docs](https://python.langchain.com/docs/integrations/chat/google_vertex_ai_palm/)

### 8G. Terraform for GCP (replaces Databricks Asset Bundles)

- Terraform lets you define all GCP resources (BigQuery, Cloud Run, IAM, etc.) as code.
- This is the GCP equivalent of `databricks.yml`.
- **Key providers**: `google`, `google-beta`
- **Learning**: [Terraform GCP tutorial](https://developer.hashicorp.com/terraform/tutorials/gcp-get-started), [Google provider docs](https://registry.terraform.io/providers/hashicorp/google/latest/docs)

### 8H. Cloud Monitoring & Cloud Trace (replaces MLflow tracing)

- **Cloud Logging**: Structured logs from all GCP services. Use `google.cloud.logging` Python client to write custom log entries.
- **Cloud Trace**: Distributed tracing (equivalent to MLflow's LangChain autolog spans). Instrument with OpenTelemetry + Cloud Trace exporter.
- **Cloud Monitoring**: Dashboards, alerts, uptime checks.
- **Learning**: [Cloud Observability overview](https://cloud.google.com/stackdriver/docs)

### 8I. dbt on BigQuery (replaces DLT / Lakeflow)

- dbt (data build tool) is the most popular way to manage SQL transforms on BigQuery.
- Replaces Databricks DLT's materialized views and declarative pipeline pattern.
- Define models as `.sql` files; dbt handles dependency ordering, incremental materialisation, and documentation.
- **Learning**: [dbt Fundamentals course](https://learn.getdbt.com/courses/dbt-fundamentals), [dbt + BigQuery setup](https://docs.getdbt.com/docs/core/connect-data-platform/bigquery-setup)

### 8J. Recommended Certification Path

| Cert | Covers | Time |
|------|--------|------|
| [Google Cloud Associate Cloud Engineer](https://cloud.google.com/certification/cloud-engineer) | GCP fundamentals, IAM, compute, storage | 2–3 months |
| [Google Cloud Professional Data Engineer](https://cloud.google.com/certification/data-engineer) | BigQuery, Dataflow, Pub/Sub, pipelines | 2–3 months |
| [Google Cloud Professional ML Engineer](https://cloud.google.com/certification/machine-learning-engineer) | Vertex AI, model serving, MLOps | 2–3 months |

---

## Summary: What You're Building

```
OLD (Databricks on Azure):          NEW (GCP Native):
──────────────────────────────────  ─────────────────────────────────────
Azure Databricks workspace          → GCP Project: eco-resilience-ai
Delta Lake + Unity Catalog          → BigQuery (bronze/silver/gold datasets)
Lakeflow / DLT pipelines            → Cloud Run Jobs + dbt on BigQuery
Databricks Workflows (cron jobs)    → Cloud Scheduler + Cloud Run Jobs
Databricks Secrets                  → Secret Manager
Databricks Vector Search            → Vertex AI Vector Search
Databricks Model Serving (agent)    → Vertex AI Agent Engine (or Cloud Run)
Databricks Apps (Flask)             → Cloud Run
Lakebase (PostgreSQL OLTP)          → Cloud SQL (PostgreSQL)
MLflow tracing                      → Cloud Trace (OpenTelemetry)
Genie (self-serve analytics)        → Looker Studio on BigQuery
DAB (databricks.yml)                → Terraform / shell scripts
```

The **application logic stays the same**. You're replacing the infrastructure layer underneath it.
