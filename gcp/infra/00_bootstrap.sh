#!/usr/bin/env bash
# Phase 0 — GCP project bootstrap.
# Replaces: Databricks workspace + Unity Catalog setup + databricks.yml targets.
# Run once. Idempotent-ish (re-running existing resources prints errors you can ignore).
set -euo pipefail

export PROJECT_ID="${PROJECT_ID:-eco-resilience-ai}"
export REGION="${REGION:-australia-southeast1}"
export BUCKET="${BUCKET:-eco-resilience-landing}"

echo "── Project: $PROJECT_ID | Region: $REGION ──"
gcloud config set project "$PROJECT_ID"

# 1. Enable APIs
gcloud services enable \
  bigquery.googleapis.com \
  run.googleapis.com \
  cloudscheduler.googleapis.com \
  secretmanager.googleapis.com \
  aiplatform.googleapis.com \
  storage.googleapis.com \
  sqladmin.googleapis.com \
  artifactregistry.googleapis.com \
  cloudbuild.googleapis.com \
  logging.googleapis.com \
  monitoring.googleapis.com \
  cloudtrace.googleapis.com

# 2. GCS landing bucket (replaces UC Volumes)
gcloud storage buckets create "gs://$BUCKET" --location="$REGION" || true

# 3. BigQuery datasets (replace Unity Catalog schemas bronze/silver/gold)
for ds in eco_bronze eco_silver eco_gold; do
  bq --location="$REGION" mk --dataset "$PROJECT_ID:$ds" || true
done

# 4. Artifact Registry repo for job/app images
gcloud artifacts repositories create eco-resilience \
  --repository-format=docker --location="$REGION" \
  --description="EcoResilience images" || true

# 5. Service accounts (replace the Databricks app service principal)
gcloud iam service-accounts create eco-app \
  --display-name="EcoResilience App (Cloud Run)" || true
gcloud iam service-accounts create eco-pipeline \
  --display-name="EcoResilience Pipeline (Cloud Run Jobs)" || true

APP_SA="eco-app@$PROJECT_ID.iam.gserviceaccount.com"
PIPE_SA="eco-pipeline@$PROJECT_ID.iam.gserviceaccount.com"

# 6. IAM — least privilege per SA
# App: query BigQuery, call Vertex AI, read secrets, connect to Cloud SQL
for role in roles/bigquery.dataEditor roles/bigquery.jobUser \
            roles/aiplatform.user roles/secretmanager.secretAccessor \
            roles/cloudsql.client roles/cloudsql.instanceUser \
            roles/cloudtrace.agent; do
  gcloud projects add-iam-policy-binding "$PROJECT_ID" \
    --member="serviceAccount:$APP_SA" --role="$role" --condition=None -q
done

# Pipeline: write BigQuery, read/write GCS, call Vertex AI (embeddings), read secrets
for role in roles/bigquery.dataEditor roles/bigquery.jobUser \
            roles/storage.objectAdmin roles/aiplatform.user \
            roles/secretmanager.secretAccessor; do
  gcloud projects add-iam-policy-binding "$PROJECT_ID" \
    --member="serviceAccount:$PIPE_SA" --role="$role" --condition=None -q
done

echo "✅ Bootstrap complete. Next: python infra/01_bigquery_schema.py"
