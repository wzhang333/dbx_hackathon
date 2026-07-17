#!/usr/bin/env bash
# Phase 6 — Secret Manager setup.
# Replaces: databricks secrets create-scope eco_resilience + put-secret.
set -euo pipefail

PROJECT_ID="${PROJECT_ID:-eco-resilience-ai}"

read -r -s -p "ABR auth GUID: " ABR_GUID; echo
read -r -s -p "TfNSW API key: " TFNSW_KEY; echo

printf '%s' "$ABR_GUID" | gcloud secrets create abr-auth-guid \
  --project="$PROJECT_ID" --data-file=- --replication-policy=automatic \
  || printf '%s' "$ABR_GUID" | gcloud secrets versions add abr-auth-guid \
       --project="$PROJECT_ID" --data-file=-

printf '%s' "$TFNSW_KEY" | gcloud secrets create tfnsw-api-key \
  --project="$PROJECT_ID" --data-file=- --replication-policy=automatic \
  || printf '%s' "$TFNSW_KEY" | gcloud secrets versions add tfnsw-api-key \
       --project="$PROJECT_ID" --data-file=-

echo "✅ Secrets stored. (SA access was granted project-wide in 00_bootstrap.sh;"
echo "   for stricter scoping use: gcloud secrets add-iam-policy-binding <secret> ...)"
