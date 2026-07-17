#!/usr/bin/env bash
# Phase 5 — build + deploy the Flask app (with in-process agent) to Cloud Run.
# Replaces: eco_resilience_ai.app.yml (Databricks Apps) +
#           agent_endpoint.serving.yml (Model Serving) — the agent now runs
#           inside the same container, so one deploy covers both.
# Run from the repo root.
set -euo pipefail

PROJECT_ID="${PROJECT_ID:-eco-resilience-ai}"
REGION="${REGION:-australia-southeast1}"
REPO="$REGION-docker.pkg.dev/$PROJECT_ID/eco-resilience"
APP_SA="eco-app@$PROJECT_ID.iam.gserviceaccount.com"

# Optional Cloud SQL wiring — leave empty to disable grant history
CLOUD_SQL_CONNECTION_NAME="${CLOUD_SQL_CONNECTION_NAME:-}"
CLOUD_SQL_USER="${CLOUD_SQL_USER:-}"

# LLM_PROVIDER=gemini (default, australia-southeast1) or claude (us-east5,
# requires enabling Anthropic models in Vertex AI Model Garden first)
LLM_PROVIDER="${LLM_PROVIDER:-gemini}"

echo "── Building app image ──"
cat > /tmp/cloudbuild-app.yaml <<EOF
steps:
- name: gcr.io/cloud-builders/docker
  args: ['build', '-f', 'app/Dockerfile', '-t', '$REPO/eco-app:latest', '.']
images: ['$REPO/eco-app:latest']
EOF
gcloud builds submit gcp/ --project="$PROJECT_ID" --config=/tmp/cloudbuild-app.yaml

ENV_VARS="GOOGLE_CLOUD_PROJECT=$PROJECT_ID,LLM_PROVIDER=$LLM_PROVIDER"
EXTRA_FLAGS=()
if [[ -n "$CLOUD_SQL_CONNECTION_NAME" ]]; then
  ENV_VARS="$ENV_VARS,CLOUD_SQL_CONNECTION_NAME=$CLOUD_SQL_CONNECTION_NAME,CLOUD_SQL_USER=$CLOUD_SQL_USER"
  EXTRA_FLAGS+=(--add-cloudsql-instances "$CLOUD_SQL_CONNECTION_NAME")
fi

echo "── Deploying to Cloud Run ──"
gcloud run deploy eco-resilience-app \
  --project="$PROJECT_ID" --region="$REGION" \
  --image "$REPO/eco-app:latest" \
  --service-account "$APP_SA" \
  --allow-unauthenticated \
  --set-env-vars "$ENV_VARS" \
  --min-instances 0 --max-instances 5 \
  --memory 1Gi --cpu 1 --timeout 300 \
  "${EXTRA_FLAGS[@]}"

echo
echo "✅ Deployed. Smoke test:"
echo '   curl "$(gcloud run services describe eco-resilience-app --region '"$REGION"' --format="value(status.url)")/api/health"'
