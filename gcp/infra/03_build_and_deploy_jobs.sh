#!/usr/bin/env bash
# Phase 2 — build all Cloud Run Job images and create the jobs.
# Replaces: resources/*.job.yml in the Databricks Asset Bundle.
# Run from the repo root (the directory containing gcp/).
set -euo pipefail

PROJECT_ID="${PROJECT_ID:-eco-resilience-ai}"
REGION="${REGION:-australia-southeast1}"
BUCKET="${BUCKET:-eco-resilience-landing}"
REPO="$REGION-docker.pkg.dev/$PROJECT_ID/eco-resilience"
PIPE_SA="eco-pipeline@$PROJECT_ID.iam.gserviceaccount.com"

JOBS=(ingest_weather ingest_hazards seed_reference ingest_drfa_rag)

for job in "${JOBS[@]}"; do
  image_name="${job//_/-}"

  echo "── Building $image_name (Cloud Build) ──"
  # gcloud builds submit --tag only supports ./Dockerfile, so use an inline
  # build config to point at the per-job Dockerfile within the gcp/ context.
  cat > /tmp/cloudbuild-$image_name.yaml <<EOF
steps:
- name: gcr.io/cloud-builders/docker
  args: ['build', '-f', 'jobs/$job/Dockerfile', '-t', '$REPO/$image_name:latest', '.']
images: ['$REPO/$image_name:latest']
EOF
  gcloud builds submit gcp/ --project="$PROJECT_ID" \
    --config=/tmp/cloudbuild-$image_name.yaml

  echo "── Creating/updating Cloud Run Job $image_name ──"
  gcloud run jobs deploy "$image_name" \
    --project="$PROJECT_ID" --region="$REGION" \
    --image "$REPO/$image_name:latest" \
    --service-account "$PIPE_SA" \
    --set-env-vars "GOOGLE_CLOUD_PROJECT=$PROJECT_ID,LANDING_BUCKET=$BUCKET" \
    --memory 1Gi --cpu 1 --max-retries 2 --task-timeout 1800
done

echo
echo "✅ Jobs deployed. One-time seeds (run in this order):"
echo "   gcloud run jobs execute seed-reference   --region $REGION --wait"
echo "   gcloud run jobs execute ingest-weather   --region $REGION --wait   # needs poa_centroids from seed"
echo "   gcloud run jobs execute ingest-hazards   --region $REGION --wait"
echo "   gcloud run jobs execute ingest-drfa-rag  --region $REGION --wait"
