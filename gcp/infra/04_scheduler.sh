#!/usr/bin/env bash
# Phase 2 — Cloud Scheduler triggers for the recurring pipelines.
# Replaces: refresh_weather.job.yml + refresh_hazards.job.yml cron schedules.
set -euo pipefail

PROJECT_ID="${PROJECT_ID:-eco-resilience-ai}"
REGION="${REGION:-australia-southeast1}"
PIPE_SA="eco-pipeline@$PROJECT_ID.iam.gserviceaccount.com"

# Scheduler needs permission to execute the jobs
gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member="serviceAccount:$PIPE_SA" --role="roles/run.invoker" --condition=None -q

run_job_uri() {
  echo "https://$REGION-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/$PROJECT_ID/jobs/$1:run"
}

# Weather: every 6 hours (spec: 6h granularity — faster is wasteful)
gcloud scheduler jobs create http refresh-weather \
  --project="$PROJECT_ID" --location="$REGION" \
  --schedule "0 */6 * * *" --time-zone "Australia/Sydney" \
  --uri "$(run_job_uri ingest-weather)" --http-method POST \
  --oauth-service-account-email "$PIPE_SA" \
  || gcloud scheduler jobs update http refresh-weather \
       --project="$PROJECT_ID" --location="$REGION" --schedule "0 */6 * * *"

# Hazards: hourly (hazards change fast)
gcloud scheduler jobs create http refresh-hazards \
  --project="$PROJECT_ID" --location="$REGION" \
  --schedule "30 * * * *" --time-zone "Australia/Sydney" \
  --uri "$(run_job_uri ingest-hazards)" --http-method POST \
  --oauth-service-account-email "$PIPE_SA" \
  || gcloud scheduler jobs update http refresh-hazards \
       --project="$PROJECT_ID" --location="$REGION" --schedule "30 * * * *"

echo "✅ Schedules created (weather 6-hourly, hazards hourly)."
