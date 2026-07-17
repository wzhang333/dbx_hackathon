#!/usr/bin/env bash
# Optional — Cloud SQL (PostgreSQL) for grant submission history.
# Replaces: Lakebase (src/Lakebase/setup_lakebase_grant_history.py).
# Skip this entirely and /api/grant-history returns [] gracefully.
set -euo pipefail

PROJECT_ID="${PROJECT_ID:-eco-resilience-ai}"
REGION="${REGION:-australia-southeast1}"
INSTANCE="eco-resilience-oltp"
APP_SA="eco-app@$PROJECT_ID.iam.gserviceaccount.com"

# Smallest shared-core instance — ~US$10/month. Enable IAM auth so the app
# connects with its service account (no password), the GCP analogue of
# Lakebase's minted-JWT auth.
gcloud sql instances create "$INSTANCE" \
  --project="$PROJECT_ID" --region="$REGION" \
  --database-version=POSTGRES_16 --tier=db-f1-micro \
  --storage-size=10GB \
  --database-flags=cloudsql.iam_authentication=on || true

gcloud sql databases create eco_resilience \
  --project="$PROJECT_ID" --instance="$INSTANCE" || true

# IAM database user for the app SA (username = SA email minus .gserviceaccount.com)
gcloud sql users create "eco-app@$PROJECT_ID.iam" \
  --project="$PROJECT_ID" --instance="$INSTANCE" --type=cloud_iam_service_account || true

echo
echo "Now create the table (once) via Cloud SQL Studio or psql:"
cat <<'SQL'
  CREATE TABLE IF NOT EXISTS grant_submissions (
      id              BIGSERIAL PRIMARY KEY,
      abn             VARCHAR(11),
      business_name   TEXT,
      postcode        VARCHAR(4),
      state           VARCHAR(8),
      application_id  TEXT,
      grant_status    TEXT,
      user_query      TEXT,
      generated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
  );
  -- Grant the IAM user access:
  GRANT SELECT, INSERT ON grant_submissions TO "eco-app@<PROJECT_ID>.iam";
  GRANT USAGE ON SEQUENCE grant_submissions_id_seq TO "eco-app@<PROJECT_ID>.iam";
SQL
echo
echo "App env vars to set in 06_deploy_app.sh:"
echo "  CLOUD_SQL_CONNECTION_NAME=$PROJECT_ID:$REGION:$INSTANCE"
echo "  CLOUD_SQL_USER=eco-app@$PROJECT_ID.iam"
