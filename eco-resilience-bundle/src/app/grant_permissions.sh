#!/bin/bash
# =============================================================================
# EcoResilience AI - Databricks App Permissions Setup
# =============================================================================
# Run this script after creating the app to grant its service principal
# the required permissions.
#
# Prerequisites:
#   - Databricks CLI installed and authenticated (databricks auth login)
#   - You have workspace admin or metastore admin privileges
#
# Usage:
#   chmod +x grant_permissions.sh
#   ./grant_permissions.sh <SERVICE_PRINCIPAL_APP_ID>
#
# Find the service principal ID in:
#   Compute > Apps > ecoresilience-ai > Authorization tab
# =============================================================================

set -e

SP_APP_ID="${1:?Usage: ./grant_permissions.sh <SERVICE_PRINCIPAL_APP_ID>}"
WAREHOUSE_ID="dc189fe4fd0f924b"
JOB_ID="1009568797220506"
AGENT_ENDPOINT="eco_resilience_agent"

echo "=========================================="
echo "EcoResilience AI - Permission Setup"
echo "=========================================="
echo "Service Principal: ${SP_APP_ID}"
echo "SQL Warehouse:     ${WAREHOUSE_ID}"
echo "Job ID:            ${JOB_ID}"
echo "Agent Endpoint:    ${AGENT_ENDPOINT}"
echo ""

# --------------------------------------------------------------------------
# 1. Grant CAN_USE on the SQL Warehouse
# --------------------------------------------------------------------------
echo "[1/3] Granting CAN_USE on SQL Warehouse..."

databricks warehouses update-permissions ${WAREHOUSE_ID} --json "{
  \"access_control_list\": [
    {
      \"service_principal_name\": \"${SP_APP_ID}\",
      \"permission_level\": \"CAN_USE\"
    }
  ]
}"

echo "  Done."

# --------------------------------------------------------------------------
# 2. Grant CAN_MANAGE_RUN on the Job
# --------------------------------------------------------------------------
echo "[2/3] Granting CAN_MANAGE_RUN on Job_ETL_Process_ABN..."

databricks jobs update-permissions ${JOB_ID} --json "{
  \"access_control_list\": [
    {
      \"service_principal_name\": \"${SP_APP_ID}\",
      \"permission_level\": \"CAN_MANAGE_RUN\"
    }
  ]
}"

echo "  Done."

# --------------------------------------------------------------------------
# 3. Grant SELECT on Unity Catalog table (+ USE CATALOG / USE SCHEMA)
# --------------------------------------------------------------------------
echo "[3/3] Granting SELECT on eco_resilience.silver.abn_lookup_structured..."

# USE CATALOG
databricks sql execute --warehouse-id ${WAREHOUSE_ID} --statement \
  "GRANT USE CATALOG ON CATALOG eco_resilience TO \\`${SP_APP_ID}\\`;"

# USE SCHEMA
databricks sql execute --warehouse-id ${WAREHOUSE_ID} --statement \
  "GRANT USE SCHEMA ON SCHEMA eco_resilience.silver TO \\`${SP_APP_ID}\\`;"

# SELECT on table
databricks sql execute --warehouse-id ${WAREHOUSE_ID} --statement \
  "GRANT SELECT ON TABLE eco_resilience.silver.abn_lookup_structured TO \\`${SP_APP_ID}\\`;"

echo "  Done."

# --------------------------------------------------------------------------
# 4. Grant CAN_QUERY on the eco_resilience_agent serving endpoint
# --------------------------------------------------------------------------
echo "[4/5] Granting CAN_QUERY on Model Serving endpoint ${AGENT_ENDPOINT}..."

databricks serving-endpoints update-permissions ${AGENT_ENDPOINT} --json "{
  \"access_control_list\": [
    {
      \"service_principal_name\": \"${SP_APP_ID}\",
      \"permission_level\": \"CAN_QUERY\"
    }
  ]
}"

echo "  Done."

# --------------------------------------------------------------------------
# 5. Grant EXECUTE on silver.generate_grant_pdf
# --------------------------------------------------------------------------
echo "[5/5] Granting EXECUTE on eco_resilience.silver.generate_grant_pdf..."

databricks sql execute --warehouse-id ${WAREHOUSE_ID} --statement \
  "GRANT EXECUTE ON FUNCTION eco_resilience.silver.generate_grant_pdf TO \\`${SP_APP_ID}\\`;"

echo "  Done."

echo ""
echo "=========================================="
echo "All permissions granted successfully!"
echo "=========================================="
echo ""
echo "You can now deploy the app:"
echo "  databricks apps deploy ecoresilience-ai --source-code-path /Workspace/Shared/eco_resilience/Frontend"
