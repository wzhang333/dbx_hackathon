# Databricks notebook source
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()

app = w.apps.get(name="eco-resilience-ai")
SP_ID = app.service_principal_client_id
print(f"App service principal ID: {SP_ID}")

# COMMAND ----------

from databricks.sdk.service.serving import (
    ServingEndpointAccessControlRequest,
    ServingEndpointPermissionLevel,
) 

w.serving_endpoints.update_permissions(
    name="eco_resilience_agent", 
    access_control_list=[
        ServingEndpointAccessControlRequest(
            service_principal_name=SP_ID,
            permission_level=ServingEndpointPermissionLevel.CAN_QUERY,
        )
    ], 
) 
print(f"✅ Granted CAN_QUERY on eco_resilience_agent to {SP_ID}")

# COMMAND ----------

spark.sql(f"GRANT EXECUTE ON FUNCTION eco_resilience.silver.generate_grant_pdf TO `{SP_ID}`")
spark.sql(f"GRANT USE CATALOG ON CATALOG eco_resilience TO `{SP_ID}`")
spark.sql(f"GRANT USE SCHEMA ON SCHEMA eco_resilience.silver TO `{SP_ID}`")
print("✅ Granted UC function + traversal perms")

# COMMAND ----------

# Verify endpoint permission
ep_perms = w.serving_endpoints.get_permissions(serving_endpoint_id=w.serving_endpoints.get(name="eco_resilience_agent").id)
print("Endpoint permissions:")
for acl in ep_perms.access_control_list:
    if acl.service_principal_name == SP_ID:
        print(f"  ✅ {SP_ID}: {[p.permission_level.value for p in acl.all_permissions]}")

# Verify function permission
print("\nFunction grants:")
display(spark.sql("SHOW GRANTS ON FUNCTION eco_resilience.silver.generate_grant_pdf"))