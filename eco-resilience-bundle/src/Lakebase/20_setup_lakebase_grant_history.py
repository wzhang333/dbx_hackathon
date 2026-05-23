# Databricks notebook source
# MAGIC %md
# MAGIC # 20 — Lakebase Schema Setup: `grant_submissions`
# MAGIC
# MAGIC Creates the OLTP table that backs the "Your recent applications" panel in
# MAGIC the Databricks App frontend. Every grant the agent generates is persisted
# MAGIC to this Lakebase Postgres table so the user can see their application history.
# MAGIC
# MAGIC ### Why Lakebase
# MAGIC
# MAGIC The analytical side of this project lives in Delta (Unity Catalog gold tables).
# MAGIC But "show me the last 10 things I just did" is an **OLTP question**, not an
# MAGIC analytical one — low latency, point reads, append-heavy, per-user.
# MAGIC **Lakebase** is Databricks' managed Postgres for exactly this kind of workload,
# MAGIC governed by Unity Catalog like the rest of the lakehouse.
# MAGIC
# MAGIC This notebook is idempotent: re-runs are safe. The `CREATE TABLE IF NOT EXISTS`
# MAGIC pattern means you can run this notebook after deploys, after teardowns, after
# MAGIC schema tweaks — it converges to the desired state.
# MAGIC
# MAGIC ### Prerequisites (done in the Databricks UI, one-time)
# MAGIC
# MAGIC 1. **Provision the Lakebase instance:**
# MAGIC    - Sidebar → **Compute** → **Database instances** (Lakebase tab)
# MAGIC    - Create instance: name `eco_resilience_lakebase`, capacity `CU_1` (smallest)
# MAGIC    - Wait ~3-5 min for status → `AVAILABLE`
# MAGIC
# MAGIC 2. **Note the connection details** from the instance detail page:
# MAGIC    - Host (e.g. `eco-resilience-lakebase-xxxxx.database.cloud.databricks.com`)
# MAGIC    - Port `5432`
# MAGIC    - Default DB name: `databricks_postgres`
# MAGIC
# MAGIC 3. **Grant the Databricks App's service principal** the `databricks_superuser`
# MAGIC    role on the instance (Permissions tab). This lets the app's Flask backend
# MAGIC    INSERT/SELECT into the table at runtime.
# MAGIC
# MAGIC ### What this notebook does
# MAGIC
# MAGIC - §2: Installs `psycopg[binary]` (one-time per cluster)
# MAGIC - §3: Connects to Lakebase via OAuth token from `WorkspaceClient`
# MAGIC - §4: Creates `grant_submissions` table + indexes (idempotent)
# MAGIC - §5: Validates with a round-trip insert/select/delete
# MAGIC - §6: Prints the env-var values the Databricks App needs

# COMMAND ----------

# MAGIC %md
# MAGIC ## §1 — Configuration
# MAGIC
# MAGIC **Update `LAKEBASE_HOST` below** with the host from your instance detail page
# MAGIC before running the rest of the notebook.

# COMMAND ----------

# Replace with YOUR instance host (from the Compute → Database instances UI):
LAKEBASE_HOST = "ep-noisy-sky-d8m7yunu.database.us-east-2.cloud.databricks.com"
LAKEBASE_PORT = 5432
LAKEBASE_DB   = "databricks_postgres"   # default Lakebase database name

# Instance NAME (not host) — used by the SDK's database.generate_database_credential API
# to mint a Lakebase-compatible JWT. Get it via `databricks database list-database-instances`.
INSTANCE_NAME = "grant-history-db"

TABLE_NAME    = "grant_submissions"

# COMMAND ----------

# MAGIC %md
# MAGIC ## §2 — Install psycopg2 + upgrade databricks-sdk
# MAGIC
# MAGIC `psycopg2` is the Postgres client. `databricks-sdk>=0.40` adds the
# MAGIC `w.database` namespace we need for `generate_database_credential()`.

# COMMAND ----------

# MAGIC %pip install --quiet --upgrade "databricks-sdk>=0.40.0" "psycopg2-binary>=2.9"
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## §3 — Connect to Lakebase via OAuth
# MAGIC
# MAGIC We use the Databricks SDK's `WorkspaceClient` to obtain a short-lived OAuth
# MAGIC access token, and pass it as the Postgres password. No long-lived password
# MAGIC management — the token auto-refreshes.

# COMMAND ----------

import uuid
import psycopg2
from databricks.sdk import WorkspaceClient

# Re-set after the kernel restart in §2.
LAKEBASE_HOST = "ep-noisy-sky-d8m7yunu.database.us-east-2.cloud.databricks.com"
LAKEBASE_PORT = 5432
LAKEBASE_DB   = "databricks_postgres"
INSTANCE_NAME = "grant-history-db"
TABLE_NAME    = "grant_submissions"

w = WorkspaceClient()

# The Postgres "username" is your Databricks email. The "password" is a JWT
# minted by the Database service API — that's the only token format Lakebase accepts.
current_user_email = w.current_user.me().user_name


def _get_lakebase_token():
    """Mint a short-lived (~1h) JWT credential for Lakebase Postgres auth.

    Lakebase rejects plain PATs and standard OAuth tokens — it requires a
    JWT minted via the Database service API. The SDK exposes this as
    `w.database.generate_database_credential()`.
    """
    cred = w.database.generate_database_credential(
        request_id=str(uuid.uuid4()),
        instance_names=[INSTANCE_NAME],
    )
    return cred.token


def lakebase_conn():
    return psycopg2.connect(
        host=LAKEBASE_HOST,
        port=LAKEBASE_PORT,
        dbname=LAKEBASE_DB,
        user=current_user_email,
        password=_get_lakebase_token(),
        sslmode="require",
    )

# Smoke-test the connection
with lakebase_conn() as conn, conn.cursor() as cur:
    cur.execute("SELECT version()")
    print("Lakebase reachable:", cur.fetchone()[0])

# COMMAND ----------

# MAGIC %md
# MAGIC ## §4 — Create schema (idempotent)

# COMMAND ----------

DDL = f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
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

CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_abn
    ON {TABLE_NAME} (abn);

CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_generated_at
    ON {TABLE_NAME} (generated_at DESC);
"""

with lakebase_conn() as conn, conn.cursor() as cur:
    cur.execute(DDL)
    conn.commit()

print(f"OK — {TABLE_NAME} ready (created or already existed).")

# COMMAND ----------

# MAGIC %md
# MAGIC ## §5 — Validate with a round-trip insert / select / delete

# COMMAND ----------

with lakebase_conn() as conn, conn.cursor() as cur:
    # Insert a test row
    cur.execute(
        f"""
        INSERT INTO {TABLE_NAME}
          (abn, business_name, postcode, state, application_id, grant_status, user_query)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        RETURNING id, generated_at
        """,
        (
            "00000000000",
            "Lakebase Smoke Test Pty Ltd",
            "2000",
            "NSW",
            "DRFA-SMOKE-TEST",
            "DRAFT",
            "smoke test — please ignore",
        ),
    )
    test_id, ts = cur.fetchone()
    print(f"Inserted test row id={test_id} at {ts}")

    # Read it back
    cur.execute(f"SELECT abn, business_name, postcode FROM {TABLE_NAME} WHERE id = %s", (test_id,))
    print("Read back:", cur.fetchone())

    # Clean up
    cur.execute(f"DELETE FROM {TABLE_NAME} WHERE id = %s", (test_id,))
    conn.commit()
    print(f"Deleted test row id={test_id}. Round-trip complete.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## §6 — Connection details for the Databricks App
# MAGIC
# MAGIC Copy these into the Databricks App's environment variables (Workspace →
# MAGIC Compute → Apps → your-app → Edit → Environment variables):

# COMMAND ----------

print("=" * 70)
print("Set these env vars in the Databricks App config:")
print("=" * 70)
print(f"LAKEBASE_HOST = {LAKEBASE_HOST}")
print(f"LAKEBASE_DB   = {LAKEBASE_DB}")
print(f"LAKEBASE_USER = <the service principal's email or client_id>")
print()
print("Then redeploy the app and smoke-test by generating a grant via the UI.")

# COMMAND ----------

SP_PRINCIPAL = "692e74d4-b772-4c97-8b2c-c0edbc5ea1f8"

with lakebase_conn() as conn:
    conn.autocommit = True   # CREATE USER outside a transaction
    with conn.cursor() as cur:
        # 1. Create the Postgres role for the service principal
        try:
            cur.execute(f'CREATE USER "{SP_PRINCIPAL}" WITH LOGIN')
            print(f"Created role for {SP_PRINCIPAL}")
        except Exception as e:
            print(f"CREATE USER skipped ({type(e).__name__}): {e}")

        # 2. Grant the role permission on the table + auto-increment sequence
        cur.execute(f'GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE grant_submissions TO "{SP_PRINCIPAL}"')
        cur.execute(f'GRANT USAGE, SELECT ON SEQUENCE grant_submissions_id_seq TO "{SP_PRINCIPAL}"')
        print(f"Granted table + sequence to {SP_PRINCIPAL}")

        # 3. Verify the role exists with LOGIN privilege
        cur.execute(f"SELECT rolname, rolcanlogin FROM pg_roles WHERE rolname = '{SP_PRINCIPAL}'")
        row = cur.fetchone()
        if row:
            print(f"Verified: role={row[0]}, can_login={row[1]}")
        else:
            print(f"WARNING: role still missing!")

# COMMAND ----------

# Inspect Lakebase's IAM-related roles to find the right group to grant
with lakebase_conn() as conn, conn.cursor() as cur:
      cur.execute("""
          SELECT rolname, rolcanlogin, rolinherit
          FROM pg_roles
          WHERE rolname LIKE 'databricks%'
             OR rolname LIKE '%iam%'
             OR rolname LIKE '%oauth%'
          ORDER BY rolname
      """)
      print("Databricks/IAM-related roles in this Lakebase instance:")
      for r in cur.fetchall():
          print(f"  {r[0]}  can_login={r[1]}  inherit={r[2]}")
  
      print() 
  
      # Also check what groups our SP role is currently a member of
      cur.execute("""
          SELECT r.rolname AS role, parent.rolname AS member_of
          FROM pg_roles r
          LEFT JOIN pg_auth_members m ON r.oid = m.member
          LEFT JOIN pg_roles parent ON m.roleid = parent.oid
          WHERE r.rolname = '692e74d4-b772-4c97-8b2c-c0edbc5ea1f8'
      """)
      print("Our SP role's group memberships:")
      for r in cur.fetchall():
          print(f"  role={r[0]}  member_of={r[1]}")


# COMMAND ----------

help(w.database.create_database_instance_role)

# COMMAND ----------

from databricks.sdk.service.database import DatabaseInstanceRole
    
# Show the dataclass field names + types
import dataclasses
print("DatabaseInstanceRole fields:")
for f in dataclasses.fields(DatabaseInstanceRole):
    print(f"  {f.name}: {f.type}")

print()

# Look at one existing role assignment for the instance — copy its shape
existing = list(w.database.list_database_instance_roles(instance_name="grant-history-db"))
print(f"Existing roles on the instance ({len(existing)} total):")
for r in existing[:5]:
    print(f"  {r}")



# COMMAND ----------

SP_UUID       = "692e74d4-b772-4c97-8b2c-c0edbc5ea1f8"
INSTANCE_NAME = "grant-history-db"

from databricks.sdk.service.database import (
    DatabaseInstanceRole,
    DatabaseInstanceRoleIdentityType,
    DatabaseInstanceRoleMembershipRole,
) 

# First — see what enum values are actually available
print("Identity types:", [e.name for e in DatabaseInstanceRoleIdentityType])
print("Membership roles:", [e.name for e in DatabaseInstanceRoleMembershipRole])
print()

# Step 1: Delete the broken PG_ONLY role
try:
    w.database.delete_database_instance_role(instance_name=INSTANCE_NAME, name=SP_UUID)
    print(f"Deleted old PG_ONLY role for {SP_UUID}")
except Exception as e:
    print(f"Delete: {type(e).__name__}: {e}")

# Step 2: Create a proper SERVICE_PRINCIPAL role with DATABRICKS_SUPERUSER membership
new_role = DatabaseInstanceRole(
    name=SP_UUID,
    identity_type=DatabaseInstanceRoleIdentityType.SERVICE_PRINCIPAL,
    membership_role=DatabaseInstanceRoleMembershipRole.DATABRICKS_SUPERUSER,
)

result = w.database.create_database_instance_role(
    instance_name=INSTANCE_NAME,
    database_instance_role=new_role,
) 
print(f"\nCreated SERVICE_PRINCIPAL role:")
print(f"  {result}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## §7 — Useful follow-up queries
# MAGIC
# MAGIC ```sql
# MAGIC -- Recent submissions
# MAGIC SELECT id, abn, business_name, postcode, generated_at
# MAGIC FROM grant_submissions
# MAGIC ORDER BY generated_at DESC
# MAGIC LIMIT 10;
# MAGIC
# MAGIC -- Submissions per postcode (analytical view of OLTP data)
# MAGIC SELECT postcode, COUNT(*) AS n_grants
# MAGIC FROM grant_submissions
# MAGIC GROUP BY postcode
# MAGIC ORDER BY n_grants DESC;
# MAGIC
# MAGIC -- Drop everything (DANGER — only if rebuilding from scratch)
# MAGIC -- DROP TABLE grant_submissions;
# MAGIC ```