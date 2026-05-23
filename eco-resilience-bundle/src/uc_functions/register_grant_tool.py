# Databricks notebook source
# MAGIC %md
# MAGIC # 12 — Register `generate_grant_pdf` (the Magic Moment tool)
# MAGIC ## Phase 4 Step 3c — Agent's final tool: structured grant draft
# MAGIC
# MAGIC **What this notebook does**
# MAGIC
# MAGIC Registers `eco_resilience.silver.generate_grant_pdf` as the seventh and
# MAGIC final UC tool for the agent. It composes the agent's reasoning into a
# MAGIC structured DRAFT grant application under the DRFA.
# MAGIC
# MAGIC **Why "Magic Moment"**
# MAGIC
# MAGIC The spec calls out the pre-filled NEMA grant as the demo's pivotal
# MAGIC output. This tool produces the structured DATA. The Streamlit app
# MAGIC (Phase 5) renders the data through Jinja2 + a PDF library to produce
# MAGIC the actual downloadable file. Per the spec:
# MAGIC
# MAGIC > *Don't let the Agent generate the final PDF directly — it might
# MAGIC > hallucinate the layout. Have the Agent output a JSON object...
# MAGIC > and use a standard Python template library (like Jinja2) to inject
# MAGIC > that data into a legally compliant PDF template.*
# MAGIC
# MAGIC So this tool **returns a STRUCT, not PDF bytes**. PDF rendering lives
# MAGIC outside the UC sandbox.
# MAGIC
# MAGIC **Pattern**
# MAGIC
# MAGIC Pure SQL UDF that takes the applicant identity fields (abn, entity_name,
# MAGIC entity_state, entity_postcode) as ARGS — the agent passes them through
# MAGIC from its prior `verify_abn` call — plus the grant-specific fields
# MAGIC (disaster_type, disaster_date, drfa_category, estimated_loss_aud,
# MAGIC justification). Validates inputs and emits a STRUCT with status
# MAGIC ('DRAFT' or 'INVALID'), next_steps checklist, and optional error message.
# MAGIC
# MAGIC **Identity is now passed by the agent**, not looked up internally —
# MAGIC `verify_abn` runs in `eco_agent.py` as a Python tool (not a UC function)
# MAGIC because the SQL `secret()` function doesn't resolve in Mosaic AI Model
# MAGIC Serving auth context. The agent threads the identity fields through.
# MAGIC
# MAGIC **Compute:** Serverless. Pure SQL — no Python sandbox concerns.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration

# COMMAND ----------

CATALOG       = "eco_resilience"
SILVER_SCHEMA = "silver"

FUNCTION_NAME    = "generate_grant_pdf"
FQN              = f"{CATALOG}.{SILVER_SCHEMA}.{FUNCTION_NAME}"

print(f"Will register: {FQN}")
print("No UC dependencies — identity passed in as args by the agent.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Pre-flight — no UC dependencies needed
# MAGIC
# MAGIC This function takes identity fields directly as args, so it has no UC
# MAGIC function dependencies. The agent threads identity fields from its prior
# MAGIC `verify_abn` Python tool call into this function's args.

# COMMAND ----------

print("  ✅ No UC dependencies — identity fields are passed as args.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Tool description — the LLM-facing COMMENT
# MAGIC
# MAGIC Same conventions as our other tool descriptions:
# MAGIC - **No single quotes / apostrophes** (would break the SQL string literal).
# MAGIC - **Says when to call it** (after gathering reasoning material from other tools).
# MAGIC - **Says what it returns** (STRUCT with status, applicant, disaster, grant_request, next_steps, error).
# MAGIC - **Says what it does NOT do** (no PDF bytes — that is the Streamlit app's job).

# COMMAND ----------

TOOL_DESCRIPTION = (
    "Composes agent reasoning into a structured DRAFT grant application under "
    "the Australian Disaster Recovery Funding Arrangements (DRFA). Takes the "
    "applicant identity fields (abn, entity_name, entity_state, entity_postcode) "
    "directly as args — pass them verbatim from your prior verify_abn tool "
    "call. Use this AS THE FINAL STEP after you have already called the "
    "relevant data tools (verify_abn, get_active_hazards, query_nema_guidelines, "
    "get_industry_context, and so on) — the args you pass should be your "
    "reasoned synthesis: which DRFA category applies based on the cited "
    "rules, the estimated loss informed by industry context, and the "
    "justification narrative written in plain English. Returns a STRUCT "
    "with application_id, applicant identity, disaster details, grant "
    "request, draft status, and a next-steps checklist. The application is "
    "a DRAFT — it is rendered to PDF by the Streamlit application; this "
    "tool does NOT itself produce PDF bytes. Status is one of: DRAFT "
    "(success) or INVALID (input format wrong). After calling this tool, "
    "present the application_id and next_steps list to the user and explain "
    "that the document is a draft ready for review before submission."
)

# Per-arg comments
ABN_COMMENT             = "Australian Business Number of the applicant (11 digits, spaces tolerated)"
ENTITY_NAME_COMMENT     = "Business entity name as returned by verify_abn (e.g. BATHURST REGIONAL COUNCIL)"
ENTITY_STATE_COMMENT    = "Australian state code as returned by verify_abn (e.g. NSW, VIC, QLD)"
ENTITY_POSTCODE_COMMENT = "Australian postcode as returned by verify_abn (4-digit string)"
DISASTER_TYPE_COMMENT   = "Disaster type. One of: flood, fire, storm, earthquake, drought, cyclone"
DISASTER_DATE_COMMENT   = "Date the disaster occurred or began. Must be ISO format YYYY-MM-DD."
DRFA_CATEGORY_COMMENT   = "DRFA category the applicant is requesting. One of: A, B, C, D"
LOSS_COMMENT            = "Estimated total loss in AUD, as a positive number (whole dollars)"
JUSTIFICATION_COMMENT   = "Plain-English narrative justifying the application, composed from agent reasoning. 2-4 sentences."

print(f"Tool description: {len(TOOL_DESCRIPTION)} chars")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Register the function
# MAGIC
# MAGIC One `CREATE OR REPLACE FUNCTION` with:
# MAGIC - **9 typed input args** (abn, entity_name, entity_state, entity_postcode, disaster_type, disaster_date, drfa_category, estimated_loss_aud, justification)
# MAGIC - **Nested STRUCT return** (applicant, disaster, grant_request, status, next_steps, error)
# MAGIC - **No internal UC dependencies** — identity flows in via args from the agent
# MAGIC - **Input validation** via CASE expressions (status: DRAFT / INVALID)
# MAGIC - **Auto-generated `application_id`** via `uuid()`
# MAGIC - **Next-steps checklist** populated only when status='DRAFT'

# COMMAND ----------

create_sql = f"""
CREATE OR REPLACE FUNCTION {FQN}(
  abn                  STRING COMMENT '{ABN_COMMENT}',
  entity_name          STRING COMMENT '{ENTITY_NAME_COMMENT}',
  entity_state         STRING COMMENT '{ENTITY_STATE_COMMENT}',
  entity_postcode      STRING COMMENT '{ENTITY_POSTCODE_COMMENT}',
  disaster_type        STRING COMMENT '{DISASTER_TYPE_COMMENT}',
  disaster_date        STRING COMMENT '{DISASTER_DATE_COMMENT}',
  drfa_category        STRING COMMENT '{DRFA_CATEGORY_COMMENT}',
  estimated_loss_aud   DOUBLE COMMENT '{LOSS_COMMENT}',
  justification        STRING COMMENT '{JUSTIFICATION_COMMENT}'
)
RETURNS STRUCT<
  application_id   STRING,
  draft_timestamp  TIMESTAMP,
  applicant        STRUCT<
                     abn         STRING,
                     entity_name STRING,
                     state       STRING,
                     postcode    STRING
                   >,
  disaster         STRUCT<
                     type   STRING,
                     date   STRING,
                     in_nsw BOOLEAN
                   >,
  grant_request    STRUCT<
                     drfa_category       STRING,
                     estimated_loss_aud  DOUBLE,
                     justification       STRING
                   >,
  status           STRING,
  next_steps       ARRAY<STRING>,
  error            STRING
>
COMMENT '{TOOL_DESCRIPTION}'
RETURN (
  WITH
  status_eval AS (
    SELECT
      CASE
        WHEN abn IS NULL OR length(trim(abn)) = 0
             THEN 'INVALID'
        WHEN entity_name IS NULL OR length(trim(entity_name)) = 0
             THEN 'INVALID'
        WHEN disaster_type NOT IN ('flood', 'fire', 'storm', 'earthquake', 'drought', 'cyclone')
             THEN 'INVALID'
        WHEN drfa_category NOT IN ('A', 'B', 'C', 'D')
             THEN 'INVALID'
        WHEN try_cast(disaster_date AS DATE) IS NULL
             THEN 'INVALID'
        WHEN estimated_loss_aud IS NULL OR estimated_loss_aud < 0
             THEN 'INVALID'
        WHEN justification IS NULL OR length(trim(justification)) < 10
             THEN 'INVALID'
        ELSE 'DRAFT'
      END AS status
  )
  SELECT named_struct(
    'application_id',  uuid(),
    'draft_timestamp', current_timestamp(),
    'applicant', named_struct(
      'abn',         abn,
      'entity_name', entity_name,
      'state',       entity_state,
      'postcode',    entity_postcode
    ),
    'disaster', named_struct(
      'type',   disaster_type,
      'date',   disaster_date,
      'in_nsw', entity_state = 'NSW'
    ),
    'grant_request', named_struct(
      'drfa_category',      drfa_category,
      'estimated_loss_aud', estimated_loss_aud,
      'justification',      justification
    ),
    'status', status,
    'next_steps',
      CASE status
        WHEN 'DRAFT' THEN array(
          'Review the draft for accuracy',
          'Attach disaster damage evidence (photos, repair quotes, insurance records)',
          'Confirm the DRFA category cites the right authority',
          'Submit to NEMA via the official portal'
        )
        ELSE array()
      END,
    'error',
      CASE status
        WHEN 'INVALID' THEN
          'One or more inputs invalid. Check abn, entity_name, disaster_type (flood/fire/storm/earthquake/drought/cyclone), disaster_date (YYYY-MM-DD), drfa_category (A/B/C/D), estimated_loss_aud (positive number), and justification (at least 10 characters).'
        ELSE NULL
      END
  )
  FROM status_eval
)
"""

spark.sql(create_sql)
print(f"✅ Registered {FQN}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Smoke tests
# MAGIC
# MAGIC Five test cases exercising the happy path and four error modes.
# MAGIC Each test prints the full STRUCT so you can inspect every field.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5a — Happy path: Bathurst Council, flood, Cat C, $48,500
# MAGIC
# MAGIC Expected: `status='DRAFT'`, populated `applicant.entity_name='BATHURST REGIONAL COUNCIL'`,
# MAGIC `disaster.in_nsw=true`, 4 `next_steps` items, null `error`.

# COMMAND ----------

display(spark.sql(f"""
    select r.*
    from (
    SELECT eco_resilience.silver.generate_grant_pdf(
      '42173522302',
      'BATHURST REGIONAL COUNCIL',
      'NSW',
      '2795',
      'flood',
      '2026-05-12',
      'C',
      48500.0,
      'The applicant operates within the Australian agriculture sector. An active flood in the Bathurst region has affected primary production assets. The applicant seeks reconstruction support under DRFA Category C.'
    ) as r)
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5b — Missing identity (empty entity_name)
# MAGIC
# MAGIC Expected: `status='INVALID'`, error mentions missing identity fields.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5c — Invalid disaster_type
# MAGIC
# MAGIC Expected: `status='INVALID'`, error explains valid disaster types.

# COMMAND ----------

display(spark.sql(f"""
    SELECT {FQN}(
      '42173522302',
      'BATHURST REGIONAL COUNCIL',
      'NSW',
      '2795',
      'meteorite',
      '2026-05-12',
      'C',
      10000.0,
      'A meteorite struck the property and caused damage. This narrative is for testing purposes.'
    ).*
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5d — Invalid date format (DD/MM/YYYY instead of YYYY-MM-DD)
# MAGIC
# MAGIC Expected: `status='INVALID'`, error mentions date format requirement.

# COMMAND ----------

display(spark.sql(f"""
    SELECT {FQN}(
      '42173522302',
      'BATHURST REGIONAL COUNCIL',
      'NSW',
      '2795',
      'flood',
      '12/05/2026',
      'C',
      10000.0,
      'Date provided in the wrong format. Test that the validator catches this.'
    ).*
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5e — Negative loss
# MAGIC
# MAGIC Expected: `status='INVALID'`, error mentions positive number requirement.

# COMMAND ----------

display(spark.sql(f"""
    SELECT {FQN}(
      '42173522302',
      'BATHURST REGIONAL COUNCIL',
      'NSW',
      '2795',
      'flood',
      '2026-05-12',
      'C',
      -1000.0,
      'Negative loss amount should be rejected by the validator.'
    ).*
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Discoverability — what the agent sees

# COMMAND ----------

display(spark.sql(f"DESCRIBE FUNCTION EXTENDED {FQN}"))

# COMMAND ----------

display(spark.sql(f"SHOW USER FUNCTIONS IN {CATALOG}.{SILVER_SCHEMA}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Done
# MAGIC
# MAGIC | What we built | Why it matters |
# MAGIC |---|---|
# MAGIC | `silver.generate_grant_pdf` UC SQL function | Seventh and final agent tool — completes the toolkit before MLflow wrapping |
# MAGIC | Identity fields passed as args | Agent threads identity from its prior verify_abn (Python tool) call — no UC dependency on verify_abn |
# MAGIC | Strict input validation | INVALID status with helpful error messages |
# MAGIC | `next_steps` checklist | Gives the user concrete actions after the draft lands |
# MAGIC | Auto-generated `application_id` (uuid()) | Future-proofs draft tracking when we add an Inference Table audit |
# MAGIC | Returns STRUCT, NOT bytes | PDF rendering happens in Streamlit (Phase 5) — keeps AI in the reasoning lane |
# MAGIC
# MAGIC ### What's next — Phase 4 Step 3c update + Step 4
# MAGIC
# MAGIC **Immediate (Phase 4 Step 3c, part 2):**
# MAGIC - Update `notebooks/07_minimal_agent.py`:
# MAGIC   1. Add `silver.generate_grant_pdf` to `TOOL_FUNCTIONS` (7 tools total)
# MAGIC   2. Extend system prompt with tool #7 and guidance about call ordering (this tool is the LAST step, never before)
# MAGIC   3. Add a multi-tool smoke test that produces a real grant draft end-to-end
# MAGIC
# MAGIC **Then Phase 4 Step 4:**
# MAGIC - Wrap the agent in `mlflow.pyfunc.ChatAgent` + deploy via `databricks.agents.deploy()` for MLflow tracing, Inference Tables, and UC lineage.
# MAGIC
# MAGIC **Then Phase 5:**
# MAGIC - Streamlit Lakehouse App with a Jinja2-rendered PDF download button consuming this tool's STRUCT output.