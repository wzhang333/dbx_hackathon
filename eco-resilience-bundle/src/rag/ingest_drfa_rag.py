# Databricks notebook source
# MAGIC %md
# MAGIC # 00 — DRFA RAG Ingestion Pipeline
# MAGIC ## Prerequisite for notebook 09 (RAG tool registration)
# MAGIC
# MAGIC **What this notebook does**
# MAGIC
# MAGIC End-to-end Retrieval-Augmented Generation ingestion over the 16 Disaster
# MAGIC Recovery Funding Arrangements (DRFA) PDF documents that live in a Volume.
# MAGIC Produces the Bronze tables and Vector Search index that
# MAGIC `notebooks/09_register_rag_tool.py` consumes via the `vector_search()`
# MAGIC SQL function.
# MAGIC
# MAGIC **Pipeline steps**
# MAGIC
# MAGIC | Step | Action | Output |
# MAGIC |---|---|---|
# MAGIC | 1 | Parse PDFs with `ai_parse_document v2.0` | `bronze.parsed_docs` |
# MAGIC | 2 | Chunk with `ai_prep_search` (preserves page metadata) | `bronze.drfa_chunks` |
# MAGIC | 3 | Create Vector Search endpoint + Delta Sync index | `bronze.drfa_chunks_index` on `drfa-rag-endpoint` |
# MAGIC | 4 | Wait for sync to complete + smoke-test the index | confirmation |
# MAGIC
# MAGIC **Inputs**
# MAGIC - 16 DRFA PDF files in `/Volumes/eco_resilience/bronze/raw_docs/`
# MAGIC
# MAGIC **Outputs**
# MAGIC - `bronze.parsed_docs` — raw parsed structure (audit/debug)
# MAGIC - `bronze.drfa_chunks` — chunked corpus with `chunk_id`, `chunk_position`, `chunk_to_retrieve`, `chunk_to_embed`, `source_uri`, **`page_number`**
# MAGIC - `bronze.drfa_chunks_index` — Vector Search index synced to the chunks table; embedding model = `databricks-gte-large-en`
# MAGIC
# MAGIC **Why `page_number` matters**
# MAGIC
# MAGIC The agent (via notebook 09's `query_nema_guidelines` tool) cites every
# MAGIC retrieved DRFA rule with the source PDF and page number — e.g.
# MAGIC *"(DRFA Determination 2018.pdf, page 47)"* — so the user (or a judge)
# MAGIC can verify the citation by opening the actual PDF.
# MAGIC
# MAGIC **Compute:** Serverless. `ai_parse_document` and `ai_prep_search` are
# MAGIC native Databricks AI Functions — no model deployment required.
# MAGIC
# MAGIC **Re-run policy**
# MAGIC - **Add new PDFs:** drop them into the Volume, then re-run Steps 1 + 2.
# MAGIC   The index auto-syncs via Delta Change Data Feed; or trigger a manual
# MAGIC   sync with `vsc.get_index(...).sync()` (Step 3c).
# MAGIC - **Change the chunks schema** (e.g. add a new column): re-run Steps 1
# MAGIC   + 2, then DROP and recreate the index (Step 3a) so the new column is
# MAGIC   included in `columns_to_sync`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Install + restart

# COMMAND ----------

# MAGIC %pip install databricks-vectorsearch --quiet

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Configuration

# COMMAND ----------

CATALOG       = "eco_resilience"
BRONZE_SCHEMA = "bronze"

PARSED_TABLE  = f"{CATALOG}.{BRONZE_SCHEMA}.parsed_docs"
CHUNKS_TABLE  = f"{CATALOG}.{BRONZE_SCHEMA}.drfa_chunks"

VOLUME_PATH   = f"/Volumes/{CATALOG}/{BRONZE_SCHEMA}/raw_docs/"

ENDPOINT_NAME       = "drfa-rag-endpoint"
INDEX_NAME          = f"{CATALOG}.{BRONZE_SCHEMA}.drfa_chunks_index"
EMBEDDING_MODEL     = "databricks-gte-large-en"

# columns_to_sync defines which chunk columns the index exposes via
# vector_search(). Keep this list in sync with what notebook 09's
# RAG tool reads — currently {chunk_to_retrieve, source_uri, page_number}.
COLUMNS_TO_SYNC = [
    "chunk_id",
    "chunk_to_retrieve",
    "chunk_to_embed",
    "source_uri",
    "page_number",       # for human-readable citations
    "chunk_position",    # bonus — useful for "near start vs end of doc" framing
]

print(f"PARSED_TABLE  = {PARSED_TABLE}")
print(f"CHUNKS_TABLE  = {CHUNKS_TABLE}")
print(f"VOLUME_PATH   = {VOLUME_PATH}")
print(f"INDEX_NAME    = {INDEX_NAME}")
print(f"ENDPOINT_NAME = {ENDPOINT_NAME}")
print(f"EMBEDDING     = {EMBEDDING_MODEL}")
print(f"SYNC COLS     = {COLUMNS_TO_SYNC}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Pre-flight — confirm PDFs are in the Volume

# COMMAND ----------

import os

pdf_files = [f for f in os.listdir(VOLUME_PATH) if f.lower().endswith(".pdf")]
print(f"Found {len(pdf_files)} PDF files in {VOLUME_PATH}:\n")
for f in sorted(pdf_files):
    size_kb = os.path.getsize(os.path.join(VOLUME_PATH, f)) / 1024
    print(f"  • {f}  ({size_kb:,.0f} KB)")

assert len(pdf_files) >= 1, f"No PDFs found in {VOLUME_PATH} — upload DRFA documents before running"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Step 1 — Parse all DRFA PDFs
# MAGIC
# MAGIC `ai_parse_document(content, MAP('version', '2.0'))` reads PDF bytes and
# MAGIC extracts a structured VARIANT containing paragraphs, tables, headers,
# MAGIC and — critically for us — **page-level metadata**. The v2.0 contract
# MAGIC preserves source page IDs for every parsed element.
# MAGIC
# MAGIC ~30 seconds for 16 PDFs.

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE TABLE {PARSED_TABLE} AS
    SELECT
      path,
      ai_parse_document(content, MAP('version', '2.0')) AS parsed
    FROM READ_FILES(
      '{VOLUME_PATH}',
      format => 'binaryFile'
    )
""")

print(f"✅ Wrote {PARSED_TABLE}")
display(spark.sql(f"""
    SELECT
      regexp_extract(path, '/([^/]+)$', 1)  AS filename,
      CASE WHEN try_cast(parsed:error_status AS STRING) IS NULL
           THEN 'OK' ELSE try_cast(parsed:error_status AS STRING)
      END                                    AS parse_status
    FROM {PARSED_TABLE}
    ORDER BY filename
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Step 2 — Chunk into vector-searchable rows
# MAGIC
# MAGIC `ai_prep_search(parsed)` splits each parsed document into semantic
# MAGIC chunks (sized appropriately for retrieval) and emits a rich VARIANT
# MAGIC structure per chunk. We extract the fields we'll persist downstream:
# MAGIC
# MAGIC | Field | Source | Type |
# MAGIC |---|---|---|
# MAGIC | `chunk_id` | `chunk.value:chunk_id` | STRING (primary key for the index) |
# MAGIC | `chunk_position` | `chunk.value:chunk_position` | INT (sequential within doc) |
# MAGIC | `chunk_to_retrieve` | `chunk.value:chunk_to_retrieve` | STRING (text shown to the LLM) |
# MAGIC | `chunk_to_embed` | `chunk.value:chunk_to_embed` | STRING (text used for embeddings) |
# MAGIC | `source_uri` | document-level / file path | STRING |
# MAGIC | **`page_number`** | **`chunk.value:pages[0]:page_id`** | **INT (first page the chunk appears on)** |
# MAGIC
# MAGIC The `pages` field is an ARRAY because a chunk *can* span pages. For
# MAGIC citation purposes we take the first element — agent says "page 47"
# MAGIC even if the chunk continues onto 48.

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE TABLE {CHUNKS_TABLE}
    TBLPROPERTIES (delta.enableChangeDataFeed = true)
    AS
    WITH prepped_documents AS (
      SELECT
        path,
        ai_prep_search(parsed) AS result
      FROM {PARSED_TABLE}
      WHERE try_cast(parsed:error_status AS STRING) IS NULL
    )
    SELECT
      chunk.value:chunk_id::STRING            AS chunk_id,
      chunk.value:chunk_position::INT         AS chunk_position,
      chunk.value:chunk_to_retrieve::STRING   AS chunk_to_retrieve,
      chunk.value:chunk_to_embed::STRING      AS chunk_to_embed,
      COALESCE(
        prepped_documents.result:document:source_uri::STRING,
        prepped_documents.path
      )                                       AS source_uri,
      chunk.value:pages[0]:page_id::INT       AS page_number
    FROM
      prepped_documents,
      LATERAL variant_explode(prepped_documents.result:document:contents) AS chunk
""")

print(f"✅ Wrote {CHUNKS_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verify chunks — every row should have a populated `page_number`

# COMMAND ----------

display(spark.sql(f"""
    SELECT
      COUNT(*)                                                                AS total_chunks,
      COUNT(DISTINCT source_uri)                                              AS distinct_docs,
      SUM(CASE WHEN page_number IS NOT NULL THEN 1 ELSE 0 END)                AS rows_with_page,
      MIN(page_number)                                                        AS min_page,
      MAX(page_number)                                                        AS max_page
    FROM {CHUNKS_TABLE}
"""))

# COMMAND ----------

# Sample 5 chunks across documents — visual sanity check
display(spark.sql(f"""
    SELECT
      regexp_extract(source_uri, '/([^/]+)$', 1)  AS filename,
      chunk_position,
      page_number,
      SUBSTRING(chunk_to_retrieve, 1, 120)        AS text_preview
    FROM {CHUNKS_TABLE}
    ORDER BY filename, chunk_position
    LIMIT 5
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Step 3 — Vector Search endpoint + Delta Sync index
# MAGIC
# MAGIC ### 6a. Endpoint
# MAGIC
# MAGIC Provisioning takes 2–5 minutes on first creation; idempotent on re-run.

# COMMAND ----------

from databricks.vector_search.client import VectorSearchClient

vsc = VectorSearchClient(disable_notice=True)

try:
    vsc.create_endpoint(name=ENDPOINT_NAME, endpoint_type="STANDARD")
    print(f"✓ Endpoint '{ENDPOINT_NAME}' created — provisioning")
except Exception as e:
    if "already exists" in str(e).lower() or "RESOURCE_ALREADY_EXISTS" in str(e):
        print(f"ℹ️  Endpoint '{ENDPOINT_NAME}' already exists — skipping create")
    else:
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6b. Index — DROP + create (idempotent)
# MAGIC
# MAGIC We DROP the index before recreating so any change to `COLUMNS_TO_SYNC`
# MAGIC (or other config) takes effect cleanly. Sync time after recreation is
# MAGIC 3–5 minutes for our ~hundreds of chunks.
# MAGIC
# MAGIC **Note:** if the index doesn't exist yet, the drop is a safe no-op.

# COMMAND ----------

# Drop existing index (safe no-op if it doesn't exist yet)
try:
    vsc.delete_index(endpoint_name=ENDPOINT_NAME, index_name=INDEX_NAME)
    print(f"✓ Dropped existing index")
except Exception as e:
    msg = str(e).lower()
    if "not found" in msg or "does not exist" in msg or "resource_does_not_exist" in msg:
        print(f"ℹ️  No existing index — creating fresh")
    else:
        print(f"  (drop returned: {e})")

# Create fresh
index = vsc.create_delta_sync_index(
    endpoint_name=ENDPOINT_NAME,
    index_name=INDEX_NAME,
    source_table_name=CHUNKS_TABLE,
    pipeline_type="TRIGGERED",
    primary_key="chunk_id",
    embedding_source_column="chunk_to_embed",
    embedding_model_endpoint_name=EMBEDDING_MODEL,
    columns_to_sync=COLUMNS_TO_SYNC,
)
print(f"✓ Index '{INDEX_NAME}' created — embedding sync starting")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6c. (Optional) Trigger a sync manually
# MAGIC
# MAGIC With `pipeline_type='TRIGGERED'`, the index doesn't auto-sync — you have
# MAGIC to trigger it. After recreate, the first sync happens automatically as
# MAGIC part of creation. For *incremental* updates after re-running Steps 1+2,
# MAGIC uncomment and run the line below.

# COMMAND ----------

# vsc.get_index(endpoint_name=ENDPOINT_NAME, index_name=INDEX_NAME).sync()
# print("✓ Sync triggered")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Step 4 — Wait for sync to complete
# MAGIC
# MAGIC The index reports `ready: True` once the first sync has finished. Re-run
# MAGIC this cell every minute or so until it prints the success line.
# MAGIC Typical: ~3 minutes after Step 6b finishes.

# COMMAND ----------

import time

def wait_for_ready(timeout_seconds: int = 600, poll_seconds: int = 20) -> dict:
    """Poll index status until ready or timeout."""
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        status = vsc.get_index(endpoint_name=ENDPOINT_NAME, index_name=INDEX_NAME).describe()
        ready = status["status"]["ready"]
        detail = status["status"]["detailed_state"]
        msg = status["status"].get("message", "")
        print(f"  ready={ready}  state={detail}  msg={msg[:80]}")
        if ready:
            return status
        time.sleep(poll_seconds)
    raise TimeoutError(f"Index not ready after {timeout_seconds}s")

status = wait_for_ready(timeout_seconds=600, poll_seconds=20)
print(f"\n✅ Index is ONLINE — proceeding to smoke test")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Step 5 — Smoke test the index
# MAGIC
# MAGIC One vector_search query against a known DRFA topic. The result rows
# MAGIC should include `chunk_id`, `chunk_to_retrieve`, `source_uri`,
# MAGIC **`page_number`**, `chunk_position`, and `search_score`.
# MAGIC
# MAGIC If `page_number` is NULL across all rows, the new column didn't make
# MAGIC it into the index — re-run Step 6b after confirming Step 2 produced
# MAGIC populated `page_number` values in the chunks table.

# COMMAND ----------

display(spark.sql(f"""
    SELECT
      regexp_extract(source_uri, '/([^/]+)$', 1) AS source_pdf,
      page_number,
      chunk_position,
      ROUND(search_score, 3)                     AS score,
      SUBSTRING(chunk_to_retrieve, 1, 200)       AS text_preview
    FROM vector_search(
      index       => '{INDEX_NAME}',
      query_text  => 'Category C disaster recovery assistance eligibility',
      num_results => 5
    )
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Done
# MAGIC
# MAGIC | Layer | Object | Purpose |
# MAGIC |---|---|---|
# MAGIC | Bronze | `bronze.parsed_docs` | Raw structured output of `ai_parse_document` for each PDF — audit/debug only |
# MAGIC | Bronze | `bronze.drfa_chunks` | One row per chunk with text, embed, source PDF, page number, chunk position |
# MAGIC | Vector Search | `bronze.drfa_chunks_index` on `drfa-rag-endpoint` | Embedded index queried by the agent's `query_nema_guidelines` tool |
# MAGIC
# MAGIC ### What's next
# MAGIC
# MAGIC Run **`notebooks/09_register_rag_tool.py`** — registers the
# MAGIC `silver.query_nema_guidelines(question)` UC function on top of this
# MAGIC index. The function's `RETURNS STRUCT<...>` includes `page_number INT`
# MAGIC so the agent can cite "(DRFA Determination 2018.pdf, page 47)" in
# MAGIC natural-language responses.
# MAGIC
# MAGIC ### Re-run cheatsheet
# MAGIC
# MAGIC | Scenario | Action |
# MAGIC |---|---|
# MAGIC | Add new PDF(s) to the Volume | Re-run Steps 1 + 2; trigger sync via Step 6c |
# MAGIC | Change chunk schema (add/rename columns) | Re-run Steps 1 + 2 + 6b (drop+recreate index) |
# MAGIC | Index is stale / suspect | Re-run Step 6c (manual sync) |
# MAGIC | Switch embedding model | Update `EMBEDDING_MODEL` in §2; re-run §6b |
# MAGIC | Demote endpoint to STANDARD_HA (or change endpoint type) | Edit §6a and recreate |