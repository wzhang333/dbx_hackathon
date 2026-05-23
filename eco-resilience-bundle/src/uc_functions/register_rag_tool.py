# Databricks notebook source
# MAGIC %md
# MAGIC # 09 — Register `query_nema_guidelines` (Vector Search RAG Tool)
# MAGIC ## Phase 4 Step 3b — Agent's "legal brain"
# MAGIC
# MAGIC **What this notebook does**
# MAGIC
# MAGIC Registers a single UC SQL function — `silver.query_nema_guidelines` —
# MAGIC that wraps Databricks' native `vector_search()` SQL function against the
# MAGIC NEMA DRFA Vector Search index. The agent calls it whenever the user asks
# MAGIC about disaster-recovery rules, grant eligibility, Category A/B/C/D
# MAGIC assistance, evidence requirements, or any other DRFA-specific rule.
# MAGIC
# MAGIC **Pre-existing infrastructure (built outside our notebooks)**
# MAGIC
# MAGIC | | Value |
# MAGIC |---|---|
# MAGIC | Vector Search endpoint | `drfa-rag-endpoint` |
# MAGIC | Vector Search index | `eco_resilience.bronze.drfa_chunks_index` |
# MAGIC | Source chunks table | `eco_resilience.bronze.drfa_chunks` |
# MAGIC | Embedding model | `databricks-gte-large-en` |
# MAGIC | Chunks schema | `chunk_id, chunk_position, chunk_to_retrieve, chunk_to_embed, source_uri` |
# MAGIC
# MAGIC The two-column `chunk_to_retrieve` / `chunk_to_embed` design lets the
# MAGIC embedding model see a normalised version while the agent gets the rich
# MAGIC human-readable text — we expose `chunk_to_retrieve` in the tool output.
# MAGIC
# MAGIC **Why pure SQL (not Python)**
# MAGIC
# MAGIC Databricks SQL has a native `vector_search()` table-valued function that
# MAGIC queries any UC-managed Vector Search index. No Python dependencies, no UC
# MAGIC sandbox concerns — matches the pattern of the three SQL tools in
# MAGIC notebook 08.
# MAGIC
# MAGIC **Compute:** Serverless. No `%pip install` needed.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration

# COMMAND ----------

CATALOG        = "eco_resilience"
SILVER_SCHEMA  = "silver"
BRONZE_SCHEMA  = "bronze"

FUNCTION_NAME  = "query_nema_guidelines"
FQN            = f"{CATALOG}.{SILVER_SCHEMA}.{FUNCTION_NAME}"

VS_INDEX       = f"{CATALOG}.{BRONZE_SCHEMA}.drfa_chunks_index"
CHUNKS_TABLE   = f"{CATALOG}.{BRONZE_SCHEMA}.drfa_chunks"
NUM_RESULTS    = 5

print(f"Will register: {FQN}")
print(f"Backed by:     {VS_INDEX}  (top-{NUM_RESULTS} results)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Pre-flight — confirm chunks table and index are queryable
# MAGIC
# MAGIC Two checks:
# MAGIC 1. The chunks table exists and has rows (confirms ingest happened).
# MAGIC 2. The `vector_search()` SQL function can query the index (confirms the
# MAGIC    endpoint is up, the index is in sync, and we have permissions).

# COMMAND ----------

# Chunks table
cnt = spark.table(CHUNKS_TABLE).count()
print(f"  ✅ {CHUNKS_TABLE}  ({cnt:,} chunks)")

# COMMAND ----------

# Vector Search probe — should return 1 row with the score
display(spark.sql(f"""
    SELECT chunk_id, search_score
    FROM vector_search(
        index       => '{VS_INDEX}',
        query_text  => 'disaster recovery',
        num_results => 1
    )
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Tool description — the LLM-facing `COMMENT ON FUNCTION`
# MAGIC
# MAGIC The most important sentence: **always cite the source PDF and chunk
# MAGIC position**. The whole point of grounding agent output in retrieved
# MAGIC chunks is that every claim is traceable. Without citations the agent
# MAGIC is no better than uncited prose.

# COMMAND ----------

TOOL_DESCRIPTION = (
    "Searches the official NEMA Disaster Recovery Funding Arrangements (DRFA) "
    "documents using semantic similarity, and returns the top 5 most relevant "
    "text chunks for a natural-language question. Use this whenever the user "
    "asks about disaster recovery grant eligibility, application requirements, "
    "Category A/B/C/D assistance, what costs are claimable, evidence "
    "requirements, or any specific DRFA rule. The tool returns the chunk text, "
    "the source PDF filename, the page number it came from, and a similarity "
    "score. ALWAYS cite the source PDF filename and page number when "
    "summarising the results. NEVER invent rules — only state what the "
    "retrieved chunks explicitly say. If the chunks do not contain enough "
    "information to answer, say so clearly rather than guessing. The argument "
    "is a natural-language question; no postcode needed."
)

ARG_COMMENT = (
    "Natural-language question about NEMA DRFA disaster recovery rules, "
    "e.g. What evidence is needed for Category C reconstruction claims?"
)

print(f"Tool description length: {len(TOOL_DESCRIPTION)} chars")
print()
print(TOOL_DESCRIPTION)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Register the function
# MAGIC
# MAGIC Same SQL UDF pattern as notebook 08's three data tools. The body is a
# MAGIC `RETURN (WITH … SELECT named_struct(…))` expression that:
# MAGIC
# MAGIC 1. Calls `vector_search()` for top-N hits.
# MAGIC 2. Renames `chunk_to_retrieve` → `text`, extracts the PDF filename from
# MAGIC    `source_uri` using a regex over the last path segment.
# MAGIC 3. Aggregates hits into the declared STRUCT.

# COMMAND ----------

create_function_sql = f"""
CREATE OR REPLACE FUNCTION {FQN}(
  question STRING COMMENT '{ARG_COMMENT}'
)
RETURNS STRUCT<
  question      STRING,
  chunks_count  INT,
  chunks        ARRAY<STRUCT<
                  text             STRING,
                  source_pdf       STRING,
                  page_number      INT,
                  similarity_score DOUBLE
                >>,
  error         STRING
>
COMMENT '{TOOL_DESCRIPTION}'
RETURN (
  WITH
  hits AS (
    SELECT
      chunk_to_retrieve                              AS text,
      regexp_extract(source_uri, '/([^/]+)$', 1)     AS source_pdf,
      page_number,
      CAST(search_score AS DOUBLE)                   AS similarity_score
    FROM vector_search(
      index       => '{VS_INDEX}',
      query_text  => question,
      num_results => {NUM_RESULTS}
    )
  ),
  agg AS (
    SELECT
      array_agg(named_struct(
        'text',             text,
        'source_pdf',       source_pdf,
        'page_number',      page_number,
        'similarity_score', similarity_score
      )) AS chunks_arr,
      COUNT(*) AS n
    FROM hits
  )
  SELECT named_struct(
    'question',     question,
    'chunks_count', CAST(COALESCE((SELECT n FROM agg), 0) AS INT),
    'chunks',       COALESCE((SELECT chunks_arr FROM agg), array()),
    'error',        NULL
  )
)
"""

spark.sql(create_function_sql)
print(f"✅ Registered {FQN}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Smoke tests via SQL
# MAGIC
# MAGIC Four targeted questions exercising different parts of the DRFA corpus.
# MAGIC For each: expect 5 chunks, plausible source PDFs matching the question,
# MAGIC and similarity scores roughly in the 0.3–0.9 range.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5a — Category framework question
# MAGIC
# MAGIC Expected: chunks tilted toward "Guideline 3 — Category C assessment
# MAGIC framework" and the core DRFA 2018 Determination.

# COMMAND ----------

display(spark.sql(f"""
    WITH result AS (
        SELECT {FQN}('What is a Category C disaster under DRFA?').chunks AS chunks
    )
    SELECT
        c.source_pdf,
        c.page_number,
        ROUND(c.similarity_score, 3) AS score,
        SUBSTRING(c.text, 1, 200)    AS text_preview
    FROM result
    LATERAL VIEW explode(chunks) t AS c
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5b — Evidence-requirements question
# MAGIC
# MAGIC Expected: chunks from Guideline 1 ("essential public asset") and the
# MAGIC evidentiary-requirements national guidance note.

# COMMAND ----------

display(spark.sql(f"""
    WITH result AS (
        SELECT {FQN}('What evidence is needed for EPA reconstruction claims?').chunks AS chunks
    )
    SELECT
        c.source_pdf,
        c.page_number,
        ROUND(c.similarity_score, 3) AS score,
        SUBSTRING(c.text, 1, 200)    AS text_preview
    FROM result
    LATERAL VIEW explode(chunks) t AS c
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5c — Labour costs question
# MAGIC
# MAGIC Expected: chunks from "DRFA national guidance note — Labour Costs.pdf".

# COMMAND ----------

display(spark.sql(f"""
    WITH result AS (
        SELECT {FQN}('Can I claim labour costs for clean-up work?').chunks AS chunks
    )
    SELECT
        c.source_pdf,
        c.page_number,
        ROUND(c.similarity_score, 3) AS score,
        SUBSTRING(c.text, 1, 200)    AS text_preview
    FROM result
    LATERAL VIEW explode(chunks) t AS c
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5d — Tom-shaped question (primary producer, flood)
# MAGIC
# MAGIC Free-form natural-language question that mimics what the agent would
# MAGIC actually send. Expected: a mix of primary-producer-related guidance,
# MAGIC clean-up rules, and infrastructure provisions.

# COMMAND ----------

display(spark.sql(f"""
    WITH result AS (
        SELECT {FQN}('primary producer dairy farmer fence repair flood assistance eligibility').chunks AS chunks
    )
    SELECT
        c.source_pdf,
        c.page_number,
        ROUND(c.similarity_score, 3) AS score,
        SUBSTRING(c.text, 1, 200)    AS text_preview
    FROM result
    LATERAL VIEW explode(chunks) t AS c
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5e — Full STRUCT output (so we can see chunks_count, error fields)

# COMMAND ----------

display(spark.sql(f"""
    SELECT {FQN}('What is a Category C disaster under DRFA?').*
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
# MAGIC | `silver.query_nema_guidelines` SQL UDF | The agent's "legal brain" — semantic search over NEMA DRFA documents |
# MAGIC | Citation contract in tool description | Forces every DRFA claim to be traceable to a specific PDF and chunk |
# MAGIC | Pure SQL using native `vector_search()` | No Python sandbox issues; matches the data-tools pattern |
# MAGIC
# MAGIC ### What's next (Phase 4 step 3b update + 3c)
# MAGIC
# MAGIC 1. **Update `notebooks/07_minimal_agent.py`:**
# MAGIC    - Add `silver.query_nema_guidelines` to `TOOL_FUNCTIONS` (now 5 tools)
# MAGIC    - Extend system prompt with tool #5 + the citation rule
# MAGIC    - Add a new smoke test (7g) that exercises both `verify_abn` and
# MAGIC      `query_nema_guidelines` — Tom-the-farmer asks about disaster-recovery
# MAGIC      grants after a flood, agent grounds its answer in DRFA chunks with
# MAGIC      citations.
# MAGIC
# MAGIC 2. **Phase 4 Step 3c** — `generate_grant_pdf` — Python UDF that takes the
# MAGIC    agent's JSON plus the retrieved DRFA chunks and renders a pre-filled
# MAGIC    grant application PDF via Jinja2. This is the spec's "Magic Moment".
# MAGIC
# MAGIC ### Troubleshooting note
# MAGIC
# MAGIC If the `vector_search()` call errors with **"column not found: search_score"**,
# MAGIC the column name in your DBR version is one of `__search_score__` or
# MAGIC `__db_score__`. Swap the `CAST(search_score AS DOUBLE)` reference in
# MAGIC section 4 accordingly and re-register.