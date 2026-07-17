"""Cloud Run Job (run once, re-run when PDFs change): DRFA RAG ingestion.

GCP port of eco-resilience-bundle/src/rag/ingest_drfa_rag.py:
  ai_parse_document (Databricks AI Function)  → PyMuPDF page-text extraction
  ai_prep_search chunking                     → fixed-size chunking w/ overlap
  databricks-gte-large-en embeddings          → Vertex AI text-embedding-005
  Vector Search Delta-Sync index + endpoint   → embeddings stored in BigQuery,
                                                queried with BQ VECTOR_SEARCH
                                                (serverless — no always-on
                                                 index endpoint to pay for)

Inputs:  gs://<LANDING_BUCKET>/reference/drfa_pdfs/*.pdf   (the 16 NEMA PDFs)
Output:  eco_bronze.drfa_chunks (chunk text + metadata + 768-dim embedding)

At ~1k chunks a brute-force VECTOR_SEARCH is instant; if the corpus grows
past ~1M rows, add a vector index:
  CREATE VECTOR INDEX ... ON drfa_chunks(embedding) OPTIONS(index_type='IVF')
"""

import logging
import os
import sys

import fitz  # PyMuPDF
from google.cloud import bigquery, storage

sys.path.insert(0, "/srv")
from agent import config  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("ingest_drfa_rag")

BUCKET = os.environ.get("LANDING_BUCKET", "eco-resilience-landing")
PDF_PREFIX = os.environ.get("PDF_PREFIX", "reference/drfa_pdfs/")
CHUNKS_TABLE = config.bq_table(config.BRONZE, "drfa_chunks")

CHUNK_SIZE = 1200      # characters per chunk (~300 tokens)
CHUNK_OVERLAP = 200
MIN_CHUNK_CHARS = 100  # skip page furniture / empty fragments
EMBED_BATCH = 25       # Vertex AI embedding API max batch size for 005


def chunk_pdf(pdf_bytes: bytes, source_pdf: str) -> list[dict]:
    """Extract per-page text and split into overlapping chunks.

    Page numbers are preserved because the agent's citation contract is
    "(DRFA Determination 2018.pdf, page 47)" — same as the Databricks build.
    """
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    chunks = []
    position = 0
    for page_num, page in enumerate(doc, start=1):
        text = " ".join(page.get_text().split())
        start = 0
        while start < len(text):
            piece = text[start : start + CHUNK_SIZE].strip()
            if len(piece) >= MIN_CHUNK_CHARS:
                chunks.append({
                    "chunk_id": f"{source_pdf}::p{page_num}::c{position}",
                    "chunk_position": position,
                    "chunk_to_retrieve": piece,
                    "source_pdf": source_pdf,
                    "page_number": page_num,
                })
                position += 1
            start += CHUNK_SIZE - CHUNK_OVERLAP
    return chunks


def embed_chunks(chunks: list[dict]) -> list[dict]:
    import vertexai
    from vertexai.language_models import TextEmbeddingInput, TextEmbeddingModel

    vertexai.init(project=config.PROJECT_ID, location=config.VERTEX_LOCATION)
    model = TextEmbeddingModel.from_pretrained(config.EMBEDDING_MODEL)
    for i in range(0, len(chunks), EMBED_BATCH):
        batch = chunks[i : i + EMBED_BATCH]
        inputs = [TextEmbeddingInput(c["chunk_to_retrieve"], task_type="RETRIEVAL_DOCUMENT")
                  for c in batch]
        embeddings = model.get_embeddings(inputs)
        for chunk, emb in zip(batch, embeddings):
            chunk["embedding"] = emb.values
        log.info(f"  embedded {min(i + EMBED_BATCH, len(chunks))}/{len(chunks)}")
    return chunks


def main() -> int:
    gcs = storage.Client(project=config.PROJECT_ID)
    bq = bigquery.Client(project=config.PROJECT_ID, location=config.BQ_LOCATION)

    blobs = [b for b in gcs.list_blobs(BUCKET, prefix=PDF_PREFIX)
             if b.name.lower().endswith(".pdf")]
    if not blobs:
        log.error(f"No PDFs under gs://{BUCKET}/{PDF_PREFIX} — upload the DRFA documents first")
        return 1
    log.info(f"Found {len(blobs)} PDFs")

    all_chunks = []
    for blob in blobs:
        name = os.path.basename(blob.name)
        chunks = chunk_pdf(blob.download_as_bytes(), name)
        all_chunks.extend(chunks)
        log.info(f"  {name}: {len(chunks)} chunks")

    log.info(f"Embedding {len(all_chunks)} chunks with {config.EMBEDDING_MODEL} …")
    all_chunks = embed_chunks(all_chunks)

    schema = [
        bigquery.SchemaField("chunk_id", "STRING"),
        bigquery.SchemaField("chunk_position", "INT64"),
        bigquery.SchemaField("chunk_to_retrieve", "STRING"),
        bigquery.SchemaField("source_pdf", "STRING"),
        bigquery.SchemaField("page_number", "INT64"),
        bigquery.SchemaField("embedding", "FLOAT64", mode="REPEATED"),
    ]
    job = bq.load_table_from_json(
        all_chunks, CHUNKS_TABLE,
        job_config=bigquery.LoadJobConfig(schema=schema, write_disposition="WRITE_TRUNCATE"),
    )
    job.result()
    log.info(f"Loaded {len(all_chunks):,} chunks → {CHUNKS_TABLE}")

    # Smoke test — mirrors the Databricks notebook's final vector_search probe
    probe = bq.query(f"""
        SELECT base.source_pdf, base.page_number,
               SUBSTR(base.chunk_to_retrieve, 1, 120) AS preview, distance
        FROM VECTOR_SEARCH(
          TABLE `{CHUNKS_TABLE}`, 'embedding',
          (SELECT embedding FROM `{CHUNKS_TABLE}` LIMIT 1),
          top_k => 3, distance_type => 'COSINE')
    """).result()
    for row in probe:
        log.info(f"  probe: {row.source_pdf} p{row.page_number} d={row.distance:.3f}")
    log.info("RAG corpus ready.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
