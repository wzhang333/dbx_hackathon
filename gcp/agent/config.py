"""Central configuration for the GCP-native EcoResilience agent.

Everything is driven by environment variables so the same code runs
locally (with `gcloud auth application-default login`), inside Cloud Run,
and inside Cloud Run Jobs without edits.
"""

import os

PROJECT_ID = os.environ.get("GOOGLE_CLOUD_PROJECT", "eco-resilience-ai")
BQ_LOCATION = os.environ.get("BQ_LOCATION", "australia-southeast1")

# BigQuery datasets replacing Unity Catalog schemas
# (BigQuery has no catalog.schema.table 3-level namespace; datasets play
#  the role of schemas: eco_resilience.bronze.* -> eco_bronze.*)
BRONZE = os.environ.get("BQ_DATASET_BRONZE", "eco_bronze")
SILVER = os.environ.get("BQ_DATASET_SILVER", "eco_silver")
GOLD = os.environ.get("BQ_DATASET_GOLD", "eco_gold")

def bq_table(dataset: str, table: str) -> str:
    return f"{PROJECT_ID}.{dataset}.{table}"

# ── LLM (replaces ChatDatabricks / databricks-claude-sonnet-4) ──
# Two supported providers on Vertex AI:
#   gemini : gemini-2.5-pro, available in australia-southeast1 (default)
#   claude : claude-sonnet-4-5@20250929 via Vertex AI Model Garden (us-east5)
LLM_PROVIDER = os.environ.get("LLM_PROVIDER", "gemini")
GEMINI_MODEL = os.environ.get("GEMINI_MODEL", "gemini-2.5-pro")
CLAUDE_MODEL = os.environ.get("CLAUDE_MODEL", "claude-sonnet-4-5@20250929")
CLAUDE_LOCATION = os.environ.get("CLAUDE_LOCATION", "us-east5")
VERTEX_LOCATION = os.environ.get("VERTEX_LOCATION", "australia-southeast1")

# ── Embeddings (replaces databricks-gte-large-en) ──
EMBEDDING_MODEL = os.environ.get("EMBEDDING_MODEL", "text-embedding-005")
EMBEDDING_DIM = 768

# ── Secrets (replaces Databricks secret scope `eco_resilience`) ──
ABR_GUID_SECRET = os.environ.get("ABR_GUID_SECRET", "abr-auth-guid")
TFNSW_KEY_SECRET = os.environ.get("TFNSW_KEY_SECRET", "tfnsw-api-key")

H3_RESOLUTION = 8


def get_secret(secret_name: str) -> str:
    """Read latest secret version from Secret Manager.

    Falls back to an env var of the same name (upper-snake) so local dev
    can run with e.g. ABR_AUTH_GUID=... without touching Secret Manager.
    """
    env_key = secret_name.replace("-", "_").upper()
    if os.environ.get(env_key):
        return os.environ[env_key]
    from google.cloud import secretmanager

    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{PROJECT_ID}/secrets/{secret_name}/versions/latest"
    return client.access_secret_version(name=name).payload.data.decode("utf-8")
