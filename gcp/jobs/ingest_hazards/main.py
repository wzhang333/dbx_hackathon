"""Cloud Run Job: TfNSW live hazard feeds → BigQuery bronze + silver.

GCP port of eco-resilience-bundle/src/ingestion/ingest_tfnsw_hazards.py:
  dbutils.secrets.get("eco_resilience", "tfnsw_api_key") → Secret Manager
  Delta append + Liquid Clustering                       → BQ load + CLUSTER BY
"""

import json
import logging
import sys
import time
from datetime import datetime, timezone

import h3
import requests
from google.cloud import bigquery

sys.path.insert(0, "/srv")
from agent import config  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("ingest_hazards")

API_BASE = "https://api.transport.nsw.gov.au/v1/live/hazards"
HAZARD_TYPES = ["incident", "flood", "fire", "roadwork"]

BRONZE_TABLE = config.bq_table(config.BRONZE, "tfnsw_hazards")
SILVER_TABLE = config.bq_table(config.SILVER, "hazards_current")


def fetch_hazards(hazard_type: str, key: str) -> list[dict]:
    resp = requests.get(
        f"{API_BASE}/{hazard_type}/open",
        headers={"Authorization": f"apikey {key}"},
        timeout=15,
    )
    resp.raise_for_status()
    features = resp.json().get("features", []) or []

    rows = []
    for feat in features:
        props = feat.get("properties", {}) or {}
        geom = feat.get("geometry") or {}
        coords = geom.get("coordinates") or [None, None]
        lon = coords[0] if isinstance(coords, list) else None
        lat = coords[1] if isinstance(coords, list) else None
        created_ms = props.get("created")
        updated_ms = props.get("lastUpdated")
        rows.append({
            "hazard_type": hazard_type,
            "hazard_id": str(feat.get("id")),
            "main_category": props.get("mainCategory"),
            "display_name": props.get("displayName"),
            "headline": props.get("headline") or None,
            "expected_delay_min": props.get("expectedDelay"),
            "impacting_network": props.get("impactingNetwork"),
            "is_major": props.get("isMajor"),
            "ended": props.get("ended"),
            "advice_a": props.get("adviceA"),
            "advice_b": props.get("adviceB"),
            "advice_c": props.get("adviceC"),
            "other_advice": props.get("otherAdvice"),
            "created_ts": datetime.fromtimestamp(created_ms / 1000, tz=timezone.utc).isoformat() if created_ms else None,
            "last_updated_ts": datetime.fromtimestamp(updated_ms / 1000, tz=timezone.utc).isoformat() if updated_ms else None,
            "longitude": lon,
            "latitude": lat,
            # NULL geometry (statewide announcements) → NULL h3_cell, kept for audit
            "h3_cell": h3.latlng_to_cell(lat, lon, config.H3_RESOLUTION)
                       if lat is not None and lon is not None else None,
            "encoded_polylines_json": json.dumps(props.get("encodedPolylines") or []),
            "roads_json": json.dumps(props.get("roads") or []),
        })
    return rows


def main() -> int:
    api_key = config.get_secret(config.TFNSW_KEY_SECRET)
    bq = bigquery.Client(project=config.PROJECT_ID, location=config.BQ_LOCATION)
    ingest_time = datetime.now(timezone.utc).isoformat()

    t0 = time.time()
    all_rows = []
    for ht in HAZARD_TYPES:
        rows = fetch_hazards(ht, api_key)
        all_rows.extend(rows)
        log.info(f"  /{ht}/open → {len(rows)} features")
    log.info(f"Fetched {len(all_rows):,} features in {time.time() - t0:.1f}s")

    if not all_rows:
        log.warning("No hazards returned by any endpoint — skipping write.")
        return 0

    for r in all_rows:
        r["_source"] = "tfnsw"
        r["_ingest_time"] = ingest_time

    job = bq.load_table_from_json(
        all_rows, BRONZE_TABLE,
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_APPEND"),
    )
    job.result()
    log.info(f"Appended {len(all_rows)} rows to {BRONZE_TABLE}")

    bq.query(f"""
        CREATE OR REPLACE TABLE `{SILVER_TABLE}`
        CLUSTER BY h3_cell AS
        SELECT * FROM `{BRONZE_TABLE}`
        WHERE _ingest_time = (SELECT MAX(_ingest_time) FROM `{BRONZE_TABLE}`)
    """).result()
    log.info(f"Rebuilt {SILVER_TABLE}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
