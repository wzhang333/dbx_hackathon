"""On-demand ABN ingestion — GCP-native replacement for the Databricks job chain.

Databricks version: Flask called w.jobs.run_now(JOB_ID, {"abn": ...}) which ran
  1. abr_business_runtime.py  (ABR API → bronze XML in a Volume)
  2. ABN_Silver_Gold_Transformation.py  (parse → MERGE silver → write gold)
then the frontend polled /api/job-status until the run finished.

GCP version: the ABR lookup takes ~1 second, so the whole chain runs
synchronously inside the request. No job scheduler, no polling, no cluster
spin-up — this alone removes ~30–60s of latency per lookup.
"""

import json
import logging
from datetime import datetime, timezone

import requests
from google.cloud import bigquery

from agent import config

logger = logging.getLogger("ecoresilience")

ABR_JSON_URL = "https://abr.business.gov.au/json/AbnDetails.aspx"


def _parse_abr_body(body: str) -> dict:
    body = body.strip()
    if body.startswith("{"):
        return json.loads(body)
    first, last = body.find("{"), body.rfind("}")
    if first == -1 or last == -1:
        raise ValueError(f"No JSON in ABR response: {body[:120]!r}")
    return json.loads(body[first : last + 1])


def fetch_abr_record(abn: str) -> dict:
    """Live ABR lookup. Raises on network failure; returns {} if ABN unknown."""
    guid = config.get_secret(config.ABR_GUID_SECRET)
    r = requests.get(ABR_JSON_URL, params={"abn": abn, "guid": guid, "callback": ""}, timeout=15)
    r.raise_for_status()
    record = _parse_abr_body(r.text)
    return record if record.get("Abn") else {}


def ingest_abn(bq: bigquery.Client, abn: str) -> dict:
    """ABR API → MERGE eco_silver.abn_lookup_structured → MERGE eco_gold.business_details.

    Returns the parsed record, or {'error': ...} when the ABN isn't registered.
    """
    record = fetch_abr_record(abn)
    if not record:
        return {"error": f"ABN {abn} not found in the Australian Business Register"}

    silver_tbl = config.bq_table(config.SILVER, "abn_lookup_structured")
    gold_tbl = config.bq_table(config.GOLD, "business_details")
    poa_map = config.bq_table(config.SILVER, "poa_to_weather_location")

    row = {
        "abn": int(record["Abn"]),
        "status": record.get("AbnStatus"),
        "entity_type": record.get("EntityTypeName"),
        "state": record.get("AddressState"),
        "postcode": int(record["AddressPostcode"]) if str(record.get("AddressPostcode") or "").isdigit() else None,
        "organisation_name": record.get("EntityName"),
        "ingested_at": datetime.now(timezone.utc).isoformat(),
    }

    merge_silver = f"""
        MERGE `{silver_tbl}` t
        USING (SELECT @abn AS abn, @status AS status, @entity_type AS entity_type,
                      @state AS state, @postcode AS postcode,
                      @organisation_name AS organisation_name,
                      CURRENT_TIMESTAMP() AS ingested_at) s
        ON t.abn = s.abn
        WHEN MATCHED THEN UPDATE SET
            status = s.status, entity_type = s.entity_type, state = s.state,
            postcode = s.postcode, organisation_name = s.organisation_name,
            ingested_at = s.ingested_at
        WHEN NOT MATCHED THEN INSERT (abn, status, entity_type, state, postcode, organisation_name, ingested_at)
            VALUES (s.abn, s.status, s.entity_type, s.state, s.postcode, s.organisation_name, s.ingested_at)
    """
    params = [
        bigquery.ScalarQueryParameter("abn", "INT64", row["abn"]),
        bigquery.ScalarQueryParameter("status", "STRING", row["status"]),
        bigquery.ScalarQueryParameter("entity_type", "STRING", row["entity_type"]),
        bigquery.ScalarQueryParameter("state", "STRING", row["state"]),
        bigquery.ScalarQueryParameter("postcode", "INT64", row["postcode"]),
        bigquery.ScalarQueryParameter("organisation_name", "STRING", row["organisation_name"]),
    ]
    bq.query(merge_silver, job_config=bigquery.QueryJobConfig(query_parameters=params)).result()

    # Gold: attach nearest weather location (same spatial join the Databricks
    # transformation task did through poa_to_weather_location).
    merge_gold = f"""
        MERGE `{gold_tbl}` t
        USING (
            SELECT s.abn, s.entity_type, s.postcode,
                   m.nearest_weather_location AS location_name,
                   CURRENT_TIMESTAMP() AS ingested_at
            FROM `{silver_tbl}` s
            LEFT JOIN `{poa_map}` m ON CAST(s.postcode AS STRING) = m.poa_code
            WHERE s.abn = @abn
        ) s
        ON t.abn = s.abn
        WHEN MATCHED THEN UPDATE SET
            entity_type = s.entity_type, postcode = s.postcode,
            location_name = s.location_name, ingested_at = s.ingested_at
        WHEN NOT MATCHED THEN INSERT (abn, entity_type, postcode, location_name, ingested_at)
            VALUES (s.abn, s.entity_type, s.postcode, s.location_name, s.ingested_at)
    """
    bq.query(merge_gold, job_config=bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("abn", "INT64", row["abn"])]
    )).result()

    logger.info(f"[abn-ingest] {abn} → silver + gold updated ({row['organisation_name']})")
    return row
