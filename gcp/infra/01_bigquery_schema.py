"""Phase 1 — create all BigQuery tables (replaces Unity Catalog DDL / DLT table
declarations). Run once after 00_bootstrap.sh:

    GOOGLE_CLOUD_PROJECT=<your-project> python infra/01_bigquery_schema.py

Tables whose ingestion jobs use CREATE OR REPLACE (weather/hazards silver,
lookups, drfa_chunks) don't need pre-creation — this script creates the
append-only bronze tables and the MERGE targets that must exist up front.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from google.cloud import bigquery
from agent import config

client = bigquery.Client(project=config.PROJECT_ID, location=config.BQ_LOCATION)

F = bigquery.SchemaField

TABLES = {
    # bronze: append-only ingestion history (partitioned on ingest day —
    # the BigQuery idiom that replaces Delta time travel + Z-ordering)
    config.bq_table(config.BRONZE, "open_meteo_forecast"): dict(
        schema=[
            F("location_name", "STRING"), F("latitude", "FLOAT64"), F("longitude", "FLOAT64"),
            F("forecast_time", "TIMESTAMP"), F("precipitation_mm", "FLOAT64"),
            F("windspeed_kmh", "FLOAT64"), F("temperature_c", "FLOAT64"),
            F("humidity_pct", "FLOAT64"), F("weather_code", "INT64"),
            F("h3_cell", "STRING"), F("_source", "STRING"), F("_ingest_time", "TIMESTAMP"),
        ],
        partition_field="_ingest_time", cluster=["location_name"],
    ),
    config.bq_table(config.BRONZE, "tfnsw_hazards"): dict(
        schema=[
            F("hazard_type", "STRING"), F("hazard_id", "STRING"), F("main_category", "STRING"),
            F("display_name", "STRING"), F("headline", "STRING"),
            F("expected_delay_min", "FLOAT64"), F("impacting_network", "BOOL"),
            F("is_major", "BOOL"), F("ended", "BOOL"),
            F("advice_a", "STRING"), F("advice_b", "STRING"), F("advice_c", "STRING"),
            F("other_advice", "STRING"), F("created_ts", "TIMESTAMP"),
            F("last_updated_ts", "TIMESTAMP"), F("longitude", "FLOAT64"), F("latitude", "FLOAT64"),
            F("h3_cell", "STRING"), F("encoded_polylines_json", "STRING"),
            F("roads_json", "STRING"), F("_source", "STRING"), F("_ingest_time", "TIMESTAMP"),
        ],
        partition_field="_ingest_time", cluster=["hazard_type"],
    ),
    # silver: MERGE target for on-demand ABN ingestion
    config.bq_table(config.SILVER, "abn_lookup_structured"): dict(
        schema=[
            F("abn", "INT64"), F("status", "STRING"), F("entity_type", "STRING"),
            F("state", "STRING"), F("postcode", "INT64"),
            F("organisation_name", "STRING"), F("ingested_at", "TIMESTAMP"),
        ],
        cluster=["abn"],
    ),
    # gold: MERGE target + supplier history (streaming-insert target)
    config.bq_table(config.GOLD, "business_details"): dict(
        schema=[
            F("abn", "INT64"), F("entity_type", "STRING"), F("postcode", "INT64"),
            F("location_name", "STRING"), F("ingested_at", "TIMESTAMP"),
        ],
        cluster=["abn"],
    ),
    config.bq_table(config.GOLD, "supplier_relationships_history"): dict(
        schema=[
            F("user_abn", "INT64"), F("supplier_abn", "INT64"), F("supplier_name", "STRING"),
            F("supplier_status", "STRING"), F("supplier_entity_type", "STRING"),
            F("supplier_state", "STRING"), F("supplier_postcode", "INT64"),
            F("added_at", "TIMESTAMP"), F("action", "STRING"),
        ],
        cluster=["user_abn"],
    ),
}


def main():
    for table_id, spec in TABLES.items():
        table = bigquery.Table(table_id, schema=spec["schema"])
        if spec.get("partition_field"):
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY, field=spec["partition_field"])
        if spec.get("cluster"):
            table.clustering_fields = spec["cluster"]
        try:
            client.create_table(table)
            print(f"✅ created {table_id}")
        except Exception as e:
            if "Already Exists" in str(e):
                print(f"ℹ️  exists  {table_id}")
            else:
                raise

    # supplier_relationships view: latest ADD per (user, supplier) not
    # superseded by a REMOVE — replaces the Databricks gold view
    view_id = config.bq_table(config.GOLD, "supplier_relationships")
    view_sql = f"""
        WITH latest AS (
          SELECT *,
                 ROW_NUMBER() OVER (PARTITION BY user_abn, supplier_abn
                                    ORDER BY added_at DESC) AS rn
          FROM `{config.bq_table(config.GOLD, 'supplier_relationships_history')}`
        )
        SELECT user_abn, supplier_abn, supplier_name, supplier_status,
               supplier_entity_type, supplier_state, supplier_postcode, added_at
        FROM latest
        WHERE rn = 1 AND action = 'ADD'
    """
    view = bigquery.Table(view_id)
    view.view_query = view_sql
    try:
        client.create_table(view)
        print(f"✅ created view {view_id}")
    except Exception as e:
        if "Already Exists" in str(e):
            print(f"ℹ️  view exists {view_id}")
        else:
            raise

    print("\nDone. Silver lookup/current tables are created by their ingestion jobs.")


if __name__ == "__main__":
    main()
