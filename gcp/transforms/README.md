# Transforms

The Databricks DLT pipeline (`src/dlt/dlt_ingestion_pipeline.py`) declared the
bronze→silver→gold materialisations declaratively. On GCP those transforms now
live in three places:

| Transform | Where it runs now |
|---|---|
| bronze → silver "latest snapshot" (weather, hazards) | `CREATE OR REPLACE TABLE` at the end of each ingestion job (`jobs/ingest_*/main.py`) |
| ABN silver → gold.business_details | `MERGE` inside `app/abn_ingest.py` (runs on demand per lookup) |
| gold.supplier_relationships | BigQuery **view** over the history table (`supplier_relationships_view.sql`) |
| postcode → nearest station lookups | SQL inside `ingest_weather` / `seed_reference` jobs |

If the transform layer grows, graduate to **dbt on BigQuery** (models as .sql
files, dependency ordering, tests) or **BigQuery Scheduled Queries** for
simple cron'd SQL. See docs/LEARNING_GUIDE.md § dbt.
