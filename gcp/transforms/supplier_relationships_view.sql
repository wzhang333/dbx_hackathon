-- gold.supplier_relationships — current supplier list derived from the
-- append-only history table (event-sourcing pattern kept from Databricks).
-- Created automatically by infra/01_bigquery_schema.py; kept here as the
-- canonical SQL reference.
CREATE OR REPLACE VIEW `eco_gold.supplier_relationships` AS
WITH latest AS (
  SELECT *,
         ROW_NUMBER() OVER (PARTITION BY user_abn, supplier_abn
                            ORDER BY added_at DESC) AS rn
  FROM `eco_gold.supplier_relationships_history`
)
SELECT user_abn, supplier_abn, supplier_name, supplier_status,
       supplier_entity_type, supplier_state, supplier_postcode, added_at
FROM latest
WHERE rn = 1 AND action = 'ADD';
