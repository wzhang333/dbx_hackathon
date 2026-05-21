# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC  CREATE VOLUME IF NOT EXISTS eco_resilience.dlt.landing
# MAGIC    COMMENT 'JSON landing zone for SDP pipeline. Subdirs: weather/, hazards/, _schema/.';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 1. All 7 tables created?
# MAGIC SHOW TABLES IN eco_resilience.dlt;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- 2. Row counts — the medallion shape should look like this AFTER 1 fetch:
# MAGIC --     bronze_weather         ~2520    (15 locations × 168 hourly forecasts — accumulates over time)
# MAGIC --     bronze_hazards         ~50-500  (depends on active TfNSW activity — accumulates over time)
# MAGIC --     silver_weather_history = bronze_weather (passthrough)
# MAGIC --     silver_hazards_history = bronze_hazards (passthrough)
# MAGIC --     silver_weather_current = 15     (latest batch aggregated to per-location summary)
# MAGIC --     silver_hazards_current ≤ bronze_hazards (latest batch + active + h3-joinable)
# MAGIC --     gold_*_current         ~600    (one row per NSW postcode, current state)
# MAGIC --     gold_*_history         600 × (distinct snapshot_dates we have data for)
# MAGIC -- If silver_weather_current != 15 the @dp.expect_or_fail will halt the pipeline.
# MAGIC SELECT 'bronze_weather'         AS t, COUNT(*) AS n FROM eco_resilience.dlt.bronze_weather_dlt
# MAGIC UNION ALL SELECT 'bronze_hazards',         COUNT(*) FROM eco_resilience.dlt.bronze_hazards_dlt
# MAGIC UNION ALL SELECT 'silver_weather_history', COUNT(*) FROM eco_resilience.dlt.silver_weather_history_dlt
# MAGIC UNION ALL SELECT 'silver_hazards_history', COUNT(*) FROM eco_resilience.dlt.silver_hazards_history_dlt
# MAGIC UNION ALL SELECT 'silver_weather_current', COUNT(*) FROM eco_resilience.dlt.silver_weather_current_dlt
# MAGIC UNION ALL SELECT 'silver_hazards_current', COUNT(*) FROM eco_resilience.dlt.silver_hazards_current_dlt
# MAGIC UNION ALL SELECT 'gold_current',           COUNT(*) FROM eco_resilience.dlt.gold_nsw_postcode_resilience_current_dlt
# MAGIC UNION ALL SELECT 'gold_history',           COUNT(*) FROM eco_resilience.dlt.gold_nsw_postcode_resilience_history_dlt;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- 3. Bronze accumulates batches across runs (this is THE point of the refactor)
# MAGIC SELECT COUNT(DISTINCT _ingest_time) AS num_batches,
# MAGIC        COUNT(*)                     AS total_rows
# MAGIC FROM eco_resilience.dlt.bronze_weather_dlt;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- 5. Bathurst spot-check — current shape
# MAGIC SELECT * FROM eco_resilience.dlt.gold_nsw_postcode_resilience_current_dlt
# MAGIC WHERE postcode = '2795';
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- 6. Bathurst trend — the question that history unlocks
# MAGIC SELECT *
# MAGIC FROM eco_resilience.dlt.gold_nsw_postcode_resilience_history_dlt
# MAGIC WHERE postcode = '2795'
# MAGIC ORDER BY snapshot_date DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC
# MAGIC -- 7. Did the expectations fire? (Event log query)
# MAGIC SELECT timestamp, details:flow_progress.metrics, details
# MAGIC FROM event_log("88bf2110-0b7f-4991-8e2d-bf6da029ce84")
# MAGIC ORDER BY timestamp DESC LIMIT 50;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC
# MAGIC -- 8. Auto Loader file ingestion counter (proves the streaming-source story)
# MAGIC SELECT timestamp, details:flow_progress.metrics
# MAGIC FROM event_log("88bf2110-0b7f-4991-8e2d-bf6da029ce84")
# MAGIC WHERE event_type = 'flow_progress' AND details:flow_name LIKE 'bronze_%_flow'
# MAGIC ORDER BY timestamp DESC LIMIT 20;