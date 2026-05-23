# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# 1. Define Parameters
dbutils.widgets.text("abn", "80619661988")
dbutils.widgets.text("mode", "full")  # "full" | "ingest_details" | "risk_only"
input_abn = dbutils.widgets.get("abn")
mode = dbutils.widgets.get("mode")

print(f"Processing ABN: {input_abn} | Mode: {mode}")

# ============================================================
# STEP A: SILVER LAYER - Parse Raw XML from Bronze Volume
# (Skipped in 'risk_only' mode)
# ============================================================
if mode in ("full", "ingest_details"):
    bronze_path = f"/Volumes/eco_resilience/bronze/raw_abn_data/abn_raw_{input_abn}.xml"

    try:
        df_raw = (spark.read
                  .format("xml")
                  .option("rowTag", "businessEntity202001")
                  .load(bronze_path))
    except Exception as e:
        print(f"Warning: Could not read bronze file: {e}")
        print("Skipping silver update - ABN may not have been ingested yet.")
        dbutils.notebook.exit(f"No bronze data for ABN {input_abn}")

    # Detect schema variations and handle array vs struct types
    from pyspark.sql.types import ArrayType
    schema_fields = {f.name: f.dataType for f in df_raw.schema.fields}

    # mainName is always a struct (preferred - it's the registered name)
    if "mainName" in schema_fields:
        main_name_expr = F.col("mainName.organisationName")
    else:
        main_name_expr = F.lit(None).cast("string")

    # businessName can be an array of structs (multiple trading names) or a single struct
    if "businessName" in schema_fields:
        if isinstance(schema_fields["businessName"], ArrayType):
            business_name_expr = F.col("businessName")[0]["organisationName"]
        else:
            business_name_expr = F.col("businessName.organisationName")
    else:
        business_name_expr = F.lit(None).cast("string")

    # mainTradingName as another fallback
    if "mainTradingName" in schema_fields:
        trading_name_expr = F.col("mainTradingName.organisationName")
    else:
        trading_name_expr = F.lit(None).cast("string")

    # legalName for individuals/sole traders
    if "legalName" in schema_fields:
        legal_name_expr = F.concat_ws(" ", F.col("legalName.givenName"), F.col("legalName.familyName"))
    else:
        legal_name_expr = F.lit(None).cast("string")

    # Extract fields for silver table
    df_silver = df_raw.select(
        F.col("ABN.identifierValue").cast("long").alias("abn"),
        F.col("entityStatus.entityStatusCode").alias("status"),
        F.col("entityType.entityDescription").alias("entity_type"),
        F.col("mainBusinessPhysicalAddress.stateCode").alias("state"),
        F.col("mainBusinessPhysicalAddress.postcode").cast("long").alias("postcode"),
        F.coalesce(main_name_expr, business_name_expr, trading_name_expr, legal_name_expr).alias("organisation_name"),
        F.current_timestamp().alias("ingested_at")
    )

    # MERGE into silver.abn_lookup_structured
    df_silver.createOrReplaceTempView("new_abn_data")

    spark.sql("""
        MERGE INTO eco_resilience.silver.abn_lookup_structured AS target
        USING new_abn_data AS source
        ON target.abn = source.abn
        WHEN MATCHED THEN UPDATE SET
            target.status = source.status,
            target.entity_type = source.entity_type,
            target.state = source.state,
            target.postcode = source.postcode,
            target.organisation_name = source.organisation_name,
            target.ingested_at = source.ingested_at
        WHEN NOT MATCHED THEN INSERT (abn, status, entity_type, state, postcode, organisation_name, ingested_at)
            VALUES (source.abn, source.status, source.entity_type, source.state, source.postcode, source.organisation_name, source.ingested_at)
    """)

    print(f"Silver table updated for ABN: {input_abn}")

    # ============================================================
    # STEP B: GOLD LAYER - Business Details (spatial join for location_name)
    # ============================================================
    df_poa_to_station = spark.read.table("eco_resilience.silver.poa_to_weather_location")

    df_business_details = df_silver.join(
        df_poa_to_station,
        df_silver.postcode == df_poa_to_station.poa_code,
        "left"
    ).select(
        df_silver.abn,
        F.col("entity_type"),
        F.col("postcode"),
        F.col("nearest_weather_location").alias("location_name"),
        F.current_timestamp().alias("ingested_at")
    )

    # Append to gold.business_details_history
    df_business_details.write.format("delta").mode("append").saveAsTable(
        "eco_resilience.gold.business_details_history"
    )

    print(f"Business details written to gold.business_details_history for ABN: {input_abn}")

# ============================================================
# STEP C: RISK CALCULATION (only in 'full' or 'risk_only' mode)
# ============================================================
if mode in ("full", "risk_only"):
    if mode == "risk_only":
        # Read ABN info from silver table
        df_silver = spark.sql(f"""
            SELECT abn, entity_type, state, postcode, organisation_name, ingested_at
            FROM eco_resilience.silver.abn_lookup_structured
            WHERE abn = {input_abn}
        """)
        if df_silver.count() == 0:
            print(f"No silver data found for ABN {input_abn}. Cannot calculate risk.")
            dbutils.notebook.exit(f"No silver data for ABN {input_abn}")
        
        df_poa_to_station = spark.read.table("eco_resilience.silver.poa_to_weather_location")

    df_weather = spark.read.table("eco_resilience.silver.weather_current")

    # Join: silver → poa_to_station → weather
    df_risk = df_silver.join(
        df_poa_to_station,
        df_silver.postcode == df_poa_to_station.poa_code
    ).join(
        df_weather,
        df_poa_to_station.nearest_weather_location == df_weather.location_name
    ).select(
        df_silver.abn,
        F.col("precipitation_mm"),
        F.col("windspeed_kmh"),
        F.when((F.col("precipitation_mm") > 50), "High").otherwise("Low").alias("risk_level"),
        F.current_timestamp().alias("calculation_time")
    )

    # Append to history (now without entity_type, postcode, location_name)
    df_risk.write.format("delta").mode("append").saveAsTable(
        "eco_resilience.silver.business_risk_scores_history"
    )

    print(f"Risk scores appended for ABN: {input_abn} (mode={mode})")

if mode == "ingest_details":
    print(f"Ingest-only mode complete. No risk calculation performed.")

dbutils.notebook.exit(f"Success: ABN={input_abn}, mode={mode}")