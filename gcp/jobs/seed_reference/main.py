"""Cloud Run Job (run once): seed all reference data into BigQuery.

Consolidates three one-time Databricks notebooks:
  ingest_abs_poa.py        → POA shapefile → H3 polyfill → eco_silver.poa_h3_lookup
                              + eco_silver.poa_centroids (postcode centroids)
  ingest_csiro_stations.py → CSIRO CCiA CSVs → eco_silver.csiro_projections
                              + eco_silver.poa_to_csiro_station
  ingest_abs_industry.py   → ABS AUSTRALIAN_INDUSTRY API → eco_silver.industry_context

Inputs live in GCS (replaces the Unity Catalog Volume):
  gs://<bucket>/reference/poa/POA_2021_AUST_GDA2020_SHP.zip
  gs://<bucket>/reference/csiro/*.csv
(ABS industry data comes straight from the public ABS Data API.)

Environment:
  LANDING_BUCKET  — GCS bucket name (default: eco-resilience-landing)
  STEPS           — comma list of steps to run: poa,csiro,abs (default: all)
"""

import io
import logging
import os
import sys
import zipfile
import tempfile

import h3
import pandas as pd
import requests
from google.cloud import bigquery, storage

sys.path.insert(0, "/srv")
from agent import config  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("seed_reference")

BUCKET = os.environ.get("LANDING_BUCKET", "eco-resilience-landing")
STEPS = [s.strip() for s in os.environ.get("STEPS", "poa,csiro,abs").split(",")]

bq = bigquery.Client(project=config.PROJECT_ID, location=config.BQ_LOCATION)
gcs = storage.Client(project=config.PROJECT_ID)


def load_df(df: pd.DataFrame, table: str, cluster_by: str | None = None):
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    if cluster_by:
        job_config.clustering_fields = [cluster_by]
    bq.load_table_from_dataframe(df, table, job_config=job_config).result()
    log.info(f"Loaded {len(df):,} rows → {table}")


# ───────────────────────────────────────────────────────────────────────────
# Step 1: ABS POA boundaries → H3 lookup + centroids
# ───────────────────────────────────────────────────────────────────────────
def seed_poa():
    import geopandas as gpd
    from shapely.geometry import mapping

    blob_path = "reference/poa/POA_2021_AUST_GDA2020_SHP.zip"
    log.info(f"Downloading gs://{BUCKET}/{blob_path}")
    with tempfile.TemporaryDirectory() as tmp:
        zip_path = os.path.join(tmp, "poa.zip")
        gcs.bucket(BUCKET).blob(blob_path).download_to_filename(zip_path)
        with zipfile.ZipFile(zip_path) as z:
            z.extractall(tmp)
        shp = next(f for f in os.listdir(tmp) if f.endswith(".shp"))
        gdf = gpd.read_file(os.path.join(tmp, shp))

    # NSW postcodes: 2000–2999 (same filter as the Databricks notebook)
    gdf["poa_code"] = gdf["POA_CODE21"].astype(str)
    nsw = gdf[gdf["poa_code"].str.match(r"^2\d{3}$")].copy()
    nsw = nsw[nsw.geometry.notna()]
    log.info(f"{len(nsw)} NSW postcodes to polyfill at H3 res {config.H3_RESOLUTION}")

    # h3_polyfillash3 (Databricks SQL) → h3.polygon_to_cells (h3 v4 Python)
    lookup_rows, centroid_rows = [], []
    for _, row in nsw.iterrows():
        geo = mapping(row.geometry)  # GeoJSON dict (Polygon or MultiPolygon)
        try:
            cells = h3.geo_to_cells(geo, config.H3_RESOLUTION)
        except Exception as e:
            log.warning(f"  polyfill failed for {row.poa_code}: {e}")
            continue
        lookup_rows.extend({"poa_code": row.poa_code, "h3_cell": c} for c in cells)
        c = row.geometry.centroid
        centroid_rows.append({"poa_code": row.poa_code, "poa_lat": c.y, "poa_lon": c.x})

    load_df(pd.DataFrame(lookup_rows), config.bq_table(config.SILVER, "poa_h3_lookup"),
            cluster_by="poa_code")
    load_df(pd.DataFrame(centroid_rows), config.bq_table(config.SILVER, "poa_centroids"))


# ───────────────────────────────────────────────────────────────────────────
# Step 2: CSIRO climate projections + nearest-station lookup
# ───────────────────────────────────────────────────────────────────────────
CSIRO_FILES = [
    # (filename, variable, aggregation, unit)
    ("tas_aus-station_r1i1p1_CSIRO-MnCh-wrt-1986-2005-Scl_v1_mon_seasavg-clim_1.csv", "tas", "mean", "celsius"),
    ("tasmax_aus-station_r1i1p1_CSIRO-MnCh-wrt-1986-2005-Scl_v1_mon_seasavg-clim.csv", "tasmax", "mean", "celsius"),
    ("tasmin_aus-station_r1i1p1_CSIRO-MnCh-wrt-1986-2005-Scl_v1_mon_seasavg-clim.csv", "tasmin", "mean", "celsius"),
    ("hurs9_aus-station_r1i1p1_CSIRO-MnCh-wrt-1986-2005-Scl_v1_mon_seasavg-clim.csv", "hurs9", "mean", "percent"),
    ("hurs15_aus-station_r1i1p1_CSIRO-MnCh-wrt-1986-2005-Scl_v1_mon_seasavg-clim.csv", "hurs15", "mean", "percent"),
    ("pan-evap_aus-station_r1i1p1_CSIRO-MnCh-wrt-1986-2005-Scl_v1_mon_seassum-clim.csv", "pan_evap", "sum", "mm"),
]


def seed_csiro():
    frames = []
    for filename, variable, agg, unit in CSIRO_FILES:
        blob = gcs.bucket(BUCKET).blob(f"reference/csiro/{filename}")
        if not blob.exists():
            log.warning(f"  missing gs://{BUCKET}/reference/csiro/{filename} — skipping")
            continue
        raw = pd.read_csv(io.BytesIO(blob.download_as_bytes()))
        # CCiA station CSVs are wide: STATION_NAME, LAT, LON, then one column
        # per (model, rcp, period) combination. Melt to long form.
        id_cols = [c for c in raw.columns if c.upper() in
                   ("STATION_NAME", "STATION", "NAME", "LAT", "LATITUDE", "LON", "LONGITUDE", "TIME", "SEASON")]
        value_cols = [c for c in raw.columns if c not in id_cols]
        long = raw.melt(id_vars=id_cols, value_vars=value_cols,
                        var_name="series", value_name="value")
        # Column headers look like  <model>_<rcp>_<period>  e.g. ACCESS1-0_rcp45_2020-2039
        parts = long["series"].str.extract(r"^(?P<model>.+?)_(?P<rcp>rcp\d{2})_(?P<period>\d{4}-\d{4})$")
        long = pd.concat([long, parts], axis=1).dropna(subset=["rcp", "period"])
        station_col = next(c for c in id_cols if "STATION" in c.upper() or c.upper() == "NAME")
        time_col = next((c for c in id_cols if c.upper() in ("TIME", "SEASON")), None)
        frames.append(pd.DataFrame({
            "station_name": long[station_col].astype(str),
            "latitude": pd.to_numeric(long[[c for c in id_cols if c.upper().startswith("LAT")][0]], errors="coerce"),
            "longitude": pd.to_numeric(long[[c for c in id_cols if c.upper().startswith("LON")][0]], errors="coerce"),
            "variable": variable,
            "time_aggregation": long[time_col].astype(str) if time_col else "Annual",
            "unit": unit,
            "model": long["model"],
            "rcp": long["rcp"],
            "period": long["period"],
            "value": pd.to_numeric(long["value"], errors="coerce"),
        }))
    if not frames:
        log.error("No CSIRO files found — upload them to GCS first")
        return
    df = pd.concat(frames, ignore_index=True).dropna(subset=["value"])
    load_df(df, config.bq_table(config.SILVER, "csiro_projections"), cluster_by="station_name")

    # Nearest CSIRO station per postcode (haversine over poa_centroids)
    bq.query(f"""
        CREATE OR REPLACE TABLE `{config.bq_table(config.SILVER, 'poa_to_csiro_station')}`
        CLUSTER BY poa_code AS
        WITH stations AS (
            SELECT DISTINCT station_name, latitude AS st_lat, longitude AS st_lon
            FROM `{config.bq_table(config.SILVER, 'csiro_projections')}`
            WHERE latitude IS NOT NULL
        ),
        distances AS (
            SELECT pc.poa_code, s.station_name,
                   2 * 6371 * ASIN(SQRT(
                       POW(SIN((s.st_lat - pc.poa_lat) * ACOS(-1) / 360), 2) +
                       COS(pc.poa_lat * ACOS(-1) / 180) * COS(s.st_lat * ACOS(-1) / 180) *
                       POW(SIN((s.st_lon - pc.poa_lon) * ACOS(-1) / 360), 2)
                   )) AS distance_km
            FROM `{config.bq_table(config.SILVER, 'poa_centroids')}` pc
            CROSS JOIN stations s
        )
        SELECT poa_code, station_name AS nearest_csiro_station,
               ROUND(distance_km, 1) AS distance_km
        FROM distances
        QUALIFY ROW_NUMBER() OVER (PARTITION BY poa_code ORDER BY distance_km) = 1
    """).result()
    log.info("Rebuilt poa_to_csiro_station")


# ───────────────────────────────────────────────────────────────────────────
# Step 3: ABS AUSTRALIAN_INDUSTRY dataflow → industry_context
# ───────────────────────────────────────────────────────────────────────────
ABS_DATA_URL = "https://data.api.abs.gov.au/rest/data/ABS,AUSTRALIAN_INDUSTRY,1.1.0/all"
ABS_CODELIST_URL = "https://data.api.abs.gov.au/rest/codelist/ABS/CL_ANZSIC_2006_SUBDIVISION/1.0.0"

# ABS measure code → our column name (money in $AUD millions, employment in '000)
MEASURES = {
    "EMP": "num_employees_thousand",
    "INCOME": "total_income_aud_m",
    "IVA": "industry_value_added_aud_m",
    "OPBT": "operating_profit_aud_m",
    "EBITDA": "ebitda_aud_m",
    "WAGES": "wages_salaries_aud_m",
}


def seed_abs():
    log.info("Fetching ABS AUSTRALIAN_INDUSTRY (CSV) …")
    resp = requests.get(
        ABS_DATA_URL,
        headers={"Accept": "application/vnd.sdmx.data+csv", "User-Agent": "eco-resilience-seed/1.0"},
        timeout=120,
    )
    resp.raise_for_status()
    raw = pd.read_csv(io.BytesIO(resp.content), dtype=str)
    log.info(f"  {len(raw):,} SDMX observations, columns: {list(raw.columns)[:10]}")

    # SDMX-CSV columns of interest: MEASURE, INDUSTRY (2-digit subdivision),
    # TIME_PERIOD, OBS_VALUE. Keep the latest reference year.
    cols = {c.upper(): c for c in raw.columns}
    measure_c, industry_c = cols["MEASURE"], cols["INDUSTRY"]
    time_c, value_c = cols["TIME_PERIOD"], cols["OBS_VALUE"]

    df = raw[raw[measure_c].isin(MEASURES)].copy()
    df = df[df[industry_c].str.fullmatch(r"\d{2}")]  # 2-digit subdivisions only
    latest = df[time_c].max()
    df = df[df[time_c] == latest]
    df["value"] = pd.to_numeric(df[value_c], errors="coerce")

    wide = (df.pivot_table(index=industry_c, columns=measure_c, values="value", aggfunc="first")
              .rename(columns=MEASURES).reset_index()
              .rename(columns={industry_c: "anzsic_code"}))
    wide["reference_year"] = latest

    # Attach subdivision names from the ABS codelist
    try:
        cl = requests.get(ABS_CODELIST_URL, headers={"Accept": "application/json"}, timeout=60).json()
        codes = cl["data"]["codelists"][0]["codes"]
        names = {c["id"]: c["name"] for c in codes}
        wide["anzsic_name"] = wide["anzsic_code"].map(names)
    except Exception as e:
        log.warning(f"  codelist fetch failed ({e}) — anzsic_name left null")
        wide["anzsic_name"] = None

    for col in MEASURES.values():
        if col not in wide.columns:
            wide[col] = None

    out = wide[["anzsic_code", "anzsic_name", "reference_year"] + list(MEASURES.values())]
    load_df(out, config.bq_table(config.SILVER, "industry_context"))


def main() -> int:
    if "poa" in STEPS:
        seed_poa()
    if "csiro" in STEPS:
        seed_csiro()
    if "abs" in STEPS:
        seed_abs()
    log.info("Seed complete.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
