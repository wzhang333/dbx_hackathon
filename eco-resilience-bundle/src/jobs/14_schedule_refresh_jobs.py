# Databricks notebook source
# MAGIC %md
# MAGIC # 14 — Schedule Recurring Refresh Jobs (Weather + Hazards)
# MAGIC
# MAGIC Creates (or updates) two Databricks Workflow Jobs that refresh the live
# MAGIC data tables consumed by the agent and the Lakehouse App.
# MAGIC
# MAGIC | Job              | Notebook                                  | Cron                | Refreshes                  |
# MAGIC |------------------|-------------------------------------------|---------------------|----------------------------|
# MAGIC | `refresh_weather`| `notebooks/02_ingest_open_meteo`          | Every 6 hours       | `silver.weather_current`   |
# MAGIC | `refresh_hazards`| `notebooks/03_ingest_tfnsw_hazards`       | Every 3 hours       | `silver.hazards_current`   |
# MAGIC
# MAGIC **Run this notebook ONCE to create the jobs.** Re-running updates the existing
# MAGIC jobs by name (no duplicates) — useful when you want to tweak cadence later:
# MAGIC just edit the constants in the Configuration cell and re-run.
# MAGIC
# MAGIC **What's NOT scheduled (intentional)**
# MAGIC - `00_ingest_drfa_rag` — DRFA PDFs static, re-ingest only on new release
# MAGIC - `01_ingest_abs_poa` — ABS POA boundaries static
# MAGIC - `04_ingest_csiro_stations` — CSIRO projections updated monthly at most
# MAGIC - `10_ingest_abs_industry` — ABS industry stats are quarterly
# MAGIC
# MAGIC **Compute:** Serverless (matches the rest of the project).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration — edit here to change cadence, paths, or notifications

# COMMAND ----------

NOTIFICATION_EMAIL = "wzhang3@cartology.com.au"
TIMEZONE_ID        = "Australia/Sydney"

# Workspace paths to the existing ingestion notebooks. Adjust if your workspace
# has them in a different location. Tip: open the notebook in Databricks → click
# the breadcrumb at the top → copy the full path (drop the `.py` suffix).
WEATHER_NB_PATH    = "/Workspace/Shared/eco_resilience/Ingestion/02_ingest_open_meteo"
HAZARDS_NB_PATH    = "/Workspace/Shared/eco_resilience/Ingestion/03_ingest_tfnsw_hazards"

# Quartz cron expressions — Databricks uses 7-field Quartz format:
#   sec  min  hour  dayOfMonth  month  dayOfWeek  year(optional)
# Examples:
#   "0 0 0/6 * * ?"  = every 6 hours starting at midnight (00:00, 06:00, 12:00, 18:00)
#   "0 0 0/3 * * ?"  = every 3 hours starting at midnight (00:00, 03:00, ..., 21:00)
#   "0 */15 * * * ?" = every 15 minutes
WEATHER_CRON       = "0 0 0/6 * * ?"
HAZARDS_CRON       = "0 0 0/3 * * ?"

print(f"NOTIFICATION_EMAIL = {NOTIFICATION_EMAIL}")
print(f"TIMEZONE_ID        = {TIMEZONE_ID}")
print(f"WEATHER_NB_PATH    = {WEATHER_NB_PATH}  cron={WEATHER_CRON}")
print(f"HAZARDS_NB_PATH    = {HAZARDS_NB_PATH}  cron={HAZARDS_CRON}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Pre-flight — confirm both notebook paths actually exist
# MAGIC
# MAGIC Fail fast and loud if the paths are wrong — avoids creating jobs that 404 at run time.

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ObjectType

w = WorkspaceClient()

def _assert_notebook_exists(path: str) -> None:
    try:
        info = w.workspace.get_status(path=path)
    except Exception as e:
        raise RuntimeError(
            f"Notebook not found at {path!r}. Update WEATHER_NB_PATH / HAZARDS_NB_PATH "
            f"in the Configuration cell to the actual workspace location of the notebook. "
            f"Underlying error: {type(e).__name__}: {e}"
        )
    if info.object_type != ObjectType.NOTEBOOK:
        raise RuntimeError(
            f"Path {path!r} exists but is a {info.object_type}, not a NOTEBOOK."
        )
    print(f"  ✅ Found NOTEBOOK at {path}")

_assert_notebook_exists(WEATHER_NB_PATH)
_assert_notebook_exists(HAZARDS_NB_PATH)
print("\nBoth ingestion notebooks reachable.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Upsert helper — create-if-missing, update-if-exists
# MAGIC
# MAGIC The `jobs.reset` API lets us replace the settings of an existing job with new ones.
# MAGIC This makes the notebook idempotent: re-running won't create duplicates; it'll just
# MAGIC sync the job's config to whatever this notebook currently defines.

# COMMAND ----------

from databricks.sdk.service.jobs import (
    Task, NotebookTask, CronSchedule, PauseStatus,
    JobEmailNotifications, Source, JobSettings,
)

def upsert_scheduled_notebook_job(
    *,
    job_name: str,
    notebook_path: str,
    cron_expression: str,
    description: str,
    email_on_failure: list,
):
    """Create the job if missing, or reset its settings if a same-name job exists.

    Returns the job_id. Uses 'serverless' compute by leaving cluster fields unset.
    """
    task = Task(
        task_key=job_name,
        notebook_task=NotebookTask(
            notebook_path=notebook_path,
            source=Source.WORKSPACE,
        ),
        description=description,
        # Serverless: do NOT set new_cluster, existing_cluster_id, or job_cluster_key.
    )
    schedule = CronSchedule(
        quartz_cron_expression=cron_expression,
        timezone_id=TIMEZONE_ID,
        pause_status=PauseStatus.UNPAUSED,
    )
    notifs = JobEmailNotifications(on_failure=email_on_failure)

    # Find existing job by exact-name match
    existing = next(
        (j for j in w.jobs.list(name=job_name) if j.settings and j.settings.name == job_name),
        None,
    )

    if existing:
        new_settings = JobSettings(
            name=job_name,
            tasks=[task],
            schedule=schedule,
            email_notifications=notifs,
            max_concurrent_runs=1,
        )
        w.jobs.reset(job_id=existing.job_id, new_settings=new_settings)
        print(f"  ✅ Updated existing job '{job_name}' (job_id={existing.job_id})")
        return existing.job_id

    created = w.jobs.create(
        name=job_name,
        tasks=[task],
        schedule=schedule,
        email_notifications=notifs,
        max_concurrent_runs=1,
    )
    print(f"  ✅ Created new job '{job_name}' (job_id={created.job_id})")
    return created.job_id

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Create / update `refresh_weather`

# COMMAND ----------

weather_job_id = upsert_scheduled_notebook_job(
    job_name="refresh_weather",
    notebook_path=WEATHER_NB_PATH,
    cron_expression=WEATHER_CRON,
    description=(
        "Refreshes silver.weather_current from the Open-Meteo API for the 15 "
        "seeded NSW locations. Bronze append-only with _ingest_time; silver "
        "rebuilt every run with CLUSTER BY (h3_cell). Source: notebook 02."
    ),
    email_on_failure=[NOTIFICATION_EMAIL],
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Create / update `refresh_hazards`

# COMMAND ----------

hazards_job_id = upsert_scheduled_notebook_job(
    job_name="refresh_hazards",
    notebook_path=HAZARDS_NB_PATH,
    cron_expression=HAZARDS_CRON,
    description=(
        "Refreshes silver.hazards_current from TfNSW Live Traffic API (4 hazard "
        "feeds: incident, flood, fire, roadwork). Reads tfnsw_api_key from secret "
        "scope eco_resilience. Bronze append-only; silver rebuilt every run. "
        "Source: notebook 03."
    ),
    email_on_failure=[NOTIFICATION_EMAIL],
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Kick off one-off runs to validate the configuration
# MAGIC
# MAGIC Each scheduled job will fire on its cron tick — but we shouldn't wait 3-6 hours
# MAGIC to find out the config is wrong. Trigger both jobs now and watch them succeed.

# COMMAND ----------

weather_run = w.jobs.run_now(job_id=weather_job_id)
hazards_run = w.jobs.run_now(job_id=hazards_job_id)

print(f"refresh_weather   run_id = {weather_run.run_id}")
print(f"refresh_hazards   run_id = {hazards_run.run_id}")
print()
print("Watch live: Workspace → Workflows → click 'refresh_weather' or 'refresh_hazards'")
print()
print("Both runs take ~30-90s. Run the next cell after both show as SUCCESS.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Wait for the runs to finish, then verify data freshness

# COMMAND ----------

import time
from databricks.sdk.service.jobs import RunLifeCycleState, RunResultState

def _wait_for_run(run_id: int, label: str, deadline_s: int = 300):
    """Poll a run until it terminates or the deadline expires."""
    start = time.time()
    while True:
        run = w.jobs.get_run(run_id=run_id)
        state = run.state
        life  = state.life_cycle_state if state else None
        if life == RunLifeCycleState.TERMINATED:
            result = state.result_state if state else None
            print(f"  {label} → TERMINATED with result={result}")
            return result == RunResultState.SUCCESS
        if life in (RunLifeCycleState.INTERNAL_ERROR, RunLifeCycleState.SKIPPED):
            print(f"  {label} → FAILED life_cycle_state={life}, message={state.state_message!r}")
            return False
        if time.time() - start > deadline_s:
            print(f"  {label} → timed out after {deadline_s}s (still {life})")
            return False
        time.sleep(10)

print("Waiting for both runs to complete (up to 5 min each)...\n")
ok_weather = _wait_for_run(weather_run.run_id, "refresh_weather", deadline_s=300)
ok_hazards = _wait_for_run(hazards_run.run_id, "refresh_hazards", deadline_s=300)

if not (ok_weather and ok_hazards):
    print("\n⚠️  At least one initial run did not succeed. "
          "Open Workflows UI → click the failing run → inspect the notebook output.")
else:
    print("\n✅ Both initial runs succeeded.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Confirm both Silver tables are now fresh

# COMMAND ----------

display(spark.sql("""
    SELECT
      'weather_current' AS table_name,
      COUNT(*)          AS row_count,
      MAX(_ingest_time) AS latest_ingest,
      TIMESTAMPDIFF(MINUTE, MAX(_ingest_time), current_timestamp()) AS minutes_old
    FROM eco_resilience.silver.weather_current
    UNION ALL
    SELECT
      'hazards_current',
      COUNT(*),
      MAX(_ingest_time),
      TIMESTAMPDIFF(MINUTE, MAX(_ingest_time), current_timestamp())
    FROM eco_resilience.silver.hazards_current
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Done
# MAGIC
# MAGIC Both jobs are now active:
# MAGIC
# MAGIC - `refresh_weather` — fires every 6 hours, refreshes `silver.weather_current`
# MAGIC - `refresh_hazards` — fires every 3 hours, refreshes `silver.hazards_current`
# MAGIC
# MAGIC ### Common operations
# MAGIC
# MAGIC | Want to... | How |
# MAGIC |---|---|
# MAGIC | Pause/unpause a job | Workflows UI → click job → Schedule tab → Pause / Resume |
# MAGIC | Change cadence | Edit `WEATHER_CRON` / `HAZARDS_CRON` above, re-run this notebook |
# MAGIC | See past runs | Workflows UI → click job → Runs tab |
# MAGIC | See why a run failed | Click the failed run → output of the underlying notebook is shown |
# MAGIC | Run manually now | `w.jobs.run_now(job_id=weather_job_id)` or use the UI's "Run now" button |
# MAGIC
# MAGIC ### Cost note
# MAGIC
# MAGIC Serverless billing applies per-run, not idle. Each refresh runs ~30-60s.
# MAGIC
# MAGIC | Job | Runs/day | Per-run cost (approx) | Daily cost |
# MAGIC |---|---|---|---|
# MAGIC | refresh_weather | 4 | ~$0.02 | ~$0.08 |
# MAGIC | refresh_hazards | 8 | ~$0.02 | ~$0.16 |
# MAGIC
# MAGIC Negligible for the demo period.