# Cloud Run container for the EcoResilience Flask app + in-process agent.
# Build context must be the gcp/ directory (the agent package is shared):
#   gcloud builds submit gcp/ --config=gcp/app/cloudbuild.yaml   (or)
#   docker build -f gcp/app/Dockerfile gcp/
FROM python:3.12-slim

WORKDIR /srv

COPY app/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY agent/ agent/
COPY app/app.py app/abn_ingest.py app/index.html app/

WORKDIR /srv/app
ENV PYTHONUNBUFFERED=1

# Cloud Run injects PORT (default 8080). 2 workers × 4 threads suits the
# mostly-I/O-bound BigQuery/LLM workload on a 1-CPU instance.
CMD exec gunicorn --bind :$PORT --workers 2 --threads 4 --timeout 300 app:app
