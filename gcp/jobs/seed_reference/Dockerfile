# Cloud Run Job container. Build context must be the gcp/ directory:
#   gcloud builds submit gcp/ --tag <region>-docker.pkg.dev/<proj>/eco-resilience/seed-reference
#   (see infra/03_build_and_deploy_jobs.sh)
FROM python:3.12-slim

WORKDIR /srv
COPY jobs/seed_reference/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY agent/ agent/
COPY jobs/seed_reference/main.py .
ENV PYTHONUNBUFFERED=1
CMD ["python", "main.py"]
