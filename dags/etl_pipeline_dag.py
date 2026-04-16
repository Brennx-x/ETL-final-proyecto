# dags/etl_pipeline_dag.py
# Airflow DAG: GitHub → ETL (Beam) → BigQuery/local merge
#
# Steps:
#   1. fetch_data   — download CSV from GitHub (optional)
#   2. run_etl      — Apache Beam transform (local or Dataflow)
#   3. run_merge    — upsert staging → final table (BQ or local JSONL)
#
# Required Airflow Variables (set via UI or env):
#   ETL_MODE, GCP_PROJECT, GCP_BUCKET, BQ_DATASET,
#   GITHUB_RAW_URL, GITHUB_TOKEN (optional),
#   LOCAL_INPUT, LOCAL_OUTPUT

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.models import Variable

# ─── Read config from Airflow Variables (with fallbacks) ──────────────────────
def get_var(key, default=""):
    try:
        return Variable.get(key)
    except Exception:
        return default

ETL_MODE        = get_var("ETL_MODE",        "local")
GCP_PROJECT     = get_var("GCP_PROJECT",     "")
GCP_BUCKET      = get_var("GCP_BUCKET",      "")
GCP_REGION      = get_var("GCP_REGION",      "us-central1")
BQ_DATASET      = get_var("BQ_DATASET",      "etl_dataset")
GITHUB_RAW_URL  = get_var("GITHUB_RAW_URL",  "")
GITHUB_TOKEN    = get_var("GITHUB_TOKEN",    "")
LOCAL_INPUT     = get_var("LOCAL_INPUT",     "/opt/airflow/data/orders.csv")
LOCAL_OUTPUT    = get_var("LOCAL_OUTPUT",    "/opt/airflow/data/output")

# Shared env for all tasks
COMMON_ENV = {
    "ETL_MODE":       ETL_MODE,
    "GCP_PROJECT":    GCP_PROJECT,
    "GCP_BUCKET":     GCP_BUCKET,
    "GCP_REGION":     GCP_REGION,
    "BQ_DATASET":     BQ_DATASET,
    "GITHUB_RAW_URL": GITHUB_RAW_URL,
    "GITHUB_TOKEN":   GITHUB_TOKEN,
    "LOCAL_INPUT":    LOCAL_INPUT,
    "LOCAL_OUTPUT":   LOCAL_OUTPUT,
}

# ─── DAG definition ────────────────────────────────────────────────────────────
default_args = {
    "owner":            "etl-team",
    "retries":          1,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="etl_pipeline",
    description="GitHub → Beam ETL → BigQuery/Local",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["etl", "beam", "gcp"],
) as dag:

    fetch_data = BashOperator(
        task_id="fetch_github_data",
        bash_command="python /opt/airflow/scripts/fetch_github_data.py",
        env=COMMON_ENV,
        doc_md="""
        Downloads orders CSV from GitHub.
        - LOCAL mode: saves to LOCAL_INPUT path
        - GCP mode: uploads to gs://GCP_BUCKET/data/orders.csv
        Skipped gracefully if GITHUB_RAW_URL is not set.
        """,
    )

    run_etl = BashOperator(
        task_id="run_etl",
        bash_command="python /opt/airflow/scripts/etl.py",
        env=COMMON_ENV,
        doc_md="""
        Runs Apache Beam ETL pipeline.
        - LOCAL mode: DirectRunner, outputs JSONL files
        - GCP mode: DataflowRunner, writes to BigQuery staging
        """,
    )

    run_merge = BashOperator(
        task_id="run_merge",
        bash_command="python /opt/airflow/scripts/merge.py",
        env=COMMON_ENV,
        doc_md="""
        Merges staging into final.
        - LOCAL mode: merges JSONL files (upsert by order_id)
        - GCP mode: MERGE SQL on BigQuery
        """,
    )

    fetch_data >> run_etl >> run_merge
