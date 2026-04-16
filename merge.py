# merge.py
# Merges orders_staging → orders_final in BigQuery (GCP mode)
# In LOCAL mode, merges two JSONL files into a single deduplicated output

import os
import json
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

ETL_MODE    = os.getenv("ETL_MODE", "local").lower()
GCP_PROJECT = os.getenv("GCP_PROJECT", "")
BQ_DATASET  = os.getenv("BQ_DATASET", "etl_dataset")
LOCAL_OUTPUT = os.getenv("LOCAL_OUTPUT", "/opt/airflow/data/output")


def run_gcp():
    from google.cloud import bigquery
    client = bigquery.Client(project=GCP_PROJECT)

    query = f"""
    MERGE `{GCP_PROJECT}.{BQ_DATASET}.orders_final` T
    USING `{GCP_PROJECT}.{BQ_DATASET}.orders_staging` S
    ON T.order_id = S.order_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """

    logger.info(f"Running MERGE on BigQuery | project={GCP_PROJECT}")
    client.query(query).result()
    logger.info("MERGE completed")


def run_local():
    import glob

    staging_files = glob.glob(f"{LOCAL_OUTPUT}/orders_staging*.jsonl")
    final_path    = f"{LOCAL_OUTPUT}/orders_final.jsonl"

    existing = {}

    # Load existing final if present
    try:
        with open(final_path) as f:
            for line in f:
                line = line.strip()
                if line:
                    rec = json.loads(line)
                    existing[rec["order_id"]] = rec
        logger.info(f"Loaded {len(existing)} existing records from orders_final")
    except FileNotFoundError:
        logger.info("No existing orders_final — will create fresh")

    # Merge staging into existing
    new_count = 0
    upd_count = 0
    for fpath in staging_files:
        with open(fpath) as f:
            for line in f:
                line = line.strip()
                if line:
                    rec = json.loads(line)
                    oid = rec["order_id"]
                    if oid in existing:
                        upd_count += 1
                    else:
                        new_count += 1
                    existing[oid] = rec

    with open(final_path, "w") as f:
        for rec in existing.values():
            f.write(json.dumps(rec) + "\n")

    logger.info(f"Merge done | inserted={new_count} updated={upd_count} total={len(existing)}")
    logger.info(f"Output → {final_path}")


def run():
    if ETL_MODE == "gcp":
        run_gcp()
    else:
        run_local()


if __name__ == "__main__":
    run()
