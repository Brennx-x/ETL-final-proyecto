# fetch_github_data.py
# Downloads the source CSV from a GitHub raw URL into the data folder
# Works in both LOCAL and GCP modes (for GCP it uploads to GCS after download)

import os
import logging
import requests
import tempfile

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

ETL_MODE       = os.getenv("ETL_MODE", "local").lower()
GITHUB_RAW_URL = os.getenv("GITHUB_RAW_URL", "")          # e.g. https://raw.githubusercontent.com/user/repo/main/data/orders.csv
GITHUB_TOKEN   = os.getenv("GITHUB_TOKEN", "")            # optional, for private repos
LOCAL_INPUT    = os.getenv("LOCAL_INPUT", "/opt/airflow/data/orders.csv")
GCP_BUCKET     = os.getenv("GCP_BUCKET", "")


def download_csv(url: str, dest_path: str):
    headers = {}
    if GITHUB_TOKEN:
        headers["Authorization"] = f"token {GITHUB_TOKEN}"

    logger.info(f"Downloading CSV from GitHub → {url}")
    response = requests.get(url, headers=headers, timeout=30)
    response.raise_for_status()

    os.makedirs(os.path.dirname(dest_path), exist_ok=True)
    with open(dest_path, "wb") as f:
        f.write(response.content)

    lines = response.content.count(b"\n")
    logger.info(f"Downloaded {lines} lines → {dest_path}")
    return dest_path


def upload_to_gcs(local_path: str):
    from google.cloud import storage
    client = storage.Client()
    bucket = client.bucket(GCP_BUCKET)
    blob   = bucket.blob("data/orders.csv")
    blob.upload_from_filename(local_path)
    gcs_path = f"gs://{GCP_BUCKET}/data/orders.csv"
    logger.info(f"Uploaded to GCS → {gcs_path}")
    return gcs_path


def run():
    if not GITHUB_RAW_URL:
        logger.warning("GITHUB_RAW_URL not set — skipping download, using existing file")
        return

    if ETL_MODE == "gcp":
        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as tmp:
            tmp_path = tmp.name
        download_csv(GITHUB_RAW_URL, tmp_path)
        upload_to_gcs(tmp_path)
        os.unlink(tmp_path)
    else:
        download_csv(GITHUB_RAW_URL, LOCAL_INPUT)


if __name__ == "__main__":
    run()
