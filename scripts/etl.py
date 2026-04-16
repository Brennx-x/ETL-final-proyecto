# etl.py
# Modes: LOCAL (DirectRunner + CSV output) | GCP (DataflowRunner + BigQuery)
# Set ENV: ETL_MODE=local or ETL_MODE=gcp

import os
import csv
import json
import logging
from io import StringIO
from datetime import datetime

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions
from apache_beam.io import ReadFromText, WriteToText
from apache_beam.io.gcp.bigquery import WriteToBigQuery

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ─── Config from environment ───────────────────────────────────────────────────
ETL_MODE           = os.getenv("ETL_MODE", "local").lower()          # local | gcp
GCP_PROJECT        = os.getenv("GCP_PROJECT", "")
GCP_BUCKET         = os.getenv("GCP_BUCKET", "")
GCP_REGION         = os.getenv("GCP_REGION", "us-central1")
BQ_DATASET         = os.getenv("BQ_DATASET", "etl_dataset")
LOCAL_INPUT        = "data/orders.csv"
LOCAL_OUTPUT       = "data/output"
GITHUB_RAW_URL     = os.getenv("GITHUB_RAW_URL", "")   # optional: raw CSV URL from GitHub


# ─── Parse ─────────────────────────────────────────────────────────────────────
def parse(line):
    try:
        f = next(csv.reader(StringIO(line)))
        return {
            "order_id":          f[0].strip(),
            "order_date":        f[1].strip(),
            "status":            f[2].strip(),
            "category":          f[3].strip(),
            "qty":               int(f[4]),
            "amount":            float(f[5]),
            "promotion_ids":     f[6].strip() if len(f) > 6 else "",
            "is_b2b":            f[7].strip() if len(f) > 7 else "False",
            "ship_service_level":f[8].strip() if len(f) > 8 else "",
            "ingestion_ts":      datetime.utcnow().isoformat()
        }
    except Exception as e:
        logger.warning(f"Parse error: {e} | line: {line[:80]}")
        return None


# ─── Clean ─────────────────────────────────────────────────────────────────────
def clean(e):
    if not e:
        return None
    if e["qty"] <= 0 or e["amount"] <= 0:
        return None
    if e["status"].lower() == "cancelled" and e["qty"] == 0 and e["amount"] == 0:
        return None
    return e


# ─── Enrich ────────────────────────────────────────────────────────────────────
def enrich(e):
    e["category"] = e["category"].strip().title()
    e["status"]   = e["status"].strip().title()
    e["revenue"]  = round(e["qty"] * e["amount"], 2)
    e["has_promotion"] = bool(e["promotion_ids"])
    e["is_b2b"]   = str(e["is_b2b"]).lower() == "true"
    e["is_express"]= e["ship_service_level"].lower() == "expedited"

    for fmt in ("%m-%d-%y", "%Y-%m-%d", "%d/%m/%Y"):
        try:
            dt = datetime.strptime(e["order_date"], fmt)
            e["order_year"]  = dt.year
            e["order_month"] = dt.month
            break
        except ValueError:
            pass
    else:
        e["order_year"] = None
        e["order_month"] = None

    return e


# ─── Dedup ─────────────────────────────────────────────────────────────────────
class DedupLatest(beam.PTransform):
    def expand(self, pcoll):
        return (
            pcoll
            | "KeyByOrderId"  >> beam.Map(lambda x: (x["order_id"], x))
            | "CombineLatest" >> beam.CombinePerKey(
                lambda records: max(records, key=lambda r: r["ingestion_ts"])
            )
            | "DropKey"       >> beam.Map(lambda kv: kv[1])
        )


# ─── Summary aggregation ───────────────────────────────────────────────────────
class SummaryFn(beam.CombineFn):
    def create_accumulator(self):
        return {"total_orders": 0, "total_units": 0, "total_revenue": 0.0}

    def add_input(self, acc, e):
        acc["total_orders"]  += 1
        acc["total_units"]   += e["qty"]
        acc["total_revenue"] += e["revenue"]
        return acc

    def merge_accumulators(self, accs):
        result = self.create_accumulator()
        for a in accs:
            result["total_orders"]  += a["total_orders"]
            result["total_units"]   += a["total_units"]
            result["total_revenue"] += a["total_revenue"]
        return result

    def extract_output(self, acc):
        acc["total_revenue"] = round(acc["total_revenue"], 2)
        return acc


# ─── Pipeline options ──────────────────────────────────────────────────────────
def build_options():
    options = PipelineOptions()

    if ETL_MODE == "gcp":
        if not GCP_PROJECT or not GCP_BUCKET:
            raise ValueError("GCP_PROJECT and GCP_BUCKET must be set for gcp mode")

        gcp_opts = options.view_as(GoogleCloudOptions)
        gcp_opts.project        = GCP_PROJECT
        gcp_opts.region         = GCP_REGION
        gcp_opts.staging_location = f"gs://{GCP_BUCKET}/staging"
        gcp_opts.temp_location    = f"gs://{GCP_BUCKET}/temp"

        std_opts = options.view_as(StandardOptions)
        std_opts.runner = "DataflowRunner"

        logger.info(f"[GCP mode] project={GCP_PROJECT} bucket={GCP_BUCKET}")
    else:
        options.view_as(StandardOptions).runner = "DirectRunner"
        logger.info(f"[LOCAL mode] input={LOCAL_INPUT} output={LOCAL_OUTPUT}")

    return options


# ─── Sink helpers ──────────────────────────────────────────────────────────────
def write_detail(pcoll):
    if ETL_MODE == "gcp":
        return pcoll | "WriteDetailBQ" >> WriteToBigQuery(
            f"{GCP_PROJECT}:{BQ_DATASET}.orders_staging",
            write_disposition="WRITE_TRUNCATE"
        )
    else:
        return pcoll | "WriteDetailLocal" >> (
            beam.Map(json.dumps)
            | WriteToText(f"{LOCAL_OUTPUT}/orders_staging", file_name_suffix=".jsonl")
        )


def write_summary(pcoll):
    if ETL_MODE == "gcp":
        return pcoll | "WriteSummaryBQ" >> WriteToBigQuery(
            f"{GCP_PROJECT}:{BQ_DATASET}.orders_summary",
            write_disposition="WRITE_TRUNCATE"
        )
    else:
        return pcoll | "WriteSummaryLocal" >> (
            beam.Map(json.dumps)
            | WriteToText(f"{LOCAL_OUTPUT}/orders_summary", file_name_suffix=".jsonl")
        )


# ─── Input source ──────────────────────────────────────────────────────────────
def get_input_path():
    if ETL_MODE == "gcp":
        return f"gs://{GCP_BUCKET}/data/orders.csv"
    else:
        return LOCAL_INPUT


# ─── Run ───────────────────────────────────────────────────────────────────────
def run():
    options    = build_options()
    input_path = get_input_path()

    logger.info(f"Starting ETL | mode={ETL_MODE} | source={input_path}")

    with beam.Pipeline(options=options) as p:
        data = (
            p
            | "Read"         >> ReadFromText(input_path, skip_header_lines=1)
            | "Parse"        >> beam.Map(parse)
            | "FilterParsed" >> beam.Filter(lambda x: x is not None)
            | "Clean"        >> beam.Map(clean)
            | "FilterClean"  >> beam.Filter(lambda x: x is not None)
            | "Enrich"       >> beam.Map(enrich)
            | "Dedup"        >> DedupLatest()
        )

        summary = (
            data
            | "KeySummary"     >> beam.Map(
                lambda x: ((x["category"], x["order_year"], x["order_month"]), x)
            )
            | "CombineSummary" >> beam.CombinePerKey(SummaryFn())
            | "FormatSummary"  >> beam.Map(lambda kv: {
                "category":     kv[0][0],
                "order_year":   kv[0][1],
                "order_month":  kv[0][2],
                "total_orders": kv[1]["total_orders"],
                "total_units":  kv[1]["total_units"],
                "total_revenue":kv[1]["total_revenue"]
            })
        )

        write_detail(data)
        write_summary(summary)

    logger.info("ETL completed successfully")


if __name__ == "__main__":
    run()
