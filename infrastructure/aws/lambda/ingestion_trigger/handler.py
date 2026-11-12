"""AWS Lambda handler triggered on S3 raw batch upload to invoke AWS Glue ETL."""

from __future__ import annotations

import json
import logging
import os
import urllib.parse
from pathlib import Path
from typing import Any

import boto3

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

GLUE_JOB_NAME = os.environ.get("GLUE_JOB_NAME", "retail-bronze-to-silver-dq")
PROCESSED_BUCKET = os.environ["S3_PROCESSED_BUCKET"]
REJECTED_BUCKET = os.environ["S3_REJECTED_BUCKET"]

glue_client = boto3.client("glue")


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    """Validate S3 ObjectCreated event and launch AWS Glue Bronze-to-Silver DQ job."""
    launched_runs: list[dict[str, str]] = []

    for record in event.get("Records", []):
        bucket = record["s3"]["bucket"]["name"]
        key = urllib.parse.unquote_plus(record["s3"]["object"]["key"])
        size_bytes = int(record["s3"]["object"].get("size", 0))

        if not key.lower().endswith(".csv") or size_bytes == 0:
            logger.warning("Skipping non-CSV or empty S3 object: s3://%s/%s", bucket, key)
            continue

        batch_id = Path(key).stem
        s3_input_uri = f"s3://{bucket}/{key}"

        response = glue_client.start_job_run(
            JobName=GLUE_JOB_NAME,
            Arguments={
                "--BATCH_ID": batch_id,
                "--S3_INPUT_URI": s3_input_uri,
                "--S3_PROCESSED_URI": f"s3://{PROCESSED_BUCKET}/batch_id={batch_id}/",
                "--S3_REJECTED_URI": f"s3://{REJECTED_BUCKET}/batch_id={batch_id}/",
            },
        )
        run_id = response["JobRunId"]
        logger.info("Started Glue job %s run_id=%s for %s", GLUE_JOB_NAME, run_id, s3_input_uri)
        launched_runs.append({"batch_id": batch_id, "job_run_id": run_id})

    return {
        "statusCode": 200,
        "body": json.dumps({"launched_runs": launched_runs}),
    }
