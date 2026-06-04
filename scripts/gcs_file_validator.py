
## New `scripts/gcs_file_validator.py`


# ============================================================
# gcs_file_validator.py
# Retail Data Pipeline - GCS File Validation Script
#
# Purpose:
#   Validate incoming source files from GCS landing bucket and
#   copy valid files to validated bucket and invalid files to rejected bucket.
#
# Supports:
#   - Local execution
#   - Cloud Composer / Airflow BashOperator execution
#   - argparse / --help
#   - schema drift warnings
#   - audit file generation
# ============================================================

from __future__ import annotations

import argparse
import csv
import io
import logging
import os
import re
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Tuple, Optional

import pandas as pd
import yaml
from google.cloud import storage


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger(__name__)


# ============================================================
# FILE / SCHEMA CONFIG
# ============================================================

VALID_ENTITIES = {"customers", "products", "stores", "sales"}
VALID_LOAD_TYPES = {"full", "delta"}

FILE_NAME_PATTERN = re.compile(
    r"^(customers|products|stores|sales)_(full|delta)_(\d{8})\.csv$",
    re.IGNORECASE,
)

# These are mandatory columns expected in the source CSV files.
# Metadata columns like source_file_name, batch_id, load_timestamp are added later
# in the BigQuery loader, not expected in source files.
EXPECTED_SCHEMAS: Dict[str, List[str]] = {
    "customers": [
        "customer_id",
        "customer_name",
        "email",
        "city",
        "state",
        "created_date",
        "load_type",
    ],
    "products": [
        "product_id",
        "product_name",
        "category",
        "brand",
        "price",
        "created_date",
        "load_type",
    ],
    "stores": [
        "store_id",
        "store_name",
        "city",
        "state",
        "region",
        "created_date",
        "load_type",
    ],
    "sales": [
        "order_id",
        "customer_id",
        "product_id",
        "store_id",
        "quantity",
        "unit_price",
        "discount_amount",
        "tax_amount",
        "sale_amount",
        "sale_date",
        "payment_method",
        "load_type",
    ],
}

# Allowed additive schema drift columns.
# These columns are allowed but logged as schema drift warnings.
ALLOWED_EXTRA_COLUMNS: Dict[str, List[str]] = {
    "customers": [
        "loyalty_tier",
        "customer_segment",
    ],
    "products": [
        "supplier_id",
        "product_status",
    ],
    "stores": [
        "store_type",
        "manager_name",
    ],
    "sales": [
        "coupon_code",
        "sale_channel",
        "delivery_type",
    ],
}

# Basic minimum record count check.
MIN_ROW_COUNT = 1


# ============================================================
# CONFIG LOADING
# ============================================================

def load_config(config_path: str) -> dict:
    """
    Load YAML config if available.
    """
    path = Path(config_path)

    if not path.exists():
        logger.warning("Config file not found: %s. Using CLI/default values.", config_path)
        return {}

    with open(path, "r", encoding="utf-8") as file:
        return yaml.safe_load(file) or {}


def get_config_value(
    cli_value: Optional[str],
    config: dict,
    keys: List[str],
    default: Optional[str] = None,
) -> Optional[str]:
    """
    Returns CLI value first, then nested config value, then default.
    """
    if cli_value:
        return cli_value

    current = config
    for key in keys:
        if not isinstance(current, dict) or key not in current:
            return default
        current = current[key]

    return current


# ============================================================
# VALIDATION HELPERS
# ============================================================

def parse_file_name(file_name: str) -> Tuple[bool, Optional[dict], str]:
    """
    Validate and parse filename.

    Expected:
      customers_full_20260501.csv
      sales_delta_20260506.csv
    """
    match = FILE_NAME_PATTERN.match(file_name)

    if not match:
        return False, None, "INVALID_FILE_NAME_PATTERN"

    entity, load_type, load_date = match.groups()

    try:
        datetime.strptime(load_date, "%Y%m%d")
    except ValueError:
        return False, None, "INVALID_LOAD_DATE"

    return True, {
        "entity": entity.lower(),
        "load_type": load_type.lower(),
        "load_date": load_date,
    }, ""


def normalize_columns(columns: List[str]) -> List[str]:
    """
    Lowercase and trim column names.
    """
    return [str(col).strip().lower() for col in columns]


def validate_schema(entity: str, columns: List[str]) -> Tuple[bool, List[str], List[str]]:
    """
    Validates mandatory columns and captures allowed/unexpected schema drift.

    Returns:
      is_valid
      warnings
      errors
    """
    normalized_columns = normalize_columns(columns)
    expected_columns = EXPECTED_SCHEMAS[entity]
    allowed_extra_columns = ALLOWED_EXTRA_COLUMNS.get(entity, [])

    warnings = []
    errors = []

    duplicate_columns = sorted(
        {col for col in normalized_columns if normalized_columns.count(col) > 1}
    )

    if duplicate_columns:
        errors.append(f"DUPLICATE_COLUMNS: {duplicate_columns}")

    missing_columns = [
        col for col in expected_columns if col not in normalized_columns
    ]

    if missing_columns:
        errors.append(f"MISSING_MANDATORY_COLUMNS: {missing_columns}")

    extra_columns = [
        col for col in normalized_columns if col not in expected_columns
    ]

    allowed_drift_columns = [
        col for col in extra_columns if col in allowed_extra_columns
    ]

    unexpected_extra_columns = [
        col for col in extra_columns if col not in allowed_extra_columns
    ]

    if allowed_drift_columns:
        warnings.append(f"ALLOWED_SCHEMA_DRIFT_COLUMNS: {allowed_drift_columns}")

    if unexpected_extra_columns:
        warnings.append(f"UNEXPECTED_EXTRA_COLUMNS: {unexpected_extra_columns}")

    # In this project, unexpected extra columns are warning only.
    # Reason: raw loader supports schema drift / auto ALTER.
    # If you want strict rejection, convert this warning to error.
    return len(errors) == 0, warnings, errors


def read_csv_from_gcs(blob: storage.Blob) -> pd.DataFrame:
    """
    Download CSV from GCS blob and read into pandas DataFrame.
    """
    data = blob.download_as_bytes()

    if not data:
        raise ValueError("EMPTY_FILE")

    return pd.read_csv(io.BytesIO(data))


def validate_dataframe(df: pd.DataFrame) -> Tuple[bool, List[str]]:
    """
    Basic file-level validation.
    Row-level business DQ is handled later in BigQuery DQ SQL.
    """
    errors = []

    if df.empty:
        errors.append("EMPTY_DATAFRAME")

    if len(df) < MIN_ROW_COUNT:
        errors.append(f"ROW_COUNT_LESS_THAN_{MIN_ROW_COUNT}")

    return len(errors) == 0, errors


def build_gcs_uri(bucket_name: str, object_name: str) -> str:
    return f"gs://{bucket_name}/{object_name}"


def copy_blob(
    storage_client: storage.Client,
    source_bucket_name: str,
    source_blob_name: str,
    target_bucket_name: str,
    target_blob_name: str,
) -> None:
    """
    Copy object from source bucket to target bucket.
    """
    source_bucket = storage_client.bucket(source_bucket_name)
    source_blob = source_bucket.blob(source_blob_name)
    target_bucket = storage_client.bucket(target_bucket_name)

    source_bucket.copy_blob(
        source_blob,
        target_bucket,
        new_name=target_blob_name,
    )


# ============================================================
# VALIDATION PROCESS
# ============================================================

def validate_single_blob(
    storage_client: storage.Client,
    blob: storage.Blob,
    landing_bucket: str,
    validated_bucket: str,
    rejected_bucket: str,
    run_id: str,
) -> dict:
    """
    Validate one file and copy it to validated or rejected bucket.
    """
    processed_at = datetime.now(timezone.utc).isoformat()
    source_object_name = blob.name
    file_name = os.path.basename(source_object_name)

    audit_record = {
        "run_id": run_id,
        "file_name": file_name,
        "source_uri": build_gcs_uri(landing_bucket, source_object_name),
        "target_uri": "",
        "entity": "",
        "load_type": "",
        "load_date": "",
        "row_count": 0,
        "column_count": 0,
        "status": "",
        "errors": "",
        "warnings": "",
        "processed_at": processed_at,
    }

    logger.info("Validating file: %s", file_name)

    is_name_valid, parsed_info, name_error = parse_file_name(file_name)

    if not is_name_valid or parsed_info is None:
        target_object_name = f"rejected/{file_name}"
        copy_blob(
            storage_client,
            landing_bucket,
            source_object_name,
            rejected_bucket,
            target_object_name,
        )

        audit_record.update(
            {
                "status": "FAILED",
                "errors": name_error,
                "target_uri": build_gcs_uri(rejected_bucket, target_object_name),
            }
        )

        logger.error("%s => FAILED => %s", file_name, audit_record["target_uri"])
        return audit_record

    entity = parsed_info["entity"]
    load_type = parsed_info["load_type"]
    load_date = parsed_info["load_date"]

    audit_record.update(
        {
            "entity": entity,
            "load_type": load_type.upper(),
            "load_date": load_date,
        }
    )

    errors = []
    warnings = []

    try:
        df = read_csv_from_gcs(blob)

        audit_record["row_count"] = len(df)
        audit_record["column_count"] = len(df.columns)

        schema_valid, schema_warnings, schema_errors = validate_schema(
            entity,
            list(df.columns),
        )

        warnings.extend(schema_warnings)
        errors.extend(schema_errors)

        data_valid, data_errors = validate_dataframe(df)
        errors.extend(data_errors)

        is_valid = schema_valid and data_valid

    except Exception as exception:
        is_valid = False
        errors.append(f"FILE_READ_ERROR: {str(exception)}")

    if is_valid:
        target_object_name = f"validated/{file_name}"
        target_bucket = validated_bucket
        status = "PASSED"
    else:
        target_object_name = f"rejected/{file_name}"
        target_bucket = rejected_bucket
        status = "FAILED"

    copy_blob(
        storage_client,
        landing_bucket,
        source_object_name,
        target_bucket,
        target_object_name,
    )

    audit_record.update(
        {
            "status": status,
            "errors": " | ".join(errors),
            "warnings": " | ".join(warnings),
            "target_uri": build_gcs_uri(target_bucket, target_object_name),
        }
    )

    if warnings:
        logger.warning("Schema/Data warning in %s: %s", file_name, audit_record["warnings"])

    if status == "PASSED":
        logger.info("%s => PASSED => %s", file_name, audit_record["target_uri"])
    else:
        logger.error("%s => FAILED => %s", file_name, audit_record["target_uri"])
        logger.error("Errors: %s", audit_record["errors"])

    return audit_record


def write_audit_file(audit_records: List[dict], audit_dir: str, run_id: str) -> Optional[str]:
    """
    Write validation audit CSV locally.
    In Composer, this will write inside the DAG worker local filesystem.
    For production, we can later write this to GCS/BigQuery audit table.
    """
    if not audit_records:
        logger.warning("No audit records to write.")
        return None

    os.makedirs(audit_dir, exist_ok=True)

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    audit_file_path = os.path.join(
        audit_dir,
        f"file_validation_audit_{timestamp}_{run_id}.csv",
    )

    fieldnames = list(audit_records[0].keys())

    with open(audit_file_path, "w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(audit_records)

    logger.info("Audit file created: %s", audit_file_path)
    return audit_file_path


def run_validation(args: argparse.Namespace) -> int:
    """
    Main validation runner.
    """
    config = load_config(args.config)

    project_id = get_config_value(
        args.project_id,
        config,
        ["gcp", "project_id"],
        default=None,
    )

    landing_bucket = get_config_value(
        args.landing_bucket,
        config,
        ["gcs", "landing_bucket"],
        default=None,
    )

    validated_bucket = get_config_value(
        args.validated_bucket,
        config,
        ["gcs", "validated_bucket"],
        default=None,
    )

    rejected_bucket = get_config_value(
        args.rejected_bucket,
        config,
        ["gcs", "rejected_bucket"],
        default=None,
    )

    archive_bucket = get_config_value(
        args.archive_bucket,
        config,
        ["gcs", "archive_bucket"],
        default=None,
    )

    run_id = args.run_id or str(uuid.uuid4())

    if not project_id:
        raise ValueError("Missing project_id. Pass --project-id or configure gcp.project_id.")

    if not landing_bucket:
        raise ValueError("Missing landing_bucket. Pass --landing-bucket or configure gcs.landing_bucket.")

    if not validated_bucket:
        raise ValueError("Missing validated_bucket. Pass --validated-bucket or configure gcs.validated_bucket.")

    if not rejected_bucket:
        raise ValueError("Missing rejected_bucket. Pass --rejected-bucket or configure gcs.rejected_bucket.")

    logger.info("=" * 80)
    logger.info("Retail GCS File Validation Started")
    logger.info("=" * 80)
    logger.info("Project ID       : %s", project_id)
    logger.info("Landing bucket   : %s", landing_bucket)
    logger.info("Validated bucket : %s", validated_bucket)
    logger.info("Rejected bucket  : %s", rejected_bucket)
    logger.info("Archive bucket   : %s", archive_bucket)
    logger.info("Prefix           : %s", args.prefix)
    logger.info("Run date         : %s", args.run_date)
    logger.info("Run ID           : %s", run_id)
    logger.info("=" * 80)

    storage_client = storage.Client(project=project_id)
    landing_bucket_obj = storage_client.bucket(landing_bucket)

    blobs = list(storage_client.list_blobs(landing_bucket_obj, prefix=args.prefix))

    # Ignore folder placeholders.
    blobs = [blob for blob in blobs if not blob.name.endswith("/")]

    if args.process_dates:
        process_dates = set(args.process_dates)
        filtered_blobs = []
        for blob in blobs:
            file_name = os.path.basename(blob.name)
            is_valid_name, parsed_info, _ = parse_file_name(file_name)
            if is_valid_name and parsed_info and parsed_info["load_date"] in process_dates:
                filtered_blobs.append(blob)
        blobs = filtered_blobs

    if args.max_files:
        blobs = blobs[: args.max_files]

    logger.info("Files found for validation: %s", len(blobs))

    audit_records = []

    for blob in blobs:
        audit_record = validate_single_blob(
            storage_client=storage_client,
            blob=blob,
            landing_bucket=landing_bucket,
            validated_bucket=validated_bucket,
            rejected_bucket=rejected_bucket,
            run_id=run_id,
        )
        audit_records.append(audit_record)

    audit_file_path = write_audit_file(
        audit_records=audit_records,
        audit_dir=args.audit_dir,
        run_id=run_id,
    )

    passed_count = sum(1 for record in audit_records if record["status"] == "PASSED")
    failed_count = sum(1 for record in audit_records if record["status"] == "FAILED")

    logger.info("=" * 80)
    logger.info("Retail GCS File Validation Completed")
    logger.info("Total files : %s", len(audit_records))
    logger.info("Passed      : %s", passed_count)
    logger.info("Failed      : %s", failed_count)
    logger.info("Audit file  : %s", audit_file_path)
    logger.info("=" * 80)

    if archive_bucket:
        logger.info("Archive bucket configured for later use: %s", archive_bucket)

    if failed_count > 0 and args.fail_on_rejected:
        logger.error("Validation completed with rejected files. Failing because --fail-on-rejected is enabled.")
        return 1

    return 0


# ============================================================
# ARGPARSE
# ============================================================

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate retail source files in GCS landing bucket and copy to validated/rejected buckets."
    )

    parser.add_argument(
        "--config",
        default="config/project_config.yaml",
        help="Path to project YAML config file.",
    )

    parser.add_argument(
        "--project-id",
        default=None,
        help="GCP project ID.",
    )

    parser.add_argument(
        "--landing-bucket",
        default=None,
        help="GCS landing bucket name.",
    )

    parser.add_argument(
        "--validated-bucket",
        default=None,
        help="GCS validated bucket name.",
    )

    parser.add_argument(
        "--rejected-bucket",
        default=None,
        help="GCS rejected bucket name.",
    )

    parser.add_argument(
        "--archive-bucket",
        default=None,
        help="GCS archive bucket name. Currently logged only.",
    )

    parser.add_argument(
        "--prefix",
        default="incoming/",
        help="GCS prefix inside landing bucket to scan.",
    )

    parser.add_argument(
        "--run-date",
        default=None,
        help="Airflow/business run date. Example: 2026-06-04.",
    )

    parser.add_argument(
        "--run-id",
        default=None,
        help="Airflow/manual run ID.",
    )

    parser.add_argument(
        "--process-dates",
        nargs="*",
        default=None,
        help="Optional file load dates to process. Example: --process-dates 20260505 20260506",
    )

    parser.add_argument(
        "--audit-dir",
        default="audit",
        help="Local audit output directory.",
    )

    parser.add_argument(
        "--max-files",
        type=int,
        default=None,
        help="Optional limit for testing.",
    )

    parser.add_argument(
        "--fail-on-rejected",
        action="store_true",
        help="Return exit code 1 if any file is rejected.",
    )

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    try:
        exit_code = run_validation(args)
        sys.exit(exit_code)

    except Exception as exception:
        logger.exception("Fatal error in GCS file validator: %s", str(exception))
        sys.exit(1)


if __name__ == "__main__":
    main()




