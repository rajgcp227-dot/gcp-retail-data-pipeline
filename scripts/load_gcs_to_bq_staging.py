# ============================================================
# load_gcs_to_bq_staging.py
# Retail Data Pipeline - GCS Validated Files to BigQuery Staging/Raw
#
# Purpose:
#   Load validated CSV files from GCS into BigQuery landing/raw tables.
#
# Supports:
#   - Local execution
#   - Cloud Composer / Airflow BashOperator execution
#   - argparse / --help
#   - selected incremental dates
#   - schema registry validation
#   - schema drift handling with auto ALTER TABLE
#   - idempotent raw reload by source_file_name
#   - processed_files tracking
#   - audit logging
# ============================================================

from __future__ import annotations

import argparse
import logging
import os
import re
import sys
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import yaml
from google.cloud import bigquery, storage
from google.cloud.bigquery import ScalarQueryParameter

# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger(__name__)


# ============================================================
# DEFAULT CONSTANTS
# ============================================================

DEFAULT_PROJECT_ID = "still-resource-497715-g5"
DEFAULT_VALIDATED_BUCKET = "retail-validated-dev-raj1779989813"

DEFAULT_STAGING_DATASET = "retail_staging"
DEFAULT_AUDIT_DATASET = "retail_audit_records"

DEFAULT_AUDIT_TABLE = "load_audit_log"
DEFAULT_PROCESSED_FILES_TABLE = "processed_files"

DEFAULT_SCHEMA_REGISTRY_PATH = "config/schema_registry.yaml"
DEFAULT_CONFIG_PATH = "config/project_config.yaml"

DEFAULT_PREFIX = "validated/"

TABLE_MAPPING = {
    "customers": {
        "landing_table": "customers_landing",
        "raw_table": "customers_raw",
    },
    "products": {
        "landing_table": "products_landing",
        "raw_table": "products_raw",
    },
    "stores": {
        "landing_table": "stores_landing",
        "raw_table": "stores_raw",
    },
    "sales": {
        "landing_table": "sales_landing",
        "raw_table": "sales_raw",
    },
}

METADATA_COLUMNS = {
    "source_file_name",
    "batch_id",
    "load_timestamp",
}

FILE_DATE_PATTERN = re.compile(r"_(\d{8})\.csv$", re.IGNORECASE)
VALID_BQ_COLUMN_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


# ============================================================
# RUNTIME CONFIG
# ============================================================


@dataclass
class RuntimeConfig:
    project_id: str
    validated_bucket: str
    staging_dataset: str
    audit_dataset: str
    audit_table: str
    processed_files_table: str
    schema_registry_path: str
    prefix: str
    run_date: Optional[str]
    run_id: str
    process_dates: Optional[List[str]]
    max_files: Optional[int]
    force_reprocess: bool
    fail_on_error: bool
    dry_run: bool


# ============================================================
# CONFIG HELPERS
# ============================================================


def load_project_config(config_path: str) -> dict:
    path = Path(config_path)

    if not path.exists():
        logger.warning(
            "Project config not found: %s. Using CLI/default values.", config_path
        )
        return {}

    with open(path, "r", encoding="utf-8") as file:
        return yaml.safe_load(file) or {}


def get_config_value(
    cli_value: Optional[str],
    config: dict,
    keys: List[str],
    default: Optional[str] = None,
) -> Optional[str]:
    if cli_value:
        return cli_value

    current = config
    for key in keys:
        if not isinstance(current, dict) or key not in current:
            return default
        current = current[key]

    return current


def load_schema_registry(schema_registry_path: str) -> dict:
    path = Path(schema_registry_path)

    if not path.exists():
        raise FileNotFoundError(f"Schema registry not found: {schema_registry_path}")

    with open(path, "r", encoding="utf-8") as file:
        return yaml.safe_load(file) or {}


# ============================================================
# GENERAL HELPERS
# ============================================================


def normalize_columns(columns: List[str]) -> List[str]:
    return [str(col).strip().lower() for col in columns]


def validate_bq_column_names(columns: List[str]) -> Tuple[bool, List[str]]:
    invalid_columns = [col for col in columns if not VALID_BQ_COLUMN_PATTERN.match(col)]

    return len(invalid_columns) == 0, invalid_columns


def extract_load_date(file_name: str) -> Optional[str]:
    match = FILE_DATE_PATTERN.search(file_name)
    if not match:
        return None
    return match.group(1)


def get_source_name(file_name: str) -> str:
    return file_name.split("_")[0].lower()


def build_gcs_uri(bucket_name: str, object_name: str) -> str:
    return f"gs://{bucket_name}/{object_name}"


def run_query(
    bq_client: bigquery.Client,
    query: str,
    job_config: Optional[bigquery.QueryJobConfig] = None,
) -> None:
    job = bq_client.query(query, job_config=job_config)
    job.result()


def get_row_count(bq_client: bigquery.Client, table_id: str) -> int:
    query = f"SELECT COUNT(*) AS row_count FROM `{table_id}`"
    result = list(bq_client.query(query).result())
    return int(result[0]["row_count"])


# ============================================================
# AUDIT HELPERS
# ============================================================


def insert_audit_record(
    bq_client: bigquery.Client,
    cfg: RuntimeConfig,
    record: dict,
) -> None:
    table_id = f"{cfg.project_id}.{cfg.audit_dataset}.{cfg.audit_table}"

    errors = bq_client.insert_rows_json(table_id, [record])

    if errors:
        logger.error("Audit insert error: %s", errors)


def insert_processed_file_record(
    bq_client: bigquery.Client,
    cfg: RuntimeConfig,
    file_name: str,
    gcs_uri: str,
    source_name: str,
    target_table: str,
    batch_id: str,
    row_count: int,
    status: str,
    error_message: Optional[str] = None,
) -> None:
    table_id = f"{cfg.project_id}.{cfg.audit_dataset}.{cfg.processed_files_table}"

    record = {
        "file_name": file_name,
        "gcs_uri": gcs_uri,
        "source_name": source_name,
        "target_table": target_table,
        "batch_id": batch_id,
        "row_count": row_count,
        "status": status,
        "processed_timestamp": datetime.now(timezone.utc).isoformat(),
        "error_message": error_message,
    }

    errors = bq_client.insert_rows_json(table_id, [record])

    if errors:
        logger.error("Processed file insert error: %s", errors)


def is_file_already_processed(
    bq_client: bigquery.Client,
    cfg: RuntimeConfig,
    gcs_uri: str,
) -> bool:
    query = f"""
    SELECT COUNT(*) AS cnt
    FROM `{cfg.project_id}.{cfg.audit_dataset}.{cfg.processed_files_table}`
    WHERE gcs_uri = @gcs_uri
      AND status = 'SUCCESS'
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            ScalarQueryParameter("gcs_uri", "STRING", gcs_uri),
        ]
    )

    result = list(bq_client.query(query, job_config=job_config).result())
    return int(result[0]["cnt"]) > 0


# ============================================================
# BIGQUERY SCHEMA HELPERS
# ============================================================


def get_table_columns(
    bq_client: bigquery.Client,
    cfg: RuntimeConfig,
    table_name: str,
) -> List[str]:
    query = f"""
    SELECT column_name
    FROM `{cfg.project_id}.{cfg.staging_dataset}.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = @table_name
    ORDER BY ordinal_position
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            ScalarQueryParameter("table_name", "STRING", table_name),
        ]
    )

    return [
        str(row["column_name"]).lower()
        for row in bq_client.query(query, job_config=job_config).result()
    ]


def add_new_columns_if_needed(
    bq_client: bigquery.Client,
    cfg: RuntimeConfig,
    table_name: str,
    file_columns: List[str],
) -> None:
    existing_columns = set(get_table_columns(bq_client, cfg, table_name))

    for col in file_columns:
        col = col.strip().lower()

        if col in existing_columns:
            continue

        if not VALID_BQ_COLUMN_PATTERN.match(col):
            raise ValueError(f"Invalid BigQuery column name found: {col}")

        alter_sql = f"""
        ALTER TABLE `{cfg.project_id}.{cfg.staging_dataset}.{table_name}`
        ADD COLUMN `{col}` STRING
        """

        run_query(bq_client, alter_sql)

        logger.info("ADDED COLUMN: %s.%s", table_name, col)


def create_temp_load_table(
    bq_client: bigquery.Client,
    cfg: RuntimeConfig,
    temp_table_name: str,
    file_columns: List[str],
) -> str:
    temp_table_id = f"{cfg.project_id}.{cfg.staging_dataset}.{temp_table_name}"

    schema = [bigquery.SchemaField(col, "STRING") for col in file_columns]

    table = bigquery.Table(temp_table_id, schema=schema)

    bq_client.delete_table(temp_table_id, not_found_ok=True)
    bq_client.create_table(table)

    logger.info("Created temporary load table: %s", temp_table_id)

    return temp_table_id


def drop_temp_table(
    bq_client: bigquery.Client,
    temp_table_id: str,
) -> None:
    bq_client.delete_table(temp_table_id, not_found_ok=True)
    logger.info("Dropped temporary load table: %s", temp_table_id)


# ============================================================
# GCS / FILE HELPERS
# ============================================================


def read_gcs_header(blob: storage.Blob) -> List[str]:
    text = blob.download_as_text()
    lines = text.splitlines()

    if not lines:
        raise ValueError("File is empty. Cannot read header.")

    columns = [col.strip().lower() for col in lines[0].split(",")]

    return columns


def validate_schema(
    source_name: str,
    file_columns: List[str],
    schema_registry: dict,
) -> Tuple[bool, object]:
    if source_name not in schema_registry:
        return False, f"Source not found in schema registry: {source_name}"

    rules = schema_registry[source_name]
    mandatory_columns = normalize_columns(rules.get("mandatory_columns", []))

    duplicate_columns = sorted(
        {col for col in file_columns if file_columns.count(col) > 1}
    )

    if duplicate_columns:
        return False, f"Duplicate columns found: {duplicate_columns}"

    is_valid_bq_columns, invalid_columns = validate_bq_column_names(file_columns)

    if not is_valid_bq_columns:
        return False, f"Invalid BigQuery column names found: {invalid_columns}"

    missing_columns = [col for col in mandatory_columns if col not in file_columns]

    if missing_columns:
        return False, f"Missing mandatory columns: {missing_columns}"

    new_columns = [col for col in file_columns if col not in mandatory_columns]

    if new_columns and not rules.get("allow_new_columns", False):
        return False, f"New columns not allowed: {new_columns}"

    return True, new_columns


def load_csv_to_table(
    bq_client: bigquery.Client,
    gcs_uri: str,
    table_id: str,
    file_columns: List[str],
) -> None:
    schema = [bigquery.SchemaField(col, "STRING") for col in file_columns]

    load_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=False,
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        allow_jagged_rows=True,
        ignore_unknown_values=False,
    )

    load_job = bq_client.load_table_from_uri(
        gcs_uri,
        table_id,
        job_config=load_config,
    )

    load_job.result()


# ============================================================
# LOAD LOGIC
# ============================================================


def insert_temp_to_landing(
    bq_client: bigquery.Client,
    cfg: RuntimeConfig,
    temp_table_id: str,
    landing_table: str,
) -> None:
    landing_table_id = f"{cfg.project_id}.{cfg.staging_dataset}.{landing_table}"

    landing_columns = get_table_columns(bq_client, cfg, landing_table)
    temp_table_name = temp_table_id.split(".")[-1]
    temp_columns = get_table_columns(bq_client, cfg, temp_table_name)

    select_columns = []

    for col in landing_columns:
        if col in temp_columns:
            select_columns.append(f"`{col}`")
        else:
            select_columns.append(f"CAST(NULL AS STRING) AS `{col}`")

    truncate_query = f"""
    TRUNCATE TABLE `{landing_table_id}`
    """

    insert_query = f"""
    INSERT INTO `{landing_table_id}`
    (
      {", ".join([f"`{col}`" for col in landing_columns])}
    )
    SELECT
      {", ".join(select_columns)}
    FROM `{temp_table_id}`
    """

    run_query(bq_client, truncate_query)
    run_query(bq_client, insert_query)


def insert_temp_to_raw(
    bq_client: bigquery.Client,
    cfg: RuntimeConfig,
    temp_table_id: str,
    raw_table: str,
    gcs_uri: str,
    batch_id: str,
) -> None:
    raw_table_id = f"{cfg.project_id}.{cfg.staging_dataset}.{raw_table}"

    raw_columns = get_table_columns(bq_client, cfg, raw_table)
    temp_table_name = temp_table_id.split(".")[-1]
    temp_columns = get_table_columns(bq_client, cfg, temp_table_name)

    business_columns = [col for col in raw_columns if col not in METADATA_COLUMNS]

    select_columns = []

    for col in business_columns:
        if col in temp_columns:
            select_columns.append(f"`{col}`")
        else:
            select_columns.append(f"CAST(NULL AS STRING) AS `{col}`")

    delete_existing_query = f"""
    DELETE FROM `{raw_table_id}`
    WHERE source_file_name = @gcs_uri
    """

    delete_job_config = bigquery.QueryJobConfig(
        query_parameters=[
            ScalarQueryParameter("gcs_uri", "STRING", gcs_uri),
        ]
    )

    insert_query = f"""
    INSERT INTO `{raw_table_id}`
    (
      {", ".join([f"`{col}`" for col in business_columns])},
      source_file_name,
      batch_id,
      load_timestamp
    )
    SELECT
      {", ".join(select_columns)},
      @gcs_uri AS source_file_name,
      @batch_id AS batch_id,
      CURRENT_TIMESTAMP() AS load_timestamp
    FROM `{temp_table_id}`
    """

    insert_job_config = bigquery.QueryJobConfig(
        query_parameters=[
            ScalarQueryParameter("gcs_uri", "STRING", gcs_uri),
            ScalarQueryParameter("batch_id", "STRING", batch_id),
        ]
    )

    run_query(bq_client, delete_existing_query, delete_job_config)
    run_query(bq_client, insert_query, insert_job_config)


def load_file_to_landing_and_raw(
    bq_client: bigquery.Client,
    blob: storage.Blob,
    file_name: str,
    gcs_uri: str,
    source_name: str,
    landing_table: str,
    raw_table: str,
    schema_registry: dict,
    cfg: RuntimeConfig,
) -> str:
    batch_id = str(uuid.uuid4())
    start_time = datetime.now(timezone.utc)

    raw_table_id = f"{cfg.project_id}.{cfg.staging_dataset}.{raw_table}"
    temp_table_id = ""

    try:
        logger.info("Started loading file: %s", gcs_uri)

        file_columns = read_gcs_header(blob)

        is_valid, schema_result = validate_schema(
            source_name=source_name,
            file_columns=file_columns,
            schema_registry=schema_registry,
        )

        if not is_valid:
            error_message = str(schema_result)

            insert_audit_record(
                bq_client,
                cfg,
                {
                    "audit_id": str(uuid.uuid4()),
                    "pipeline_name": "retail_gcs_to_bq_staging",
                    "source_name": source_name,
                    "source_file_name": gcs_uri,
                    "target_dataset": cfg.staging_dataset,
                    "target_table": raw_table,
                    "load_start_time": start_time.isoformat(),
                    "load_end_time": datetime.now(timezone.utc).isoformat(),
                    "status": "REJECTED",
                    "rows_loaded": 0,
                    "error_message": error_message,
                    "created_by": "schema_aware_loader",
                },
            )

            insert_processed_file_record(
                bq_client=bq_client,
                cfg=cfg,
                file_name=file_name,
                gcs_uri=gcs_uri,
                source_name=source_name,
                target_table=raw_table,
                batch_id=batch_id,
                row_count=0,
                status="REJECTED",
                error_message=error_message,
            )

            logger.error("REJECTED: %s | %s", gcs_uri, error_message)
            return "REJECTED"

        new_columns = schema_result

        if new_columns:
            logger.warning(
                "Schema drift detected for %s: new columns=%s", file_name, new_columns
            )

        add_new_columns_if_needed(bq_client, cfg, landing_table, file_columns)
        add_new_columns_if_needed(bq_client, cfg, raw_table, file_columns)

        insert_audit_record(
            bq_client,
            cfg,
            {
                "audit_id": str(uuid.uuid4()),
                "pipeline_name": "retail_gcs_to_bq_staging",
                "source_name": source_name,
                "source_file_name": gcs_uri,
                "target_dataset": cfg.staging_dataset,
                "target_table": raw_table,
                "load_start_time": start_time.isoformat(),
                "load_end_time": None,
                "status": "STARTED",
                "rows_loaded": 0,
                "error_message": None,
                "created_by": "schema_aware_loader",
            },
        )

        temp_table_name = f"_tmp_{source_name}_{batch_id.replace('-', '_')}"
        temp_table_id = create_temp_load_table(
            bq_client=bq_client,
            cfg=cfg,
            temp_table_name=temp_table_name,
            file_columns=file_columns,
        )

        if cfg.dry_run:
            logger.info(
                "DRY RUN: would load %s into %s and raw table %s",
                gcs_uri,
                temp_table_id,
                raw_table,
            )
            return "DRY_RUN"

        load_csv_to_table(
            bq_client=bq_client,
            gcs_uri=gcs_uri,
            table_id=temp_table_id,
            file_columns=file_columns,
        )

        landing_count = get_row_count(bq_client, temp_table_id)

        insert_temp_to_landing(
            bq_client=bq_client,
            cfg=cfg,
            temp_table_id=temp_table_id,
            landing_table=landing_table,
        )

        insert_temp_to_raw(
            bq_client=bq_client,
            cfg=cfg,
            temp_table_id=temp_table_id,
            raw_table=raw_table,
            gcs_uri=gcs_uri,
            batch_id=batch_id,
        )

        end_time = datetime.now(timezone.utc)

        insert_audit_record(
            bq_client,
            cfg,
            {
                "audit_id": str(uuid.uuid4()),
                "pipeline_name": "retail_gcs_to_bq_staging",
                "source_name": source_name,
                "source_file_name": gcs_uri,
                "target_dataset": cfg.staging_dataset,
                "target_table": raw_table,
                "load_start_time": start_time.isoformat(),
                "load_end_time": end_time.isoformat(),
                "status": "SUCCESS",
                "rows_loaded": landing_count,
                "error_message": None,
                "created_by": "schema_aware_loader",
            },
        )

        insert_processed_file_record(
            bq_client=bq_client,
            cfg=cfg,
            file_name=file_name,
            gcs_uri=gcs_uri,
            source_name=source_name,
            target_table=raw_table,
            batch_id=batch_id,
            row_count=landing_count,
            status="SUCCESS",
            error_message=None,
        )

        logger.info("SUCCESS: %s -> %s, rows=%s", gcs_uri, raw_table_id, landing_count)
        return "SUCCESS"

    except Exception as exception:
        error_message = str(exception)[:1000]

        logger.exception("FAILED: %s", gcs_uri)

        insert_audit_record(
            bq_client,
            cfg,
            {
                "audit_id": str(uuid.uuid4()),
                "pipeline_name": "retail_gcs_to_bq_staging",
                "source_name": source_name,
                "source_file_name": gcs_uri,
                "target_dataset": cfg.staging_dataset,
                "target_table": raw_table,
                "load_start_time": start_time.isoformat(),
                "load_end_time": datetime.now(timezone.utc).isoformat(),
                "status": "FAILED",
                "rows_loaded": 0,
                "error_message": error_message,
                "created_by": "schema_aware_loader",
            },
        )

        insert_processed_file_record(
            bq_client=bq_client,
            cfg=cfg,
            file_name=file_name,
            gcs_uri=gcs_uri,
            source_name=source_name,
            target_table=raw_table,
            batch_id=batch_id,
            row_count=0,
            status="FAILED",
            error_message=error_message,
        )

        return "FAILED"

    finally:
        if temp_table_id:
            drop_temp_table(bq_client, temp_table_id)


# ============================================================
# MAIN RUNNER
# ============================================================


def build_runtime_config(args: argparse.Namespace) -> RuntimeConfig:
    project_config = load_project_config(args.config)

    project_id = get_config_value(
        args.project_id,
        project_config,
        ["gcp", "project_id"],
        DEFAULT_PROJECT_ID,
    )

    validated_bucket = get_config_value(
        args.validated_bucket,
        project_config,
        ["gcs", "validated_bucket"],
        DEFAULT_VALIDATED_BUCKET,
    )

    return RuntimeConfig(
        project_id=project_id,
        validated_bucket=validated_bucket,
        staging_dataset=args.staging_dataset,
        audit_dataset=args.audit_dataset,
        audit_table=args.audit_table,
        processed_files_table=args.processed_files_table,
        schema_registry_path=args.schema_registry,
        prefix=args.prefix,
        run_date=args.run_date,
        run_id=args.run_id or str(uuid.uuid4()),
        process_dates=args.process_dates,
        max_files=args.max_files,
        force_reprocess=args.force_reprocess,
        fail_on_error=args.fail_on_error,
        dry_run=args.dry_run,
    )


def list_candidate_blobs(
    storage_client: storage.Client,
    cfg: RuntimeConfig,
) -> List[storage.Blob]:
    bucket = storage_client.bucket(cfg.validated_bucket)
    blobs = list(bucket.list_blobs(prefix=cfg.prefix))

    blobs = [
        blob
        for blob in blobs
        if not blob.name.endswith("/") and blob.name.lower().endswith(".csv")
    ]

    if cfg.process_dates:
        process_dates = set(cfg.process_dates)

        filtered_blobs = []

        for blob in blobs:
            file_name = os.path.basename(blob.name)
            load_date = extract_load_date(file_name)

            if load_date in process_dates:
                filtered_blobs.append(blob)

        blobs = filtered_blobs

    if cfg.max_files:
        blobs = blobs[: cfg.max_files]

    return blobs


def run_loader(args: argparse.Namespace) -> int:
    cfg = build_runtime_config(args)
    schema_registry = load_schema_registry(cfg.schema_registry_path)

    logger.info("=" * 80)
    logger.info("Retail GCS to BigQuery Staging Loader Started")
    logger.info("=" * 80)
    logger.info("Project ID          : %s", cfg.project_id)
    logger.info("Validated bucket    : %s", cfg.validated_bucket)
    logger.info("Prefix              : %s", cfg.prefix)
    logger.info("Staging dataset     : %s", cfg.staging_dataset)
    logger.info("Audit dataset       : %s", cfg.audit_dataset)
    logger.info("Schema registry     : %s", cfg.schema_registry_path)
    logger.info("Run date            : %s", cfg.run_date)
    logger.info("Run ID              : %s", cfg.run_id)
    logger.info("Process dates       : %s", cfg.process_dates)
    logger.info("Force reprocess     : %s", cfg.force_reprocess)
    logger.info("Dry run             : %s", cfg.dry_run)
    logger.info("=" * 80)

    bq_client = bigquery.Client(project=cfg.project_id)
    storage_client = storage.Client(project=cfg.project_id)

    blobs = list_candidate_blobs(storage_client, cfg)

    logger.info("Files found for loading: %s", len(blobs))

    summary = {
        "SUCCESS": 0,
        "FAILED": 0,
        "REJECTED": 0,
        "SKIPPED": 0,
        "DRY_RUN": 0,
    }

    for blob in blobs:
        file_name = os.path.basename(blob.name)
        source_name = get_source_name(file_name)

        if source_name not in TABLE_MAPPING:
            logger.warning("SKIPPED unknown file: %s", file_name)
            summary["SKIPPED"] += 1
            continue

        gcs_uri = build_gcs_uri(cfg.validated_bucket, blob.name)

        if not cfg.force_reprocess and is_file_already_processed(
            bq_client, cfg, gcs_uri
        ):
            logger.info("SKIPPED already processed: %s", gcs_uri)
            summary["SKIPPED"] += 1
            continue

        mapping = TABLE_MAPPING[source_name]

        status = load_file_to_landing_and_raw(
            bq_client=bq_client,
            blob=blob,
            file_name=file_name,
            gcs_uri=gcs_uri,
            source_name=source_name,
            landing_table=mapping["landing_table"],
            raw_table=mapping["raw_table"],
            schema_registry=schema_registry,
            cfg=cfg,
        )

        summary[status] = summary.get(status, 0) + 1

    logger.info("=" * 80)
    logger.info("Retail GCS to BigQuery Staging Loader Completed")
    logger.info("Success  : %s", summary.get("SUCCESS", 0))
    logger.info("Failed   : %s", summary.get("FAILED", 0))
    logger.info("Rejected : %s", summary.get("REJECTED", 0))
    logger.info("Skipped  : %s", summary.get("SKIPPED", 0))
    logger.info("Dry run  : %s", summary.get("DRY_RUN", 0))
    logger.info("=" * 80)

    if cfg.fail_on_error and (
        summary.get("FAILED", 0) > 0 or summary.get("REJECTED", 0) > 0
    ):
        logger.error(
            "Loader completed with failed/rejected files. Exiting with code 1."
        )
        return 1

    return 0


# ============================================================
# ARGPARSE
# ============================================================


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Load validated retail CSV files from GCS into BigQuery landing/raw staging tables."
    )

    parser.add_argument(
        "--config",
        default=DEFAULT_CONFIG_PATH,
        help="Path to project config YAML file.",
    )

    parser.add_argument(
        "--project-id",
        default=None,
        help="GCP project ID.",
    )

    parser.add_argument(
        "--validated-bucket",
        default=None,
        help="GCS validated bucket name.",
    )

    parser.add_argument(
        "--prefix",
        default=DEFAULT_PREFIX,
        help="GCS prefix inside validated bucket.",
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
        "--schema-registry",
        default=DEFAULT_SCHEMA_REGISTRY_PATH,
        help="Path to schema registry YAML file.",
    )

    parser.add_argument(
        "--staging-dataset",
        default=DEFAULT_STAGING_DATASET,
        help="BigQuery staging dataset.",
    )

    parser.add_argument(
        "--audit-dataset",
        default=DEFAULT_AUDIT_DATASET,
        help="BigQuery audit dataset.",
    )

    parser.add_argument(
        "--audit-table",
        default=DEFAULT_AUDIT_TABLE,
        help="BigQuery load audit table name.",
    )

    parser.add_argument(
        "--processed-files-table",
        default=DEFAULT_PROCESSED_FILES_TABLE,
        help="BigQuery processed files table name.",
    )

    parser.add_argument(
        "--max-files",
        type=int,
        default=None,
        help="Optional file limit for testing.",
    )

    parser.add_argument(
        "--force-reprocess",
        action="store_true",
        help="Ignore processed_files SUCCESS check and reload matching files.",
    )

    parser.add_argument(
        "--fail-on-error",
        action="store_true",
        help="Return exit code 1 if any file fails or is rejected.",
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate/list work without loading data.",
    )

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    try:
        exit_code = run_loader(args)
        sys.exit(exit_code)

    except Exception as exception:
        logger.exception(
            "Fatal error in GCS to BigQuery staging loader: %s", str(exception)
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
