import uuid
import yaml
from datetime import datetime, timezone
from google.cloud import bigquery, storage

PROJECT_ID = "still-resource-497715-g5"
BUCKET_NAME = "retail-validated-dev-raj1779989813"

STAGING_DATASET = "retail_staging"
AUDIT_DATASET = "retail_audit"
AUDIT_TABLE = "load_audit_log"
PROCESSED_FILES_TABLE = "processed_files"

SCHEMA_REGISTRY_PATH = "config/schema_registry.yaml"

TABLE_MAPPING = {
    "customers": {"landing_table": "customers_landing", "raw_table": "customers_raw"},
    "products": {"landing_table": "products_landing", "raw_table": "products_raw"},
    "stores": {"landing_table": "stores_landing", "raw_table": "stores_raw"},
    "sales": {"landing_table": "sales_landing", "raw_table": "sales_raw"},
}


def load_schema_registry():
    with open(SCHEMA_REGISTRY_PATH, "r") as file:
        return yaml.safe_load(file)


def run_query(bq_client, query):
    job = bq_client.query(query)
    job.result()


def insert_audit_record(bq_client, record):
    table_id = f"{PROJECT_ID}.{AUDIT_DATASET}.{AUDIT_TABLE}"
    errors = bq_client.insert_rows_json(table_id, [record])
    if errors:
        print(f"Audit insert error: {errors}")


def insert_processed_file_record(
    bq_client,
    file_name,
    gcs_uri,
    source_name,
    target_table,
    batch_id,
    row_count,
    status,
    error_message=None,
):
    table_id = f"{PROJECT_ID}.{AUDIT_DATASET}.{PROCESSED_FILES_TABLE}"

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
        print(f"Processed file insert error: {errors}")


def is_file_already_processed(bq_client, gcs_uri):
    query = f"""
    SELECT COUNT(*) AS cnt
    FROM `{PROJECT_ID}.{AUDIT_DATASET}.{PROCESSED_FILES_TABLE}`
    WHERE gcs_uri = '{gcs_uri}'
      AND status = 'SUCCESS'
    """
    result = list(bq_client.query(query).result())[0]["cnt"]
    return result > 0


def get_table_columns(bq_client, table_name):
    query = f"""
    SELECT column_name
    FROM `{PROJECT_ID}.{STAGING_DATASET}.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = '{table_name}'
    ORDER BY ordinal_position
    """
    return [row["column_name"] for row in bq_client.query(query).result()]


def read_gcs_header(blob):
    first_line = blob.download_as_text().splitlines()[0]
    return [col.strip() for col in first_line.split(",")]


def validate_schema(source_name, file_columns, schema_registry):
    rules = schema_registry[source_name]
    mandatory_columns = rules["mandatory_columns"]

    duplicate_columns = list(
        {col for col in file_columns if file_columns.count(col) > 1}
    )

    if duplicate_columns:
        return False, f"Duplicate columns found: {duplicate_columns}"

    missing_columns = [col for col in mandatory_columns if col not in file_columns]

    if missing_columns:
        return False, f"Missing mandatory columns: {missing_columns}"

    new_columns = [col for col in file_columns if col not in mandatory_columns]

    if new_columns and not rules.get("allow_new_columns", False):
        return False, f"New columns not allowed: {new_columns}"

    return True, new_columns


def add_new_columns_if_needed(bq_client, table_name, file_columns):
    existing_columns = get_table_columns(bq_client, table_name)

    for col in file_columns:
        if col not in existing_columns:
            alter_sql = f"""
            ALTER TABLE `{PROJECT_ID}.{STAGING_DATASET}.{table_name}`
            ADD COLUMN `{col}` STRING
            """
            run_query(bq_client, alter_sql)
            print(f"ADDED COLUMN: {table_name}.{col}")


def get_row_count(bq_client, table_id):
    query = f"SELECT COUNT(*) AS row_count FROM `{table_id}`"
    return list(bq_client.query(query).result())[0]["row_count"]


def load_file_to_landing_and_raw(
    bq_client,
    blob,
    file_name,
    gcs_uri,
    source_name,
    landing_table,
    raw_table,
    schema_registry,
):
    batch_id = str(uuid.uuid4())
    start_time = datetime.now(timezone.utc)

    landing_table_id = f"{PROJECT_ID}.{STAGING_DATASET}.{landing_table}"
    raw_table_id = f"{PROJECT_ID}.{STAGING_DATASET}.{raw_table}"

    try:
        file_columns = read_gcs_header(blob)

        is_valid, schema_result = validate_schema(
            source_name=source_name,
            file_columns=file_columns,
            schema_registry=schema_registry,
        )

        if not is_valid:
            error_message = schema_result

            insert_audit_record(
                bq_client,
                {
                    "audit_id": str(uuid.uuid4()),
                    "pipeline_name": "retail_gcs_to_bq_staging",
                    "source_name": source_name,
                    "source_file_name": gcs_uri,
                    "target_dataset": STAGING_DATASET,
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
                file_name=file_name,
                gcs_uri=gcs_uri,
                source_name=source_name,
                target_table=raw_table,
                batch_id=batch_id,
                row_count=0,
                status="REJECTED",
                error_message=error_message,
            )

            print(f"REJECTED: {gcs_uri} | {error_message}")
            return

        # Auto-add allowed new columns to landing and raw tables
        add_new_columns_if_needed(bq_client, landing_table, file_columns)
        add_new_columns_if_needed(bq_client, raw_table, file_columns)

        insert_audit_record(
            bq_client,
            {
                "audit_id": str(uuid.uuid4()),
                "pipeline_name": "retail_gcs_to_bq_staging",
                "source_name": source_name,
                "source_file_name": gcs_uri,
                "target_dataset": STAGING_DATASET,
                "target_table": raw_table,
                "load_start_time": start_time.isoformat(),
                "load_end_time": None,
                "status": "STARTED",
                "rows_loaded": 0,
                "error_message": None,
                "created_by": "schema_aware_loader",
            },
        )

        # Landing is temporary for each file
        run_query(bq_client, f"TRUNCATE TABLE `{landing_table_id}`")

        load_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            autodetect=False,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            allow_jagged_rows=True,
        )

        load_job = bq_client.load_table_from_uri(
            gcs_uri, landing_table_id, job_config=load_config
        )
        load_job.result()

        landing_count = get_row_count(bq_client, landing_table_id)

        delete_existing_query = f"""
         DELETE FROM `{raw_table_id}`
          WHERE source_file_name = '{gcs_uri}';
        """
        run_query(bq_client, delete_existing_query)

        landing_columns = get_table_columns(bq_client, landing_table)
        raw_columns = get_table_columns(bq_client, raw_table)

        business_columns = [
            col
            for col in raw_columns
            if col not in ("source_file_name", "batch_id", "load_timestamp")
        ]

        select_columns = []
        for col in business_columns:
            if col in landing_columns:
                select_columns.append(f"`{col}`")
            else:
                select_columns.append(f"CAST(NULL AS STRING) AS `{col}`")

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
          '{gcs_uri}' AS source_file_name,
          '{batch_id}' AS batch_id,
          CURRENT_TIMESTAMP() AS load_timestamp
        FROM `{landing_table_id}`;
        """

        run_query(bq_client, insert_query)

        end_time = datetime.now(timezone.utc)

        insert_audit_record(
            bq_client,
            {
                "audit_id": str(uuid.uuid4()),
                "pipeline_name": "retail_gcs_to_bq_staging",
                "source_name": source_name,
                "source_file_name": gcs_uri,
                "target_dataset": STAGING_DATASET,
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
            file_name=file_name,
            gcs_uri=gcs_uri,
            source_name=source_name,
            target_table=raw_table,
            batch_id=batch_id,
            row_count=landing_count,
            status="SUCCESS",
            error_message=None,
        )

        print(f"SUCCESS: {gcs_uri} -> {raw_table_id}, rows={landing_count}")

    except Exception as e:
        error_message = str(e)[:1000]

        insert_audit_record(
            bq_client,
            {
                "audit_id": str(uuid.uuid4()),
                "pipeline_name": "retail_gcs_to_bq_staging",
                "source_name": source_name,
                "source_file_name": gcs_uri,
                "target_dataset": STAGING_DATASET,
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
            file_name=file_name,
            gcs_uri=gcs_uri,
            source_name=source_name,
            target_table=raw_table,
            batch_id=batch_id,
            row_count=0,
            status="FAILED",
            error_message=error_message,
        )

        print(f"FAILED: {gcs_uri}")
        print(error_message)


def main():
    schema_registry = load_schema_registry()

    bq_client = bigquery.Client(project=PROJECT_ID)
    storage_client = storage.Client(project=PROJECT_ID)

    bucket = storage_client.bucket(BUCKET_NAME)
    blobs = bucket.list_blobs(prefix="validated/")

    for blob in blobs:
        file_name = blob.name.split("/")[-1]

        if not file_name.endswith(".csv"):
            continue

        source_name = file_name.split("_")[0]

        if source_name not in TABLE_MAPPING:
            print(f"SKIPPED unknown file: {file_name}")
            continue

        gcs_uri = f"gs://{BUCKET_NAME}/{blob.name}"

        if is_file_already_processed(bq_client, gcs_uri):
            print(f"SKIPPED already processed: {gcs_uri}")
            continue

        mapping = TABLE_MAPPING[source_name]

        load_file_to_landing_and_raw(
            bq_client=bq_client,
            blob=blob,
            file_name=file_name,
            gcs_uri=gcs_uri,
            source_name=source_name,
            landing_table=mapping["landing_table"],
            raw_table=mapping["raw_table"],
            schema_registry=schema_registry,
        )


if __name__ == "__main__":
    main()
