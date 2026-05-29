import csv
import io
import os
import re
from datetime import datetime

import pandas as pd
import yaml
from google.cloud import storage


def load_yaml(path):
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def get_config():
    project_config = load_yaml("config/project_config.yaml")
    schema_config = load_yaml("config/expected_schema_config.yaml")
    return project_config, schema_config


def validate_file_name(file_name):
    pattern = r"^(customers|products|stores|sales)_(full|delta)_\d{8}\.csv$"
    return re.match(pattern, file_name) is not None


def get_entity_from_file(file_name):
    return file_name.split("_")[0]


def copy_blob(
    storage_client, source_bucket, source_blob_name, target_bucket, target_blob_name
):
    source_bucket_obj = storage_client.bucket(source_bucket)
    source_blob = source_bucket_obj.blob(source_blob_name)

    target_bucket_obj = storage_client.bucket(target_bucket)

    source_bucket_obj.copy_blob(source_blob, target_bucket_obj, target_blob_name)


def validate_csv_content(blob, file_name, schema_config):
    errors = []

    content = blob.download_as_text()

    if not content.strip():
        errors.append("EMPTY_FILE")
        return errors, 0, []

    df = pd.read_csv(io.StringIO(content))
    row_count = len(df)
    columns = list(df.columns)

    entity = get_entity_from_file(file_name)

    if entity not in schema_config:
        errors.append(f"UNKNOWN_ENTITY: {entity}")
        return errors, row_count, columns

    mandatory_columns = schema_config[entity]["mandatory_columns"]

    missing_columns = [col for col in mandatory_columns if col not in columns]
    extra_columns = [col for col in columns if col not in mandatory_columns]

    if row_count == 0:
        errors.append("ZERO_ROW_COUNT")

    if missing_columns:
        errors.append(f"MISSING_COLUMNS: {missing_columns}")

    if extra_columns:
        print(
            f"Schema drift warning in {file_name}: extra columns found {extra_columns}"
        )

    return errors, row_count, columns


def main():
    project_config, schema_config = get_config()

    gcs_config = project_config["gcs"]

    landing_bucket = gcs_config["landing_bucket"]
    validated_bucket = gcs_config["validated_bucket"]
    rejected_bucket = gcs_config["rejected_bucket"]
    archive_bucket = gcs_config["archive_bucket"]
    project_id = project_config["gcp"]["project_id"]
    storage_client = storage.Client(project=project_id)

    blobs = storage_client.list_blobs(landing_bucket, prefix="incoming/")

    os.makedirs("audit", exist_ok=True)

    audit_file = (
        f"audit/file_validation_audit_"
        f"{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    )

    audit_rows = []

    for blob in blobs:
        if blob.name.endswith("/"):
            continue

        file_name = os.path.basename(blob.name)
        file_size = blob.size
        checksum = blob.md5_hash

        validation_status = "PASSED"
        errors = []
        row_count = 0
        columns = []

        if not file_name.endswith(".csv"):
            errors.append("INVALID_EXTENSION")

        if not validate_file_name(file_name):
            errors.append("INVALID_FILE_NAME_PATTERN")

        if file_size == 0:
            errors.append("EMPTY_FILE_SIZE")

        try:
            content_errors, row_count, columns = validate_csv_content(
                blob, file_name, schema_config
            )
            errors.extend(content_errors)

        except Exception as e:
            errors.append(f"CSV_READ_ERROR: {str(e)}")

        if errors:
            validation_status = "FAILED"
            target_bucket = rejected_bucket
            target_path = f"rejected/{file_name}"
        else:
            target_bucket = validated_bucket
            target_path = f"validated/{file_name}"

        copy_blob(storage_client, landing_bucket, blob.name, target_bucket, target_path)

        audit_rows.append(
            {
                "file_name": file_name,
                "source_path": f"gs://{landing_bucket}/{blob.name}",
                "target_path": f"gs://{target_bucket}/{target_path}",
                "file_size": file_size,
                "checksum": checksum,
                "row_count": row_count,
                "columns": "|".join(columns),
                "validation_status": validation_status,
                "validation_errors": "; ".join(errors),
                "validated_at": datetime.now().isoformat(),
            }
        )

        print(
            f"{file_name} => {validation_status} "
            f"=> gs://{target_bucket}/{target_path}"
        )

    if not audit_rows:
        print("No files found in landing bucket incoming/ path.")
        return

    with open(audit_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=audit_rows[0].keys())
        writer.writeheader()
        writer.writerows(audit_rows)

    print(f"\nAudit file created: {audit_file}")
    print(f"Archive bucket configured for later use: {archive_bucket}")


if __name__ == "__main__":
    main()
