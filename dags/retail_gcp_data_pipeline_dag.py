# ============================================================
# retail_gcp_data_pipeline_dag.py
# Production-style Retail Data Engineering Pipeline with Logging
#
# Flow:
# GCS Landing
#   -> File Validation
#   -> BigQuery Raw Load
#   -> DQ + Quarantine + Audit
#   -> BigQuery Snapshots
#   -> History MERGE Loads
#   -> Reporting Star Schema
#   -> Reporting Views / Materialized Views / Secure Views
#   -> Final Reconciliation + Assertions
#
# Author: Raj
# ============================================================

from __future__ import annotations

import logging
import traceback
from datetime import timedelta

import pendulum
from google.cloud import bigquery

from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

# ============================================================
# LOGGER
# ============================================================

logger = logging.getLogger("airflow.task")


# ============================================================
# AIRFLOW VARIABLES
# ============================================================
# Create these in Airflow UI later:
#
# retail_project_id
# retail_bq_location
# retail_env
# retail_landing_bucket
# retail_validated_bucket
# retail_rejected_bucket
# retail_archive_bucket
# retail_alert_email
# ============================================================

PROJECT_ID = Variable.get("retail_project_id", default_var="still-resource-497715-g5")
BQ_LOCATION = Variable.get("retail_bq_location", default_var="asia-south1")
ENV = Variable.get("retail_env", default_var="dev")

LANDING_BUCKET = Variable.get(
    "retail_landing_bucket",
    default_var="retail-landing-dev-raj1779989813",
)

VALIDATED_BUCKET = Variable.get(
    "retail_validated_bucket",
    default_var="retail-validated-dev-raj1779989813",
)

REJECTED_BUCKET = Variable.get(
    "retail_rejected_bucket",
    default_var="retail-rejected-dev-raj1779989813",
)

ARCHIVE_BUCKET = Variable.get(
    "retail_archive_bucket",
    default_var="retail-archive-dev-raj1779989813",
)

ALERT_EMAIL = Variable.get("retail_alert_email", default_var="")


# ============================================================
# COMPOSER PATHS
# ============================================================

DAGS_HOME = "/home/airflow/gcs/dags"
SCRIPTS_HOME = f"{DAGS_HOME}/scripts"


# ============================================================
# BIGQUERY LOGGING FUNCTION
# ============================================================


def write_pipeline_log(
    context,
    status: str,
    message: str,
    error_message: str | None = None,
) -> None:
    """
    Writes task/DAG status into BigQuery audit table.

    This gives permanent pipeline run history beyond Airflow logs.
    """

    try:
        dag = context.get("dag")
        task_instance = context.get("task_instance")

        dag_id = dag.dag_id if dag else "unknown_dag"
        task_id = task_instance.task_id if task_instance else "dag_level_callback"
        run_id = context.get("run_id", "unknown_run_id")
        logical_date = context.get("logical_date")

        created_at = pendulum.now("UTC")

        row = {
            "log_id": f"{dag_id}_{task_id}_{run_id}_{status}_{created_at.int_timestamp}",
            "dag_id": dag_id,
            "task_id": task_id,
            "run_id": run_id,
            "execution_date": logical_date.isoformat() if logical_date else None,
            "status": status,
            "message": message,
            "error_message": error_message,
            "project_id": PROJECT_ID,
            "environment": ENV,
            "created_at": created_at.isoformat(),
        }

        client = bigquery.Client(project=PROJECT_ID)
        table_id = f"{PROJECT_ID}.retail_audit_records.pipeline_run_log"

        errors = client.insert_rows_json(table_id, [row])

        if errors:
            logger.error("Failed to insert pipeline log into BigQuery: %s", errors)
        else:
            logger.info("Pipeline log inserted into BigQuery: %s", row)

    except Exception as log_exception:
        logger.error("Error while writing pipeline log: %s", str(log_exception))
        logger.error(traceback.format_exc())


# ============================================================
# CALLBACK FUNCTIONS
# ============================================================


def notify_failure(context):
    """
    Runs when any task fails.
    Logs failure to Airflow logs and BigQuery pipeline_run_log.
    """

    task_instance = context.get("task_instance")
    exception = context.get("exception")
    error_message = str(exception) if exception else "Unknown failure"

    logger.error("============================================================")
    logger.error("RETAIL PIPELINE FAILURE")
    logger.error(
        "DAG ID  : %s", context.get("dag").dag_id if context.get("dag") else "unknown"
    )
    logger.error("TASK ID : %s", task_instance.task_id if task_instance else "unknown")
    logger.error("RUN ID  : %s", context.get("run_id"))
    logger.error("ERROR   : %s", error_message)
    logger.error("============================================================")

    write_pipeline_log(
        context=context,
        status="FAILED",
        message="Task failed during retail data pipeline execution",
        error_message=error_message,
    )


def notify_retry(context):
    """
    Runs when a task is retried.
    Logs retry to Airflow logs and BigQuery pipeline_run_log.
    """

    task_instance = context.get("task_instance")

    logger.warning("============================================================")
    logger.warning("RETAIL PIPELINE TASK RETRY")
    logger.warning(
        "DAG ID  : %s", context.get("dag").dag_id if context.get("dag") else "unknown"
    )
    logger.warning(
        "TASK ID : %s", task_instance.task_id if task_instance else "unknown"
    )
    logger.warning("RUN ID  : %s", context.get("run_id"))
    logger.warning("============================================================")

    write_pipeline_log(
        context=context,
        status="RETRYING",
        message="Task is retrying",
        error_message=None,
    )


def notify_success(context):
    """
    Runs when the DAG succeeds.
    Logs success to Airflow logs and BigQuery pipeline_run_log.
    """

    logger.info("============================================================")
    logger.info("RETAIL PIPELINE SUCCESS")
    logger.info(
        "DAG ID : %s", context.get("dag").dag_id if context.get("dag") else "unknown"
    )
    logger.info("RUN ID : %s", context.get("run_id"))
    logger.info("============================================================")

    write_pipeline_log(
        context=context,
        status="SUCCESS",
        message="Retail data pipeline completed successfully",
        error_message=None,
    )


# ============================================================
# DEFAULT TASK ARGUMENTS
# ============================================================

default_args = {
    "owner": "raj-data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
    "on_failure_callback": notify_failure,
    "on_retry_callback": notify_retry,
    "email_on_failure": False,
    "email_on_retry": False,
}


# ============================================================
# HELPER FUNCTION: BIGQUERY SQL FILE TASK
# ============================================================


def bq_sql_file_task(
    task_id: str,
    sql_file_path: str,
    priority_weight: int = 1,
) -> BigQueryInsertJobOperator:
    """
    Creates a BigQuery task using an external SQL file.

    Example:
      bq_sql_file_task(
          task_id="customers_dq",
          sql_file_path="sql/dq/customers_dq.sql"
      )

    Airflow reads the SQL file from template_searchpath.
    """

    return BigQueryInsertJobOperator(
        task_id=task_id,
        location=BQ_LOCATION,
        priority_weight=priority_weight,
        configuration={
            "query": {
                "query": "{% include '" + sql_file_path + "' %}",
                "useLegacySql": False,
                "priority": "INTERACTIVE",
            },
            "labels": {
                "pipeline": "retail_data_pipeline",
                "env": ENV,
                "layer": "bigquery",
            },
        },
    )


# ============================================================
# DAG DEFINITION
# ============================================================

with DAG(
    dag_id="retail_gcp_data_pipeline_prod",
    description=(
        "Production-style Retail GCP Data Pipeline with validation, "
        "DQ, quarantine, audit, history, reporting, snapshots, secure views, and logging"
    ),
    default_args=default_args,
    start_date=pendulum.datetime(2026, 6, 1, tz="Asia/Kolkata"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=4),
    template_searchpath=[DAGS_HOME],
    tags=[
        "retail",
        "gcp",
        "bigquery",
        "composer",
        "data-engineering",
        "dq",
        "reporting",
        ENV,
    ],
    on_success_callback=notify_success,
) as dag:

    # ========================================================
    # START / END MARKERS
    # ========================================================

    start = EmptyOperator(task_id="start")

    end = EmptyOperator(
        task_id="end",
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    failure_stop = EmptyOperator(
        task_id="failure_stop",
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    # ========================================================
    # STEP 0: ENVIRONMENT CHECK
    # ========================================================

    environment_check = BashOperator(
        task_id="environment_check",
        bash_command=f"""
        set -euo pipefail

        echo "============================================================"
        echo "TASK STARTED: environment_check"
        echo "============================================================"
        echo "PROJECT_ID       : {PROJECT_ID}"
        echo "BQ_LOCATION      : {BQ_LOCATION}"
        echo "ENV              : {ENV}"
        echo "LANDING_BUCKET   : {LANDING_BUCKET}"
        echo "VALIDATED_BUCKET : {VALIDATED_BUCKET}"
        echo "REJECTED_BUCKET  : {REJECTED_BUCKET}"
        echo "ARCHIVE_BUCKET   : {ARCHIVE_BUCKET}"
        echo "DAGS_HOME        : {DAGS_HOME}"
        echo "SCRIPTS_HOME     : {SCRIPTS_HOME}"
        echo "RUN_DATE         : {{{{ ds }}}}"
        echo "RUN_ID           : {{{{ run_id }}}}"
        echo "============================================================"

        test -f {SCRIPTS_HOME}/gcs_file_validator.py
        test -f {SCRIPTS_HOME}/load_gcs_to_bq_staging.py

        test -d {DAGS_HOME}/sql/dq
        test -d {DAGS_HOME}/sql/history
        test -d {DAGS_HOME}/sql/reporting
        test -d {DAGS_HOME}/sql/admin

        echo "Environment check passed."
        echo "TASK COMPLETED: environment_check"
        """,
    )

    # ========================================================
    # STEP 1: CREATE PIPELINE LOG TABLE
    # ========================================================

    create_pipeline_log_table = bq_sql_file_task(
        task_id="create_pipeline_log_table",
        sql_file_path="sql/admin/create_pipeline_log_table.sql",
    )

    create_loader_audit_tables = bq_sql_file_task(
        task_id="create_loader_audit_tables",
        sql_file_path="sql/admin/create_loader_audit_tables.sql",
    )

    # ========================================================
    # STEP 2: FILE VALIDATION
    # ========================================================

    validate_gcs_files = BashOperator(
        task_id="validate_gcs_files",
        bash_command=f"""
        set -euo pipefail

        echo "============================================================"
        echo "TASK STARTED: validate_gcs_files"
        echo "PROJECT_ID       : {PROJECT_ID}"
        echo "LANDING_BUCKET   : {LANDING_BUCKET}"
        echo "VALIDATED_BUCKET : {VALIDATED_BUCKET}"
        echo "REJECTED_BUCKET  : {REJECTED_BUCKET}"
        echo "ARCHIVE_BUCKET   : {ARCHIVE_BUCKET}"
        echo "RUN_DATE         : {{{{ ds }}}}"
        echo "RUN_ID           : {{{{ run_id }}}}"
        echo "============================================================"

        cd {DAGS_HOME}

        python3 {SCRIPTS_HOME}/gcs_file_validator.py \
          --project-id "{PROJECT_ID}" \
          --landing-bucket "{LANDING_BUCKET}" \
          --validated-bucket "{VALIDATED_BUCKET}" \
          --rejected-bucket "{REJECTED_BUCKET}" \
          --archive-bucket "{ARCHIVE_BUCKET}" \
          --run-date "{{{{ ds }}}}" \
          --run-id "{{{{ run_id }}}}"

        echo "TASK COMPLETED: validate_gcs_files"
        echo "============================================================"
        """,
    )

    # ========================================================
    # STEP 3: LOAD VALIDATED FILES TO BIGQUERY RAW
    # ========================================================

    load_gcs_to_bq_raw = BashOperator(
        task_id="load_gcs_to_bq_raw",
        bash_command=f"""
        set -euo pipefail

        echo "============================================================"
        echo "TASK STARTED: load_gcs_to_bq_raw"
        echo "PROJECT_ID       : {PROJECT_ID}"
        echo "VALIDATED_BUCKET : {VALIDATED_BUCKET}"
        echo "RUN_DATE         : {{{{ ds }}}}"
        echo "RUN_ID           : {{{{ run_id }}}}"
        echo "============================================================"

        cd {DAGS_HOME}

        python3 {SCRIPTS_HOME}/load_gcs_to_bq_staging.py \
          --project-id "{PROJECT_ID}" \
          --validated-bucket "{VALIDATED_BUCKET}" \
          --run-date "{{{{ ds }}}}" \
          --run-id "{{{{ run_id }}}}"

        echo "TASK COMPLETED: load_gcs_to_bq_raw"
        echo "============================================================"
        """,
    )

    # ========================================================
    # STEP 4: DATA QUALITY LAYER
    # ========================================================

    with TaskGroup(group_id="dq_layer") as dq_layer:

        customers_dq = bq_sql_file_task(
            task_id="customers_dq",
            sql_file_path="sql/dq/customers_dq.sql",
            priority_weight=5,
        )

        products_dq = bq_sql_file_task(
            task_id="products_dq",
            sql_file_path="sql/dq/products_dq.sql",
            priority_weight=5,
        )

        stores_dq = bq_sql_file_task(
            task_id="stores_dq",
            sql_file_path="sql/dq/stores_dq.sql",
            priority_weight=5,
        )

        sales_dq = bq_sql_file_task(
            task_id="sales_dq",
            sql_file_path="sql/dq/sales_dq.sql",
            priority_weight=10,
        )

        [customers_dq, products_dq, stores_dq] >> sales_dq

    # ========================================================
    # STEP 5: BACKUP DATASET
    # ========================================================

    create_backup_dataset = bq_sql_file_task(
        task_id="create_backup_dataset",
        sql_file_path="sql/admin/create_backup_dataset.sql",
    )

    # ========================================================
    # STEP 6: HISTORY SNAPSHOTS
    # ========================================================

    create_history_snapshots = bq_sql_file_task(
        task_id="create_history_snapshots",
        sql_file_path="sql/admin/create_history_snapshots.sql",
    )

    # ========================================================
    # STEP 7: HISTORY LAYER
    # ========================================================

    with TaskGroup(group_id="history_layer") as history_layer:

        customers_history_load = bq_sql_file_task(
            task_id="customers_history_load",
            sql_file_path="sql/history/customers_history_load.sql",
            priority_weight=5,
        )

        products_history_load = bq_sql_file_task(
            task_id="products_history_load",
            sql_file_path="sql/history/products_history_load.sql",
            priority_weight=5,
        )

        stores_history_load = bq_sql_file_task(
            task_id="stores_history_load",
            sql_file_path="sql/history/stores_history_load.sql",
            priority_weight=5,
        )

        sales_history_load = bq_sql_file_task(
            task_id="sales_history_load",
            sql_file_path="sql/history/sales_history_load.sql",
            priority_weight=10,
        )

        [
            customers_history_load,
            products_history_load,
            stores_history_load,
        ] >> sales_history_load

    # ========================================================
    # STEP 8: REPORTING SNAPSHOTS
    # ========================================================

    create_reporting_snapshots = bq_sql_file_task(
        task_id="create_reporting_snapshots",
        sql_file_path="sql/admin/create_reporting_snapshots.sql",
    )

    # ========================================================
    # STEP 9: REPORTING STAR SCHEMA
    # ========================================================

    with TaskGroup(group_id="reporting_star_schema") as reporting_star_schema:

        create_dim_customer = bq_sql_file_task(
            task_id="create_dim_customer",
            sql_file_path="sql/reporting/create_dim_customer.sql",
            priority_weight=5,
        )

        create_dim_product = bq_sql_file_task(
            task_id="create_dim_product",
            sql_file_path="sql/reporting/create_dim_product.sql",
            priority_weight=5,
        )

        create_dim_store = bq_sql_file_task(
            task_id="create_dim_store",
            sql_file_path="sql/reporting/create_dim_store.sql",
            priority_weight=5,
        )

        create_fact_sales = bq_sql_file_task(
            task_id="create_fact_sales",
            sql_file_path="sql/reporting/create_fact_sales.sql",
            priority_weight=10,
        )

        [create_dim_customer, create_dim_product, create_dim_store] >> create_fact_sales

    # ========================================================
    # STEP 10: NORMAL REPORTING VIEWS
    # ========================================================

    with TaskGroup(group_id="reporting_views") as reporting_views:

        vw_sales_detail = bq_sql_file_task(
            task_id="vw_sales_detail",
            sql_file_path="sql/reporting/views/vw_sales_detail.sql",
        )

        vw_daily_sales_summary = bq_sql_file_task(
            task_id="vw_daily_sales_summary",
            sql_file_path="sql/reporting/views/vw_daily_sales_summary.sql",
        )

        vw_sales_by_state = bq_sql_file_task(
            task_id="vw_sales_by_state",
            sql_file_path="sql/reporting/views/vw_sales_by_state.sql",
        )

        vw_sales_by_product_category = bq_sql_file_task(
            task_id="vw_sales_by_product_category",
            sql_file_path="sql/reporting/views/vw_sales_by_product_category.sql",
        )

        vw_store_performance = bq_sql_file_task(
            task_id="vw_store_performance",
            sql_file_path="sql/reporting/views/vw_store_performance.sql",
        )

        vw_payment_method_summary = bq_sql_file_task(
            task_id="vw_payment_method_summary",
            sql_file_path="sql/reporting/views/vw_payment_method_summary.sql",
        )

        vw_sales_detail >> [
            vw_daily_sales_summary,
            vw_sales_by_state,
            vw_sales_by_product_category,
            vw_store_performance,
            vw_payment_method_summary,
        ]

    # ========================================================
    # STEP 11: MATERIALIZED VIEWS
    # ========================================================

    create_materialized_views = bq_sql_file_task(
        task_id="create_materialized_views",
        sql_file_path="sql/reporting/create_materialized_views.sql",
        priority_weight=5,
    )

    # ========================================================
    # STEP 12: SECURE / AUTHORIZED VIEWS
    # ========================================================

    create_authorized_views = bq_sql_file_task(
        task_id="create_authorized_views",
        sql_file_path="sql/reporting/create_authorized_views.sql",
        priority_weight=5,
    )

    # ========================================================
    # STEP 13: FINAL RECONCILIATION
    # ========================================================

    final_reconciliation = BigQueryInsertJobOperator(
        task_id="final_reconciliation",
        location=BQ_LOCATION,
        configuration={
            "query": {
                "query": f"""
                SELECT
                  'dim_customer' AS table_name,
                  COUNT(*) AS row_count
                FROM `{PROJECT_ID}.retail_reporting_records.dim_customer`

                UNION ALL

                SELECT
                  'dim_product' AS table_name,
                  COUNT(*) AS row_count
                FROM `{PROJECT_ID}.retail_reporting_records.dim_product`

                UNION ALL

                SELECT
                  'dim_store' AS table_name,
                  COUNT(*) AS row_count
                FROM `{PROJECT_ID}.retail_reporting_records.dim_store`

                UNION ALL

                SELECT
                  'fact_sales' AS table_name,
                  COUNT(*) AS row_count
                FROM `{PROJECT_ID}.retail_reporting_records.fact_sales`;
                """,
                "useLegacySql": False,
                "priority": "INTERACTIVE",
            },
            "labels": {
                "pipeline": "retail_data_pipeline",
                "env": ENV,
                "layer": "reconciliation",
            },
        },
    )

    # ========================================================
    # STEP 14: FINAL ASSERTIONS
    # ========================================================

    final_assertions = BigQueryInsertJobOperator(
        task_id="final_assertions",
        location=BQ_LOCATION,
        configuration={
            "query": {
                "query": f"""
                ASSERT (
                  SELECT COUNT(*)
                  FROM `{PROJECT_ID}.retail_reporting_records.fact_sales`
                ) > 0 AS 'fact_sales has zero records';

                ASSERT (
                  SELECT COUNT(*)
                  FROM `{PROJECT_ID}.retail_reporting_records.dim_customer`
                ) > 0 AS 'dim_customer has zero records';

                ASSERT (
                  SELECT COUNT(*)
                  FROM `{PROJECT_ID}.retail_reporting_records.dim_product`
                ) > 0 AS 'dim_product has zero records';

                ASSERT (
                  SELECT COUNT(*)
                  FROM `{PROJECT_ID}.retail_reporting_records.dim_store`
                ) > 0 AS 'dim_store has zero records';

                ASSERT (
                  SELECT COUNT(*)
                  FROM `{PROJECT_ID}.retail_reporting_records.fact_sales`
                  WHERE customer_sk IS NULL
                     OR product_sk IS NULL
                     OR store_sk IS NULL
                ) = 0 AS 'fact_sales has missing dimension surrogate keys';

                ASSERT (
                  SELECT COUNT(*)
                  FROM `{PROJECT_ID}.retail_reporting_records.fact_sales`
                  WHERE quantity IS NULL
                     OR quantity <= 0
                     OR sale_amount IS NULL
                     OR sale_amount <= 0
                     OR sale_date IS NULL
                ) = 0 AS 'fact_sales has invalid measures';

                ASSERT (
                  SELECT COUNT(*)
                  FROM (
                    SELECT order_id
                    FROM `{PROJECT_ID}.retail_reporting_records.fact_sales`
                    GROUP BY order_id
                    HAVING COUNT(*) > 1
                  )
                ) = 0 AS 'fact_sales has duplicate order_id records';
                """,
                "useLegacySql": False,
                "priority": "INTERACTIVE",
            },
            "labels": {
                "pipeline": "retail_data_pipeline",
                "env": ENV,
                "layer": "assertions",
            },
        },
    )

    # ========================================================
    # MAIN DEPENDENCY FLOW
    # ========================================================

    (
        start
        >> environment_check
        >> create_pipeline_log_table
        >> create_loader_audit_tables
        >> validate_gcs_files
        >> validate_gcs_files
        >> load_gcs_to_bq_raw
        >> dq_layer
        >> create_backup_dataset
        >> create_history_snapshots
        >> history_layer
        >> create_reporting_snapshots
        >> reporting_star_schema
        >> reporting_views
        >> create_materialized_views
        >> create_authorized_views
        >> final_reconciliation
        >> final_assertions
        >> end
    )

    # ========================================================
    # FAILURE MARKER FLOW
    # ========================================================

    [
        environment_check,
        create_pipeline_log_table,
        create_loader_audit_tables,
        validate_gcs_files,
        load_gcs_to_bq_raw,
        dq_layer,
        create_backup_dataset,
        create_history_snapshots,
        history_layer,
        create_reporting_snapshots,
        reporting_star_schema,
        reporting_views,
        create_materialized_views,
        create_authorized_views,
        final_reconciliation,
        final_assertions,
    ] >> failure_stop
