-- =====================================================
-- customers_history_load.sql
-- Purpose: Load clean and deduplicated customer records
-- Source : retail_staging.customers_raw
-- Target : retail_history_records.customers_history
-- Logic  : DQ clean filter + latest record + MERGE
-- =====================================================

MERGE `still-resource-497715-g5.retail_history_records.customers_history` AS tgt
USING (
  WITH base AS (
    SELECT
      customer_id,
      customer_name,
      email,
      city,
      state,
      SAFE_CAST(created_date AS DATE) AS created_date,
      load_type,
      source_file_name,
      batch_id,
      load_timestamp
    FROM `still-resource-497715-g5.retail_staging.customers_raw`
  ),

  ranked AS (
    SELECT
      *,
      ROW_NUMBER() OVER (
        PARTITION BY customer_id
        ORDER BY load_timestamp DESC
      ) AS rn
    FROM base
  )

  SELECT
    customer_id,
    customer_name,
    email,
    city,
    state,
    created_date,
    load_type,
    source_file_name,
    batch_id,
    load_timestamp,
    CURRENT_TIMESTAMP() AS history_created_at,
    CURRENT_TIMESTAMP() AS history_updated_at
  FROM ranked
  WHERE rn = 1
    AND customer_id IS NOT NULL
    AND TRIM(customer_id) != ''
    AND customer_name IS NOT NULL
    AND TRIM(customer_name) != ''
    AND email IS NOT NULL
    AND TRIM(email) != ''
    AND REGEXP_CONTAINS(
      LOWER(TRIM(email)),
      r'^[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}$'
    )
    AND city IS NOT NULL
    AND TRIM(city) != ''
    AND state IS NOT NULL
    AND TRIM(state) != ''
    AND created_date IS NOT NULL
    AND created_date <= CURRENT_DATE()
    AND load_type IS NOT NULL
    AND TRIM(load_type) != ''
    AND UPPER(TRIM(load_type)) IN ('FULL', 'DELTA')
    AND source_file_name IS NOT NULL
    AND TRIM(source_file_name) != ''
    AND batch_id IS NOT NULL
    AND TRIM(batch_id) != ''
    AND load_timestamp IS NOT NULL
) AS src
ON tgt.customer_id = src.customer_id

WHEN MATCHED THEN
  UPDATE SET
    tgt.customer_name = src.customer_name,
    tgt.email = src.email,
    tgt.city = src.city,
    tgt.state = src.state,
    tgt.created_date = src.created_date,
    tgt.load_type = src.load_type,
    tgt.source_file_name = src.source_file_name,
    tgt.batch_id = src.batch_id,
    tgt.load_timestamp = src.load_timestamp,
    tgt.history_updated_at = CURRENT_TIMESTAMP()

WHEN NOT MATCHED THEN
  INSERT (
    customer_id,
    customer_name,
    email,
    city,
    state,
    created_date,
    load_type,
    source_file_name,
    batch_id,
    load_timestamp,
    history_created_at,
    history_updated_at
  )
  VALUES (
    src.customer_id,
    src.customer_name,
    src.email,
    src.city,
    src.state,
    src.created_date,
    src.load_type,
    src.source_file_name,
    src.batch_id,
    src.load_timestamp,
    CURRENT_TIMESTAMP(),
    CURRENT_TIMESTAMP()
  );