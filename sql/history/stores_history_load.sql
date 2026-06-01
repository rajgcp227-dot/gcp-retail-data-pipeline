-- =====================================================
-- stores_history_load.sql
-- Purpose: Load clean and deduplicated store records
-- Source : retail_staging.stores_raw
-- Target : retail_history_records.stores_history
-- Logic  : DQ clean filter + latest record + MERGE
-- =====================================================

MERGE `still-resource-497715-g5.retail_history_records.stores_history` AS tgt
USING (
  WITH base AS (
    SELECT
      store_id,
      store_name,
      city,
      state,
      region,
      SAFE_CAST(created_date AS DATE) AS created_date,
      load_type,
      source_file_name,
      batch_id,
      load_timestamp
    FROM `still-resource-497715-g5.retail_staging.stores_raw`
  ),

  ranked AS (
    SELECT
      *,
      ROW_NUMBER() OVER (
        PARTITION BY store_id
        ORDER BY load_timestamp DESC
      ) AS rn
    FROM base
  )

  SELECT
    store_id,
    store_name,
    city,
    state,
    region,
    created_date,
    load_type,
    source_file_name,
    batch_id,
    load_timestamp,
    CURRENT_TIMESTAMP() AS history_created_at,
    CURRENT_TIMESTAMP() AS history_updated_at
  FROM ranked
  WHERE rn = 1
    AND store_id IS NOT NULL
    AND TRIM(store_id) != ''
    AND store_name IS NOT NULL
    AND TRIM(store_name) != ''
    AND city IS NOT NULL
    AND TRIM(city) != ''
    AND state IS NOT NULL
    AND TRIM(state) != ''
    AND region IS NOT NULL
    AND TRIM(region) != ''
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
ON tgt.store_id = src.store_id

WHEN MATCHED THEN
  UPDATE SET
    tgt.store_name = src.store_name,
    tgt.city = src.city,
    tgt.state = src.state,
    tgt.region = src.region,
    tgt.created_date = src.created_date,
    tgt.load_type = src.load_type,
    tgt.source_file_name = src.source_file_name,
    tgt.batch_id = src.batch_id,
    tgt.load_timestamp = src.load_timestamp,
    tgt.history_updated_at = CURRENT_TIMESTAMP()

WHEN NOT MATCHED THEN
  INSERT (
    store_id,
    store_name,
    city,
    state,
    region,
    created_date,
    load_type,
    source_file_name,
    batch_id,
    load_timestamp,
    history_created_at,
    history_updated_at
  )
  VALUES (
    src.store_id,
    src.store_name,
    src.city,
    src.state,
    src.region,
    src.created_date,
    src.load_type,
    src.source_file_name,
    src.batch_id,
    src.load_timestamp,
    CURRENT_TIMESTAMP(),
    CURRENT_TIMESTAMP()
  );