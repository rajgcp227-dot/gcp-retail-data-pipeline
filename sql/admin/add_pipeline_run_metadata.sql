-- ============================================================
-- Add shared pipeline-run metadata to raw tables
-- ============================================================

ALTER TABLE `still-resource-497715-g5.retail_staging.customers_raw`
ADD COLUMN IF NOT EXISTS pipeline_run_id STRING;

ALTER TABLE `still-resource-497715-g5.retail_staging.products_raw`
ADD COLUMN IF NOT EXISTS pipeline_run_id STRING;

ALTER TABLE `still-resource-497715-g5.retail_staging.stores_raw`
ADD COLUMN IF NOT EXISTS pipeline_run_id STRING;

ALTER TABLE `still-resource-497715-g5.retail_staging.sales_raw`
ADD COLUMN IF NOT EXISTS pipeline_run_id STRING;