-- =====================================================
-- create_reporting_snapshots.sql
-- Retail Data Pipeline - Admin / Backup Layer
-- Purpose: Create BigQuery snapshots before reporting MERGE
-- Dataset: retail_backup_records
-- =====================================================

DECLARE v_snapshot_suffix STRING DEFAULT FORMAT_TIMESTAMP('%Y%m%d_%H%M%S', CURRENT_TIMESTAMP(), 'UTC');

EXECUTE IMMEDIATE FORMAT("""
CREATE SNAPSHOT TABLE IF NOT EXISTS
`still-resource-497715-g5.retail_backup_records.dim_customer_snap_%s`
CLONE `still-resource-497715-g5.retail_reporting_records.dim_customer`
OPTIONS (
  expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
)
""", v_snapshot_suffix);

EXECUTE IMMEDIATE FORMAT("""
CREATE SNAPSHOT TABLE IF NOT EXISTS
`still-resource-497715-g5.retail_backup_records.dim_product_snap_%s`
CLONE `still-resource-497715-g5.retail_reporting_records.dim_product`
OPTIONS (
  expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
)
""", v_snapshot_suffix);

EXECUTE IMMEDIATE FORMAT("""
CREATE SNAPSHOT TABLE IF NOT EXISTS
`still-resource-497715-g5.retail_backup_records.dim_store_snap_%s`
CLONE `still-resource-497715-g5.retail_reporting_records.dim_store`
OPTIONS (
  expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
)
""", v_snapshot_suffix);

EXECUTE IMMEDIATE FORMAT("""
CREATE SNAPSHOT TABLE IF NOT EXISTS
`still-resource-497715-g5.retail_backup_records.fact_sales_snap_%s`
CLONE `still-resource-497715-g5.retail_reporting_records.fact_sales`
OPTIONS (
  expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
)
""", v_snapshot_suffix);