-- =====================================================
-- create_history_snapshots.sql
-- Retail Data Pipeline - Admin / Backup Layer
-- Purpose: Create BigQuery snapshots before history MERGE
-- Dataset: retail_backup_records
-- =====================================================

DECLARE v_snapshot_suffix STRING DEFAULT FORMAT_TIMESTAMP('%Y%m%d_%H%M%S', CURRENT_TIMESTAMP(), 'UTC');

EXECUTE IMMEDIATE FORMAT("""
CREATE SNAPSHOT TABLE IF NOT EXISTS
`still-resource-497715-g5.retail_backup_records.customers_history_snap_%s`
CLONE `still-resource-497715-g5.retail_history_records.customers_history`
OPTIONS (
  expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
)
""", v_snapshot_suffix);

EXECUTE IMMEDIATE FORMAT("""
CREATE SNAPSHOT TABLE IF NOT EXISTS
`still-resource-497715-g5.retail_backup_records.products_history_snap_%s`
CLONE `still-resource-497715-g5.retail_history_records.products_history`
OPTIONS (
  expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
)
""", v_snapshot_suffix);

EXECUTE IMMEDIATE FORMAT("""
CREATE SNAPSHOT TABLE IF NOT EXISTS
`still-resource-497715-g5.retail_backup_records.stores_history_snap_%s`
CLONE `still-resource-497715-g5.retail_history_records.stores_history`
OPTIONS (
  expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
)
""", v_snapshot_suffix);

EXECUTE IMMEDIATE FORMAT("""
CREATE SNAPSHOT TABLE IF NOT EXISTS
`still-resource-497715-g5.retail_backup_records.sales_history_snap_%s`
CLONE `still-resource-497715-g5.retail_history_records.sales_history`
OPTIONS (
  expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
)
""", v_snapshot_suffix);