-- =====================================================
-- create_backup_dataset.sql
-- Retail Data Pipeline - Admin / Backup Layer
-- Purpose: Create backup dataset for BigQuery table snapshots
-- Project: still-resource-497715-g5
-- =====================================================

CREATE SCHEMA IF NOT EXISTS `still-resource-497715-g5.retail_backup_records`
OPTIONS (
  location = 'US'
);