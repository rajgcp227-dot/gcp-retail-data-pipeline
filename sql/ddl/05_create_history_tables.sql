cat > sql/ddl/05_create_history_tables.sql <<'EOF'
-- =====================================================
-- 05_create_history_tables.sql
-- Retail Data Pipeline - History Layer Tables
-- Project: still-resource-497715-g5
-- Dataset: retail_history_records
-- =====================================================

CREATE TABLE IF NOT EXISTS `still-resource-497715-g5.retail_history_records.customers_history`
(
  customer_id STRING NOT NULL,
  customer_name STRING,
  email STRING,
  city STRING,
  state STRING,

  created_date DATE,
  load_type STRING,
  source_file_name STRING,
  batch_id STRING,
  load_timestamp TIMESTAMP,

  history_created_at TIMESTAMP,
  history_updated_at TIMESTAMP
)
PARTITION BY created_date
CLUSTER BY customer_id;

CREATE TABLE IF NOT EXISTS `still-resource-497715-g5.retail_history_records.products_history`
(
  product_id STRING NOT NULL,
  product_name STRING,
  category STRING,
  brand STRING,
  price NUMERIC,

  created_date DATE,
  load_type STRING,
  source_file_name STRING,
  batch_id STRING,
  load_timestamp TIMESTAMP,

  history_created_at TIMESTAMP,
  history_updated_at TIMESTAMP
)
PARTITION BY created_date
CLUSTER BY product_id;

CREATE TABLE IF NOT EXISTS `still-resource-497715-g5.retail_history_records.stores_history`
(
  store_id STRING NOT NULL,
  store_name STRING,
  city STRING,
  state STRING,
  region STRING,

  created_date DATE,
  load_type STRING,
  source_file_name STRING,
  batch_id STRING,
  load_timestamp TIMESTAMP,

  history_created_at TIMESTAMP,
  history_updated_at TIMESTAMP
)
PARTITION BY created_date
CLUSTER BY store_id;

CREATE TABLE IF NOT EXISTS `still-resource-497715-g5.retail_history_records.sales_history`
(
  order_id STRING NOT NULL,
  customer_id STRING,
  product_id STRING,
  store_id STRING,

  quantity INT64,
  unit_price NUMERIC,
  discount_amount NUMERIC,
  tax_amount NUMERIC,
  sale_amount NUMERIC,
  sale_date DATE,
  payment_method STRING,
  coupon_code STRING,
  sale_channel STRING,

  load_type STRING,
  source_file_name STRING,
  batch_id STRING,
  load_timestamp TIMESTAMP,

  history_created_at TIMESTAMP,
  history_updated_at TIMESTAMP
)
PARTITION BY sale_date
CLUSTER BY customer_id, product_id, store_id;
EOF