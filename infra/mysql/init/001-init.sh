#!/usr/bin/env bash
set -euo pipefail

mysql=(mysql -uroot -p"${MYSQL_ROOT_PASSWORD}")
source_db="${MYSQL_DATABASE:-cdc_lab}"
catalog_db="${ICEBERG_CATALOG_DATABASE:-iceberg_catalog}"
app_user="${MYSQL_USER:-cdc}"
app_password="${MYSQL_PASSWORD:-cdc_pw}"

"${mysql[@]}" <<SQL
CREATE DATABASE IF NOT EXISTS \`${source_db}\`;
CREATE DATABASE IF NOT EXISTS \`${catalog_db}\`;

CREATE USER IF NOT EXISTS '${app_user}'@'%' IDENTIFIED BY '${app_password}';
GRANT ALL PRIVILEGES ON \`${source_db}\`.* TO '${app_user}'@'%';
GRANT ALL PRIVILEGES ON \`${catalog_db}\`.* TO '${app_user}'@'%';
FLUSH PRIVILEGES;

CREATE TABLE IF NOT EXISTS \`${source_db}\`.orders (
  order_id BIGINT NOT NULL,
  business_key VARCHAR(64) NOT NULL,
  event_id BIGINT NOT NULL,
  customer_id BIGINT NOT NULL,
  status VARCHAR(32) NOT NULL,
  amount_cents BIGINT NOT NULL,
  updated_at DATETIME(3) NOT NULL,
  seed INT NOT NULL,
  PRIMARY KEY (order_id),
  UNIQUE KEY uq_orders_business_key (business_key),
  UNIQUE KEY uq_orders_event_id (event_id),
  KEY idx_orders_customer_id (customer_id),
  KEY idx_orders_updated_at (updated_at)
) ENGINE=InnoDB;
SQL

