DIMS="date_dim time_dim item customer customer_demographics household_demographics customer_address store promotion warehouse ship_mode reason income_band call_center web_page catalog_page web_site"
FACTS="store_sales store_returns web_sales web_returns catalog_sales catalog_returns inventory"

TPCDS_DB="tpcds"

OSS_SERVER="tpcds_oss"
HDFS_SERVER="tpcds_hdfs"

TPCDS_CLUSTER="paa_cluster"

ENV_FLAG="true"
OSS_DDL="true"
HDFS_DDL="true"
OSS_LOAD="true"
HDFS_WRITE="true"
HDFS_LOAD="true"