HIVE_CLUSTER="hive_cluster"
HDFS_CLUSTER="paa_cluster"

OSS_SERVER="foreign_server"
SIMP_SERVER="hdfs_server"
SIMP_HA_SERVER="hdfs_ha_server"
KERB_SERVER="hdfs_kerb_server"
KERB_HA_SERVER="hdfs_kerb_ha_server"

DB="mytestdb"
SCHEMA="synctab"
DB_SCHEMA="syncdb"
NORMAL_SCHEMA="vastnormal"
PARTITION_SCHEMA="vastpartition"
VAL_SCHEMA="valtab"
VAL_DB_SCHEMA="valdb"

HIVE_DB="mytestdb"
HIVE_NORMAL_DB="vast_normal"
HIVE_PARTITION_DB="vast_partition"

NORMAL_COUNT=10000
PARTITION_COUNT=5000

BASIC_TAB="test_table1"
ORC_TAB="transactional_orc normal_orc partition_orc_1 partition_orc_2 partition_orc_3 partition_orc_4"
PARQUET_TAB="normal_parquet partition_parquet_1 partition_parquet_2 partition_parquet_3"
TEXT_TAB="text_default text_custom csv_default csv_custom"
EMPTY_TAB="empty_orc_transactional empty_orc empty_parquet empty_orc_partition empty_parquet_partition"

ALL_TAB="$BASIC_TAB $ORC_TAB $PARQUET_TAB $TEXT_TAB $EMPTY_TAB"

ENV_FLAG="true"
GEN_VAST="false"
GEN_VAL="false"
HIVE_LOAD="false"
SYNC_VAST="true"
VALGRIND_FLAG="false"
