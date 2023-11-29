#!/bin/bash
PWD=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
source $PWD/../functions.sh
source $PWD/conf.sh 

all_table="$DIMS $FACTS"
db=$TPCDS_DB

if [[ "$OSS_LOAD" == "true" ]]; then
    oss_load_file="$PWD/sql/load/load_oss.sql"
    command="psql -a -d $db -f $oss_load_file"
    psql_exec_print_time "$command" "load from oss"
fi

if [[ "$HDFS_WRITE" == "true" ]]; then
    hdfs_write_file="$PWD/sql/load/write_to_hdfs.sql"
    command="psql -a -d $db -f $hdfs_write_file"
    psql_exec_print_time "$command" "write to hdfs"
fi


if [[ "$HDFS_LOAD" == "true" ]]; then
    hdfs_load_file="$PWD/sql/load/load_hdfs.sql"
    command="psql -a -d $db -f $hdfs_load_file"
    psql_exec_print_time "$command" "load from hdfs"
fi
