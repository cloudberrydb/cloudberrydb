#!/bin/bash
cd `dirname "${BASH_SOURCE[0]}"`
source ../functions.sh
source ./conf.sh

if [[ $ENV_FLAG == "true" ]]; then
    echo "Creating database and extension..."
    createdb $DB
    create_ext $DB
    create_server $DB $SIMP_SERVER $HDFS_CLUSTER
fi

if [[ $GEN_SYNC == "true" ]] || [[ $GEN_VAST == "true" ]]; then
    ./gen.sh
fi

if [[ $HIVE_LOAD == "true" ]]; then
    echo "Loading data to hive..."
    ./hive.sh > /dev/null 2>&1
fi

if [[ $SYNC_VAST == "true" ]]; then
    echo "Sync vast tables..."
    ./vast.sh
fi

if [[ $VALGRIND_FLAG == "true" ]]; then
    echo "Exec memory leak test..."
    ./valgrind.sh
fi

