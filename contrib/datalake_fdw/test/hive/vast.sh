#!/bin/bash
PWD=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
source $PWD/../functions.sh
source $PWD/conf.sh 

function exec_vast_test() {
    echo "Drop exists normal tables..."
    create_schema "$DB" "$NORMAL_SCHEMA" > /dev/null 2>&1
    command="psql -d $DB -c \"select public.sync_hive_database('$HIVE_CLUSTER', '$HIVE_NORMAL_DB', '$HDFS_CLUSTER', '$NORMAL_SCHEMA', '$SIMP_SERVER')\""
    echo "Sync normal tables..."
    psql_exec_print_time "$command" "sync $NORMAL_COUNT normal tables" > /dev/null 2>&1
    tab_count=$(psql -t -d $DB -c "SELECT count(*) FROM information_schema.tables WHERE table_schema='$NORMAL_SCHEMA'")
    if [[ $tab_count == $NORMAL_COUNT ]]; then
        echo "Success!"
    else
        echo "ERROR!" 
    fi
    echo "Sync $tab_count normal tables, $NORMAL_COUNT in total."

    echo "Drop exists partition tables..."
    create_schema "$DB" "$PARTITION_SCHEMA"  > /dev/null 2>&1
    command="psql -d $DB -c \"select public.sync_hive_database('$HIVE_CLUSTER', '$HIVE_PARTITION_DB', '$HDFS_CLUSTER', '$PARTITION_SCHEMA', '$SIMP_SERVER')\""
    echo "Sync partition tables..."
    psql_exec_print_time "$command" "sync $PARTITION_COUNT partition tables"  > /dev/null 2>&1
    tab_count=$(psql -t -d $DB -c "SELECT count(*) FROM information_schema.tables WHERE table_schema='$PARTITION_SCHEMA'")
    if [[ $tab_count == $PARTITION_COUNT ]]; then
        echo "Success!"
    else
        echo "ERROR!" 
    fi
    echo "Sync $tab_count partitioned tables, $PARTITION_COUNT in total."
}

if [[ $SYNC_VAST == "true" ]]; then
    exec_vast_test
fi
