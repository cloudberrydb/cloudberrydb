PWD=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
source $PWD/../functions.sh
source $PWD/conf.sh 

all_table="$DIMS $FACTS"
db="$TPCDS_DB"

if [[ "$ENV_FLAG" == "true" ]]; then
    createdb "$db"
    create_ext "$db"
fi

if [[ "$OSS_DDL" == "true" ]] ; then
    create_oss_server "$db" "$OSS_SERVER"
    for tab in $all_table; do
        psql -d $db -f "$PWD/sql/ddl/oss/$tab.sql"
    done
fi

if [[ "$HDFS_DDL" == "true" ]]; then
    server=$HDFS_SERVER
    create_server "$db" "$HDFS_SERVER" "$TPCDS_CLUSTER"
    for tab in $all_table; do
        psql -d $db -f "$PWD/sql/ddl/hdfs/$tab.sql"
    done
fi
