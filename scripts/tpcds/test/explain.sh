#!/bin/sh

QUERY_PATH=`pwd`/passed


EXPLAIN_QUERIES()
{
	for query_file in $QUERY_PATH/*.sql
        do
		echo
		echo "=========================================================================================="
		echo $query_file
		echo "=========================================================================================="
		echo
		sql=`cat $query_file`
		query="set gp_optimizer = on; set gp_log_optimizer = on; explain "$sql;
		psql -d $DBNAME -c "$query"
	done

}

DBNAME=$2
                                     
if [ "$DBNAME" = "" ]
then
        echo "Usage:\tsh explain.sh -d <database name>" 
        exit 0
fi


EXPLAIN_QUERIES
