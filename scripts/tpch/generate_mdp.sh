#!/bin/sh

QUERY_PATH=`pwd`/queries-original
MINIDUMP_PATH=/Users/raghav/greenplum-db-data/gp.orca-paristx-venky/gp-1/minidumps

GET_MINIDUMP()
{
	queryname=$1
	for mdp_file in `ls -r $MINIDUMP_PATH/*.mdp`
    do
        xmllint --format $mdp_file > $OUTPUT_DIR/$queryname.mdp
        break
	done
}

PROCESS_QUERIES()
{
	for query_file in $QUERY_PATH/*.sql
    do
		sql=`cat $query_file`
		query="set gp_optimizer = on; set gp_opt_minidump = on; set gp_opt_enumerate_plans= on; explain "$sql;
		echo $sql
		psql -d $DBNAME -c "$query"
		fileWithoutPath=$(basename $query_file)
		GET_MINIDUMP ${fileWithoutPath%.*}
	done
}

DBNAME=$2
OUTPUT_DIR=$4
                                     
if [ "$DBNAME" = "" ]
then
        echo "Usage:\tsh generate_mdp.sh -d <database name> -o <output directory>" 
        exit 0
fi
if [ ! -d "$OUTPUT_DIR" ]
then
        echo "$OUTPUT_DIR does not exist" 
        exit 0
fi

PROCESS_QUERIES