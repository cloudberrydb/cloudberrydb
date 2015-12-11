#!/bin/sh

QUERY_PATH=`pwd`/queries

DBNAME='test'

gpoptutils_namespace=gpoptutils


EXEC_QUERIES() {
	for query_file in `ls $QUERY_PATH/*.sql`
	do
		echo $query_file
		echo "============================================="		
#		query=`cat $QUERY_PATH/$query_file`
		query=`sed  "s/\'/\''/g" $query_file`
		
		if [ -f $query_file.setup ]
		then
			psql -d $DBNAME -f $query_file.setup
		fi
		
		plan_file=$query_file.xml
#		echo $query
		
		dump_query="select ${gpoptutils_namespace}.DumpPlanToDXLFile('$query', '$plan_file')"

#		echo $dump_query
		
#		psql -d $DBNAME -c "$dump_query"
		psql -d $DBNAME -c "select run_dxl_query('$gpoptutils_namespace', '$query_file', '$query')"
		psql -d $DBNAME -c "select run_gpdb_query('$query_file', '$query')"

		if [ -f $query_file.teardown ]
		then
			psql -d $DBNAME -f $query_file.teardown
		fi
		
		echo
		echo
	done
}


                             
#while getopts ":m" opt
#	do
#	case $opt in
#		m ) MASTER_ONLY=1 ;;
#	esac
#done

# create table to store results
psql -d $DBNAME -c "drop table dxl_tests;"
psql -d $DBNAME -c "create table dxl_tests(id text, gpdb text, dxl text);"

# create functions for storing results
psql -d $DBNAME -f store-results-funcs.sql

EXEC_QUERIES

psql -d $DBNAME -c "(select 'OK' as result, id as query, gpdb as \"native gpdb\", dxl as \"frozen plan\" from dxl_tests where gpdb=dxl) union all (select 'FAILED' as result, id as query, gpdb as \"native gpdb\", dxl as \"frozen plan\" from dxl_tests where gpdb!=dxl)  order by query;"

psql -d $DBNAME -c "(select 'OK' as result, count(*) as cnt from dxl_tests where gpdb=dxl) union all (select 'FAILED' as result, count(*) as cnt from dxl_tests where gpdb!=dxl);"
