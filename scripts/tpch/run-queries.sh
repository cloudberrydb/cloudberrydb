#!/bin/sh

QUERY_PATH=`pwd`/queries

DBNAME='test'
GPOPTUTILS_NAMESPACE=gpoptutils

query_files=(query01.sql query02.sql query03.sql query04.sql query05.sql query06.sql query07.sql query08.sql query09.sql query10.sql query11.sql query12.sql query13.sql query14.sql query15.sql query16.sql query17.sql query18.sql query19.sql query20.sql query21.sql query22.sql)

EXEC_QUERIES() {
	for query_file in "${query_files[@]}"
	do
		echo $query_file
		echo "============================================="		
#		query=`cat $QUERY_PATH/$query_file`
		query=`sed  "s/\'/\''/g" $QUERY_PATH/$query_file`
		
		plan_file=$QUERY_PATH/$query_file.xml
#		echo $query
		
		dump_query="select ${GPOPTUTILS_NAMESPACE}.DumpPlanDXL('$query')"

#		echo $dump_query
		
#		psql -d $DBNAME -c "$dump_query"
		psql -d $DBNAME -c "select run_dxl_query('$GPOPTUTILS_NAMESPACE', '$query_file', '$query')"
		psql -d $DBNAME -c "select run_gpdb_query('$query_file', '$query')"
		psql -d $DBNAME -c "select run_algebrized_query('$GPOPTUTILS_NAMESPACE', '$query_file', '$query')"

		
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
psql -d $DBNAME -c "create table dxl_tests(id text, gpdb text, algebrizerresult text, planfreezerresult text, optimizerresult text);"

# create functions for storing results
psql -d $DBNAME -f store-results-funcs.sql

EXEC_QUERIES  
 
# The following tests exercise the algebrizer and plan freezer
psql -d $DBNAME -c "(select 'OK' as result, id as query, gpdb as \"native gpdb\", planfreezerresult as \"frozen plan\", algebrizerresult as \"algebrizer\" from dxl_tests where gpdb=planfreezerresult AND gpdb=algebrizerresult) union all (select 'FAILED' as result, id as query, gpdb as \"native gpdb\", planfreezerresult as \"frozen plan\", algebrizerresult as \"algebrizer\" from dxl_tests where gpdb!=planfreezerresult OR gpdb!=algebrizerresult)  order by query;"
psql -d $DBNAME -c "(select 'OK' as result, count(*) as cnt from dxl_tests where gpdb=planfreezerresult AND gpdb=algebrizerresult) union all (select 'FAILED' as result, count(*) as cnt from dxl_tests where gpdb!=planfreezerresult or gpdb!=algebrizerresult);"
