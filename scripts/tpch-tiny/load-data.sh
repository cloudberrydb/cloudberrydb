#!/bin/sh

DATA_PATH=`pwd`


tbl_names=(nation region part supplier partsupp customer orders lineitem)

#cleanup

CLEANUP () {
	psql -d $DBNAME -c "drop view if exists revenue0"
	for tbl in "${tbl_names[@]}"
	do
		echo "$tbl"
		psql -d $DBNAME -c "drop table if exists $tbl"
		psql -d $DBNAME -c "drop table if exists $tbl""_d"
	done
}
					
# create schema

CREATE_SCHEMA() {
	psql -d $DBNAME -c 'CREATE TABLE NATION  ( N_NATIONKEY  INTEGER NOT NULL,
	                            N_NAME       CHAR(25) NOT NULL,
	                            N_REGIONKEY  INTEGER NOT NULL,
	                            N_COMMENT    VARCHAR(152));'
	
	psql -d $DBNAME -c 'CREATE TABLE REGION  ( R_REGIONKEY  INTEGER NOT NULL,
	                            R_NAME       CHAR(25) NOT NULL,
	                            R_COMMENT    VARCHAR(152));'
	
	psql -d $DBNAME -c 'CREATE TABLE PART  ( P_PARTKEY     INTEGER NOT NULL,
	                          P_NAME        VARCHAR(55) NOT NULL,
	                          P_MFGR        CHAR(25) NOT NULL,
	                          P_BRAND       CHAR(10) NOT NULL,
	                          P_TYPE        VARCHAR(25) NOT NULL,
	                          P_SIZE        INTEGER NOT NULL,
	                          P_CONTAINER   CHAR(10) NOT NULL,
	                          P_RETAILPRICE DECIMAL(15,2) NOT NULL,
	                          P_COMMENT     VARCHAR(23) NOT NULL );'
	
	psql -d $DBNAME -c 'CREATE TABLE SUPPLIER ( S_SUPPKEY     INTEGER NOT NULL,
	                             S_NAME        CHAR(25) NOT NULL,
	                             S_ADDRESS     VARCHAR(40) NOT NULL,
	                             S_NATIONKEY   INTEGER NOT NULL,
	                             S_PHONE       CHAR(15) NOT NULL,
	                             S_ACCTBAL     DECIMAL(15,2) NOT NULL,
	                             S_COMMENT     VARCHAR(101) NOT NULL);'
	
	psql -d $DBNAME -c 'CREATE TABLE PARTSUPP ( PS_PARTKEY     INTEGER NOT NULL,
	                             PS_SUPPKEY     INTEGER NOT NULL,
	                             PS_AVAILQTY    INTEGER NOT NULL,
	                             PS_SUPPLYCOST  DECIMAL(15,2)  NOT NULL,
	                             PS_COMMENT     VARCHAR(199) NOT NULL );'
	
	psql -d $DBNAME -c 'CREATE TABLE CUSTOMER ( C_CUSTKEY     INTEGER NOT NULL,
	                             C_NAME        VARCHAR(25) NOT NULL,
	                             C_ADDRESS     VARCHAR(40) NOT NULL,
	                             C_NATIONKEY   INTEGER NOT NULL,
	                             C_PHONE       CHAR(15) NOT NULL,
	                             C_ACCTBAL     DECIMAL(15,2)   NOT NULL,
	                             C_MKTSEGMENT  CHAR(10) NOT NULL,
	                             C_COMMENT     VARCHAR(117) NOT NULL);'
	
	psql -d $DBNAME -c 'CREATE TABLE ORDERS  ( O_ORDERKEY       INTEGER NOT NULL,
	                           O_CUSTKEY        INTEGER NOT NULL,
	                           O_ORDERSTATUS    CHAR(1) NOT NULL,
	                           O_TOTALPRICE     DECIMAL(15,2) NOT NULL,
	                           O_ORDERDATE      DATE NOT NULL,
	                           O_ORDERPRIORITY  CHAR(15) NOT NULL,  
	                           O_CLERK          CHAR(15) NOT NULL, 
	                           O_SHIPPRIORITY   INTEGER NOT NULL,
	                           O_COMMENT        VARCHAR(79) NOT NULL);'
	
	psql -d $DBNAME -c 'CREATE TABLE LINEITEM ( L_ORDERKEY    INTEGER NOT NULL,
	                             L_PARTKEY     INTEGER NOT NULL,
	                             L_SUPPKEY     INTEGER NOT NULL,
	                             L_LINENUMBER  INTEGER NOT NULL,
	                             L_QUANTITY    DECIMAL(15,2) NOT NULL,
	                             L_EXTENDEDPRICE  DECIMAL(15,2) NOT NULL,
	                             L_DISCOUNT    DECIMAL(15,2) NOT NULL,
	                             L_TAX         DECIMAL(15,2) NOT NULL,
	                             L_RETURNFLAG  CHAR(1) NOT NULL,
	                             L_LINESTATUS  CHAR(1) NOT NULL,
	                             L_SHIPDATE    DATE NOT NULL,
	                             L_COMMITDATE  DATE NOT NULL,
	                             L_RECEIPTDATE DATE NOT NULL,
	                             L_SHIPINSTRUCT CHAR(25) NOT NULL,
	                             L_SHIPMODE     CHAR(10) NOT NULL,
	                             L_COMMENT      VARCHAR(44) NOT NULL);'

	if [ $MASTER_ONLY -eq 1 ]; then
		echo "Creating master-only tables"
		psql -d $DBNAME -c "set allow_system_table_mods = 'DML'; delete from gp_distribution_policy where localoid in (select oid from pg_class where relname in ('nation', 'region', 'part', 'supplier', 'partsupp', 'customer', 'orders', 'lineitem'))"
		CREATE_SCHEMA_DISTRIBUTED			 
	fi

}

CREATE_SCHEMA_DISTRIBUTED() {
	psql -d $DBNAME -c 'CREATE TABLE NATION_D  ( N_NATIONKEY  INTEGER NOT NULL,
	                            N_NAME       CHAR(25) NOT NULL,
	                            N_REGIONKEY  INTEGER NOT NULL,
	                            N_COMMENT    VARCHAR(152));'
	
	psql -d $DBNAME -c 'CREATE TABLE REGION_D  ( R_REGIONKEY  INTEGER NOT NULL,
	                            R_NAME       CHAR(25) NOT NULL,
	                            R_COMMENT    VARCHAR(152));'
	
	psql -d $DBNAME -c 'CREATE TABLE PART_D  ( P_PARTKEY     INTEGER NOT NULL,
	                          P_NAME        VARCHAR(55) NOT NULL,
	                          P_MFGR        CHAR(25) NOT NULL,
	                          P_BRAND       CHAR(10) NOT NULL,
	                          P_TYPE        VARCHAR(25) NOT NULL,
	                          P_SIZE        INTEGER NOT NULL,
	                          P_CONTAINER   CHAR(10) NOT NULL,
	                          P_RETAILPRICE DECIMAL(15,2) NOT NULL,
	                          P_COMMENT     VARCHAR(23) NOT NULL );'
	
	psql -d $DBNAME -c 'CREATE TABLE SUPPLIER_D ( S_SUPPKEY     INTEGER NOT NULL,
	                             S_NAME        CHAR(25) NOT NULL,
	                             S_ADDRESS     VARCHAR(40) NOT NULL,
	                             S_NATIONKEY   INTEGER NOT NULL,
	                             S_PHONE       CHAR(15) NOT NULL,
	                             S_ACCTBAL     DECIMAL(15,2) NOT NULL,
	                             S_COMMENT     VARCHAR(101) NOT NULL);'
	
	psql -d $DBNAME -c 'CREATE TABLE PARTSUPP_D ( PS_PARTKEY     INTEGER NOT NULL,
	                             PS_SUPPKEY     INTEGER NOT NULL,
	                             PS_AVAILQTY    INTEGER NOT NULL,
	                             PS_SUPPLYCOST  DECIMAL(15,2)  NOT NULL,
	                             PS_COMMENT     VARCHAR(199) NOT NULL );'
	
	psql -d $DBNAME -c 'CREATE TABLE CUSTOMER_D ( C_CUSTKEY     INTEGER NOT NULL,
	                             C_NAME        VARCHAR(25) NOT NULL,
	                             C_ADDRESS     VARCHAR(40) NOT NULL,
	                             C_NATIONKEY   INTEGER NOT NULL,
	                             C_PHONE       CHAR(15) NOT NULL,
	                             C_ACCTBAL     DECIMAL(15,2)   NOT NULL,
	                             C_MKTSEGMENT  CHAR(10) NOT NULL,
	                             C_COMMENT     VARCHAR(117) NOT NULL);'
	
	psql -d $DBNAME -c 'CREATE TABLE ORDERS_D  ( O_ORDERKEY       INTEGER NOT NULL,
	                           O_CUSTKEY        INTEGER NOT NULL,
	                           O_ORDERSTATUS    CHAR(1) NOT NULL,
	                           O_TOTALPRICE     DECIMAL(15,2) NOT NULL,
	                           O_ORDERDATE      DATE NOT NULL,
	                           O_ORDERPRIORITY  CHAR(15) NOT NULL,  
	                           O_CLERK          CHAR(15) NOT NULL, 
	                           O_SHIPPRIORITY   INTEGER NOT NULL,
	                           O_COMMENT        VARCHAR(79) NOT NULL);'
	
	psql -d $DBNAME -c 'CREATE TABLE LINEITEM_D ( L_ORDERKEY    INTEGER NOT NULL,
	                             L_PARTKEY     INTEGER NOT NULL,
	                             L_SUPPKEY     INTEGER NOT NULL,
	                             L_LINENUMBER  INTEGER NOT NULL,
	                             L_QUANTITY    DECIMAL(15,2) NOT NULL,
	                             L_EXTENDEDPRICE  DECIMAL(15,2) NOT NULL,
	                             L_DISCOUNT    DECIMAL(15,2) NOT NULL,
	                             L_TAX         DECIMAL(15,2) NOT NULL,
	                             L_RETURNFLAG  CHAR(1) NOT NULL,
	                             L_LINESTATUS  CHAR(1) NOT NULL,
	                             L_SHIPDATE    DATE NOT NULL,
	                             L_COMMITDATE  DATE NOT NULL,
	                             L_RECEIPTDATE DATE NOT NULL,
	                             L_SHIPINSTRUCT CHAR(25) NOT NULL,
	                             L_SHIPMODE     CHAR(10) NOT NULL,
	                             L_COMMENT      VARCHAR(44) NOT NULL);'
}

LOAD_DATA() {
	for tbl in "${tbl_names[@]}"
	do
		echo "$tbl"
		if [ $MASTER_ONLY -eq 1 ]
		then
			psql -d $DBNAME -c "copy $tbl""_d from '$DATA_PATH/$tbl.tbl' delimiter '|';"
			psql -d $DBNAME -c "insert into $tbl select * from $tbl""_d"
			psql -d $DBNAME -c "select count(*) from $tbl"
		else
			psql -d $DBNAME -c "copy $tbl from '$DATA_PATH/$tbl.tbl' delimiter '|';"
			psql -d $DBNAME -c "select count(*) from $tbl"	
		fi
		
	done
	
		
	# for query 15
	psql -d $DBNAME -c "create view revenue0 (supplier_no, total_revenue) as
	select
		l_suppkey,
		sum(l_extendedprice * (1 - l_discount))
	from
		lineitem
	where
		l_shipdate >= date '1993-01-01'
		and l_shipdate < date '1993-01-01' + interval '3 month'
	group by
		l_suppkey;"
		
	psql -d $DBNAME -c "analyze;"
}


DBNAME=$2

if [ "$DBNAME" = "" ]
then
	echo "Usage:\n\tsh load-data.sh [-m|-d] <database name>"
	echo "Description:\n\t-m: Master-only tables\n\t-d: Distributed tables"
	exit 0
fi

MASTER_ONLY=0;
                             
while getopts ":m" opt
	do
	case $opt in
		m ) MASTER_ONLY=1 ;;
	esac
done
	
CLEANUP
CREATE_SCHEMA
LOAD_DATA

