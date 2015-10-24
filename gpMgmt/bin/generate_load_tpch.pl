#!/usr/bin/env perl

####################################################
#Author  : Bharath
#Comment : Takes the scale factor, number of primary segments, master port number and database name as input and load the tpch data in parallel by streaming the output of dbgen using external tables. Expects the dbgen program and dists.dss file to be present in the $GPHOME/bin directory on all the segment hosts.
#Usage   : sample usage > perl load_tpch.pl -scale <size of data to be loaded. scale of 1 means 1GB, 1000 means 1TB> -num <total number of primary segments in the cluster> -port <master port number> -db <name of the database in which the tpch tables will be created>
###################################################

#use strict;
use warnings;
use Getopt::Long;

# Arguments to the program
my ($scale, $num_primary_segs, $port, $dbname);

GetOptions(
	'scale=i' => \$scale,
	'num=i' => \$num_primary_segs,
        'port=i' => \$port,
        'db=s' => \$dbname 
	);

my $ddl_dml_file = "/tmp/load_tpch_".`date "+%Y%m%d"`;
chomp($ddl_dml_file);

unless (defined($scale) && defined($num_primary_segs) && defined($port) && defined($dbname))
{
    print "\nUsage: $0 -scale <size of data to be loaded. scale of 1 means 1GB, 1000 means 1TB> -num <total number of primary segments in the cluster> -port <master port number> -db <name of the database in which the tpch tables will be created>\n\n";

    print "The program expects database <dbname> to be present. Also, it expects dbgen program and dists.dss file to be present in GPHOME/bin directory. \n\n";
    exit;
}

open(OUT,">$ddl_dml_file") or die("Unable to open file $ddl_dml_file\n");

print "\nWriting the necessary ddl and dml statements to file: $ddl_dml_file\n";
# Drop table statments

print OUT "drop external web table if exists e_nation;\n";
print OUT "drop external web table if exists e_customer;\n";
print OUT "drop external web table if exists e_region;\n";
print OUT "drop external web table if exists e_part;\n";
print OUT "drop external web table if exists e_supplier;\n";
print OUT "drop external web table if exists e_partsupp;\n";
print OUT "drop external web table if exists e_orders;\n";
print OUT "drop external web table if exists e_lineitem;\n\n";
print OUT "drop table if exists nation cascade;\n";
print OUT "drop table if exists region cascade;\n";
print OUT "drop table if exists part cascade;\n";
print OUT "drop table if exists supplier cascade;\n";
print OUT "drop table if exists partsupp cascade;\n";
print OUT "drop table if exists customer cascade;\n";
print OUT "drop table if exists orders cascade;\n";
print OUT "drop table if exists lineitem cascade;\n\n";

# Create table statements

print OUT "CREATE TABLE NATION  ( N_NATIONKEY  INTEGER NOT NULL,
                            N_NAME       CHAR(25) NOT NULL,
                            N_REGIONKEY  INTEGER NOT NULL,
                            N_COMMENT    VARCHAR(152));\n";

print OUT "CREATE TABLE REGION  ( R_REGIONKEY  INTEGER NOT NULL,
                            R_NAME       CHAR(25) NOT NULL,
                            R_COMMENT    VARCHAR(152));\n";

print OUT "CREATE TABLE PART  ( P_PARTKEY     INTEGER NOT NULL,
                          P_NAME        VARCHAR(55) NOT NULL,
                          P_MFGR        CHAR(25) NOT NULL,
                          P_BRAND       CHAR(10) NOT NULL,
                          P_TYPE        VARCHAR(25) NOT NULL,
                          P_SIZE        INTEGER NOT NULL,
                          P_CONTAINER   CHAR(10) NOT NULL,
                          P_RETAILPRICE DECIMAL(15,2) NOT NULL,
                          P_COMMENT     VARCHAR(23) NOT NULL );\n";

print OUT "CREATE TABLE SUPPLIER ( S_SUPPKEY     INTEGER NOT NULL,
                             S_NAME        CHAR(25) NOT NULL,
                             S_ADDRESS     VARCHAR(40) NOT NULL,
                             S_NATIONKEY   INTEGER NOT NULL,
                             S_PHONE       CHAR(15) NOT NULL,
                             S_ACCTBAL     DECIMAL(15,2) NOT NULL,
                             S_COMMENT     VARCHAR(101) NOT NULL);\n";

print OUT "CREATE TABLE PARTSUPP ( PS_PARTKEY     INTEGER NOT NULL,
                             PS_SUPPKEY     INTEGER NOT NULL,
                             PS_AVAILQTY    INTEGER NOT NULL,
                             PS_SUPPLYCOST  DECIMAL(15,2)  NOT NULL,
                             PS_COMMENT     VARCHAR(199) NOT NULL );\n";

print OUT "CREATE TABLE CUSTOMER ( C_CUSTKEY     INTEGER NOT NULL,
                             C_NAME        VARCHAR(25) NOT NULL,
                             C_ADDRESS     VARCHAR(40) NOT NULL,
                             C_NATIONKEY   INTEGER NOT NULL,
                             C_PHONE       CHAR(15) NOT NULL,
                             C_ACCTBAL     DECIMAL(15,2)   NOT NULL,
                             C_MKTSEGMENT  CHAR(10) NOT NULL,
                             C_COMMENT     VARCHAR(117) NOT NULL);\n";

print OUT "CREATE TABLE ORDERS  ( O_ORDERKEY       INT8 NOT NULL,
                           O_CUSTKEY        INTEGER NOT NULL,
                           O_ORDERSTATUS    CHAR(1) NOT NULL,
                           O_TOTALPRICE     DECIMAL(15,2) NOT NULL,
                           O_ORDERDATE      DATE NOT NULL,
                           O_ORDERPRIORITY  CHAR(15) NOT NULL,
                           O_CLERK          CHAR(15) NOT NULL,
                           O_SHIPPRIORITY   INTEGER NOT NULL,
                           O_COMMENT        VARCHAR(79) NOT NULL);\n";

print OUT "CREATE TABLE LINEITEM ( L_ORDERKEY    INT8 NOT NULL,
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
                              L_COMMENT      VARCHAR(44) NOT NULL);\n\n";

print OUT "create external web table e_nation (N_NATIONKEY  INTEGER ,
                            N_NAME       CHAR(25) ,
                            N_REGIONKEY  INTEGER ,
                            N_COMMENT    VARCHAR(152))
                        execute 'bash -c \"\$GPHOME/bin/dbgen -b \$GPHOME/bin/dists.dss -T n -s $scale\"' on 1 format 'text' (delimiter '|');\n";

print OUT "CREATE external web TABLE e_REGION  ( R_REGIONKEY  INTEGER ,
                            R_NAME       CHAR(25) ,
                            R_COMMENT    VARCHAR(152))
                        execute 'bash -c \"\$GPHOME/bin/dbgen -b \$GPHOME/bin/dists.dss -T r -s $scale\"'
                        on 1 format 'text' (delimiter '|');\n";

print OUT "CREATE external web TABLE e_PART  ( P_PARTKEY     INTEGER ,
                          P_NAME        VARCHAR(55) ,
                          P_MFGR        CHAR(25) ,
                          P_BRAND       CHAR(10) ,
                          P_TYPE        VARCHAR(25) ,
                          P_SIZE        INTEGER ,
                          P_CONTAINER   CHAR(10) ,
                          P_RETAILPRICE DECIMAL(15,2) ,
                          P_COMMENT     VARCHAR(23) )
                        execute 'bash -c \"\$GPHOME/bin/dbgen -b \$GPHOME/bin/dists.dss -T P -s $scale -N $num_primary_segs -n \$((GP_SEGMENT_ID + 1))\"'
                        on $num_primary_segs format 'text' (delimiter '|');\n";

print OUT "CREATE external web TABLE e_SUPPLIER ( S_SUPPKEY     INTEGER ,
                             S_NAME        CHAR(25) ,
                             S_ADDRESS     VARCHAR(40) ,
                             S_NATIONKEY   INTEGER ,
                             S_PHONE       CHAR(15) ,
                             S_ACCTBAL     DECIMAL(15,2) ,
                             S_COMMENT     VARCHAR(101) )
                        execute 'bash -c \"\$GPHOME/bin/dbgen -b \$GPHOME/bin/dists.dss -T s -s $scale -N $num_primary_segs -n \$((GP_SEGMENT_ID + 1))\"'
                        on $num_primary_segs format 'text' (delimiter '|');\n";

print OUT "CREATE external web TABLE e_PARTSUPP ( PS_PARTKEY     INTEGER ,
                             PS_SUPPKEY     INTEGER ,
                             PS_AVAILQTY    INTEGER ,
                             PS_SUPPLYCOST  DECIMAL(15,2)  ,
                             PS_COMMENT     VARCHAR(199) )
                        execute 'bash -c \"\$GPHOME/bin/dbgen -b \$GPHOME/bin/dists.dss -T S -s $scale -N $num_primary_segs -n \$((GP_SEGMENT_ID + 1))\"'
                        on $num_primary_segs format 'text' (delimiter '|');\n";

print OUT "CREATE external web TABLE e_CUSTOMER ( C_CUSTKEY     INTEGER ,
                             C_NAME        VARCHAR(25) ,
                             C_ADDRESS     VARCHAR(40) ,
                             C_NATIONKEY   INTEGER ,
                             C_PHONE       CHAR(15) ,
                             C_ACCTBAL     DECIMAL(15,2) ,
                             C_MKTSEGMENT  CHAR(10) ,
                             C_COMMENT     VARCHAR(117) )
                        execute 'bash -c \"\$GPHOME/bin/dbgen -b \$GPHOME/bin/dists.dss -T c -s $scale -N $num_primary_segs -n \$((GP_SEGMENT_ID + 1))\"'
                        on $num_primary_segs format 'text' (delimiter '|');\n";

print OUT "CREATE external web TABLE e_ORDERS  ( O_ORDERKEY       INT8 ,
                           O_CUSTKEY        INTEGER ,
                           O_ORDERSTATUS    CHAR(1) ,
                           O_TOTALPRICE     DECIMAL(15,2) ,
                           O_ORDERDATE      DATE ,
                           O_ORDERPRIORITY  CHAR(15) ,
                           O_CLERK          CHAR(15) ,
                           O_SHIPPRIORITY   INTEGER ,
                           O_COMMENT        VARCHAR(79) )
                        execute 'bash -c \"\$GPHOME/bin/dbgen -b \$GPHOME/bin/dists.dss -T O -s $scale -N $num_primary_segs -n \$((GP_SEGMENT_ID + 1))\"'
                        on $num_primary_segs format 'text' (delimiter '|');\n";

print OUT "CREATE EXTERNAL WEB TABLE E_LINEITEM ( L_ORDERKEY    INT8 ,
                              L_PARTKEY     INTEGER ,
                              L_SUPPKEY     INTEGER ,
                              L_LINENUMBER  INTEGER ,
                              L_QUANTITY    DECIMAL(15,2) ,
                              L_EXTENDEDPRICE  DECIMAL(15,2) ,
                              L_DISCOUNT    DECIMAL(15,2) ,
                              L_TAX         DECIMAL(15,2) ,
                              L_RETURNFLAG  CHAR(1) ,
                              L_LINESTATUS  CHAR(1) ,
                              L_SHIPDATE    DATE ,
                              L_COMMITDATE  DATE ,
                              L_RECEIPTDATE DATE ,
                              L_SHIPINSTRUCT CHAR(25) ,
                              L_SHIPMODE     CHAR(10) ,
                              L_COMMENT      VARCHAR(44) )
                              EXECUTE 'bash -c \"\$GPHOME/bin/dbgen -b \$GPHOME/bin/dists.dss -T L -s $scale -N $num_primary_segs -n \$((GP_SEGMENT_ID + 1))\"' 
                              on $num_primary_segs format 'text' (delimiter '|');\n\n";

# Now statements to load the data

print OUT "insert into nation select * from e_nation;\n";
print OUT "insert into region select * from e_region;\n";
print OUT "insert into part select * from e_part;\n";
print OUT "insert into supplier select * from e_supplier;\n";
print OUT "insert into partsupp select * from e_partsupp;\n";
print OUT "insert into customer select * from e_customer;\n";
print OUT "insert into orders select * from e_orders;\n";
print OUT "insert into lineitem select * from e_lineitem;\n";
print OUT "VACUUM ANALYZE";

close(OUT);

print "Creating tables, loading data and running VACUUM ANALYZE\n";

my $tmp_outfile = $ddl_dml_file."_out";

system("psql -p $port -d $dbname -a -f $ddl_dml_file > $tmp_outfile 2>&1");

print "Done.\n";
