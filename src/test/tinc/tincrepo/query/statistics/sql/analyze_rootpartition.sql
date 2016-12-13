-- @author shene
-- @created 2014-05-02 12:00:00
-- @product_version gpdb: [4.3-]
-- @db_name analyze_db
-- @tags analyze ORCA
-- @optimizer_mode both
-- @description Test 'ANALYZE ROOTPARTITION' functionality which only analyzes the root table of a partition table
select * from gp_toolkit.gp_stats_missing where smitable like 'arp_%' order by smitable;

analyze rootpartition arp_test;
select * from gp_toolkit.gp_stats_missing where smitable like 'arp_%' order by smitable;

analyze rootpartition all;
select * from gp_toolkit.gp_stats_missing where smitable like 'arp_%' order by smitable;

analyze rootpartition arp_test(a);

-- negative tests below

-- to analyze all the root parts in the database, 'ALL' is required after rootpartition
analyze rootpartition;
-- if a table is specified, it must be a root part
analyze rootpartition arp_test_1_prt_1;
-- rootpartition cannot be specified with VACUUM
vacuum analyze rootpartition arp_test;
vacuum analyze rootpartition all;
vacuum rootpartition arp_test;

drop table arp_test;
drop table arp_test2;

-- when there are no partition tables in the database, analyze rootpartition should give a warning
analyze rootpartition all;


