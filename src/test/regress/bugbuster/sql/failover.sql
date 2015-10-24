\! gpfaultinjector -f filerep_consumer -m async -y fault -r mirror -H ALL > bugbuster/data/outputFaultInjector.txt
\! grep -i "error" bugbuster/data/outputFaultInjector.txt | wc -l
-- start_ignore
drop table if exists test;
-- end_ignore
-- start_ignore
create table test ( a int, b text);
-- end_ignore
\! gprecoverseg -a > bugbuster/data/outputRecoverSeg.txt
\! sleep 10
--start_ignore
\! psql --pset pager --dbname template1 --command "select dbid, content, role, preferred_role, mode, status from gp_segment_configuration"
--end_ignore
