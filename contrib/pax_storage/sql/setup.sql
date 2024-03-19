-- start_ignore
create schema pax_test;
CREATE EXTENSION gp_inject_fault;
\! gpconfig -c session_preload_libraries -v pax
\! gpstop -ar
-- end_ignore
