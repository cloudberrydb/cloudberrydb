DROP TABLE IF EXISTS reindex_pg_class_test;
CREATE TABLE reindex_pg_class_test(a int);
REINDEX INDEX pg_class_oid_index;
REINDEX INDEX pg_class_relname_nsp_index;
