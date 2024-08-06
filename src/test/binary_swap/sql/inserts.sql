\c regression

-- Insert into tables generated from src/test/regress
-- Following tables from freeze_aux_tables:

-- start_ignore
create table if not exists test_table_heap_with_toast (id int, col1 int, col2 text) with (appendonly=false);
create table if not exists test_table_ao_with_toast (id int, col1 int, col2 text) with (appendonly=true, orientation=row);
create table if not exists test_table_co_with_toast (id int, col1 int, col2 text) with (appendonly=true, orientation=column);
-- end_ignore
INSERT INTO test_table_heap_with_toast SELECT i, i*2, i*5 FROM generate_series(1, 20)i;
INSERT INTO test_table_ao_with_toast SELECT * FROM test_table_heap_with_toast;
INSERT INTO test_table_co_with_toast SELECT * FROM test_table_heap_with_toast;
