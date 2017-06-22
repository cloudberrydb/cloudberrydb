-- start_ignore
DROP RESOURCE GROUP rg_callback_test;
-- end_ignore

-- process callbacks on ABORT
BEGIN;
CREATE RESOURCE GROUP rg_callback_test WITH (concurrency=10, cpu_rate_limit=0.1, memory_limit=0.1);
ALTER RESOURCE GROUP rg_callback_test SET concurrency 20;
DROP RESOURCE GROUP rg_callback_test;
ABORT;

-- process callbacks on COMMIT
BEGIN;
CREATE RESOURCE GROUP rg_callback_test WITH (concurrency=10, cpu_rate_limit=0.1, memory_limit=0.1);
ALTER RESOURCE GROUP rg_callback_test SET concurrency 20;
DROP RESOURCE GROUP rg_callback_test;
COMMIT;

-- process callbacks on ABORT after CREATE
CREATE RESOURCE GROUP rg_callback_test WITH (concurrency=10, cpu_rate_limit=0.1, memory_limit=0.1);
BEGIN;
ALTER RESOURCE GROUP rg_callback_test SET concurrency 20;
DROP RESOURCE GROUP rg_callback_test;
ABORT;

-- process callbacks on COMMIT after CREATE
BEGIN;
ALTER RESOURCE GROUP rg_callback_test SET concurrency 20;
DROP RESOURCE GROUP rg_callback_test;
COMMIT;

-- process callbacks on ABORT without DROP
BEGIN;
CREATE RESOURCE GROUP rg_callback_test WITH (concurrency=10, cpu_rate_limit=0.1, memory_limit=0.1);
ALTER RESOURCE GROUP rg_callback_test SET concurrency 20;
ABORT;

-- process callbacks on COMMIT without DROP
BEGIN;
CREATE RESOURCE GROUP rg_callback_test WITH (concurrency=10, cpu_rate_limit=0.1, memory_limit=0.1);
ALTER RESOURCE GROUP rg_callback_test SET concurrency 20;
COMMIT;

DROP RESOURCE GROUP rg_callback_test;
