-- WARNING
-- This file is executed against the postgres database, as that is known to
-- exist at the time of running upgrades. If objects are to be manipulated
-- in other databases, make sure to change to the correct database first.

-- drop all AO tables
\ir pre_drop_ao.sql

DROP DATABASE IF EXISTS isolation2test;

\c regression;

-- drop all AO tables
\ir pre_drop_ao.sql

-- Greenplum pg_upgrade doesn't support indexes on partitions since they can't
-- be reliably dump/restored in all situations. Drop all such indexes before
-- attempting the upgrade.
CREATE OR REPLACE FUNCTION drop_indexes() RETURNS void AS $$
DECLARE
       part_indexes RECORD;
BEGIN
       FOR part_indexes IN
       WITH partitions AS (
           SELECT DISTINCT n.nspname,
                  c.relname
           FROM pg_catalog.pg_partition p
                JOIN pg_catalog.pg_class c ON (p.parrelid = c.oid)
                JOIN pg_catalog.pg_namespace n ON (n.oid = c.relnamespace)
           UNION
           SELECT n.nspname,
                  partitiontablename AS relname
           FROM pg_catalog.pg_partitions p
                JOIN pg_catalog.pg_class c ON (p.partitiontablename = c.relname)
                JOIN pg_catalog.pg_namespace n ON (n.oid = c.relnamespace)
       )
       SELECT nspname,
              relname,
              indexname
       FROM partitions
            JOIN pg_catalog.pg_indexes ON (relname = tablename
                                                                               AND nspname = schemaname)
       LOOP
               EXECUTE 'DROP INDEX IF EXISTS ' || quote_ident(part_indexes.nspname) || '.' || quote_ident(part_indexes.indexname);
       END LOOP;
       RETURN;
END;
$$ LANGUAGE plpgsql;

SELECT drop_indexes();
DROP FUNCTION drop_indexes();

\c dsp1;

-- drop all AO tables
\ir pre_drop_ao.sql

\c dsp2;

-- drop all AO tables
\ir pre_drop_ao.sql

\c dsp3;

-- drop all AO tables
\ir pre_drop_ao.sql

-- toast table and index aren't correctly migrated for this relation, for some
-- reason
DROP TABLE IF EXISTS public.alter_table_reorg_heap CASCADE;
