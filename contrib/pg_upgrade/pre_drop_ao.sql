-- OID preassignment for AO tables appears to be borked; get rid of all of them.
-- This includes any tables with AO (sub)partitions.
CREATE OR REPLACE FUNCTION drop_ao_tables() RETURNS void AS $$
DECLARE
       ao_parent RECORD;
       ao_table RECORD;
BEGIN
	FOR ao_parent IN
	SELECT DISTINCT(parent.oid),
		   parent.relname,
		   n.nspname
	FROM pg_catalog.pg_class parent
		JOIN pg_catalog.pg_namespace n ON (n.oid = parent.relnamespace)
		JOIN pg_catalog.pg_partition part ON (part.parrelid = parent.oid)
		JOIN pg_catalog.pg_partition_rule rule ON (rule.paroid = part.oid)
		JOIN pg_catalog.pg_class child ON (rule.parchildrelid = child.oid)
	WHERE child.relstorage IN ('a', 'c')
	LOOP
		EXECUTE 'DROP TABLE IF EXISTS ' || quote_ident(ao_parent.nspname) || '.' || quote_ident(ao_parent.relname) || ' CASCADE';
	END LOOP;

	FOR ao_table IN
	SELECT n.nspname,
		   c.relname
	FROM pg_catalog.pg_class c
		JOIN pg_catalog.pg_namespace n ON (n.oid = c.relnamespace)
	WHERE c.relstorage IN ('a', 'c')
	LOOP
		EXECUTE 'DROP TABLE IF EXISTS ' || quote_ident(ao_table.nspname) || '.' || quote_ident(ao_table.relname) || ' CASCADE';
	END LOOP;

	RETURN;
END;
$$ LANGUAGE plpgsql;

SELECT drop_ao_tables();
DROP FUNCTION drop_ao_tables();
