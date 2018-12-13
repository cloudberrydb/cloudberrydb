--
-- testing that constraint types that are backed by indices(UNIQUE, PRIMARY KEY,
-- EXCLUSION) have constraint names that match the name of their corresponding
-- indices.  This file covers partition tables only, and covers all mechanisms
-- of DDL use of partition tables(EXCHANGE, ADD PARTITION, CREATE, etc).  We do
-- so for a TWO LEVEL(root and leaf) hierarchy and a THREE LEVEL hierarchy
--
-- we have our test cases here, but the expected file contains the desired
-- outcomes.  In other words, we do not validate the results here but do so
-- in the expected file.

DROP schema IF EXISTS index_constraint_naming_partition cascade;
CREATE schema index_constraint_naming_partition;
SET search_path='index_constraint_naming_partition';

DROP FUNCTION IF EXISTS partition_tables();
CREATE FUNCTION partition_tables() RETURNS TABLE(partition_name name, parent_name name, root_name name)
  LANGUAGE SQL STABLE STRICT AS $fn$
(SELECT partitiontablename, parentpartitiontablename, tablename
 FROM pg_partitions
 WHERE partitionschemaname='index_constraint_naming_partition'
 ORDER BY tablename
) UNION
(SELECT CAST(parrelid::regclass as name),      --pg_partition contains root
        NULL,
        CAST(p.parrelid::regclass AS name)
		FROM pg_partition p
		JOIN pg_class c ON p.parrelid=c.oid
		JOIN pg_namespace ns ON c.relnamespace=ns.oid
		WHERE ns.nspname='index_constraint_naming_partition'
);
$fn$;

DROP FUNCTION IF EXISTS constraints_and_indices();
CREATE FUNCTION constraints_and_indices() RETURNS TABLE(table_name regclass, constraint_name name, index_name regclass, constraint_type char)
  LANGUAGE SQL STABLE STRICT AS $fn$
    SELECT
      con.conrelid::regclass,
      con.conname,
      con.conindid::regclass,
      con.contype::char
    FROM
        pg_constraint con
    WHERE
        con.contype != 'c'
    ORDER BY conrelid
  ;
$fn$;

--displays all partition hierarchies in the database.
DROP FUNCTION IF EXISTS dependencies();
SET gp_recursive_cte_prototype TO on;
CREATE FUNCTION dependencies() RETURNS TABLE( depname NAME,
                                              refname NAME, classid REGCLASS, objid OID, objsubid INTEGER,
                                              refclassid REGCLASS, refobjid OID, refobjsubid OID, deptype "char" )
  LANGUAGE SQL STABLE STRICT AS $fn$
WITH RECURSIVE
     w AS (
       SELECT classid::regclass,
              objid,
              objsubid,
              refclassid::regclass,
              refobjid,
              refobjsubid,
              deptype
       FROM pg_depend d
       WHERE classid IN ('pg_constraint'::regclass, 'pg_class'::regclass)
         AND (objid > 16384 OR refobjid > 16384)

       UNION

       SELECT d2.*
       FROM w
              INNER JOIN pg_depend d2
                         ON (w.refclassid, w.refobjid, w.refobjsubid) =
                            (d2.classid, d2.objid, d2.objsubid)
     )
SELECT COALESCE(con.conname, c.relname, t.typname, nsp.nspname)     AS depname,
       COALESCE(con2.conname, c2.relname, t2.typname, nsp2.nspname) AS refname,
       w.*
FROM w
       LEFT JOIN pg_constraint con
                 ON classid = 'pg_constraint'::regclass AND objid = con.oid
       LEFT JOIN pg_class c ON classid = 'pg_class'::regclass AND objid = c.oid
       LEFT JOIN pg_type t ON classid = 'pg_type'::regclass AND objid = t.oid
       LEFT JOIN pg_namespace nsp
                 ON classid = 'pg_namespace'::regclass AND objid = nsp.oid

       LEFT JOIN pg_constraint con2
                 ON refclassid = 'pg_constraint'::regclass AND
                    refobjid = con2.oid
       LEFT JOIN pg_class c2
                 ON refclassid = 'pg_class'::regclass AND refobjid = c2.oid
       LEFT JOIN pg_type t2
                 ON refclassid = 'pg_type'::regclass AND refobjid = t2.oid
       LEFT JOIN pg_namespace nsp2 ON refclassid = 'pg_namespace'::regclass AND
                                      refobjid = nsp2.oid
  ;
$fn$;

--displays all partition hierarchies in the database.
DROP FUNCTION IF EXISTS constraints_and_indices_for_partition_tables();
CREATE FUNCTION constraints_and_indices_for_partition_tables() RETURNS TABLE(table_name regclass, parent_table name, root_table name, constraint_name name, index_name regclass, constraint_type char)
  LANGUAGE SQL STABLE STRICT AS $fn$
  (SELECT con.conrelid::regclass,      --pg_partitions contains all but root
          part.parentpartitiontablename,
          part.tablename,
          con.conname,
          con.conindid::regclass,
          con.contype::char
   FROM pg_constraint con
          INNER JOIN pg_partitions part
                     ON CAST(con.conrelid::regclass AS name) =
                        part.partitiontablename
   WHERE con.contype != 'c'
  ) UNION
  (SELECT con.conrelid::regclass,      --pg_partition contains root
          NULL,
          NULL,
          con.conname,
          con.conindid::regclass,
          con.contype::char
   FROM pg_constraint con
          INNER JOIN pg_partition rootpart
                     ON con.conrelid::regclass =
                        rootpart.parrelid::regclass
   WHERE con.contype != 'c' AND rootpart.parlevel = 0
  )
  ORDER BY conrelid
    ;
$fn$;


DROP FUNCTION IF EXISTS partition_tables_show_all();
CREATE FUNCTION partition_tables_show_all() RETURNS TABLE(partition_name name, parent_name name, root_name name, constraint_name name, index_name name, constraint_type char) --, deptype "char")
  LANGUAGE SQL STABLE STRICT AS $fn$
  SELECT p.partition_name, p.parent_name, p.root_name, c.constraint_name, c.index_name, c.constraint_type --, d.deptype
    FROM 
         (SELECT partition_name, parent_name, root_name from partition_tables()) as p
    LEFT JOIN 
         (SELECT table_name, constraint_name, CAST(index_name as name), constraint_type from constraints_and_indices()) as c
    ON p.partition_name = CAST(c.table_name as name)
   -- LEFT JOIN 
   --     (SELECT depname, refname, classid, objid, objsubid, refclassid, refobjid, refobjsubid, deptype  from dependencies()) as d
   -- ON CAST(c.table_name as NAME) = d.depname  
    ORDER BY p.partition_name;

$fn$;

--############################ TWO LEVEL ##################################

DROP FUNCTION IF EXISTS recreate_two_level_table();
CREATE FUNCTION recreate_two_level_table() RETURNS VOID
  LANGUAGE SQL VOLATILE STRICT AS $fn$
    DROP TABLE IF EXISTS r;
    CREATE TABLE r (
        r_key INTEGER NOT NULL,
        r_name CHAR(25)
    )
    DISTRIBUTED BY (r_key)
    PARTITION BY range (r_key)
    (
        PARTITION r1 START (0),
        PARTITION r2 START (5) END (8)
    );
$fn$;

-- validate that there are no constraints when we start
SELECT partition_name, parent_name, root_name, constraint_name, index_name, constraint_type FROM partition_tables_show_all();

-- UNIQUE constraint: validate we correctly add it and can only drop from root
SELECT recreate_two_level_table();
ALTER TABLE r ADD UNIQUE(r_key,r_name);
SELECT partition_name, parent_name, root_name, constraint_name, index_name, constraint_type FROM partition_tables_show_all();
INSERT INTO r VALUES (1,'xxxx'),(1,'xxxx'); --should be prevented by constraint
INSERT INTO r VALUES (6,'xxxx'),(6,'xxxx'); --should be prevented by constraint

ALTER TABLE r ADD PARTITION added_part START(8) INCLUSIVE END (10) EXCLUSIVE;
INSERT INTO r VALUES (9,'xxxx'),(9,'xxxx'); --should be prevented by constraint

SELECT partition_name, parent_name, root_name, constraint_name, index_name, constraint_type FROM partition_tables_show_all();
ALTER TABLE r_1_prt_r1 DROP CONSTRAINT r_1_prt_r1_r_key_r_name_key; --should fail
INSERT INTO r VALUES (1,'xxxx'),(1,'xxxx'); --should be prevented by constraint
INSERT INTO r VALUES (6,'xxxx'),(6,'xxxx'); --should be prevented by constraint

SELECT partition_name, parent_name, root_name, constraint_name, index_name, constraint_type FROM partition_tables_show_all();
ALTER TABLE r ADD DEFAULT PARTITION d;
INSERT INTO r VALUES (22,'xxxx'),(22,'xxxx'); --should be prevented by constraint

SELECT partition_name, parent_name, root_name, constraint_name, index_name, constraint_type FROM partition_tables_show_all();
ALTER TABLE r SPLIT PARTITION r2 AT (6) INTO (PARTITION r2_split_l, PARTITION r2_split_r);
SELECT partition_name, parent_name, root_name, constraint_name, index_name, constraint_type FROM partition_tables_show_all();
INSERT INTO r VALUES (5,'xxxx'),(5,'xxxx'); --should be prevented by constraint
INSERT INTO r VALUES (7,'xxxx'),(7,'xxxx'); --should be prevented by constraint


alter table r rename partition r1 to r1_renamed;
SELECT partition_name, parent_name, root_name, constraint_name, index_name, constraint_type FROM partition_tables_show_all();
INSERT INTO r VALUES (1,'xxxx'),(1,'xxxx'); --should be prevented by constraint

ALTER TABLE r DROP PARTITION r1_renamed;
INSERT INTO r VALUES (1,'xxxx'),(1,'xxxx'); --should be prevented by constraint

SELECT partition_name, parent_name, root_name, constraint_name, index_name, constraint_type FROM partition_tables_show_all();
ALTER TABLE r DROP CONSTRAINT r_r_key_r_name_key;                   --should work
SELECT partition_name, parent_name, root_name, constraint_name, index_name, constraint_type FROM partition_tables_show_all();
INSERT INTO r VALUES (1,'xxxx'),(1,'xxxx');     --should be allowed: no unique constraint exists
INSERT INTO r VALUES (6,'xxxx'),(6,'xxxx');     --should be allowed: no unique constraint exists
INSERT INTO r VALUES (9,'xxxx'),(9,'xxxx');     --should be allowed: no unique constraint exists
INSERT INTO r VALUES (22,'xxxx'),(22,'xxxx');   --should be allowed: no unique constraint exists


-- EXCHANGE... prepare table e and swap with partition
SELECT recreate_two_level_table();
ALTER TABLE r ADD UNIQUE(r_key,r_name);
SELECT partition_name, parent_name, root_name, constraint_name, index_name, constraint_type FROM partition_tables_show_all();
CREATE TABLE e (LIKE r INCLUDING CONSTRAINTS INCLUDING INDEXES);
SELECT partition_name, parent_name, root_name, constraint_name, index_name, constraint_type FROM partition_tables_show_all();
ALTER TABLE r EXCHANGE PARTITION r1 WITH TABLE e;
SELECT partition_name, parent_name, root_name, constraint_name, index_name, constraint_type FROM partition_tables_show_all();
--MAKE SURE PARTITION CONSTRAINT EXISTS...

-- UNIQUE create constraint inside CREATE TABLE DDL...this is different
DROP TABLE IF EXISTS r;
CREATE TABLE r (r_key INTEGER NOT NULL,UNIQUE(r_key)) DISTRIBUTED BY (r_key)
PARTITION BY range (r_key) (PARTITION r1 START (0), PARTITION r2 START (5) END (8));
SELECT partition_name, parent_name, root_name, constraint_name, index_name, constraint_type FROM partition_tables_show_all();
INSERT INTO r VALUES (1),(1);
INSERT INTO r VALUES (6),(6);

ALTER TABLE r DROP CONSTRAINT r_r_key_key;                   --should work
SELECT partition_name, parent_name, root_name, constraint_name, index_name, constraint_type FROM partition_tables_show_all();
INSERT INTO r VALUES (1),(1);
INSERT INTO r VALUES (6),(6);

--PRIMARY KEY constraint: validate we correctly add it and can only drop from root
SELECT recreate_two_level_table();
ALTER TABLE r ADD PRIMARY KEY(r_key,r_name);
SELECT partition_name, parent_name, root_name, constraint_name, index_name, constraint_type FROM partition_tables_show_all();
ALTER TABLE r_1_prt_r1 DROP CONSTRAINT r_1_prt_r1_pkey; --should fail
SELECT partition_name, parent_name, root_name, constraint_name, index_name, constraint_type FROM partition_tables_show_all();
ALTER TABLE r DROP CONSTRAINT r_pkey;                   --should work
SELECT partition_name, parent_name, root_name, constraint_name, index_name, constraint_type FROM partition_tables_show_all();

--EXCLUSION constraint: validate we cannot create an exclusion constraint on a
--partition table. These are disallowed in GPDB.
SELECT recreate_two_level_table();
ALTER TABLE r ADD EXCLUDE (r_key WITH =,r_name WITH =);
SELECT partition_name, parent_name, root_name, constraint_name, index_name, constraint_type FROM partition_tables_show_all();
ALTER TABLE r_1_prt_r1 DROP CONSTRAINT r_1_prt_r1_r_key_r_name_excl;   --should fail
SELECT partition_name, parent_name, root_name, constraint_name, index_name, constraint_type FROM partition_tables_show_all();

--new partition table (4) should be created with constraints.
DROP TABLE IF EXISTS r;
CREATE TABLE r (
    r_key INTEGER NOT NULL,
    r_name CHAR(25)
)
DISTRIBUTED BY (r_key)
PARTITION BY list (r_key)
(
    PARTITION r1 VALUES (0),
    PARTITION r2 VALUES (1)
);
ALTER TABLE r ADD UNIQUE(r_key,r_name);
SELECT partition_name, parent_name, root_name, constraint_name, index_name, constraint_type FROM partition_tables_show_all();
ALTER TABLE r ADD PARTITION r3 VALUES(2);
SELECT partition_name, parent_name, root_name, constraint_name, index_name, constraint_type FROM partition_tables_show_all();

-- PRIMARY create constraint inside CREATE TABLE DDL...this is different
DROP TABLE IF EXISTS r;
CREATE TABLE r (r_key INTEGER NOT NULL,PRIMARY KEY (r_key)) DISTRIBUTED BY (r_key)
  PARTITION BY range (r_key) (PARTITION r1 START (0), PARTITION r2 START (5) END (8));
SELECT partition_name, parent_name, root_name, constraint_name, index_name, constraint_type FROM partition_tables_show_all();
ALTER TABLE r DROP CONSTRAINT r_pkey;                   --should work
SELECT partition_name, parent_name, root_name, constraint_name, index_name, constraint_type FROM partition_tables_show_all();


DROP TABLE r;
-- --########### THREE LEVEL ###############################
DROP FUNCTION IF EXISTS recreate_three_level_table();
CREATE FUNCTION recreate_three_level_table() RETURNS VOID
  LANGUAGE SQL VOLATILE STRICT AS $fn$
    DROP TABLE IF EXISTS r;
    CREATE TABLE r (
        r_key INTEGER NOT NULL,
        r_name CHAR(25)
    )
    DISTRIBUTED BY (r_key)
    PARTITION BY range (r_key)
    SUBPARTITION BY list (r_name) SUBPARTITION TEMPLATE
    (
        SUBPARTITION a VALUES ('A'),
        SUBPARTITION b VALUES ('B')
    )
    (
        PARTITION r1 START (0),
        PARTITION r2 START (5) END (8)
    );
$fn$;

-- validate that there are no constraints when we start
SELECT partition_name, parent_name, root_name, constraint_name, index_name, constraint_type FROM partition_tables_show_all();

-- UNIQUE constraint: validate we correctly add it and can only drop from root
SELECT recreate_three_level_table();
ALTER TABLE r ADD UNIQUE(r_key,r_name);
SELECT partition_name, parent_name, root_name, constraint_name, index_name, constraint_type FROM partition_tables_show_all();
ALTER TABLE r_1_prt_r1 DROP CONSTRAINT r_1_prt_r1_r_key_r_name_key;                --should fail
SELECT partition_name, parent_name, root_name, constraint_name, index_name, constraint_type FROM partition_tables_show_all();
ALTER TABLE r_1_prt_r1_2_prt_a DROP CONSTRAINT r_1_prt_r1_2_prt_a_r_key_r_name_key; --should fail
SELECT partition_name, parent_name, root_name, constraint_name, index_name, constraint_type FROM partition_tables_show_all();
ALTER TABLE r DROP CONSTRAINT r_r_key_r_name_key;                                   --should work
SELECT partition_name, parent_name, root_name, constraint_name, index_name, constraint_type FROM partition_tables_show_all();

--PRIMARY KEY constraint: validate we correctly add it and can only drop from root
SELECT recreate_three_level_table();
ALTER TABLE r ADD PRIMARY KEY(r_key,r_name);
SELECT partition_name, parent_name, root_name, constraint_name, index_name, constraint_type FROM partition_tables_show_all();
ALTER TABLE r_1_prt_r1 DROP CONSTRAINT r_1_prt_r1_pkey;                --should fail
SELECT partition_name, parent_name, root_name, constraint_name, index_name, constraint_type FROM partition_tables_show_all();
ALTER TABLE r_1_prt_r1_2_prt_a DROP CONSTRAINT r_1_prt_r1_2_prt_a_pkey; --should fail
SELECT partition_name, parent_name, root_name, constraint_name, index_name, constraint_type FROM partition_tables_show_all();
ALTER TABLE r DROP CONSTRAINT r_pkey;                                   --should work
SELECT partition_name, parent_name, root_name, constraint_name, index_name, constraint_type FROM partition_tables_show_all();

--New partition table 'C' should be created with constraints.
SELECT recreate_three_level_table();
ALTER TABLE r ADD UNIQUE(r_key,r_name);
SELECT partition_name, parent_name, root_name, constraint_name, index_name, constraint_type FROM partition_tables_show_all();
ALTER TABLE r ALTER PARTITION r1 ADD PARTITION c VALUES ('C');
SELECT partition_name, parent_name, root_name, constraint_name, index_name, constraint_type FROM partition_tables_show_all();

-- EXCHANGE... prepare table e2 and swap with partition
SELECT recreate_three_level_table();
ALTER TABLE r ADD UNIQUE(r_key,r_name);
SELECT partition_name, parent_name, root_name, constraint_name, index_name, constraint_type FROM partition_tables_show_all();
CREATE TABLE e2 (LIKE r INCLUDING CONSTRAINTS INCLUDING INDEXES);
SELECT partition_name, parent_name, root_name, constraint_name, index_name, constraint_type FROM partition_tables_show_all();
ALTER TABLE r ALTER PARTITION r1 EXCHANGE PARTITION a WITH TABLE e2;
SELECT partition_name, parent_name, root_name, constraint_name, index_name, constraint_type FROM partition_tables_show_all();

-- UNIQUE: create constraint inside CREATE TABLE DDL...this is different
DROP TABLE IF EXISTS r;
CREATE TABLE r (r_key INTEGER NOT NULL,r_name CHAR(25), UNIQUE(r_key, r_name))
  DISTRIBUTED BY (r_key) PARTITION BY range (r_key)
  SUBPARTITION BY list (r_name) SUBPARTITION TEMPLATE
  (SUBPARTITION a VALUES ('A'),SUBPARTITION b VALUES ('B'))
  (PARTITION r1 START (0),PARTITION r2 START (5) END (8));
SELECT partition_name, parent_name, root_name, constraint_name, index_name, constraint_type FROM partition_tables_show_all();
ALTER TABLE r DROP CONSTRAINT r_key;
SELECT partition_name, parent_name, root_name, constraint_name, index_name, constraint_type FROM partition_tables_show_all();

-- PRIMARY: create constraint inside CREATE TABLE DDL...this is different
DROP TABLE IF EXISTS r;
CREATE TABLE r (r_key INTEGER NOT NULL,r_name CHAR(25), PRIMARY KEY(r_key, r_name))
  DISTRIBUTED BY (r_key) PARTITION BY range (r_key)
  SUBPARTITION BY list (r_name) SUBPARTITION TEMPLATE
(SUBPARTITION a VALUES ('A'),SUBPARTITION b VALUES ('B'))
(PARTITION r1 START (0),PARTITION r2 START (5) END (8));
SELECT partition_name, parent_name, root_name, constraint_name, index_name, constraint_type FROM partition_tables_show_all();
ALTER TABLE r DROP CONSTRAINT r_pkey;
SELECT partition_name, parent_name, root_name, constraint_name, index_name, constraint_type FROM partition_tables_show_all();

---------------------------------------------------------------------
-- You should not be able to add a constraint to partition leaves
---------------------------------------------------------------------
SELECT recreate_two_level_table();
ALTER TABLE r_1_prt_r2 ADD UNIQUE(r_key); -- Should fail.

---------------------------------------------------------------------
-- Cannot add a dangling constraint to a leaf partition of a multi-level table
---------------------------------------------------------------------
SELECT recreate_three_level_table();
ALTER TABLE r_1_prt_r1_2_prt_a ADD UNIQUE(r_key,r_name); -- should fail

---------------------------------------------------------------------
-- EXCHANGE PARTITION
---------------------------------------------------------------------
-- exchange partition should fail with dangling constraint ONLY on new table
---------------------------------------------------------------------
SELECT recreate_two_level_table();
DROP TABLE IF EXISTS e;
CREATE TABLE e (LIKE r INCLUDING CONSTRAINTS INCLUDING INDEXES);
ALTER TABLE e ADD UNIQUE(r_key); -- add dangling constraint to new table
SELECT partition_name, parent_name, root_name, constraint_name, index_name, constraint_type FROM partition_tables_show_all();
ALTER TABLE r EXCHANGE PARTITION r2 WITH TABLE e; -- should fail
SELECT partition_name, parent_name, root_name, constraint_name, index_name, constraint_type FROM partition_tables_show_all();

---------------------------------------------------------------------
-- exchange partition: constraint on root should be added to old partition before exchange
---------------------------------------------------------------------
SELECT recreate_two_level_table();
DROP TABLE IF EXISTS e;
CREATE TABLE e (LIKE r INCLUDING CONSTRAINTS INCLUDING INDEXES);
SET sql_inheritance TO off;
ALTER TABLE r ADD UNIQUE(r_key); -- add dangling constraint to new table
SET sql_inheritance TO on;
SELECT partition_name, parent_name, root_name, constraint_name, index_name, constraint_type FROM partition_tables_show_all();
ALTER TABLE r EXCHANGE PARTITION r2 WITH TABLE e; -- should fail
SELECT partition_name, parent_name, root_name, constraint_name, index_name, constraint_type FROM partition_tables_show_all();

DROP TABLE IF EXISTS r;
