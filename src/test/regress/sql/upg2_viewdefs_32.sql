-- The 32 version of various system and information_schema views

--
-- PG_CATALOG views
--
set search_path='pg_catalog';

-- pg_catalog.pg_partitions
create view public.pg_partitions as
SELECT p1.schemaname, p1.tablename, p1.partitionschemaname, p1.partitiontablename, p1.partitionname, p1.parentpartitiontablename, p1.parentpartitionname, p1.partitiontype, p1.partitionlevel,
        CASE
            WHEN p1.partitiontype <> 'range'::text THEN NULL::bigint
            WHEN p1.partitionnodefault > 0 THEN p1.partitionrank
            WHEN p1.partitionrank = 1 THEN NULL::bigint
            ELSE p1.partitionrank - 1
        END AS partitionrank, p1.partitionposition, p1.partitionlistvalues, p1.partitionrangestart,
        CASE
            WHEN p1.partitiontype = 'range'::text THEN p1.partitionstartinclusive
            ELSE NULL::boolean
        END AS partitionstartinclusive, p1.partitionrangeend,
        CASE
            WHEN p1.partitiontype = 'range'::text THEN p1.partitionendinclusive
            ELSE NULL::boolean
        END AS partitionendinclusive, p1.partitioneveryclause, p1.parisdefault AS partitionisdefault, p1.partitionboundary
   FROM ( SELECT n.nspname AS schemaname, cl.relname AS tablename, n2.nspname AS partitionschemaname, cl2.relname AS partitiontablename, pr1.parname AS partitionname, cl3.relname AS parentpartitiontablename, pr2.parname AS parentpartitionname,
                CASE
                    WHEN pp.parkind = 'h'::"char" THEN 'hash'::text
                    WHEN pp.parkind = 'r'::"char" THEN 'range'::text
                    WHEN pp.parkind = 'l'::"char" THEN 'list'::text
                    ELSE NULL::text
                END AS partitiontype, pp.parlevel AS partitionlevel, pr1.parruleord AS partitionposition, rank() OVER(
         PARTITION BY pp.oid, cl.relname, pp.parlevel, cl3.relname
          ORDER BY pr1.parruleord) AS partitionrank, pg_get_expr(pr1.parlistvalues, pr1.parchildrelid) AS partitionlistvalues, pg_get_expr(pr1.parrangestart, pr1.parchildrelid) AS partitionrangestart, pr1.parrangestartincl AS partitionstartinclusive, pg_get_expr(pr1.parrangeend, pr1.parchildrelid) AS partitionrangeend, pr1.parrangeendincl AS partitionendinclusive, pg_get_expr(pr1.parrangeevery, pr1.parchildrelid) AS partitioneveryclause, min(pr1.parruleord) OVER(
         PARTITION BY pp.oid, cl.relname, pp.parlevel, cl3.relname
          ORDER BY pr1.parruleord) AS partitionnodefault, pr1.parisdefault, pg_get_partition_rule_def(pr1.oid, true) AS partitionboundary
           FROM pg_namespace n, pg_namespace n2, pg_class cl, pg_class cl2, pg_partition pp, pg_partition_rule pr1
      LEFT JOIN pg_partition_rule pr2 ON pr1.parparentrule = pr2.oid
   LEFT JOIN pg_class cl3 ON pr2.parchildrelid = cl3.oid
  WHERE pp.paristemplate = false AND pp.parrelid = cl.oid AND pr1.paroid = pp.oid AND cl2.oid = pr1.parchildrelid AND cl.relnamespace = n.oid AND cl2.relnamespace = n2.oid) p1;

create view public.pg_partition_templates as
select
schemaname,
tablename, 
partitionname,
partitiontype, 
partitionlevel,
case when (partitiontype != 'range') then NULL
	 when (partitionnodefault > 0) then partitionrank
	 when (partitionrank = 1) then NULL
	 else  partitionrank - 1
end as partitionrank,
partitionposition,
partitionlistvalues,
partitionrangestart,
case when (partitiontype = 'range') then partitionstartinclusive
	 else NULL
end as partitionstartinclusive,
partitionrangeend,
case when (partitiontype = 'range') then partitionendinclusive
	else NULL
end as partitionendinclusive,
partitioneveryclause,
parisdefault as partitionisdefault,
partitionboundary
from (
select
n.nspname as schemaname,
cl.relname as tablename,
pr1.parname as partitionname,
p.parlevel as partitionlevel,
pr1.parruleord as partitionposition,
rank() over (partition by p.oid, cl.relname, p.parlevel 
			 order by pr1.parruleord) as partitionrank,
pg_get_expr(pr1.parlistvalues, p.parrelid) as partitionlistvalues,
pg_get_expr(pr1.parrangestart, p.parrelid) as partitionrangestart,
pr1.parrangestartincl as partitionstartinclusive,
pg_get_expr(pr1.parrangeend, p.parrelid) as partitionrangeend,
pr1.parrangeendincl as partitionendinclusive,
pg_get_expr(pr1.parrangeevery, p.parrelid) as partitioneveryclause,
min(pr1.parruleord) over (partition by p.oid, cl.relname, p.parlevel
	order by pr1.parruleord) as partitionnodefault,
pr1.parisdefault,
case when p.parkind = 'h' then 'hash' when p.parkind = 'r' then 'range'
	 when p.parkind = 'l' then 'list' else null end as partitiontype, 
pg_get_partition_rule_def(pr1.oid, true) as partitionboundary
from pg_namespace n, pg_class cl, pg_partition p, pg_partition_rule pr1
where 
 p.parrelid = cl.oid and 
 pr1.paroid = p.oid and
 cl.relnamespace = n.oid and
 p.paristemplate = 't'
 ) p1;

-- pg_catalog.pg_roles
create view public.pg_roles as 
    select 
        rolname,
        rolsuper,
        rolinherit,
        rolcreaterole,
        rolcreatedb,
        rolcatupdate,
        rolcanlogin,
        rolconnlimit,
        '********'::text as rolpassword,
        rolvaliduntil,
        rolconfig,
		rolresqueue,
        oid 
    from pg_authid;

--
-- INFORMATION_SCHEMA views
--
set search_path='information_schema';


-- information_schema.parameters
CREATE VIEW public.parameters AS
    SELECT CAST(current_database() AS sql_identifier) AS specific_catalog,
           CAST(n_nspname AS sql_identifier) AS specific_schema,
           CAST(proname || '_' || CAST(p_oid AS text) AS sql_identifier) AS specific_name,
           CAST((ss.x).n AS cardinal_number) AS ordinal_position,
           CAST(
             CASE WHEN proargmodes IS NULL THEN 'IN'
                WHEN proargmodes[(ss.x).n] = 'i' THEN 'IN'
                WHEN proargmodes[(ss.x).n] = 'o' THEN 'OUT'
                WHEN proargmodes[(ss.x).n] = 'b' THEN 'INOUT'
             END AS character_data) AS parameter_mode,
           CAST('NO' AS character_data) AS is_result,
           CAST('NO' AS character_data) AS as_locator,
           CAST(NULLIF(proargnames[(ss.x).n], '') AS sql_identifier) AS parameter_name,
           CAST(
             CASE WHEN t.typelem <> 0 AND t.typlen = -1 THEN 'ARRAY'
                  WHEN nt.nspname = 'pg_catalog' THEN format_type(t.oid, null)
                  ELSE 'USER-DEFINED' END AS character_data)
             AS data_type,
           CAST(null AS cardinal_number) AS character_maximum_length,
           CAST(null AS cardinal_number) AS character_octet_length,
           CAST(null AS sql_identifier) AS character_set_catalog,
           CAST(null AS sql_identifier) AS character_set_schema,
           CAST(null AS sql_identifier) AS character_set_name,
           CAST(null AS sql_identifier) AS collation_catalog,
           CAST(null AS sql_identifier) AS collation_schema,
           CAST(null AS sql_identifier) AS collation_name,
           CAST(null AS cardinal_number) AS numeric_precision,
           CAST(null AS cardinal_number) AS numeric_precision_radix,
           CAST(null AS cardinal_number) AS numeric_scale,
           CAST(null AS cardinal_number) AS datetime_precision,
           CAST(null AS character_data) AS interval_type,
           CAST(null AS character_data) AS interval_precision,
           CAST(current_database() AS sql_identifier) AS udt_catalog,
           CAST(nt.nspname AS sql_identifier) AS udt_schema,
           CAST(t.typname AS sql_identifier) AS udt_name,
           CAST(null AS sql_identifier) AS scope_catalog,
           CAST(null AS sql_identifier) AS scope_schema,
           CAST(null AS sql_identifier) AS scope_name,
           CAST(null AS cardinal_number) AS maximum_cardinality,
           CAST((ss.x).n AS sql_identifier) AS dtd_identifier

    FROM pg_type t, pg_namespace nt,
         (SELECT n.nspname AS n_nspname, p.proname, p.oid AS p_oid,
                 p.proargnames, p.proargmodes,
                 _pg_expandarray(coalesce(p.proallargtypes, p.proargtypes::oid[])) AS x
          FROM pg_namespace n, pg_proc p
          WHERE n.oid = p.pronamespace
                AND (pg_has_role(p.proowner, 'USAGE') OR
                     has_function_privilege(p.oid, 'EXECUTE'))) AS ss
    WHERE t.oid = (ss.x).x AND t.typnamespace = nt.oid;

-- information_schema.role_usage_grants
CREATE VIEW public.role_usage_grants AS
    SELECT CAST(null AS sql_identifier) AS grantor,
           CAST(null AS sql_identifier) AS grantee,
           CAST(current_database() AS sql_identifier) AS object_catalog,
           CAST(null AS sql_identifier) AS object_schema,
           CAST(null AS sql_identifier) AS object_name,
           CAST(null AS character_data) AS object_type,
           CAST('USAGE' AS character_data) AS privilege_type,
           CAST(null AS character_data) AS is_grantable
    WHERE false;

-- information_schema.usage_privileges
CREATE VIEW public.usage_privileges AS
    SELECT CAST(u.rolname AS sql_identifier) AS grantor,
           CAST('PUBLIC' AS sql_identifier) AS grantee,
           CAST(current_database() AS sql_identifier) AS object_catalog,
           CAST(n.nspname AS sql_identifier) AS object_schema,
           CAST(t.typname AS sql_identifier) AS object_name,
           CAST('DOMAIN' AS character_data) AS object_type,
           CAST('USAGE' AS character_data) AS privilege_type,
           CAST('NO' AS character_data) AS is_grantable
    FROM pg_authid u,
         pg_namespace n,
         pg_type t
    WHERE u.oid = t.typowner
          AND t.typnamespace = n.oid
          AND t.typtype = 'd';

-- information_schema.check_constraints
CREATE VIEW public.check_constraints AS
    SELECT CAST(current_database() AS sql_identifier) AS constraint_catalog,
           CAST(rs.nspname AS sql_identifier) AS constraint_schema,
           CAST(con.conname AS sql_identifier) AS constraint_name,
           CAST(substring(pg_get_constraintdef(con.oid) from 7) AS character_data)
             AS check_clause
    FROM pg_constraint con
           LEFT OUTER JOIN pg_namespace rs ON (rs.oid = con.connamespace)
           LEFT OUTER JOIN pg_class c ON (c.oid = con.conrelid)
           LEFT OUTER JOIN pg_type t ON (t.oid = con.contypid)
    WHERE pg_has_role(coalesce(c.relowner, t.typowner), 'USAGE')
      AND con.contype = 'c'
    UNION
    -- not-null constraints
    SELECT CAST(current_database() AS sql_identifier) AS constraint_catalog,
           CAST(n.nspname AS sql_identifier) AS constraint_schema,
           CAST(n.oid || '_' || r.oid || '_' || a.attnum || '_not_null' AS sql_identifier) AS constraint_name, -- XXX
           CAST(a.attname || ' IS NOT NULL' AS character_data)
             AS check_clause
    FROM pg_namespace n, pg_class r, pg_attribute a
    WHERE n.oid = r.relnamespace
      AND r.oid = a.attrelid
      AND a.attnum > 0
      AND NOT a.attisdropped
      AND a.attnotnull
      AND r.relkind = 'r'
      AND pg_has_role(r.relowner, 'USAGE');

-- information_schema.table_constraints
CREATE VIEW public.table_constraints AS
    SELECT CAST(current_database() AS sql_identifier) AS constraint_catalog,
           CAST(nc.nspname AS sql_identifier) AS constraint_schema,
           CAST(c.conname AS sql_identifier) AS constraint_name,
           CAST(current_database() AS sql_identifier) AS table_catalog,
           CAST(nr.nspname AS sql_identifier) AS table_schema,
           CAST(r.relname AS sql_identifier) AS table_name,
           CAST(
             CASE c.contype WHEN 'c' THEN 'CHECK'
                            WHEN 'f' THEN 'FOREIGN KEY'
                            WHEN 'p' THEN 'PRIMARY KEY'
                            WHEN 'u' THEN 'UNIQUE' END
             AS character_data) AS constraint_type,
           CAST(CASE WHEN c.condeferrable THEN 'YES' ELSE 'NO' END AS character_data)
             AS is_deferrable,
           CAST(CASE WHEN c.condeferred THEN 'YES' ELSE 'NO' END AS character_data)
             AS initially_deferred
    FROM pg_namespace nc,
         pg_namespace nr,
         pg_constraint c,
         pg_class r
    WHERE nc.oid = c.connamespace AND nr.oid = r.relnamespace
          AND c.conrelid = r.oid
          AND r.relkind = 'r'
          AND (NOT pg_is_other_temp_schema(nr.oid))
          AND (pg_has_role(r.relowner, 'USAGE')
               -- SELECT privilege omitted, per SQL standard
               OR has_table_privilege(r.oid, 'INSERT')
               OR has_table_privilege(r.oid, 'UPDATE')
               OR has_table_privilege(r.oid, 'DELETE')
               OR has_table_privilege(r.oid, 'REFERENCES')
               OR has_table_privilege(r.oid, 'TRIGGER') )
    UNION
    -- not-null constraints
    SELECT CAST(current_database() AS sql_identifier) AS constraint_catalog,
           CAST(nr.nspname AS sql_identifier) AS constraint_schema,
           CAST(nr.oid || '_' || r.oid || '_' || a.attnum || '_not_null' AS sql_identifier) AS constraint_name, -- XXX
           CAST(current_database() AS sql_identifier) AS table_catalog,
           CAST(nr.nspname AS sql_identifier) AS table_schema,
           CAST(r.relname AS sql_identifier) AS table_name,
           CAST('CHECK' AS character_data) AS constraint_type,
           CAST('NO' AS character_data) AS is_deferrable,
           CAST('NO' AS character_data) AS initially_deferred
    FROM pg_namespace nr,
         pg_class r,
         pg_attribute a
    WHERE nr.oid = r.relnamespace
          AND r.oid = a.attrelid
          AND a.attnotnull
          AND a.attnum > 0
          AND NOT a.attisdropped
          AND r.relkind = 'r'
          AND (NOT pg_is_other_temp_schema(nr.oid))
          AND (pg_has_role(r.relowner, 'USAGE')
               OR has_table_privilege(r.oid, 'SELECT')
               OR has_table_privilege(r.oid, 'INSERT')
               OR has_table_privilege(r.oid, 'UPDATE')
               OR has_table_privilege(r.oid, 'DELETE')
               OR has_table_privilege(r.oid, 'REFERENCES')
               OR has_table_privilege(r.oid, 'TRIGGER') );

-- information_schema.views
CREATE VIEW public.views AS
    SELECT CAST(current_database() AS sql_identifier) AS table_catalog,
           CAST(nc.nspname AS sql_identifier) AS table_schema,
           CAST(c.relname AS sql_identifier) AS table_name,
           CAST(
             CASE WHEN pg_has_role(c.relowner, 'USAGE')
                  THEN pg_get_viewdef(c.oid)
                  ELSE null END
             AS character_data) AS view_definition,
           CAST('NONE' AS character_data) AS check_option,
           CAST(
             CASE WHEN EXISTS (SELECT 1 FROM pg_rewrite WHERE ev_class = c.oid AND ev_type = 2 AND is_instead)
                   AND EXISTS (SELECT 1 FROM pg_rewrite WHERE ev_class = c.oid AND ev_type = 4 AND is_instead)
                  THEN 'YES' ELSE 'NO' END
             AS character_data) AS is_updatable,
           CAST(
             CASE WHEN EXISTS (SELECT 1 FROM pg_rewrite WHERE ev_class = c.oid AND ev_type = 3 AND is_instead)
                  THEN 'YES' ELSE 'NO' END
             AS character_data) AS is_insertable_into
    FROM pg_namespace nc, pg_class c
    WHERE c.relnamespace = nc.oid
          AND c.relkind = 'v'
          AND (NOT pg_is_other_temp_schema(nc.oid))
          AND (pg_has_role(c.relowner, 'USAGE')
               OR has_table_privilege(c.oid, 'SELECT')
               OR has_table_privilege(c.oid, 'INSERT')
               OR has_table_privilege(c.oid, 'UPDATE')
               OR has_table_privilege(c.oid, 'DELETE')
               OR has_table_privilege(c.oid, 'REFERENCES')
               OR has_table_privilege(c.oid, 'TRIGGER') );

-- information_schema.element_types
CREATE VIEW public.element_types AS
    SELECT CAST(current_database() AS sql_identifier) AS object_catalog,
           CAST(n.nspname AS sql_identifier) AS object_schema,
           CAST(x.objname AS sql_identifier) AS object_name,
           CAST(x.objtype AS character_data) AS object_type,
           CAST(x.objdtdid AS sql_identifier) AS collection_type_identifier,
           CAST(
             CASE WHEN nbt.nspname = 'pg_catalog' THEN format_type(bt.oid, null)
                  ELSE 'USER-DEFINED' END AS character_data) AS data_type,
           CAST(null AS cardinal_number) AS character_maximum_length,
           CAST(null AS cardinal_number) AS character_octet_length,
           CAST(null AS sql_identifier) AS character_set_catalog,
           CAST(null AS sql_identifier) AS character_set_schema,
           CAST(null AS sql_identifier) AS character_set_name,
           CAST(null AS sql_identifier) AS collation_catalog,
           CAST(null AS sql_identifier) AS collation_schema,
           CAST(null AS sql_identifier) AS collation_name,
           CAST(null AS cardinal_number) AS numeric_precision,
           CAST(null AS cardinal_number) AS numeric_precision_radix,
           CAST(null AS cardinal_number) AS numeric_scale,
           CAST(null AS cardinal_number) AS datetime_precision,
           CAST(null AS character_data) AS interval_type,
           CAST(null AS character_data) AS interval_precision,           
           CAST(null AS character_data) AS domain_default, -- XXX maybe a bug in the standard
           CAST(current_database() AS sql_identifier) AS udt_catalog,
           CAST(nbt.nspname AS sql_identifier) AS udt_schema,
           CAST(bt.typname AS sql_identifier) AS udt_name,
           CAST(null AS sql_identifier) AS scope_catalog,
           CAST(null AS sql_identifier) AS scope_schema,
           CAST(null AS sql_identifier) AS scope_name,
           CAST(null AS cardinal_number) AS maximum_cardinality,
           CAST('a' || x.objdtdid AS sql_identifier) AS dtd_identifier
    FROM pg_namespace n, pg_type at, pg_namespace nbt, pg_type bt,
         (
           /* columns */
           SELECT c.relnamespace, CAST(c.relname AS sql_identifier),
                  'TABLE'::text, a.attnum, a.atttypid
           FROM pg_class c, pg_attribute a
           WHERE c.oid = a.attrelid
                 AND c.relkind IN ('r', 'v')
                 AND attnum > 0 AND NOT attisdropped
           UNION ALL
           /* domains */
           SELECT t.typnamespace, CAST(t.typname AS sql_identifier),
                  'DOMAIN'::text, 1, t.typbasetype
           FROM pg_type t
           WHERE t.typtype = 'd'
           UNION ALL
           /* parameters */
           SELECT pronamespace, CAST(proname || '_' || CAST(oid AS text) AS sql_identifier),
                  'ROUTINE'::text, (ss.x).n, (ss.x).x
           FROM (SELECT p.pronamespace, p.proname, p.oid,
                        _pg_expandarray(coalesce(p.proallargtypes, p.proargtypes::oid[])) AS x
                 FROM pg_proc p) AS ss
           UNION ALL
           /* result types */
           SELECT p.pronamespace, CAST(p.proname || '_' || CAST(p.oid AS text) AS sql_identifier),
                  'ROUTINE'::text, 0, p.prorettype
           FROM pg_proc p
         ) AS x (objschema, objname, objtype, objdtdid, objtypeid)
    WHERE n.oid = x.objschema
          AND at.oid = x.objtypeid
          AND (at.typelem <> 0 AND at.typlen = -1)
          AND at.typelem = bt.oid
          AND nbt.oid = bt.typnamespace
          AND (n.nspname, x.objname, x.objtype, x.objdtdid) IN
              ( SELECT object_schema, object_name, object_type, dtd_identifier
                    FROM data_type_privileges );

-- information_schema.tables
CREATE VIEW public.tables AS
    SELECT CAST(current_database() AS sql_identifier) AS table_catalog,
           CAST(nc.nspname AS sql_identifier) AS table_schema,
           CAST(c.relname AS sql_identifier) AS table_name,

           CAST(
             CASE WHEN nc.oid = pg_my_temp_schema() THEN 'LOCAL TEMPORARY'
                  WHEN c.relkind = 'r' THEN 'BASE TABLE'
                  WHEN c.relkind = 'v' THEN 'VIEW'
                  ELSE null END
             AS character_data) AS table_type,

           CAST(null AS sql_identifier) AS self_referencing_column_name,
           CAST(null AS character_data) AS reference_generation,

           CAST(null AS sql_identifier) AS user_defined_type_catalog,
           CAST(null AS sql_identifier) AS user_defined_type_schema,
           CAST(null AS sql_identifier) AS user_defined_type_name,

           CAST(CASE WHEN c.relkind = 'r'
                THEN 'YES' ELSE 'NO' END AS character_data) AS is_insertable_into,
           CAST('NO' AS character_data) AS is_typed,
           CAST(
             CASE WHEN nc.oid = pg_my_temp_schema() THEN 'PRESERVE' -- FIXME
                  ELSE null END
             AS character_data) AS commit_action

    FROM pg_namespace nc, pg_class c

    WHERE c.relnamespace = nc.oid
          AND c.relkind IN ('r', 'v')
          AND (NOT pg_is_other_temp_schema(nc.oid))
          AND (pg_has_role(c.relowner, 'USAGE')
               OR has_table_privilege(c.oid, 'SELECT')
               OR has_table_privilege(c.oid, 'INSERT')
               OR has_table_privilege(c.oid, 'UPDATE')
               OR has_table_privilege(c.oid, 'DELETE')
               OR has_table_privilege(c.oid, 'REFERENCES')
               OR has_table_privilege(c.oid, 'TRIGGER') );

-- Done creating views, restore search path to public
set search_path='public';
