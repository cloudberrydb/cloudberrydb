-- Operator Classes defined in the 4.0 Catalog:
--
-- Note: There are a couple operator classes that cannot be built with
-- the definition that exists within the catalog, including
--    cidr_ops            : operators are over "inet", but should be "cidr"
--    varchar_pattern_ops : operators are over "text", but should be "varchar"
--    varchar_ops         : operators are over "text", but should be "varchar"
--
-- To deal with this the test manually changes the type these operators
-- are over and replaces them with the type referred to in the operator,
-- this in turn requires the operators not to be "Default".  The checks
-- on operators must take this into account.
--
-- Derived via the following SQL run within a 4.0 catalog
--
/*
\o /tmp/opclass
SELECT 'CREATE OPERATOR CLASS ' || quote_ident(opcname) || ' '
    || case when t.typname = 'varchar' then ' FOR TYPE upg_catalog.text '
            when t.typname = 'cidr'    then ' FOR TYPE upg_catalog.inet '
            else case when opcdefault then 'DEFAULT ' else '' end
            || 'FOR TYPE upg_catalog.' || quote_ident(t.typname) || ' '
            end
    || 'USING ' || amname || ' '
    || E'AS\n'
    || array_to_string(array(
          SELECT '  '||kind||' '||strategy||' '||name||'('||param||')'||option
          FROM (
              SELECT amopclaid    as classid,
                     amopsubtype  as subtype,
                     0            as sort,
                     amopstrategy as strategy,
                     'OPERATOR'   as kind,
                     'upg_catalog.' || op.oprname as name,
                     'upg_catalog.' || t1.typname 
                                    || ', upg_catalog.' 
                                    || t2.typname as param,
                     case when amopreqcheck then ' RECHECK' else '' end as option
              FROM pg_amop amop
                   join pg_operator op on (amopopr = op.oid)
                   left join pg_type t1 on (op.oprleft = t1.oid)
                   left join pg_type t2 on (op.oprright = t2.oid)
              UNION ALL
              SELECT amopclaid     as classid,
                     amprocsubtype as subtype,
                     1             as sort,
                     amprocnum     as opstrategy,
                     'FUNCTION'    as opkind,
                     'upg_catalog.' || quote_ident(p.proname) as name,
                      coalesce(array_to_string(array(
                          SELECT case when typname = 'internal' 
                                      then 'pg_catalog.' 
                                      else 'upg_catalog.' end || quote_ident(typname)
                          FROM pg_type t, generate_series(1, pronargs) i
                          WHERE t.oid = proargtypes[i-1]
                          ORDER BY i), ', '), '') as param,
                      '' as option
              FROM pg_amproc amproc
                   join pg_proc p on (amproc.amproc = p.oid)
              ) q
          WHERE classid = opc.oid  -- correlate to outer query block
          ORDER BY subtype, sort, strategy), E',\n')
    || coalesce(E',\n  STORAGE ' || quote_ident(keytype.typname), '') || ';'
FROM pg_opclass opc
     join pg_type t on (opc.opcintype = t.oid)
     join pg_am am on (opc.opcamid = am.oid)
     join pg_namespace n on (opc.opcnamespace = n.oid)
     left join pg_type keytype on (opc.opckeytype = keytype.oid)
WHERE n.nspname = 'pg_catalog';
*/
 CREATE OPERATOR CLASS bool_ops DEFAULT FOR TYPE upg_catalog.bool USING bitmap AS
   OPERATOR 1 upg_catalog.<(upg_catalog.bool, upg_catalog.bool),
   OPERATOR 2 upg_catalog.<=(upg_catalog.bool, upg_catalog.bool),
   OPERATOR 3 upg_catalog.=(upg_catalog.bool, upg_catalog.bool),
   OPERATOR 4 upg_catalog.>=(upg_catalog.bool, upg_catalog.bool),
   OPERATOR 5 upg_catalog.>(upg_catalog.bool, upg_catalog.bool),
   FUNCTION 1 upg_catalog.btboolcmp(upg_catalog.bool, upg_catalog.bool);
 CREATE OPERATOR CLASS bool_ops DEFAULT FOR TYPE upg_catalog.bool USING hash AS
   OPERATOR 1 upg_catalog.=(upg_catalog.bool, upg_catalog.bool),
   FUNCTION 1 upg_catalog.hashchar(upg_catalog."char");
 CREATE OPERATOR CLASS bool_ops DEFAULT FOR TYPE upg_catalog.bool USING btree AS
   OPERATOR 1 upg_catalog.<(upg_catalog.bool, upg_catalog.bool),
   OPERATOR 2 upg_catalog.<=(upg_catalog.bool, upg_catalog.bool),
   OPERATOR 3 upg_catalog.=(upg_catalog.bool, upg_catalog.bool),
   OPERATOR 4 upg_catalog.>=(upg_catalog.bool, upg_catalog.bool),
   OPERATOR 5 upg_catalog.>(upg_catalog.bool, upg_catalog.bool),
   FUNCTION 1 upg_catalog.btboolcmp(upg_catalog.bool, upg_catalog.bool);
 CREATE OPERATOR CLASS bytea_ops DEFAULT FOR TYPE upg_catalog.bytea USING bitmap AS
   OPERATOR 1 upg_catalog.<(upg_catalog.bytea, upg_catalog.bytea),
   OPERATOR 2 upg_catalog.<=(upg_catalog.bytea, upg_catalog.bytea),
   OPERATOR 3 upg_catalog.=(upg_catalog.bytea, upg_catalog.bytea),
   OPERATOR 4 upg_catalog.>=(upg_catalog.bytea, upg_catalog.bytea),
   OPERATOR 5 upg_catalog.>(upg_catalog.bytea, upg_catalog.bytea),
   FUNCTION 1 upg_catalog.byteacmp(upg_catalog.bytea, upg_catalog.bytea);
 CREATE OPERATOR CLASS bytea_ops DEFAULT FOR TYPE upg_catalog.bytea USING hash AS
   OPERATOR 1 upg_catalog.=(upg_catalog.bytea, upg_catalog.bytea),
   FUNCTION 1 upg_catalog.hashvarlena(pg_catalog.internal);
 CREATE OPERATOR CLASS bytea_ops DEFAULT FOR TYPE upg_catalog.bytea USING btree AS
   OPERATOR 1 upg_catalog.<(upg_catalog.bytea, upg_catalog.bytea),
   OPERATOR 2 upg_catalog.<=(upg_catalog.bytea, upg_catalog.bytea),
   OPERATOR 3 upg_catalog.=(upg_catalog.bytea, upg_catalog.bytea),
   OPERATOR 4 upg_catalog.>=(upg_catalog.bytea, upg_catalog.bytea),
   OPERATOR 5 upg_catalog.>(upg_catalog.bytea, upg_catalog.bytea),
   FUNCTION 1 upg_catalog.byteacmp(upg_catalog.bytea, upg_catalog.bytea);
 CREATE OPERATOR CLASS char_ops DEFAULT FOR TYPE upg_catalog."char" USING bitmap AS
   OPERATOR 1 upg_catalog.<(upg_catalog.char, upg_catalog.char),
   OPERATOR 2 upg_catalog.<=(upg_catalog.char, upg_catalog.char),
   OPERATOR 3 upg_catalog.=(upg_catalog.char, upg_catalog.char),
   OPERATOR 4 upg_catalog.>=(upg_catalog.char, upg_catalog.char),
   OPERATOR 5 upg_catalog.>(upg_catalog.char, upg_catalog.char),
   FUNCTION 1 upg_catalog.btcharcmp(upg_catalog."char", upg_catalog."char");
 CREATE OPERATOR CLASS char_ops DEFAULT FOR TYPE upg_catalog."char" USING hash AS
   OPERATOR 1 upg_catalog.=(upg_catalog.char, upg_catalog.char),
   FUNCTION 1 upg_catalog.hashchar(upg_catalog."char");
 CREATE OPERATOR CLASS char_ops DEFAULT FOR TYPE upg_catalog."char" USING btree AS
   OPERATOR 1 upg_catalog.<(upg_catalog.char, upg_catalog.char),
   OPERATOR 2 upg_catalog.<=(upg_catalog.char, upg_catalog.char),
   OPERATOR 3 upg_catalog.=(upg_catalog.char, upg_catalog.char),
   OPERATOR 4 upg_catalog.>=(upg_catalog.char, upg_catalog.char),
   OPERATOR 5 upg_catalog.>(upg_catalog.char, upg_catalog.char),
   FUNCTION 1 upg_catalog.btcharcmp(upg_catalog."char", upg_catalog."char");
 CREATE OPERATOR CLASS name_pattern_ops FOR TYPE upg_catalog.name USING bitmap AS
   OPERATOR 1 upg_catalog.~<~(upg_catalog.name, upg_catalog.name),
   OPERATOR 2 upg_catalog.~<=~(upg_catalog.name, upg_catalog.name),
   OPERATOR 3 upg_catalog.~=~(upg_catalog.name, upg_catalog.name),
   OPERATOR 4 upg_catalog.~>=~(upg_catalog.name, upg_catalog.name),
   OPERATOR 5 upg_catalog.~>~(upg_catalog.name, upg_catalog.name),
   FUNCTION 1 upg_catalog.btname_pattern_cmp(upg_catalog.name, upg_catalog.name);
 CREATE OPERATOR CLASS name_ops DEFAULT FOR TYPE upg_catalog.name USING bitmap AS
   OPERATOR 1 upg_catalog.<(upg_catalog.name, upg_catalog.name),
   OPERATOR 2 upg_catalog.<=(upg_catalog.name, upg_catalog.name),
   OPERATOR 3 upg_catalog.=(upg_catalog.name, upg_catalog.name),
   OPERATOR 4 upg_catalog.>=(upg_catalog.name, upg_catalog.name),
   OPERATOR 5 upg_catalog.>(upg_catalog.name, upg_catalog.name),
   FUNCTION 1 upg_catalog.btnamecmp(upg_catalog.name, upg_catalog.name);
 CREATE OPERATOR CLASS name_pattern_ops FOR TYPE upg_catalog.name USING hash AS
   OPERATOR 1 upg_catalog.~=~(upg_catalog.name, upg_catalog.name),
   FUNCTION 1 upg_catalog.hashname(upg_catalog.name);
 CREATE OPERATOR CLASS name_pattern_ops FOR TYPE upg_catalog.name USING btree AS
   OPERATOR 1 upg_catalog.~<~(upg_catalog.name, upg_catalog.name),
   OPERATOR 2 upg_catalog.~<=~(upg_catalog.name, upg_catalog.name),
   OPERATOR 3 upg_catalog.~=~(upg_catalog.name, upg_catalog.name),
   OPERATOR 4 upg_catalog.~>=~(upg_catalog.name, upg_catalog.name),
   OPERATOR 5 upg_catalog.~>~(upg_catalog.name, upg_catalog.name),
   FUNCTION 1 upg_catalog.btname_pattern_cmp(upg_catalog.name, upg_catalog.name);
 CREATE OPERATOR CLASS name_ops DEFAULT FOR TYPE upg_catalog.name USING hash AS
   OPERATOR 1 upg_catalog.=(upg_catalog.name, upg_catalog.name),
   FUNCTION 1 upg_catalog.hashname(upg_catalog.name);
 CREATE OPERATOR CLASS name_ops DEFAULT FOR TYPE upg_catalog.name USING btree AS
   OPERATOR 1 upg_catalog.<(upg_catalog.name, upg_catalog.name),
   OPERATOR 2 upg_catalog.<=(upg_catalog.name, upg_catalog.name),
   OPERATOR 3 upg_catalog.=(upg_catalog.name, upg_catalog.name),
   OPERATOR 4 upg_catalog.>=(upg_catalog.name, upg_catalog.name),
   OPERATOR 5 upg_catalog.>(upg_catalog.name, upg_catalog.name),
   FUNCTION 1 upg_catalog.btnamecmp(upg_catalog.name, upg_catalog.name);
 CREATE OPERATOR CLASS int8_ops DEFAULT FOR TYPE upg_catalog.int8 USING bitmap AS
   OPERATOR 1 upg_catalog.<(upg_catalog.int8, upg_catalog.int8),
   OPERATOR 2 upg_catalog.<=(upg_catalog.int8, upg_catalog.int8),
   OPERATOR 3 upg_catalog.=(upg_catalog.int8, upg_catalog.int8),
   OPERATOR 4 upg_catalog.>=(upg_catalog.int8, upg_catalog.int8),
   OPERATOR 5 upg_catalog.>(upg_catalog.int8, upg_catalog.int8),
   FUNCTION 1 upg_catalog.btint8cmp(upg_catalog.int8, upg_catalog.int8),
   OPERATOR 1 upg_catalog.<(upg_catalog.int8, upg_catalog.int2),
   OPERATOR 2 upg_catalog.<=(upg_catalog.int8, upg_catalog.int2),
   OPERATOR 3 upg_catalog.=(upg_catalog.int8, upg_catalog.int2),
   OPERATOR 4 upg_catalog.>=(upg_catalog.int8, upg_catalog.int2),
   OPERATOR 5 upg_catalog.>(upg_catalog.int8, upg_catalog.int2),
   FUNCTION 1 upg_catalog.btint82cmp(upg_catalog.int8, upg_catalog.int2),
   OPERATOR 1 upg_catalog.<(upg_catalog.int8, upg_catalog.int4),
   OPERATOR 2 upg_catalog.<=(upg_catalog.int8, upg_catalog.int4),
   OPERATOR 3 upg_catalog.=(upg_catalog.int8, upg_catalog.int4),
   OPERATOR 4 upg_catalog.>=(upg_catalog.int8, upg_catalog.int4),
   OPERATOR 5 upg_catalog.>(upg_catalog.int8, upg_catalog.int4),
   FUNCTION 1 upg_catalog.btint84cmp(upg_catalog.int8, upg_catalog.int4);
 CREATE OPERATOR CLASS int8_ops DEFAULT FOR TYPE upg_catalog.int8 USING hash AS
   OPERATOR 1 upg_catalog.=(upg_catalog.int8, upg_catalog.int8),
   FUNCTION 1 upg_catalog.hashint8(upg_catalog.int8);
 CREATE OPERATOR CLASS int8_ops DEFAULT FOR TYPE upg_catalog.int8 USING btree AS
   OPERATOR 1 upg_catalog.<(upg_catalog.int8, upg_catalog.int8),
   OPERATOR 2 upg_catalog.<=(upg_catalog.int8, upg_catalog.int8),
   OPERATOR 3 upg_catalog.=(upg_catalog.int8, upg_catalog.int8),
   OPERATOR 4 upg_catalog.>=(upg_catalog.int8, upg_catalog.int8),
   OPERATOR 5 upg_catalog.>(upg_catalog.int8, upg_catalog.int8),
   FUNCTION 1 upg_catalog.btint8cmp(upg_catalog.int8, upg_catalog.int8),
   OPERATOR 1 upg_catalog.<(upg_catalog.int8, upg_catalog.int2),
   OPERATOR 2 upg_catalog.<=(upg_catalog.int8, upg_catalog.int2),
   OPERATOR 3 upg_catalog.=(upg_catalog.int8, upg_catalog.int2),
   OPERATOR 4 upg_catalog.>=(upg_catalog.int8, upg_catalog.int2),
   OPERATOR 5 upg_catalog.>(upg_catalog.int8, upg_catalog.int2),
   FUNCTION 1 upg_catalog.btint82cmp(upg_catalog.int8, upg_catalog.int2),
   OPERATOR 1 upg_catalog.<(upg_catalog.int8, upg_catalog.int4),
   OPERATOR 2 upg_catalog.<=(upg_catalog.int8, upg_catalog.int4),
   OPERATOR 3 upg_catalog.=(upg_catalog.int8, upg_catalog.int4),
   OPERATOR 4 upg_catalog.>=(upg_catalog.int8, upg_catalog.int4),
   OPERATOR 5 upg_catalog.>(upg_catalog.int8, upg_catalog.int4),
   FUNCTION 1 upg_catalog.btint84cmp(upg_catalog.int8, upg_catalog.int4);
 CREATE OPERATOR CLASS int2_ops DEFAULT FOR TYPE upg_catalog.int2 USING bitmap AS
   OPERATOR 1 upg_catalog.<(upg_catalog.int2, upg_catalog.int2),
   OPERATOR 2 upg_catalog.<=(upg_catalog.int2, upg_catalog.int2),
   OPERATOR 3 upg_catalog.=(upg_catalog.int2, upg_catalog.int2),
   OPERATOR 4 upg_catalog.>=(upg_catalog.int2, upg_catalog.int2),
   OPERATOR 5 upg_catalog.>(upg_catalog.int2, upg_catalog.int2),
   FUNCTION 1 upg_catalog.btint2cmp(upg_catalog.int2, upg_catalog.int2),
   OPERATOR 1 upg_catalog.<(upg_catalog.int2, upg_catalog.int8),
   OPERATOR 2 upg_catalog.<=(upg_catalog.int2, upg_catalog.int8),
   OPERATOR 3 upg_catalog.=(upg_catalog.int2, upg_catalog.int8),
   OPERATOR 4 upg_catalog.>=(upg_catalog.int2, upg_catalog.int8),
   OPERATOR 5 upg_catalog.>(upg_catalog.int2, upg_catalog.int8),
   FUNCTION 1 upg_catalog.btint28cmp(upg_catalog.int2, upg_catalog.int8),
   OPERATOR 1 upg_catalog.<(upg_catalog.int2, upg_catalog.int4),
   OPERATOR 2 upg_catalog.<=(upg_catalog.int2, upg_catalog.int4),
   OPERATOR 3 upg_catalog.=(upg_catalog.int2, upg_catalog.int4),
   OPERATOR 4 upg_catalog.>=(upg_catalog.int2, upg_catalog.int4),
   OPERATOR 5 upg_catalog.>(upg_catalog.int2, upg_catalog.int4),
   FUNCTION 1 upg_catalog.btint24cmp(upg_catalog.int2, upg_catalog.int4);
 CREATE OPERATOR CLASS int2_ops DEFAULT FOR TYPE upg_catalog.int2 USING hash AS
   OPERATOR 1 upg_catalog.=(upg_catalog.int2, upg_catalog.int2),
   FUNCTION 1 upg_catalog.hashint2(upg_catalog.int2);
 CREATE OPERATOR CLASS int2_ops DEFAULT FOR TYPE upg_catalog.int2 USING btree AS
   OPERATOR 1 upg_catalog.<(upg_catalog.int2, upg_catalog.int2),
   OPERATOR 2 upg_catalog.<=(upg_catalog.int2, upg_catalog.int2),
   OPERATOR 3 upg_catalog.=(upg_catalog.int2, upg_catalog.int2),
   OPERATOR 4 upg_catalog.>=(upg_catalog.int2, upg_catalog.int2),
   OPERATOR 5 upg_catalog.>(upg_catalog.int2, upg_catalog.int2),
   FUNCTION 1 upg_catalog.btint2cmp(upg_catalog.int2, upg_catalog.int2),
   OPERATOR 1 upg_catalog.<(upg_catalog.int2, upg_catalog.int8),
   OPERATOR 2 upg_catalog.<=(upg_catalog.int2, upg_catalog.int8),
   OPERATOR 3 upg_catalog.=(upg_catalog.int2, upg_catalog.int8),
   OPERATOR 4 upg_catalog.>=(upg_catalog.int2, upg_catalog.int8),
   OPERATOR 5 upg_catalog.>(upg_catalog.int2, upg_catalog.int8),
   FUNCTION 1 upg_catalog.btint28cmp(upg_catalog.int2, upg_catalog.int8),
   OPERATOR 1 upg_catalog.<(upg_catalog.int2, upg_catalog.int4),
   OPERATOR 2 upg_catalog.<=(upg_catalog.int2, upg_catalog.int4),
   OPERATOR 3 upg_catalog.=(upg_catalog.int2, upg_catalog.int4),
   OPERATOR 4 upg_catalog.>=(upg_catalog.int2, upg_catalog.int4),
   OPERATOR 5 upg_catalog.>(upg_catalog.int2, upg_catalog.int4),
   FUNCTION 1 upg_catalog.btint24cmp(upg_catalog.int2, upg_catalog.int4);
 CREATE OPERATOR CLASS int2vector_ops DEFAULT FOR TYPE upg_catalog.int2vector USING hash AS
   OPERATOR 1 upg_catalog.=(upg_catalog.int2vector, upg_catalog.int2vector),
   FUNCTION 1 upg_catalog.hashint2vector(upg_catalog.int2vector);
 CREATE OPERATOR CLASS int4_ops DEFAULT FOR TYPE upg_catalog.int4 USING bitmap AS
   OPERATOR 1 upg_catalog.<(upg_catalog.int4, upg_catalog.int4),
   OPERATOR 2 upg_catalog.<=(upg_catalog.int4, upg_catalog.int4),
   OPERATOR 3 upg_catalog.=(upg_catalog.int4, upg_catalog.int4),
   OPERATOR 4 upg_catalog.>=(upg_catalog.int4, upg_catalog.int4),
   OPERATOR 5 upg_catalog.>(upg_catalog.int4, upg_catalog.int4),
   FUNCTION 1 upg_catalog.btint4cmp(upg_catalog.int4, upg_catalog.int4),
   OPERATOR 1 upg_catalog.<(upg_catalog.int4, upg_catalog.int8),
   OPERATOR 2 upg_catalog.<=(upg_catalog.int4, upg_catalog.int8),
   OPERATOR 3 upg_catalog.=(upg_catalog.int4, upg_catalog.int8),
   OPERATOR 4 upg_catalog.>=(upg_catalog.int4, upg_catalog.int8),
   OPERATOR 5 upg_catalog.>(upg_catalog.int4, upg_catalog.int8),
   FUNCTION 1 upg_catalog.btint42cmp(upg_catalog.int4, upg_catalog.int2),
   OPERATOR 1 upg_catalog.<(upg_catalog.int4, upg_catalog.int2),
   OPERATOR 2 upg_catalog.<=(upg_catalog.int4, upg_catalog.int2),
   OPERATOR 3 upg_catalog.=(upg_catalog.int4, upg_catalog.int2),
   OPERATOR 4 upg_catalog.>=(upg_catalog.int4, upg_catalog.int2),
   OPERATOR 5 upg_catalog.>(upg_catalog.int4, upg_catalog.int2),
   FUNCTION 1 upg_catalog.btint48cmp(upg_catalog.int4, upg_catalog.int8);
 CREATE OPERATOR CLASS int4_ops DEFAULT FOR TYPE upg_catalog.int4 USING hash AS
   OPERATOR 1 upg_catalog.=(upg_catalog.int4, upg_catalog.int4),
   FUNCTION 1 upg_catalog.hashint4(upg_catalog.int4);
 CREATE OPERATOR CLASS int4_ops DEFAULT FOR TYPE upg_catalog.int4 USING btree AS
   OPERATOR 1 upg_catalog.<(upg_catalog.int4, upg_catalog.int4),
   OPERATOR 2 upg_catalog.<=(upg_catalog.int4, upg_catalog.int4),
   OPERATOR 3 upg_catalog.=(upg_catalog.int4, upg_catalog.int4),
   OPERATOR 4 upg_catalog.>=(upg_catalog.int4, upg_catalog.int4),
   OPERATOR 5 upg_catalog.>(upg_catalog.int4, upg_catalog.int4),
   FUNCTION 1 upg_catalog.btint4cmp(upg_catalog.int4, upg_catalog.int4),
   OPERATOR 1 upg_catalog.<(upg_catalog.int4, upg_catalog.int8),
   OPERATOR 2 upg_catalog.<=(upg_catalog.int4, upg_catalog.int8),
   OPERATOR 3 upg_catalog.=(upg_catalog.int4, upg_catalog.int8),
   OPERATOR 4 upg_catalog.>=(upg_catalog.int4, upg_catalog.int8),
   OPERATOR 5 upg_catalog.>(upg_catalog.int4, upg_catalog.int8),
   FUNCTION 1 upg_catalog.btint48cmp(upg_catalog.int4, upg_catalog.int8),
   OPERATOR 1 upg_catalog.<(upg_catalog.int4, upg_catalog.int2),
   OPERATOR 2 upg_catalog.<=(upg_catalog.int4, upg_catalog.int2),
   OPERATOR 3 upg_catalog.=(upg_catalog.int4, upg_catalog.int2),
   OPERATOR 4 upg_catalog.>=(upg_catalog.int4, upg_catalog.int2),
   OPERATOR 5 upg_catalog.>(upg_catalog.int4, upg_catalog.int2),
   FUNCTION 1 upg_catalog.btint42cmp(upg_catalog.int4, upg_catalog.int2);
 CREATE OPERATOR CLASS text_pattern_ops FOR TYPE upg_catalog.text USING bitmap AS
   OPERATOR 1 upg_catalog.~<~(upg_catalog.text, upg_catalog.text),
   OPERATOR 2 upg_catalog.~<=~(upg_catalog.text, upg_catalog.text),
   OPERATOR 3 upg_catalog.~=~(upg_catalog.text, upg_catalog.text),
   OPERATOR 4 upg_catalog.~>=~(upg_catalog.text, upg_catalog.text),
   OPERATOR 5 upg_catalog.~>~(upg_catalog.text, upg_catalog.text),
   FUNCTION 1 upg_catalog.bttext_pattern_cmp(upg_catalog.text, upg_catalog.text);
 CREATE OPERATOR CLASS text_ops DEFAULT FOR TYPE upg_catalog.text USING bitmap AS
   OPERATOR 1 upg_catalog.<(upg_catalog.text, upg_catalog.text),
   OPERATOR 2 upg_catalog.<=(upg_catalog.text, upg_catalog.text),
   OPERATOR 3 upg_catalog.=(upg_catalog.text, upg_catalog.text),
   OPERATOR 4 upg_catalog.>=(upg_catalog.text, upg_catalog.text),
   OPERATOR 5 upg_catalog.>(upg_catalog.text, upg_catalog.text),
   FUNCTION 1 upg_catalog.bttextcmp(upg_catalog.text, upg_catalog.text);
 CREATE OPERATOR CLASS text_pattern_ops FOR TYPE upg_catalog.text USING hash AS
   OPERATOR 1 upg_catalog.~=~(upg_catalog.text, upg_catalog.text),
   FUNCTION 1 upg_catalog.hashvarlena(pg_catalog.internal);
 CREATE OPERATOR CLASS text_pattern_ops FOR TYPE upg_catalog.text USING btree AS
   OPERATOR 1 upg_catalog.~<~(upg_catalog.text, upg_catalog.text),
   OPERATOR 2 upg_catalog.~<=~(upg_catalog.text, upg_catalog.text),
   OPERATOR 3 upg_catalog.~=~(upg_catalog.text, upg_catalog.text),
   OPERATOR 4 upg_catalog.~>=~(upg_catalog.text, upg_catalog.text),
   OPERATOR 5 upg_catalog.~>~(upg_catalog.text, upg_catalog.text),
   FUNCTION 1 upg_catalog.bttext_pattern_cmp(upg_catalog.text, upg_catalog.text);
 CREATE OPERATOR CLASS text_ops DEFAULT FOR TYPE upg_catalog.text USING hash AS
   OPERATOR 1 upg_catalog.=(upg_catalog.text, upg_catalog.text),
   FUNCTION 1 upg_catalog.hashtext(upg_catalog.text);
 CREATE OPERATOR CLASS text_ops DEFAULT FOR TYPE upg_catalog.text USING btree AS
   OPERATOR 1 upg_catalog.<(upg_catalog.text, upg_catalog.text),
   OPERATOR 2 upg_catalog.<=(upg_catalog.text, upg_catalog.text),
   OPERATOR 3 upg_catalog.=(upg_catalog.text, upg_catalog.text),
   OPERATOR 4 upg_catalog.>=(upg_catalog.text, upg_catalog.text),
   OPERATOR 5 upg_catalog.>(upg_catalog.text, upg_catalog.text),
   FUNCTION 1 upg_catalog.bttextcmp(upg_catalog.text, upg_catalog.text);
 CREATE OPERATOR CLASS oid_ops DEFAULT FOR TYPE upg_catalog.oid USING bitmap AS
   OPERATOR 1 upg_catalog.<(upg_catalog.oid, upg_catalog.oid),
   OPERATOR 2 upg_catalog.<=(upg_catalog.oid, upg_catalog.oid),
   OPERATOR 3 upg_catalog.=(upg_catalog.oid, upg_catalog.oid),
   OPERATOR 4 upg_catalog.>=(upg_catalog.oid, upg_catalog.oid),
   OPERATOR 5 upg_catalog.>(upg_catalog.oid, upg_catalog.oid),
   FUNCTION 1 upg_catalog.btoidcmp(upg_catalog.oid, upg_catalog.oid);
 CREATE OPERATOR CLASS oid_ops DEFAULT FOR TYPE upg_catalog.oid USING hash AS
   OPERATOR 1 upg_catalog.=(upg_catalog.oid, upg_catalog.oid),
   FUNCTION 1 upg_catalog.hashoid(upg_catalog.oid);
 CREATE OPERATOR CLASS oid_ops DEFAULT FOR TYPE upg_catalog.oid USING btree AS
   OPERATOR 1 upg_catalog.<(upg_catalog.oid, upg_catalog.oid),
   OPERATOR 2 upg_catalog.<=(upg_catalog.oid, upg_catalog.oid),
   OPERATOR 3 upg_catalog.=(upg_catalog.oid, upg_catalog.oid),
   OPERATOR 4 upg_catalog.>=(upg_catalog.oid, upg_catalog.oid),
   OPERATOR 5 upg_catalog.>(upg_catalog.oid, upg_catalog.oid),
   FUNCTION 1 upg_catalog.btoidcmp(upg_catalog.oid, upg_catalog.oid);
 CREATE OPERATOR CLASS tid_ops DEFAULT FOR TYPE upg_catalog.tid USING btree AS
   OPERATOR 1 upg_catalog.<(upg_catalog.tid, upg_catalog.tid),
   OPERATOR 2 upg_catalog.<=(upg_catalog.tid, upg_catalog.tid),
   OPERATOR 3 upg_catalog.=(upg_catalog.tid, upg_catalog.tid),
   OPERATOR 4 upg_catalog.>=(upg_catalog.tid, upg_catalog.tid),
   OPERATOR 5 upg_catalog.>(upg_catalog.tid, upg_catalog.tid),
   FUNCTION 1 upg_catalog.bttidcmp(upg_catalog.tid, upg_catalog.tid);
 CREATE OPERATOR CLASS xid_ops DEFAULT FOR TYPE upg_catalog.xid USING hash AS
   OPERATOR 1 upg_catalog.=(upg_catalog.xid, upg_catalog.xid),
   FUNCTION 1 upg_catalog.hashint4(upg_catalog.int4);
 CREATE OPERATOR CLASS cid_ops DEFAULT FOR TYPE upg_catalog.cid USING hash AS
   OPERATOR 1 upg_catalog.=(upg_catalog.cid, upg_catalog.cid),
   FUNCTION 1 upg_catalog.hashint4(upg_catalog.int4);
 CREATE OPERATOR CLASS oidvector_ops DEFAULT FOR TYPE upg_catalog.oidvector USING bitmap AS
   OPERATOR 1 upg_catalog.<(upg_catalog.oidvector, upg_catalog.oidvector),
   OPERATOR 2 upg_catalog.<=(upg_catalog.oidvector, upg_catalog.oidvector),
   OPERATOR 3 upg_catalog.=(upg_catalog.oidvector, upg_catalog.oidvector),
   OPERATOR 4 upg_catalog.>=(upg_catalog.oidvector, upg_catalog.oidvector),
   OPERATOR 5 upg_catalog.>(upg_catalog.oidvector, upg_catalog.oidvector),
   FUNCTION 1 upg_catalog.btoidvectorcmp(upg_catalog.oidvector, upg_catalog.oidvector);
 CREATE OPERATOR CLASS oidvector_ops DEFAULT FOR TYPE upg_catalog.oidvector USING hash AS
   OPERATOR 1 upg_catalog.=(upg_catalog.oidvector, upg_catalog.oidvector),
   FUNCTION 1 upg_catalog.hashoidvector(upg_catalog.oidvector);
 CREATE OPERATOR CLASS oidvector_ops DEFAULT FOR TYPE upg_catalog.oidvector USING btree AS
   OPERATOR 1 upg_catalog.<(upg_catalog.oidvector, upg_catalog.oidvector),
   OPERATOR 2 upg_catalog.<=(upg_catalog.oidvector, upg_catalog.oidvector),
   OPERATOR 3 upg_catalog.=(upg_catalog.oidvector, upg_catalog.oidvector),
   OPERATOR 4 upg_catalog.>=(upg_catalog.oidvector, upg_catalog.oidvector),
   OPERATOR 5 upg_catalog.>(upg_catalog.oidvector, upg_catalog.oidvector),
   FUNCTION 1 upg_catalog.btoidvectorcmp(upg_catalog.oidvector, upg_catalog.oidvector);
 CREATE OPERATOR CLASS box_ops DEFAULT FOR TYPE upg_catalog.box USING gist AS
   OPERATOR 1 upg_catalog.<<(upg_catalog.box, upg_catalog.box),
   OPERATOR 2 upg_catalog.&<(upg_catalog.box, upg_catalog.box),
   OPERATOR 3 upg_catalog.&&(upg_catalog.box, upg_catalog.box),
   OPERATOR 4 upg_catalog.&>(upg_catalog.box, upg_catalog.box),
   OPERATOR 5 upg_catalog.>>(upg_catalog.box, upg_catalog.box),
   OPERATOR 6 upg_catalog.~=(upg_catalog.box, upg_catalog.box),
   OPERATOR 7 upg_catalog.@>(upg_catalog.box, upg_catalog.box),
   OPERATOR 8 upg_catalog.<@(upg_catalog.box, upg_catalog.box),
   OPERATOR 9 upg_catalog.&<|(upg_catalog.box, upg_catalog.box),
   OPERATOR 10 upg_catalog.<<|(upg_catalog.box, upg_catalog.box),
   OPERATOR 11 upg_catalog.|>>(upg_catalog.box, upg_catalog.box),
   OPERATOR 12 upg_catalog.|&>(upg_catalog.box, upg_catalog.box),
   OPERATOR 13 upg_catalog.~(upg_catalog.box, upg_catalog.box),
   OPERATOR 14 upg_catalog.@(upg_catalog.box, upg_catalog.box),
   FUNCTION 1 upg_catalog.gist_box_consistent(pg_catalog.internal, upg_catalog.box, upg_catalog.int4),
   FUNCTION 2 upg_catalog.gist_box_union(pg_catalog.internal, pg_catalog.internal),
   FUNCTION 3 upg_catalog.gist_box_compress(pg_catalog.internal),
   FUNCTION 4 upg_catalog.gist_box_decompress(pg_catalog.internal),
   FUNCTION 5 upg_catalog.gist_box_penalty(pg_catalog.internal, pg_catalog.internal, pg_catalog.internal),
   FUNCTION 6 upg_catalog.gist_box_picksplit(pg_catalog.internal, pg_catalog.internal),
   FUNCTION 7 upg_catalog.gist_box_same(upg_catalog.box, upg_catalog.box, pg_catalog.internal);
 CREATE OPERATOR CLASS poly_ops DEFAULT FOR TYPE upg_catalog.polygon USING gist AS
   OPERATOR 1 upg_catalog.<<(upg_catalog.polygon, upg_catalog.polygon) RECHECK,
   OPERATOR 2 upg_catalog.&<(upg_catalog.polygon, upg_catalog.polygon) RECHECK,
   OPERATOR 3 upg_catalog.&&(upg_catalog.polygon, upg_catalog.polygon) RECHECK,
   OPERATOR 4 upg_catalog.&>(upg_catalog.polygon, upg_catalog.polygon) RECHECK,
   OPERATOR 5 upg_catalog.>>(upg_catalog.polygon, upg_catalog.polygon) RECHECK,
   OPERATOR 6 upg_catalog.~=(upg_catalog.polygon, upg_catalog.polygon) RECHECK,
   OPERATOR 7 upg_catalog.@>(upg_catalog.polygon, upg_catalog.polygon) RECHECK,
   OPERATOR 8 upg_catalog.<@(upg_catalog.polygon, upg_catalog.polygon) RECHECK,
   OPERATOR 9 upg_catalog.&<|(upg_catalog.polygon, upg_catalog.polygon) RECHECK,
   OPERATOR 10 upg_catalog.<<|(upg_catalog.polygon, upg_catalog.polygon) RECHECK,
   OPERATOR 11 upg_catalog.|>>(upg_catalog.polygon, upg_catalog.polygon) RECHECK,
   OPERATOR 12 upg_catalog.|&>(upg_catalog.polygon, upg_catalog.polygon) RECHECK,
   OPERATOR 13 upg_catalog.~(upg_catalog.polygon, upg_catalog.polygon) RECHECK,
   OPERATOR 14 upg_catalog.@(upg_catalog.polygon, upg_catalog.polygon) RECHECK,
   FUNCTION 1 upg_catalog.gist_poly_consistent(pg_catalog.internal, upg_catalog.polygon, upg_catalog.int4),
   FUNCTION 2 upg_catalog.gist_box_union(pg_catalog.internal, pg_catalog.internal),
   FUNCTION 3 upg_catalog.gist_poly_compress(pg_catalog.internal),
   FUNCTION 4 upg_catalog.gist_box_decompress(pg_catalog.internal),
   FUNCTION 5 upg_catalog.gist_box_penalty(pg_catalog.internal, pg_catalog.internal, pg_catalog.internal),
   FUNCTION 6 upg_catalog.gist_box_picksplit(pg_catalog.internal, pg_catalog.internal),
   FUNCTION 7 upg_catalog.gist_box_same(upg_catalog.box, upg_catalog.box, pg_catalog.internal),
   STORAGE box;
 CREATE OPERATOR CLASS float4_ops DEFAULT FOR TYPE upg_catalog.float4 USING bitmap AS
   OPERATOR 1 upg_catalog.<(upg_catalog.float4, upg_catalog.float4),
   OPERATOR 2 upg_catalog.<=(upg_catalog.float4, upg_catalog.float4),
   OPERATOR 3 upg_catalog.=(upg_catalog.float4, upg_catalog.float4),
   OPERATOR 4 upg_catalog.>=(upg_catalog.float4, upg_catalog.float4),
   OPERATOR 5 upg_catalog.>(upg_catalog.float4, upg_catalog.float4),
   FUNCTION 1 upg_catalog.btfloat4cmp(upg_catalog.float4, upg_catalog.float4),
   OPERATOR 1 upg_catalog.<(upg_catalog.float4, upg_catalog.float8),
   OPERATOR 2 upg_catalog.<=(upg_catalog.float4, upg_catalog.float8),
   OPERATOR 3 upg_catalog.=(upg_catalog.float4, upg_catalog.float8),
   OPERATOR 4 upg_catalog.>=(upg_catalog.float4, upg_catalog.float8),
   OPERATOR 5 upg_catalog.>(upg_catalog.float4, upg_catalog.float8),
   FUNCTION 1 upg_catalog.btfloat48cmp(upg_catalog.float4, upg_catalog.float8);
 CREATE OPERATOR CLASS float4_ops DEFAULT FOR TYPE upg_catalog.float4 USING hash AS
   OPERATOR 1 upg_catalog.=(upg_catalog.float4, upg_catalog.float4),
   FUNCTION 1 upg_catalog.hashfloat4(upg_catalog.float4);
 CREATE OPERATOR CLASS float4_ops DEFAULT FOR TYPE upg_catalog.float4 USING btree AS
   OPERATOR 1 upg_catalog.<(upg_catalog.float4, upg_catalog.float4),
   OPERATOR 2 upg_catalog.<=(upg_catalog.float4, upg_catalog.float4),
   OPERATOR 3 upg_catalog.=(upg_catalog.float4, upg_catalog.float4),
   OPERATOR 4 upg_catalog.>=(upg_catalog.float4, upg_catalog.float4),
   OPERATOR 5 upg_catalog.>(upg_catalog.float4, upg_catalog.float4),
   FUNCTION 1 upg_catalog.btfloat4cmp(upg_catalog.float4, upg_catalog.float4),
   OPERATOR 1 upg_catalog.<(upg_catalog.float4, upg_catalog.float8),
   OPERATOR 2 upg_catalog.<=(upg_catalog.float4, upg_catalog.float8),
   OPERATOR 3 upg_catalog.=(upg_catalog.float4, upg_catalog.float8),
   OPERATOR 4 upg_catalog.>=(upg_catalog.float4, upg_catalog.float8),
   OPERATOR 5 upg_catalog.>(upg_catalog.float4, upg_catalog.float8),
   FUNCTION 1 upg_catalog.btfloat48cmp(upg_catalog.float4, upg_catalog.float8);
 CREATE OPERATOR CLASS float8_ops DEFAULT FOR TYPE upg_catalog.float8 USING bitmap AS
   OPERATOR 1 upg_catalog.<(upg_catalog.float8, upg_catalog.float8),
   OPERATOR 2 upg_catalog.<=(upg_catalog.float8, upg_catalog.float8),
   OPERATOR 3 upg_catalog.=(upg_catalog.float8, upg_catalog.float8),
   OPERATOR 4 upg_catalog.>=(upg_catalog.float8, upg_catalog.float8),
   OPERATOR 5 upg_catalog.>(upg_catalog.float8, upg_catalog.float8),
   FUNCTION 1 upg_catalog.btfloat8cmp(upg_catalog.float8, upg_catalog.float8),
   OPERATOR 1 upg_catalog.<(upg_catalog.float8, upg_catalog.float4),
   OPERATOR 2 upg_catalog.<=(upg_catalog.float8, upg_catalog.float4),
   OPERATOR 3 upg_catalog.=(upg_catalog.float8, upg_catalog.float4),
   OPERATOR 4 upg_catalog.>=(upg_catalog.float8, upg_catalog.float4),
   OPERATOR 5 upg_catalog.>(upg_catalog.float8, upg_catalog.float4),
   FUNCTION 1 upg_catalog.btfloat84cmp(upg_catalog.float8, upg_catalog.float4);
 CREATE OPERATOR CLASS float8_ops DEFAULT FOR TYPE upg_catalog.float8 USING hash AS
   OPERATOR 1 upg_catalog.=(upg_catalog.float8, upg_catalog.float8),
   FUNCTION 1 upg_catalog.hashfloat8(upg_catalog.float8);
 CREATE OPERATOR CLASS float8_ops DEFAULT FOR TYPE upg_catalog.float8 USING btree AS
   OPERATOR 1 upg_catalog.<(upg_catalog.float8, upg_catalog.float8),
   OPERATOR 2 upg_catalog.<=(upg_catalog.float8, upg_catalog.float8),
   OPERATOR 3 upg_catalog.=(upg_catalog.float8, upg_catalog.float8),
   OPERATOR 4 upg_catalog.>=(upg_catalog.float8, upg_catalog.float8),
   OPERATOR 5 upg_catalog.>(upg_catalog.float8, upg_catalog.float8),
   FUNCTION 1 upg_catalog.btfloat8cmp(upg_catalog.float8, upg_catalog.float8),
   OPERATOR 1 upg_catalog.<(upg_catalog.float8, upg_catalog.float4),
   OPERATOR 2 upg_catalog.<=(upg_catalog.float8, upg_catalog.float4),
   OPERATOR 3 upg_catalog.=(upg_catalog.float8, upg_catalog.float4),
   OPERATOR 4 upg_catalog.>=(upg_catalog.float8, upg_catalog.float4),
   OPERATOR 5 upg_catalog.>(upg_catalog.float8, upg_catalog.float4),
   FUNCTION 1 upg_catalog.btfloat84cmp(upg_catalog.float8, upg_catalog.float4);
 CREATE OPERATOR CLASS abstime_ops DEFAULT FOR TYPE upg_catalog.abstime USING bitmap AS
   OPERATOR 1 upg_catalog.<(upg_catalog.abstime, upg_catalog.abstime),
   OPERATOR 2 upg_catalog.<=(upg_catalog.abstime, upg_catalog.abstime),
   OPERATOR 3 upg_catalog.=(upg_catalog.abstime, upg_catalog.abstime),
   OPERATOR 4 upg_catalog.>=(upg_catalog.abstime, upg_catalog.abstime),
   OPERATOR 5 upg_catalog.>(upg_catalog.abstime, upg_catalog.abstime),
   FUNCTION 1 upg_catalog.btabstimecmp(upg_catalog.abstime, upg_catalog.abstime);
 CREATE OPERATOR CLASS abstime_ops DEFAULT FOR TYPE upg_catalog.abstime USING hash AS
   OPERATOR 1 upg_catalog.=(upg_catalog.abstime, upg_catalog.abstime),
   FUNCTION 1 upg_catalog.hashint4(upg_catalog.int4);
 CREATE OPERATOR CLASS abstime_ops DEFAULT FOR TYPE upg_catalog.abstime USING btree AS
   OPERATOR 1 upg_catalog.<(upg_catalog.abstime, upg_catalog.abstime),
   OPERATOR 2 upg_catalog.<=(upg_catalog.abstime, upg_catalog.abstime),
   OPERATOR 3 upg_catalog.=(upg_catalog.abstime, upg_catalog.abstime),
   OPERATOR 4 upg_catalog.>=(upg_catalog.abstime, upg_catalog.abstime),
   OPERATOR 5 upg_catalog.>(upg_catalog.abstime, upg_catalog.abstime),
   FUNCTION 1 upg_catalog.btabstimecmp(upg_catalog.abstime, upg_catalog.abstime);
 CREATE OPERATOR CLASS reltime_ops DEFAULT FOR TYPE upg_catalog.reltime USING bitmap AS
   OPERATOR 1 upg_catalog.<(upg_catalog.reltime, upg_catalog.reltime),
   OPERATOR 2 upg_catalog.<=(upg_catalog.reltime, upg_catalog.reltime),
   OPERATOR 3 upg_catalog.=(upg_catalog.reltime, upg_catalog.reltime),
   OPERATOR 4 upg_catalog.>=(upg_catalog.reltime, upg_catalog.reltime),
   OPERATOR 5 upg_catalog.>(upg_catalog.reltime, upg_catalog.reltime),
   FUNCTION 1 upg_catalog.btreltimecmp(upg_catalog.reltime, upg_catalog.reltime);
 CREATE OPERATOR CLASS reltime_ops DEFAULT FOR TYPE upg_catalog.reltime USING btree AS
   OPERATOR 1 upg_catalog.<(upg_catalog.reltime, upg_catalog.reltime),
   OPERATOR 2 upg_catalog.<=(upg_catalog.reltime, upg_catalog.reltime),
   OPERATOR 3 upg_catalog.=(upg_catalog.reltime, upg_catalog.reltime),
   OPERATOR 4 upg_catalog.>=(upg_catalog.reltime, upg_catalog.reltime),
   OPERATOR 5 upg_catalog.>(upg_catalog.reltime, upg_catalog.reltime),
   FUNCTION 1 upg_catalog.btreltimecmp(upg_catalog.reltime, upg_catalog.reltime);
 CREATE OPERATOR CLASS reltime_ops DEFAULT FOR TYPE upg_catalog.reltime USING hash AS
   OPERATOR 1 upg_catalog.=(upg_catalog.reltime, upg_catalog.reltime),
   FUNCTION 1 upg_catalog.hashint4(upg_catalog.int4);
 CREATE OPERATOR CLASS tinterval_ops DEFAULT FOR TYPE upg_catalog.tinterval USING bitmap AS
   OPERATOR 1 upg_catalog.<(upg_catalog.tinterval, upg_catalog.tinterval),
   OPERATOR 2 upg_catalog.<=(upg_catalog.tinterval, upg_catalog.tinterval),
   OPERATOR 3 upg_catalog.=(upg_catalog.tinterval, upg_catalog.tinterval),
   OPERATOR 4 upg_catalog.>=(upg_catalog.tinterval, upg_catalog.tinterval),
   OPERATOR 5 upg_catalog.>(upg_catalog.tinterval, upg_catalog.tinterval),
   FUNCTION 1 upg_catalog.bttintervalcmp(upg_catalog.tinterval, upg_catalog.tinterval);
 CREATE OPERATOR CLASS tinterval_ops DEFAULT FOR TYPE upg_catalog.tinterval USING btree AS
   OPERATOR 1 upg_catalog.<(upg_catalog.tinterval, upg_catalog.tinterval),
   OPERATOR 2 upg_catalog.<=(upg_catalog.tinterval, upg_catalog.tinterval),
   OPERATOR 3 upg_catalog.=(upg_catalog.tinterval, upg_catalog.tinterval),
   OPERATOR 4 upg_catalog.>=(upg_catalog.tinterval, upg_catalog.tinterval),
   OPERATOR 5 upg_catalog.>(upg_catalog.tinterval, upg_catalog.tinterval),
   FUNCTION 1 upg_catalog.bttintervalcmp(upg_catalog.tinterval, upg_catalog.tinterval);
 CREATE OPERATOR CLASS circle_ops DEFAULT FOR TYPE upg_catalog.circle USING gist AS
   OPERATOR 1 upg_catalog.<<(upg_catalog.circle, upg_catalog.circle) RECHECK,
   OPERATOR 2 upg_catalog.&<(upg_catalog.circle, upg_catalog.circle) RECHECK,
   OPERATOR 3 upg_catalog.&&(upg_catalog.circle, upg_catalog.circle) RECHECK,
   OPERATOR 4 upg_catalog.&>(upg_catalog.circle, upg_catalog.circle) RECHECK,
   OPERATOR 5 upg_catalog.>>(upg_catalog.circle, upg_catalog.circle) RECHECK,
   OPERATOR 6 upg_catalog.~=(upg_catalog.circle, upg_catalog.circle) RECHECK,
   OPERATOR 7 upg_catalog.@>(upg_catalog.circle, upg_catalog.circle) RECHECK,
   OPERATOR 8 upg_catalog.<@(upg_catalog.circle, upg_catalog.circle) RECHECK,
   OPERATOR 9 upg_catalog.&<|(upg_catalog.circle, upg_catalog.circle) RECHECK,
   OPERATOR 10 upg_catalog.<<|(upg_catalog.circle, upg_catalog.circle) RECHECK,
   OPERATOR 11 upg_catalog.|>>(upg_catalog.circle, upg_catalog.circle) RECHECK,
   OPERATOR 12 upg_catalog.|&>(upg_catalog.circle, upg_catalog.circle) RECHECK,
   OPERATOR 13 upg_catalog.~(upg_catalog.circle, upg_catalog.circle) RECHECK,
   OPERATOR 14 upg_catalog.@(upg_catalog.circle, upg_catalog.circle) RECHECK,
   FUNCTION 1 upg_catalog.gist_circle_consistent(pg_catalog.internal, upg_catalog.circle, upg_catalog.int4),
   FUNCTION 2 upg_catalog.gist_box_union(pg_catalog.internal, pg_catalog.internal),
   FUNCTION 3 upg_catalog.gist_circle_compress(pg_catalog.internal),
   FUNCTION 4 upg_catalog.gist_box_decompress(pg_catalog.internal),
   FUNCTION 5 upg_catalog.gist_box_penalty(pg_catalog.internal, pg_catalog.internal, pg_catalog.internal),
   FUNCTION 6 upg_catalog.gist_box_picksplit(pg_catalog.internal, pg_catalog.internal),
   FUNCTION 7 upg_catalog.gist_box_same(upg_catalog.box, upg_catalog.box, pg_catalog.internal),
   STORAGE box;
 CREATE OPERATOR CLASS money_ops DEFAULT FOR TYPE upg_catalog.money USING bitmap AS
   OPERATOR 1 upg_catalog.<(upg_catalog.money, upg_catalog.money),
   OPERATOR 2 upg_catalog.<=(upg_catalog.money, upg_catalog.money),
   OPERATOR 3 upg_catalog.=(upg_catalog.money, upg_catalog.money),
   OPERATOR 4 upg_catalog.>=(upg_catalog.money, upg_catalog.money),
   OPERATOR 5 upg_catalog.>(upg_catalog.money, upg_catalog.money),
   FUNCTION 1 upg_catalog.cash_cmp(upg_catalog.money, upg_catalog.money);
 CREATE OPERATOR CLASS money_ops DEFAULT FOR TYPE upg_catalog.money USING btree AS
   OPERATOR 1 upg_catalog.<(upg_catalog.money, upg_catalog.money),
   OPERATOR 2 upg_catalog.<=(upg_catalog.money, upg_catalog.money),
   OPERATOR 3 upg_catalog.=(upg_catalog.money, upg_catalog.money),
   OPERATOR 4 upg_catalog.>=(upg_catalog.money, upg_catalog.money),
   OPERATOR 5 upg_catalog.>(upg_catalog.money, upg_catalog.money),
   FUNCTION 1 upg_catalog.cash_cmp(upg_catalog.money, upg_catalog.money);
 CREATE OPERATOR CLASS _money_ops DEFAULT FOR TYPE upg_catalog._money USING gin AS
   OPERATOR 1 upg_catalog.&&(upg_catalog.anyarray, upg_catalog.anyarray),
   OPERATOR 2 upg_catalog.@>(upg_catalog.anyarray, upg_catalog.anyarray),
   OPERATOR 3 upg_catalog.<@(upg_catalog.anyarray, upg_catalog.anyarray) RECHECK,
   OPERATOR 4 upg_catalog.=(upg_catalog.anyarray, upg_catalog.anyarray) RECHECK,
   FUNCTION 1 upg_catalog.cash_cmp(upg_catalog.money, upg_catalog.money),
   FUNCTION 2 upg_catalog.ginarrayextract(upg_catalog.anyarray, pg_catalog.internal),
   FUNCTION 3 upg_catalog.ginarrayextract(upg_catalog.anyarray, pg_catalog.internal),
   FUNCTION 4 upg_catalog.ginarrayconsistent(pg_catalog.internal, upg_catalog.int2, pg_catalog.internal),
   STORAGE money;
 CREATE OPERATOR CLASS macaddr_ops DEFAULT FOR TYPE upg_catalog.macaddr USING bitmap AS
   OPERATOR 1 upg_catalog.<(upg_catalog.macaddr, upg_catalog.macaddr),
   OPERATOR 2 upg_catalog.<=(upg_catalog.macaddr, upg_catalog.macaddr),
   OPERATOR 3 upg_catalog.=(upg_catalog.macaddr, upg_catalog.macaddr),
   OPERATOR 4 upg_catalog.>=(upg_catalog.macaddr, upg_catalog.macaddr),
   OPERATOR 5 upg_catalog.>(upg_catalog.macaddr, upg_catalog.macaddr),
   FUNCTION 1 upg_catalog.macaddr_cmp(upg_catalog.macaddr, upg_catalog.macaddr);
 CREATE OPERATOR CLASS macaddr_ops DEFAULT FOR TYPE upg_catalog.macaddr USING hash AS
   OPERATOR 1 upg_catalog.=(upg_catalog.macaddr, upg_catalog.macaddr),
   FUNCTION 1 upg_catalog.hashmacaddr(upg_catalog.macaddr);
 CREATE OPERATOR CLASS macaddr_ops DEFAULT FOR TYPE upg_catalog.macaddr USING btree AS
   OPERATOR 1 upg_catalog.<(upg_catalog.macaddr, upg_catalog.macaddr),
   OPERATOR 2 upg_catalog.<=(upg_catalog.macaddr, upg_catalog.macaddr),
   OPERATOR 3 upg_catalog.=(upg_catalog.macaddr, upg_catalog.macaddr),
   OPERATOR 4 upg_catalog.>=(upg_catalog.macaddr, upg_catalog.macaddr),
   OPERATOR 5 upg_catalog.>(upg_catalog.macaddr, upg_catalog.macaddr),
   FUNCTION 1 upg_catalog.macaddr_cmp(upg_catalog.macaddr, upg_catalog.macaddr);
 CREATE OPERATOR CLASS inet_ops DEFAULT FOR TYPE upg_catalog.inet USING bitmap AS
   OPERATOR 1 upg_catalog.<(upg_catalog.inet, upg_catalog.inet),
   OPERATOR 2 upg_catalog.<=(upg_catalog.inet, upg_catalog.inet),
   OPERATOR 3 upg_catalog.=(upg_catalog.inet, upg_catalog.inet),
   OPERATOR 4 upg_catalog.>=(upg_catalog.inet, upg_catalog.inet),
   OPERATOR 5 upg_catalog.>(upg_catalog.inet, upg_catalog.inet),
   FUNCTION 1 upg_catalog.network_cmp(upg_catalog.inet, upg_catalog.inet);
 CREATE OPERATOR CLASS inet_ops DEFAULT FOR TYPE upg_catalog.inet USING hash AS
   OPERATOR 1 upg_catalog.=(upg_catalog.inet, upg_catalog.inet),
   FUNCTION 1 upg_catalog.hashinet(upg_catalog.inet);
 CREATE OPERATOR CLASS inet_ops DEFAULT FOR TYPE upg_catalog.inet USING btree AS
   OPERATOR 1 upg_catalog.<(upg_catalog.inet, upg_catalog.inet),
   OPERATOR 2 upg_catalog.<=(upg_catalog.inet, upg_catalog.inet),
   OPERATOR 3 upg_catalog.=(upg_catalog.inet, upg_catalog.inet),
   OPERATOR 4 upg_catalog.>=(upg_catalog.inet, upg_catalog.inet),
   OPERATOR 5 upg_catalog.>(upg_catalog.inet, upg_catalog.inet),
   FUNCTION 1 upg_catalog.network_cmp(upg_catalog.inet, upg_catalog.inet);
 CREATE OPERATOR CLASS cidr_ops  FOR TYPE upg_catalog.inet USING bitmap AS
   OPERATOR 1 upg_catalog.<(upg_catalog.inet, upg_catalog.inet),
   OPERATOR 2 upg_catalog.<=(upg_catalog.inet, upg_catalog.inet),
   OPERATOR 3 upg_catalog.=(upg_catalog.inet, upg_catalog.inet),
   OPERATOR 4 upg_catalog.>=(upg_catalog.inet, upg_catalog.inet),
   OPERATOR 5 upg_catalog.>(upg_catalog.inet, upg_catalog.inet),
   FUNCTION 1 upg_catalog.network_cmp(upg_catalog.inet, upg_catalog.inet);
 CREATE OPERATOR CLASS cidr_ops  FOR TYPE upg_catalog.inet USING hash AS
   OPERATOR 1 upg_catalog.=(upg_catalog.inet, upg_catalog.inet),
   FUNCTION 1 upg_catalog.hashinet(upg_catalog.inet);
 CREATE OPERATOR CLASS cidr_ops  FOR TYPE upg_catalog.inet USING btree AS
   OPERATOR 1 upg_catalog.<(upg_catalog.inet, upg_catalog.inet),
   OPERATOR 2 upg_catalog.<=(upg_catalog.inet, upg_catalog.inet),
   OPERATOR 3 upg_catalog.=(upg_catalog.inet, upg_catalog.inet),
   OPERATOR 4 upg_catalog.>=(upg_catalog.inet, upg_catalog.inet),
   OPERATOR 5 upg_catalog.>(upg_catalog.inet, upg_catalog.inet),
   FUNCTION 1 upg_catalog.network_cmp(upg_catalog.inet, upg_catalog.inet);
 CREATE OPERATOR CLASS _bool_ops DEFAULT FOR TYPE upg_catalog._bool USING gin AS
   OPERATOR 1 upg_catalog.&&(upg_catalog.anyarray, upg_catalog.anyarray),
   OPERATOR 2 upg_catalog.@>(upg_catalog.anyarray, upg_catalog.anyarray),
   OPERATOR 3 upg_catalog.<@(upg_catalog.anyarray, upg_catalog.anyarray) RECHECK,
   OPERATOR 4 upg_catalog.=(upg_catalog.anyarray, upg_catalog.anyarray) RECHECK,
   FUNCTION 1 upg_catalog.btboolcmp(upg_catalog.bool, upg_catalog.bool),
   FUNCTION 2 upg_catalog.ginarrayextract(upg_catalog.anyarray, pg_catalog.internal),
   FUNCTION 3 upg_catalog.ginarrayextract(upg_catalog.anyarray, pg_catalog.internal),
   FUNCTION 4 upg_catalog.ginarrayconsistent(pg_catalog.internal, upg_catalog.int2, pg_catalog.internal),
   STORAGE bool;
 CREATE OPERATOR CLASS _bytea_ops DEFAULT FOR TYPE upg_catalog._bytea USING gin AS
   OPERATOR 1 upg_catalog.&&(upg_catalog.anyarray, upg_catalog.anyarray),
   OPERATOR 2 upg_catalog.@>(upg_catalog.anyarray, upg_catalog.anyarray),
   OPERATOR 3 upg_catalog.<@(upg_catalog.anyarray, upg_catalog.anyarray) RECHECK,
   OPERATOR 4 upg_catalog.=(upg_catalog.anyarray, upg_catalog.anyarray) RECHECK,
   FUNCTION 1 upg_catalog.byteacmp(upg_catalog.bytea, upg_catalog.bytea),
   FUNCTION 2 upg_catalog.ginarrayextract(upg_catalog.anyarray, pg_catalog.internal),
   FUNCTION 3 upg_catalog.ginarrayextract(upg_catalog.anyarray, pg_catalog.internal),
   FUNCTION 4 upg_catalog.ginarrayconsistent(pg_catalog.internal, upg_catalog.int2, pg_catalog.internal),
   STORAGE bytea;
 CREATE OPERATOR CLASS _char_ops DEFAULT FOR TYPE upg_catalog._char USING gin AS
   OPERATOR 1 upg_catalog.&&(upg_catalog.anyarray, upg_catalog.anyarray),
   OPERATOR 2 upg_catalog.@>(upg_catalog.anyarray, upg_catalog.anyarray),
   OPERATOR 3 upg_catalog.<@(upg_catalog.anyarray, upg_catalog.anyarray) RECHECK,
   OPERATOR 4 upg_catalog.=(upg_catalog.anyarray, upg_catalog.anyarray) RECHECK,
   FUNCTION 1 upg_catalog.btcharcmp(upg_catalog."char", upg_catalog."char"),
   FUNCTION 2 upg_catalog.ginarrayextract(upg_catalog.anyarray, pg_catalog.internal),
   FUNCTION 3 upg_catalog.ginarrayextract(upg_catalog.anyarray, pg_catalog.internal),
   FUNCTION 4 upg_catalog.ginarrayconsistent(pg_catalog.internal, upg_catalog.int2, pg_catalog.internal),
   STORAGE "char";
 CREATE OPERATOR CLASS _name_ops DEFAULT FOR TYPE upg_catalog._name USING gin AS
   OPERATOR 1 upg_catalog.&&(upg_catalog.anyarray, upg_catalog.anyarray),
   OPERATOR 2 upg_catalog.@>(upg_catalog.anyarray, upg_catalog.anyarray),
   OPERATOR 3 upg_catalog.<@(upg_catalog.anyarray, upg_catalog.anyarray) RECHECK,
   OPERATOR 4 upg_catalog.=(upg_catalog.anyarray, upg_catalog.anyarray) RECHECK,
   FUNCTION 1 upg_catalog.btnamecmp(upg_catalog.name, upg_catalog.name),
   FUNCTION 2 upg_catalog.ginarrayextract(upg_catalog.anyarray, pg_catalog.internal),
   FUNCTION 3 upg_catalog.ginarrayextract(upg_catalog.anyarray, pg_catalog.internal),
   FUNCTION 4 upg_catalog.ginarrayconsistent(pg_catalog.internal, upg_catalog.int2, pg_catalog.internal),
   STORAGE name;
 CREATE OPERATOR CLASS _int2_ops DEFAULT FOR TYPE upg_catalog._int2 USING gin AS
   OPERATOR 1 upg_catalog.&&(upg_catalog.anyarray, upg_catalog.anyarray),
   OPERATOR 2 upg_catalog.@>(upg_catalog.anyarray, upg_catalog.anyarray),
   OPERATOR 3 upg_catalog.<@(upg_catalog.anyarray, upg_catalog.anyarray) RECHECK,
   OPERATOR 4 upg_catalog.=(upg_catalog.anyarray, upg_catalog.anyarray) RECHECK,
   FUNCTION 1 upg_catalog.btint2cmp(upg_catalog.int2, upg_catalog.int2),
   FUNCTION 2 upg_catalog.ginarrayextract(upg_catalog.anyarray, pg_catalog.internal),
   FUNCTION 3 upg_catalog.ginarrayextract(upg_catalog.anyarray, pg_catalog.internal),
   FUNCTION 4 upg_catalog.ginarrayconsistent(pg_catalog.internal, upg_catalog.int2, pg_catalog.internal),
   STORAGE int2;
 CREATE OPERATOR CLASS _int4_ops DEFAULT FOR TYPE upg_catalog._int4 USING gin AS
   OPERATOR 1 upg_catalog.&&(upg_catalog.anyarray, upg_catalog.anyarray),
   OPERATOR 2 upg_catalog.@>(upg_catalog.anyarray, upg_catalog.anyarray),
   OPERATOR 3 upg_catalog.<@(upg_catalog.anyarray, upg_catalog.anyarray) RECHECK,
   OPERATOR 4 upg_catalog.=(upg_catalog.anyarray, upg_catalog.anyarray) RECHECK,
   FUNCTION 1 upg_catalog.btint4cmp(upg_catalog.int4, upg_catalog.int4),
   FUNCTION 2 upg_catalog.ginarrayextract(upg_catalog.anyarray, pg_catalog.internal),
   FUNCTION 3 upg_catalog.ginarrayextract(upg_catalog.anyarray, pg_catalog.internal),
   FUNCTION 4 upg_catalog.ginarrayconsistent(pg_catalog.internal, upg_catalog.int2, pg_catalog.internal),
   STORAGE int4;
 CREATE OPERATOR CLASS _text_ops DEFAULT FOR TYPE upg_catalog._text USING gin AS
   OPERATOR 1 upg_catalog.&&(upg_catalog.anyarray, upg_catalog.anyarray),
   OPERATOR 2 upg_catalog.@>(upg_catalog.anyarray, upg_catalog.anyarray),
   OPERATOR 3 upg_catalog.<@(upg_catalog.anyarray, upg_catalog.anyarray) RECHECK,
   OPERATOR 4 upg_catalog.=(upg_catalog.anyarray, upg_catalog.anyarray) RECHECK,
   FUNCTION 1 upg_catalog.bttextcmp(upg_catalog.text, upg_catalog.text),
   FUNCTION 2 upg_catalog.ginarrayextract(upg_catalog.anyarray, pg_catalog.internal),
   FUNCTION 3 upg_catalog.ginarrayextract(upg_catalog.anyarray, pg_catalog.internal),
   FUNCTION 4 upg_catalog.ginarrayconsistent(pg_catalog.internal, upg_catalog.int2, pg_catalog.internal),
   STORAGE text;
 CREATE OPERATOR CLASS _oid_ops DEFAULT FOR TYPE upg_catalog._oid USING gin AS
   OPERATOR 1 upg_catalog.&&(upg_catalog.anyarray, upg_catalog.anyarray),
   OPERATOR 2 upg_catalog.@>(upg_catalog.anyarray, upg_catalog.anyarray),
   OPERATOR 3 upg_catalog.<@(upg_catalog.anyarray, upg_catalog.anyarray) RECHECK,
   OPERATOR 4 upg_catalog.=(upg_catalog.anyarray, upg_catalog.anyarray) RECHECK,
   FUNCTION 1 upg_catalog.btoidcmp(upg_catalog.oid, upg_catalog.oid),
   FUNCTION 2 upg_catalog.ginarrayextract(upg_catalog.anyarray, pg_catalog.internal),
   FUNCTION 3 upg_catalog.ginarrayextract(upg_catalog.anyarray, pg_catalog.internal),
   FUNCTION 4 upg_catalog.ginarrayconsistent(pg_catalog.internal, upg_catalog.int2, pg_catalog.internal),
   STORAGE oid;
 CREATE OPERATOR CLASS _oidvector_ops DEFAULT FOR TYPE upg_catalog._oidvector USING gin AS
   OPERATOR 1 upg_catalog.&&(upg_catalog.anyarray, upg_catalog.anyarray),
   OPERATOR 2 upg_catalog.@>(upg_catalog.anyarray, upg_catalog.anyarray),
   OPERATOR 3 upg_catalog.<@(upg_catalog.anyarray, upg_catalog.anyarray) RECHECK,
   OPERATOR 4 upg_catalog.=(upg_catalog.anyarray, upg_catalog.anyarray) RECHECK,
   FUNCTION 1 upg_catalog.btoidvectorcmp(upg_catalog.oidvector, upg_catalog.oidvector),
   FUNCTION 2 upg_catalog.ginarrayextract(upg_catalog.anyarray, pg_catalog.internal),
   FUNCTION 3 upg_catalog.ginarrayextract(upg_catalog.anyarray, pg_catalog.internal),
   FUNCTION 4 upg_catalog.ginarrayconsistent(pg_catalog.internal, upg_catalog.int2, pg_catalog.internal),
   STORAGE oidvector;
 CREATE OPERATOR CLASS _bpchar_ops DEFAULT FOR TYPE upg_catalog._bpchar USING gin AS
   OPERATOR 1 upg_catalog.&&(upg_catalog.anyarray, upg_catalog.anyarray),
   OPERATOR 2 upg_catalog.@>(upg_catalog.anyarray, upg_catalog.anyarray),
   OPERATOR 3 upg_catalog.<@(upg_catalog.anyarray, upg_catalog.anyarray) RECHECK,
   OPERATOR 4 upg_catalog.=(upg_catalog.anyarray, upg_catalog.anyarray) RECHECK,
   FUNCTION 1 upg_catalog.bpcharcmp(upg_catalog.bpchar, upg_catalog.bpchar),
   FUNCTION 2 upg_catalog.ginarrayextract(upg_catalog.anyarray, pg_catalog.internal),
   FUNCTION 3 upg_catalog.ginarrayextract(upg_catalog.anyarray, pg_catalog.internal),
   FUNCTION 4 upg_catalog.ginarrayconsistent(pg_catalog.internal, upg_catalog.int2, pg_catalog.internal),
   STORAGE bpchar;
 CREATE OPERATOR CLASS _varchar_ops DEFAULT FOR TYPE upg_catalog._varchar USING gin AS
   OPERATOR 1 upg_catalog.&&(upg_catalog.anyarray, upg_catalog.anyarray),
   OPERATOR 2 upg_catalog.@>(upg_catalog.anyarray, upg_catalog.anyarray),
   OPERATOR 3 upg_catalog.<@(upg_catalog.anyarray, upg_catalog.anyarray) RECHECK,
   OPERATOR 4 upg_catalog.=(upg_catalog.anyarray, upg_catalog.anyarray) RECHECK,
   FUNCTION 1 upg_catalog.bttextcmp(upg_catalog.text, upg_catalog.text),
   FUNCTION 2 upg_catalog.ginarrayextract(upg_catalog.anyarray, pg_catalog.internal),
   FUNCTION 3 upg_catalog.ginarrayextract(upg_catalog.anyarray, pg_catalog.internal),
   FUNCTION 4 upg_catalog.ginarrayconsistent(pg_catalog.internal, upg_catalog.int2, pg_catalog.internal),
   STORAGE "varchar";
 CREATE OPERATOR CLASS _int8_ops DEFAULT FOR TYPE upg_catalog._int8 USING gin AS
   OPERATOR 1 upg_catalog.&&(upg_catalog.anyarray, upg_catalog.anyarray),
   OPERATOR 2 upg_catalog.@>(upg_catalog.anyarray, upg_catalog.anyarray),
   OPERATOR 3 upg_catalog.<@(upg_catalog.anyarray, upg_catalog.anyarray) RECHECK,
   OPERATOR 4 upg_catalog.=(upg_catalog.anyarray, upg_catalog.anyarray) RECHECK,
   FUNCTION 1 upg_catalog.btint8cmp(upg_catalog.int8, upg_catalog.int8),
   FUNCTION 2 upg_catalog.ginarrayextract(upg_catalog.anyarray, pg_catalog.internal),
   FUNCTION 3 upg_catalog.ginarrayextract(upg_catalog.anyarray, pg_catalog.internal),
   FUNCTION 4 upg_catalog.ginarrayconsistent(pg_catalog.internal, upg_catalog.int2, pg_catalog.internal),
   STORAGE int8;
 CREATE OPERATOR CLASS _float4_ops DEFAULT FOR TYPE upg_catalog._float4 USING gin AS
   OPERATOR 1 upg_catalog.&&(upg_catalog.anyarray, upg_catalog.anyarray),
   OPERATOR 2 upg_catalog.@>(upg_catalog.anyarray, upg_catalog.anyarray),
   OPERATOR 3 upg_catalog.<@(upg_catalog.anyarray, upg_catalog.anyarray) RECHECK,
   OPERATOR 4 upg_catalog.=(upg_catalog.anyarray, upg_catalog.anyarray) RECHECK,
   FUNCTION 1 upg_catalog.btfloat4cmp(upg_catalog.float4, upg_catalog.float4),
   FUNCTION 2 upg_catalog.ginarrayextract(upg_catalog.anyarray, pg_catalog.internal),
   FUNCTION 3 upg_catalog.ginarrayextract(upg_catalog.anyarray, pg_catalog.internal),
   FUNCTION 4 upg_catalog.ginarrayconsistent(pg_catalog.internal, upg_catalog.int2, pg_catalog.internal),
   STORAGE float4;
 CREATE OPERATOR CLASS _float8_ops DEFAULT FOR TYPE upg_catalog._float8 USING gin AS
   OPERATOR 1 upg_catalog.&&(upg_catalog.anyarray, upg_catalog.anyarray),
   OPERATOR 2 upg_catalog.@>(upg_catalog.anyarray, upg_catalog.anyarray),
   OPERATOR 3 upg_catalog.<@(upg_catalog.anyarray, upg_catalog.anyarray) RECHECK,
   OPERATOR 4 upg_catalog.=(upg_catalog.anyarray, upg_catalog.anyarray) RECHECK,
   FUNCTION 1 upg_catalog.btfloat8cmp(upg_catalog.float8, upg_catalog.float8),
   FUNCTION 2 upg_catalog.ginarrayextract(upg_catalog.anyarray, pg_catalog.internal),
   FUNCTION 3 upg_catalog.ginarrayextract(upg_catalog.anyarray, pg_catalog.internal),
   FUNCTION 4 upg_catalog.ginarrayconsistent(pg_catalog.internal, upg_catalog.int2, pg_catalog.internal),
   STORAGE float8;
 CREATE OPERATOR CLASS _abstime_ops DEFAULT FOR TYPE upg_catalog._abstime USING gin AS
   OPERATOR 1 upg_catalog.&&(upg_catalog.anyarray, upg_catalog.anyarray),
   OPERATOR 2 upg_catalog.@>(upg_catalog.anyarray, upg_catalog.anyarray),
   OPERATOR 3 upg_catalog.<@(upg_catalog.anyarray, upg_catalog.anyarray) RECHECK,
   OPERATOR 4 upg_catalog.=(upg_catalog.anyarray, upg_catalog.anyarray) RECHECK,
   FUNCTION 1 upg_catalog.btabstimecmp(upg_catalog.abstime, upg_catalog.abstime),
   FUNCTION 2 upg_catalog.ginarrayextract(upg_catalog.anyarray, pg_catalog.internal),
   FUNCTION 3 upg_catalog.ginarrayextract(upg_catalog.anyarray, pg_catalog.internal),
   FUNCTION 4 upg_catalog.ginarrayconsistent(pg_catalog.internal, upg_catalog.int2, pg_catalog.internal),
   STORAGE abstime;
 CREATE OPERATOR CLASS _reltime_ops DEFAULT FOR TYPE upg_catalog._reltime USING gin AS
   OPERATOR 1 upg_catalog.&&(upg_catalog.anyarray, upg_catalog.anyarray),
   OPERATOR 2 upg_catalog.@>(upg_catalog.anyarray, upg_catalog.anyarray),
   OPERATOR 3 upg_catalog.<@(upg_catalog.anyarray, upg_catalog.anyarray) RECHECK,
   OPERATOR 4 upg_catalog.=(upg_catalog.anyarray, upg_catalog.anyarray) RECHECK,
   FUNCTION 1 upg_catalog.btreltimecmp(upg_catalog.reltime, upg_catalog.reltime),
   FUNCTION 2 upg_catalog.ginarrayextract(upg_catalog.anyarray, pg_catalog.internal),
   FUNCTION 3 upg_catalog.ginarrayextract(upg_catalog.anyarray, pg_catalog.internal),
   FUNCTION 4 upg_catalog.ginarrayconsistent(pg_catalog.internal, upg_catalog.int2, pg_catalog.internal),
   STORAGE reltime;
 CREATE OPERATOR CLASS _tinterval_ops DEFAULT FOR TYPE upg_catalog._tinterval USING gin AS
   OPERATOR 1 upg_catalog.&&(upg_catalog.anyarray, upg_catalog.anyarray),
   OPERATOR 2 upg_catalog.@>(upg_catalog.anyarray, upg_catalog.anyarray),
   OPERATOR 3 upg_catalog.<@(upg_catalog.anyarray, upg_catalog.anyarray) RECHECK,
   OPERATOR 4 upg_catalog.=(upg_catalog.anyarray, upg_catalog.anyarray) RECHECK,
   FUNCTION 1 upg_catalog.bttintervalcmp(upg_catalog.tinterval, upg_catalog.tinterval),
   FUNCTION 2 upg_catalog.ginarrayextract(upg_catalog.anyarray, pg_catalog.internal),
   FUNCTION 3 upg_catalog.ginarrayextract(upg_catalog.anyarray, pg_catalog.internal),
   FUNCTION 4 upg_catalog.ginarrayconsistent(pg_catalog.internal, upg_catalog.int2, pg_catalog.internal),
   STORAGE tinterval;
 CREATE OPERATOR CLASS aclitem_ops DEFAULT FOR TYPE upg_catalog.aclitem USING hash AS
   OPERATOR 1 upg_catalog.=(upg_catalog.aclitem, upg_catalog.aclitem),
   FUNCTION 1 upg_catalog.hash_aclitem(upg_catalog.aclitem);
 CREATE OPERATOR CLASS _macaddr_ops DEFAULT FOR TYPE upg_catalog._macaddr USING gin AS
   OPERATOR 1 upg_catalog.&&(upg_catalog.anyarray, upg_catalog.anyarray),
   OPERATOR 2 upg_catalog.@>(upg_catalog.anyarray, upg_catalog.anyarray),
   OPERATOR 3 upg_catalog.<@(upg_catalog.anyarray, upg_catalog.anyarray) RECHECK,
   OPERATOR 4 upg_catalog.=(upg_catalog.anyarray, upg_catalog.anyarray) RECHECK,
   FUNCTION 1 upg_catalog.macaddr_cmp(upg_catalog.macaddr, upg_catalog.macaddr),
   FUNCTION 2 upg_catalog.ginarrayextract(upg_catalog.anyarray, pg_catalog.internal),
   FUNCTION 3 upg_catalog.ginarrayextract(upg_catalog.anyarray, pg_catalog.internal),
   FUNCTION 4 upg_catalog.ginarrayconsistent(pg_catalog.internal, upg_catalog.int2, pg_catalog.internal),
   STORAGE macaddr;
 CREATE OPERATOR CLASS _inet_ops DEFAULT FOR TYPE upg_catalog._inet USING gin AS
   OPERATOR 1 upg_catalog.&&(upg_catalog.anyarray, upg_catalog.anyarray),
   OPERATOR 2 upg_catalog.@>(upg_catalog.anyarray, upg_catalog.anyarray),
   OPERATOR 3 upg_catalog.<@(upg_catalog.anyarray, upg_catalog.anyarray) RECHECK,
   OPERATOR 4 upg_catalog.=(upg_catalog.anyarray, upg_catalog.anyarray) RECHECK,
   FUNCTION 1 upg_catalog.network_cmp(upg_catalog.inet, upg_catalog.inet),
   FUNCTION 2 upg_catalog.ginarrayextract(upg_catalog.anyarray, pg_catalog.internal),
   FUNCTION 3 upg_catalog.ginarrayextract(upg_catalog.anyarray, pg_catalog.internal),
   FUNCTION 4 upg_catalog.ginarrayconsistent(pg_catalog.internal, upg_catalog.int2, pg_catalog.internal),
   STORAGE inet;
 CREATE OPERATOR CLASS _cidr_ops DEFAULT FOR TYPE upg_catalog._cidr USING gin AS
   OPERATOR 1 upg_catalog.&&(upg_catalog.anyarray, upg_catalog.anyarray),
   OPERATOR 2 upg_catalog.@>(upg_catalog.anyarray, upg_catalog.anyarray),
   OPERATOR 3 upg_catalog.<@(upg_catalog.anyarray, upg_catalog.anyarray) RECHECK,
   OPERATOR 4 upg_catalog.=(upg_catalog.anyarray, upg_catalog.anyarray) RECHECK,
   FUNCTION 1 upg_catalog.network_cmp(upg_catalog.inet, upg_catalog.inet),
   FUNCTION 2 upg_catalog.ginarrayextract(upg_catalog.anyarray, pg_catalog.internal),
   FUNCTION 3 upg_catalog.ginarrayextract(upg_catalog.anyarray, pg_catalog.internal),
   FUNCTION 4 upg_catalog.ginarrayconsistent(pg_catalog.internal, upg_catalog.int2, pg_catalog.internal),
   STORAGE cidr;
 CREATE OPERATOR CLASS bpchar_pattern_ops FOR TYPE upg_catalog.bpchar USING bitmap AS
   OPERATOR 1 upg_catalog.~<~(upg_catalog.bpchar, upg_catalog.bpchar),
   OPERATOR 2 upg_catalog.~<=~(upg_catalog.bpchar, upg_catalog.bpchar),
   OPERATOR 3 upg_catalog.~=~(upg_catalog.bpchar, upg_catalog.bpchar),
   OPERATOR 4 upg_catalog.~>=~(upg_catalog.bpchar, upg_catalog.bpchar),
   OPERATOR 5 upg_catalog.~>~(upg_catalog.bpchar, upg_catalog.bpchar),
   FUNCTION 1 upg_catalog.btbpchar_pattern_cmp(upg_catalog.bpchar, upg_catalog.bpchar);
 CREATE OPERATOR CLASS bpchar_ops DEFAULT FOR TYPE upg_catalog.bpchar USING bitmap AS
   OPERATOR 1 upg_catalog.<(upg_catalog.bpchar, upg_catalog.bpchar),
   OPERATOR 2 upg_catalog.<=(upg_catalog.bpchar, upg_catalog.bpchar),
   OPERATOR 3 upg_catalog.=(upg_catalog.bpchar, upg_catalog.bpchar),
   OPERATOR 4 upg_catalog.>=(upg_catalog.bpchar, upg_catalog.bpchar),
   OPERATOR 5 upg_catalog.>(upg_catalog.bpchar, upg_catalog.bpchar),
   FUNCTION 1 upg_catalog.bpcharcmp(upg_catalog.bpchar, upg_catalog.bpchar);
 CREATE OPERATOR CLASS bpchar_pattern_ops FOR TYPE upg_catalog.bpchar USING hash AS
   OPERATOR 1 upg_catalog.~=~(upg_catalog.bpchar, upg_catalog.bpchar),
   FUNCTION 1 upg_catalog.hashvarlena(pg_catalog.internal);
 CREATE OPERATOR CLASS bpchar_pattern_ops FOR TYPE upg_catalog.bpchar USING btree AS
   OPERATOR 1 upg_catalog.~<~(upg_catalog.bpchar, upg_catalog.bpchar),
   OPERATOR 2 upg_catalog.~<=~(upg_catalog.bpchar, upg_catalog.bpchar),
   OPERATOR 3 upg_catalog.~=~(upg_catalog.bpchar, upg_catalog.bpchar),
   OPERATOR 4 upg_catalog.~>=~(upg_catalog.bpchar, upg_catalog.bpchar),
   OPERATOR 5 upg_catalog.~>~(upg_catalog.bpchar, upg_catalog.bpchar),
   FUNCTION 1 upg_catalog.btbpchar_pattern_cmp(upg_catalog.bpchar, upg_catalog.bpchar);
 CREATE OPERATOR CLASS bpchar_ops DEFAULT FOR TYPE upg_catalog.bpchar USING hash AS
   OPERATOR 1 upg_catalog.=(upg_catalog.bpchar, upg_catalog.bpchar),
   FUNCTION 1 upg_catalog.hashbpchar(upg_catalog.bpchar);
 CREATE OPERATOR CLASS bpchar_ops DEFAULT FOR TYPE upg_catalog.bpchar USING btree AS
   OPERATOR 1 upg_catalog.<(upg_catalog.bpchar, upg_catalog.bpchar),
   OPERATOR 2 upg_catalog.<=(upg_catalog.bpchar, upg_catalog.bpchar),
   OPERATOR 3 upg_catalog.=(upg_catalog.bpchar, upg_catalog.bpchar),
   OPERATOR 4 upg_catalog.>=(upg_catalog.bpchar, upg_catalog.bpchar),
   OPERATOR 5 upg_catalog.>(upg_catalog.bpchar, upg_catalog.bpchar),
   FUNCTION 1 upg_catalog.bpcharcmp(upg_catalog.bpchar, upg_catalog.bpchar);
 CREATE OPERATOR CLASS varchar_pattern_ops  FOR TYPE upg_catalog.text USING bitmap AS
   OPERATOR 1 upg_catalog.~<~(upg_catalog.text, upg_catalog.text),
   OPERATOR 2 upg_catalog.~<=~(upg_catalog.text, upg_catalog.text),
   OPERATOR 3 upg_catalog.~=~(upg_catalog.text, upg_catalog.text),
   OPERATOR 4 upg_catalog.~>=~(upg_catalog.text, upg_catalog.text),
   OPERATOR 5 upg_catalog.~>~(upg_catalog.text, upg_catalog.text),
   FUNCTION 1 upg_catalog.bttext_pattern_cmp(upg_catalog.text, upg_catalog.text);
 CREATE OPERATOR CLASS varchar_ops  FOR TYPE upg_catalog.text USING bitmap AS
   OPERATOR 1 upg_catalog.<(upg_catalog.text, upg_catalog.text),
   OPERATOR 2 upg_catalog.<=(upg_catalog.text, upg_catalog.text),
   OPERATOR 3 upg_catalog.=(upg_catalog.text, upg_catalog.text),
   OPERATOR 4 upg_catalog.>=(upg_catalog.text, upg_catalog.text),
   OPERATOR 5 upg_catalog.>(upg_catalog.text, upg_catalog.text),
   FUNCTION 1 upg_catalog.bttextcmp(upg_catalog.text, upg_catalog.text);
 CREATE OPERATOR CLASS varchar_pattern_ops  FOR TYPE upg_catalog.text USING hash AS
   OPERATOR 1 upg_catalog.~=~(upg_catalog.text, upg_catalog.text),
   FUNCTION 1 upg_catalog.hashvarlena(pg_catalog.internal);
 CREATE OPERATOR CLASS varchar_pattern_ops  FOR TYPE upg_catalog.text USING btree AS
   OPERATOR 1 upg_catalog.~<~(upg_catalog.text, upg_catalog.text),
   OPERATOR 2 upg_catalog.~<=~(upg_catalog.text, upg_catalog.text),
   OPERATOR 3 upg_catalog.~=~(upg_catalog.text, upg_catalog.text),
   OPERATOR 4 upg_catalog.~>=~(upg_catalog.text, upg_catalog.text),
   OPERATOR 5 upg_catalog.~>~(upg_catalog.text, upg_catalog.text),
   FUNCTION 1 upg_catalog.bttext_pattern_cmp(upg_catalog.text, upg_catalog.text);
 CREATE OPERATOR CLASS varchar_ops  FOR TYPE upg_catalog.text USING hash AS
   OPERATOR 1 upg_catalog.=(upg_catalog.text, upg_catalog.text),
   FUNCTION 1 upg_catalog.hashtext(upg_catalog.text);
 CREATE OPERATOR CLASS varchar_ops  FOR TYPE upg_catalog.text USING btree AS
   OPERATOR 1 upg_catalog.<(upg_catalog.text, upg_catalog.text),
   OPERATOR 2 upg_catalog.<=(upg_catalog.text, upg_catalog.text),
   OPERATOR 3 upg_catalog.=(upg_catalog.text, upg_catalog.text),
   OPERATOR 4 upg_catalog.>=(upg_catalog.text, upg_catalog.text),
   OPERATOR 5 upg_catalog.>(upg_catalog.text, upg_catalog.text),
   FUNCTION 1 upg_catalog.bttextcmp(upg_catalog.text, upg_catalog.text);
 CREATE OPERATOR CLASS date_ops DEFAULT FOR TYPE upg_catalog.date USING bitmap AS
   OPERATOR 1 upg_catalog.<(upg_catalog.date, upg_catalog.date),
   OPERATOR 2 upg_catalog.<=(upg_catalog.date, upg_catalog.date),
   OPERATOR 3 upg_catalog.=(upg_catalog.date, upg_catalog.date),
   OPERATOR 4 upg_catalog.>=(upg_catalog.date, upg_catalog.date),
   OPERATOR 5 upg_catalog.>(upg_catalog.date, upg_catalog.date),
   FUNCTION 1 upg_catalog.date_cmp(upg_catalog.date, upg_catalog.date),
   OPERATOR 1 upg_catalog.<(upg_catalog.date, upg_catalog.timestamp),
   OPERATOR 2 upg_catalog.<=(upg_catalog.date, upg_catalog.timestamp),
   OPERATOR 3 upg_catalog.=(upg_catalog.date, upg_catalog.timestamp),
   OPERATOR 4 upg_catalog.>=(upg_catalog.date, upg_catalog.timestamp),
   OPERATOR 5 upg_catalog.>(upg_catalog.date, upg_catalog.timestamp),
   FUNCTION 1 upg_catalog.date_cmp_timestamp(upg_catalog.date, upg_catalog."timestamp"),
   OPERATOR 1 upg_catalog.<(upg_catalog.date, upg_catalog.timestamptz),
   OPERATOR 2 upg_catalog.<=(upg_catalog.date, upg_catalog.timestamptz),
   OPERATOR 3 upg_catalog.=(upg_catalog.date, upg_catalog.timestamptz),
   OPERATOR 4 upg_catalog.>=(upg_catalog.date, upg_catalog.timestamptz),
   OPERATOR 5 upg_catalog.>(upg_catalog.date, upg_catalog.timestamptz),
   FUNCTION 1 upg_catalog.date_cmp_timestamptz(upg_catalog.date, upg_catalog.timestamptz);
 CREATE OPERATOR CLASS date_ops DEFAULT FOR TYPE upg_catalog.date USING hash AS
   OPERATOR 1 upg_catalog.=(upg_catalog.date, upg_catalog.date),
   FUNCTION 1 upg_catalog.hashint4(upg_catalog.int4);
 CREATE OPERATOR CLASS date_ops DEFAULT FOR TYPE upg_catalog.date USING btree AS
   OPERATOR 1 upg_catalog.<(upg_catalog.date, upg_catalog.date),
   OPERATOR 2 upg_catalog.<=(upg_catalog.date, upg_catalog.date),
   OPERATOR 3 upg_catalog.=(upg_catalog.date, upg_catalog.date),
   OPERATOR 4 upg_catalog.>=(upg_catalog.date, upg_catalog.date),
   OPERATOR 5 upg_catalog.>(upg_catalog.date, upg_catalog.date),
   FUNCTION 1 upg_catalog.date_cmp(upg_catalog.date, upg_catalog.date),
   OPERATOR 1 upg_catalog.<(upg_catalog.date, upg_catalog.timestamp),
   OPERATOR 2 upg_catalog.<=(upg_catalog.date, upg_catalog.timestamp),
   OPERATOR 3 upg_catalog.=(upg_catalog.date, upg_catalog.timestamp),
   OPERATOR 4 upg_catalog.>=(upg_catalog.date, upg_catalog.timestamp),
   OPERATOR 5 upg_catalog.>(upg_catalog.date, upg_catalog.timestamp),
   FUNCTION 1 upg_catalog.date_cmp_timestamp(upg_catalog.date, upg_catalog."timestamp"),
   OPERATOR 1 upg_catalog.<(upg_catalog.date, upg_catalog.timestamptz),
   OPERATOR 2 upg_catalog.<=(upg_catalog.date, upg_catalog.timestamptz),
   OPERATOR 3 upg_catalog.=(upg_catalog.date, upg_catalog.timestamptz),
   OPERATOR 4 upg_catalog.>=(upg_catalog.date, upg_catalog.timestamptz),
   OPERATOR 5 upg_catalog.>(upg_catalog.date, upg_catalog.timestamptz),
   FUNCTION 1 upg_catalog.date_cmp_timestamptz(upg_catalog.date, upg_catalog.timestamptz);
 CREATE OPERATOR CLASS time_ops DEFAULT FOR TYPE upg_catalog."time" USING bitmap AS
   OPERATOR 1 upg_catalog.<(upg_catalog.time, upg_catalog.time),
   OPERATOR 2 upg_catalog.<=(upg_catalog.time, upg_catalog.time),
   OPERATOR 3 upg_catalog.=(upg_catalog.time, upg_catalog.time),
   OPERATOR 4 upg_catalog.>=(upg_catalog.time, upg_catalog.time),
   OPERATOR 5 upg_catalog.>(upg_catalog.time, upg_catalog.time),
   FUNCTION 1 upg_catalog.time_cmp(upg_catalog."time", upg_catalog."time");
 CREATE OPERATOR CLASS time_ops DEFAULT FOR TYPE upg_catalog."time" USING hash AS
   OPERATOR 1 upg_catalog.=(upg_catalog.time, upg_catalog.time),
   FUNCTION 1 upg_catalog.hashfloat8(upg_catalog.float8);
 CREATE OPERATOR CLASS time_ops DEFAULT FOR TYPE upg_catalog."time" USING btree AS
   OPERATOR 1 upg_catalog.<(upg_catalog.time, upg_catalog.time),
   OPERATOR 2 upg_catalog.<=(upg_catalog.time, upg_catalog.time),
   OPERATOR 3 upg_catalog.=(upg_catalog.time, upg_catalog.time),
   OPERATOR 4 upg_catalog.>=(upg_catalog.time, upg_catalog.time),
   OPERATOR 5 upg_catalog.>(upg_catalog.time, upg_catalog.time),
   FUNCTION 1 upg_catalog.time_cmp(upg_catalog."time", upg_catalog."time");
 CREATE OPERATOR CLASS timestamp_ops DEFAULT FOR TYPE upg_catalog."timestamp" USING bitmap AS
   OPERATOR 1 upg_catalog.<(upg_catalog.timestamp, upg_catalog.timestamp),
   OPERATOR 2 upg_catalog.<=(upg_catalog.timestamp, upg_catalog.timestamp),
   OPERATOR 3 upg_catalog.=(upg_catalog.timestamp, upg_catalog.timestamp),
   OPERATOR 4 upg_catalog.>=(upg_catalog.timestamp, upg_catalog.timestamp),
   OPERATOR 5 upg_catalog.>(upg_catalog.timestamp, upg_catalog.timestamp),
   FUNCTION 1 upg_catalog.timestamp_cmp(upg_catalog."timestamp", upg_catalog."timestamp"),
   OPERATOR 1 upg_catalog.<(upg_catalog.timestamp, upg_catalog.date),
   OPERATOR 2 upg_catalog.<=(upg_catalog.timestamp, upg_catalog.date),
   OPERATOR 3 upg_catalog.=(upg_catalog.timestamp, upg_catalog.date),
   OPERATOR 4 upg_catalog.>=(upg_catalog.timestamp, upg_catalog.date),
   OPERATOR 5 upg_catalog.>(upg_catalog.timestamp, upg_catalog.date),
   FUNCTION 1 upg_catalog.timestamp_cmp_date(upg_catalog."timestamp", upg_catalog.date),
   OPERATOR 1 upg_catalog.<(upg_catalog.timestamp, upg_catalog.timestamptz),
   OPERATOR 2 upg_catalog.<=(upg_catalog.timestamp, upg_catalog.timestamptz),
   OPERATOR 3 upg_catalog.=(upg_catalog.timestamp, upg_catalog.timestamptz),
   OPERATOR 4 upg_catalog.>=(upg_catalog.timestamp, upg_catalog.timestamptz),
   OPERATOR 5 upg_catalog.>(upg_catalog.timestamp, upg_catalog.timestamptz),
   FUNCTION 1 upg_catalog.timestamp_cmp_timestamptz(upg_catalog."timestamp", upg_catalog.timestamptz);
 CREATE OPERATOR CLASS timestamp_ops DEFAULT FOR TYPE upg_catalog."timestamp" USING hash AS
   OPERATOR 1 upg_catalog.=(upg_catalog.timestamp, upg_catalog.timestamp),
   FUNCTION 1 upg_catalog.hashfloat8(upg_catalog.float8);
 CREATE OPERATOR CLASS timestamp_ops DEFAULT FOR TYPE upg_catalog."timestamp" USING btree AS
   OPERATOR 1 upg_catalog.<(upg_catalog.timestamp, upg_catalog.timestamp),
   OPERATOR 2 upg_catalog.<=(upg_catalog.timestamp, upg_catalog.timestamp),
   OPERATOR 3 upg_catalog.=(upg_catalog.timestamp, upg_catalog.timestamp),
   OPERATOR 4 upg_catalog.>=(upg_catalog.timestamp, upg_catalog.timestamp),
   OPERATOR 5 upg_catalog.>(upg_catalog.timestamp, upg_catalog.timestamp),
   FUNCTION 1 upg_catalog.timestamp_cmp(upg_catalog."timestamp", upg_catalog."timestamp"),
   OPERATOR 1 upg_catalog.<(upg_catalog.timestamp, upg_catalog.date),
   OPERATOR 2 upg_catalog.<=(upg_catalog.timestamp, upg_catalog.date),
   OPERATOR 3 upg_catalog.=(upg_catalog.timestamp, upg_catalog.date),
   OPERATOR 4 upg_catalog.>=(upg_catalog.timestamp, upg_catalog.date),
   OPERATOR 5 upg_catalog.>(upg_catalog.timestamp, upg_catalog.date),
   FUNCTION 1 upg_catalog.timestamp_cmp_date(upg_catalog."timestamp", upg_catalog.date),
   OPERATOR 1 upg_catalog.<(upg_catalog.timestamp, upg_catalog.timestamptz),
   OPERATOR 2 upg_catalog.<=(upg_catalog.timestamp, upg_catalog.timestamptz),
   OPERATOR 3 upg_catalog.=(upg_catalog.timestamp, upg_catalog.timestamptz),
   OPERATOR 4 upg_catalog.>=(upg_catalog.timestamp, upg_catalog.timestamptz),
   OPERATOR 5 upg_catalog.>(upg_catalog.timestamp, upg_catalog.timestamptz),
   FUNCTION 1 upg_catalog.timestamp_cmp_timestamptz(upg_catalog."timestamp", upg_catalog.timestamptz);
 CREATE OPERATOR CLASS _timestamp_ops DEFAULT FOR TYPE upg_catalog._timestamp USING gin AS
   OPERATOR 1 upg_catalog.&&(upg_catalog.anyarray, upg_catalog.anyarray),
   OPERATOR 2 upg_catalog.@>(upg_catalog.anyarray, upg_catalog.anyarray),
   OPERATOR 3 upg_catalog.<@(upg_catalog.anyarray, upg_catalog.anyarray) RECHECK,
   OPERATOR 4 upg_catalog.=(upg_catalog.anyarray, upg_catalog.anyarray) RECHECK,
   FUNCTION 1 upg_catalog.timestamp_cmp(upg_catalog."timestamp", upg_catalog."timestamp"),
   FUNCTION 2 upg_catalog.ginarrayextract(upg_catalog.anyarray, pg_catalog.internal),
   FUNCTION 3 upg_catalog.ginarrayextract(upg_catalog.anyarray, pg_catalog.internal),
   FUNCTION 4 upg_catalog.ginarrayconsistent(pg_catalog.internal, upg_catalog.int2, pg_catalog.internal),
   STORAGE "timestamp";
 CREATE OPERATOR CLASS _date_ops DEFAULT FOR TYPE upg_catalog._date USING gin AS
   OPERATOR 1 upg_catalog.&&(upg_catalog.anyarray, upg_catalog.anyarray),
   OPERATOR 2 upg_catalog.@>(upg_catalog.anyarray, upg_catalog.anyarray),
   OPERATOR 3 upg_catalog.<@(upg_catalog.anyarray, upg_catalog.anyarray) RECHECK,
   OPERATOR 4 upg_catalog.=(upg_catalog.anyarray, upg_catalog.anyarray) RECHECK,
   FUNCTION 1 upg_catalog.date_cmp(upg_catalog.date, upg_catalog.date),
   FUNCTION 2 upg_catalog.ginarrayextract(upg_catalog.anyarray, pg_catalog.internal),
   FUNCTION 3 upg_catalog.ginarrayextract(upg_catalog.anyarray, pg_catalog.internal),
   FUNCTION 4 upg_catalog.ginarrayconsistent(pg_catalog.internal, upg_catalog.int2, pg_catalog.internal),
   STORAGE date;
 CREATE OPERATOR CLASS _time_ops DEFAULT FOR TYPE upg_catalog._time USING gin AS
   OPERATOR 1 upg_catalog.&&(upg_catalog.anyarray, upg_catalog.anyarray),
   OPERATOR 2 upg_catalog.@>(upg_catalog.anyarray, upg_catalog.anyarray),
   OPERATOR 3 upg_catalog.<@(upg_catalog.anyarray, upg_catalog.anyarray) RECHECK,
   OPERATOR 4 upg_catalog.=(upg_catalog.anyarray, upg_catalog.anyarray) RECHECK,
   FUNCTION 1 upg_catalog.time_cmp(upg_catalog."time", upg_catalog."time"),
   FUNCTION 2 upg_catalog.ginarrayextract(upg_catalog.anyarray, pg_catalog.internal),
   FUNCTION 3 upg_catalog.ginarrayextract(upg_catalog.anyarray, pg_catalog.internal),
   FUNCTION 4 upg_catalog.ginarrayconsistent(pg_catalog.internal, upg_catalog.int2, pg_catalog.internal),
   STORAGE "time";
 CREATE OPERATOR CLASS timestamptz_ops DEFAULT FOR TYPE upg_catalog.timestamptz USING bitmap AS
   OPERATOR 1 upg_catalog.<(upg_catalog.timestamptz, upg_catalog.timestamptz),
   OPERATOR 2 upg_catalog.<=(upg_catalog.timestamptz, upg_catalog.timestamptz),
   OPERATOR 3 upg_catalog.=(upg_catalog.timestamptz, upg_catalog.timestamptz),
   OPERATOR 4 upg_catalog.>=(upg_catalog.timestamptz, upg_catalog.timestamptz),
   OPERATOR 5 upg_catalog.>(upg_catalog.timestamptz, upg_catalog.timestamptz),
   FUNCTION 1 upg_catalog.timestamptz_cmp(upg_catalog.timestamptz, upg_catalog.timestamptz),
   OPERATOR 1 upg_catalog.<(upg_catalog.timestamptz, upg_catalog.date),
   OPERATOR 2 upg_catalog.<=(upg_catalog.timestamptz, upg_catalog.date),
   OPERATOR 3 upg_catalog.=(upg_catalog.timestamptz, upg_catalog.date),
   OPERATOR 4 upg_catalog.>=(upg_catalog.timestamptz, upg_catalog.date),
   OPERATOR 5 upg_catalog.>(upg_catalog.timestamptz, upg_catalog.date),
   FUNCTION 1 upg_catalog.timestamptz_cmp_date(upg_catalog.timestamptz, upg_catalog.date),
   OPERATOR 1 upg_catalog.<(upg_catalog.timestamptz, upg_catalog.timestamp),
   OPERATOR 2 upg_catalog.<=(upg_catalog.timestamptz, upg_catalog.timestamp),
   OPERATOR 3 upg_catalog.=(upg_catalog.timestamptz, upg_catalog.timestamp),
   OPERATOR 4 upg_catalog.>=(upg_catalog.timestamptz, upg_catalog.timestamp),
   OPERATOR 5 upg_catalog.>(upg_catalog.timestamptz, upg_catalog.timestamp),
   FUNCTION 1 upg_catalog.timestamptz_cmp_timestamp(upg_catalog.timestamptz, upg_catalog."timestamp");
 CREATE OPERATOR CLASS timestamptz_ops DEFAULT FOR TYPE upg_catalog.timestamptz USING hash AS
   OPERATOR 1 upg_catalog.=(upg_catalog.timestamptz, upg_catalog.timestamptz),
   FUNCTION 1 upg_catalog.hashfloat8(upg_catalog.float8);
 CREATE OPERATOR CLASS timestamptz_ops DEFAULT FOR TYPE upg_catalog.timestamptz USING btree AS
   OPERATOR 1 upg_catalog.<(upg_catalog.timestamptz, upg_catalog.timestamptz),
   OPERATOR 2 upg_catalog.<=(upg_catalog.timestamptz, upg_catalog.timestamptz),
   OPERATOR 3 upg_catalog.=(upg_catalog.timestamptz, upg_catalog.timestamptz),
   OPERATOR 4 upg_catalog.>=(upg_catalog.timestamptz, upg_catalog.timestamptz),
   OPERATOR 5 upg_catalog.>(upg_catalog.timestamptz, upg_catalog.timestamptz),
   FUNCTION 1 upg_catalog.timestamptz_cmp(upg_catalog.timestamptz, upg_catalog.timestamptz),
   OPERATOR 1 upg_catalog.<(upg_catalog.timestamptz, upg_catalog.date),
   OPERATOR 2 upg_catalog.<=(upg_catalog.timestamptz, upg_catalog.date),
   OPERATOR 3 upg_catalog.=(upg_catalog.timestamptz, upg_catalog.date),
   OPERATOR 4 upg_catalog.>=(upg_catalog.timestamptz, upg_catalog.date),
   OPERATOR 5 upg_catalog.>(upg_catalog.timestamptz, upg_catalog.date),
   FUNCTION 1 upg_catalog.timestamptz_cmp_date(upg_catalog.timestamptz, upg_catalog.date),
   OPERATOR 1 upg_catalog.<(upg_catalog.timestamptz, upg_catalog.timestamp),
   OPERATOR 2 upg_catalog.<=(upg_catalog.timestamptz, upg_catalog.timestamp),
   OPERATOR 3 upg_catalog.=(upg_catalog.timestamptz, upg_catalog.timestamp),
   OPERATOR 4 upg_catalog.>=(upg_catalog.timestamptz, upg_catalog.timestamp),
   OPERATOR 5 upg_catalog.>(upg_catalog.timestamptz, upg_catalog.timestamp),
   FUNCTION 1 upg_catalog.timestamptz_cmp_timestamp(upg_catalog.timestamptz, upg_catalog."timestamp");
 CREATE OPERATOR CLASS _timestamptz_ops DEFAULT FOR TYPE upg_catalog._timestamptz USING gin AS
   OPERATOR 1 upg_catalog.&&(upg_catalog.anyarray, upg_catalog.anyarray),
   OPERATOR 2 upg_catalog.@>(upg_catalog.anyarray, upg_catalog.anyarray),
   OPERATOR 3 upg_catalog.<@(upg_catalog.anyarray, upg_catalog.anyarray) RECHECK,
   OPERATOR 4 upg_catalog.=(upg_catalog.anyarray, upg_catalog.anyarray) RECHECK,
   FUNCTION 1 upg_catalog.timestamptz_cmp(upg_catalog.timestamptz, upg_catalog.timestamptz),
   FUNCTION 2 upg_catalog.ginarrayextract(upg_catalog.anyarray, pg_catalog.internal),
   FUNCTION 3 upg_catalog.ginarrayextract(upg_catalog.anyarray, pg_catalog.internal),
   FUNCTION 4 upg_catalog.ginarrayconsistent(pg_catalog.internal, upg_catalog.int2, pg_catalog.internal),
   STORAGE timestamptz;
 CREATE OPERATOR CLASS interval_ops DEFAULT FOR TYPE upg_catalog."interval" USING bitmap AS
   OPERATOR 1 upg_catalog.<(upg_catalog.interval, upg_catalog.interval),
   OPERATOR 2 upg_catalog.<=(upg_catalog.interval, upg_catalog.interval),
   OPERATOR 3 upg_catalog.=(upg_catalog.interval, upg_catalog.interval),
   OPERATOR 4 upg_catalog.>=(upg_catalog.interval, upg_catalog.interval),
   OPERATOR 5 upg_catalog.>(upg_catalog.interval, upg_catalog.interval),
   FUNCTION 1 upg_catalog.interval_cmp(upg_catalog."interval", upg_catalog."interval");
 CREATE OPERATOR CLASS interval_ops DEFAULT FOR TYPE upg_catalog."interval" USING hash AS
   OPERATOR 1 upg_catalog.=(upg_catalog.interval, upg_catalog.interval),
   FUNCTION 1 upg_catalog.interval_hash(upg_catalog."interval");
 CREATE OPERATOR CLASS interval_ops DEFAULT FOR TYPE upg_catalog."interval" USING btree AS
   OPERATOR 1 upg_catalog.<(upg_catalog.interval, upg_catalog.interval),
   OPERATOR 2 upg_catalog.<=(upg_catalog.interval, upg_catalog.interval),
   OPERATOR 3 upg_catalog.=(upg_catalog.interval, upg_catalog.interval),
   OPERATOR 4 upg_catalog.>=(upg_catalog.interval, upg_catalog.interval),
   OPERATOR 5 upg_catalog.>(upg_catalog.interval, upg_catalog.interval),
   FUNCTION 1 upg_catalog.interval_cmp(upg_catalog."interval", upg_catalog."interval");
 CREATE OPERATOR CLASS _interval_ops DEFAULT FOR TYPE upg_catalog._interval USING gin AS
   OPERATOR 1 upg_catalog.&&(upg_catalog.anyarray, upg_catalog.anyarray),
   OPERATOR 2 upg_catalog.@>(upg_catalog.anyarray, upg_catalog.anyarray),
   OPERATOR 3 upg_catalog.<@(upg_catalog.anyarray, upg_catalog.anyarray) RECHECK,
   OPERATOR 4 upg_catalog.=(upg_catalog.anyarray, upg_catalog.anyarray) RECHECK,
   FUNCTION 1 upg_catalog.interval_cmp(upg_catalog."interval", upg_catalog."interval"),
   FUNCTION 2 upg_catalog.ginarrayextract(upg_catalog.anyarray, pg_catalog.internal),
   FUNCTION 3 upg_catalog.ginarrayextract(upg_catalog.anyarray, pg_catalog.internal),
   FUNCTION 4 upg_catalog.ginarrayconsistent(pg_catalog.internal, upg_catalog.int2, pg_catalog.internal),
   STORAGE "interval";
 CREATE OPERATOR CLASS _numeric_ops DEFAULT FOR TYPE upg_catalog._numeric USING gin AS
   OPERATOR 1 upg_catalog.&&(upg_catalog.anyarray, upg_catalog.anyarray),
   OPERATOR 2 upg_catalog.@>(upg_catalog.anyarray, upg_catalog.anyarray),
   OPERATOR 3 upg_catalog.<@(upg_catalog.anyarray, upg_catalog.anyarray) RECHECK,
   OPERATOR 4 upg_catalog.=(upg_catalog.anyarray, upg_catalog.anyarray) RECHECK,
   FUNCTION 1 upg_catalog.numeric_cmp(upg_catalog."numeric", upg_catalog."numeric"),
   FUNCTION 2 upg_catalog.ginarrayextract(upg_catalog.anyarray, pg_catalog.internal),
   FUNCTION 3 upg_catalog.ginarrayextract(upg_catalog.anyarray, pg_catalog.internal),
   FUNCTION 4 upg_catalog.ginarrayconsistent(pg_catalog.internal, upg_catalog.int2, pg_catalog.internal),
   STORAGE "numeric";
 CREATE OPERATOR CLASS timetz_ops DEFAULT FOR TYPE upg_catalog.timetz USING bitmap AS
   OPERATOR 1 upg_catalog.<(upg_catalog.timetz, upg_catalog.timetz),
   OPERATOR 2 upg_catalog.<=(upg_catalog.timetz, upg_catalog.timetz),
   OPERATOR 3 upg_catalog.=(upg_catalog.timetz, upg_catalog.timetz),
   OPERATOR 4 upg_catalog.>=(upg_catalog.timetz, upg_catalog.timetz),
   OPERATOR 5 upg_catalog.>(upg_catalog.timetz, upg_catalog.timetz),
   FUNCTION 1 upg_catalog.timetz_cmp(upg_catalog.timetz, upg_catalog.timetz);
 CREATE OPERATOR CLASS timetz_ops DEFAULT FOR TYPE upg_catalog.timetz USING hash AS
   OPERATOR 1 upg_catalog.=(upg_catalog.timetz, upg_catalog.timetz),
   FUNCTION 1 upg_catalog.timetz_hash(upg_catalog.timetz);
 CREATE OPERATOR CLASS timetz_ops DEFAULT FOR TYPE upg_catalog.timetz USING btree AS
   OPERATOR 1 upg_catalog.<(upg_catalog.timetz, upg_catalog.timetz),
   OPERATOR 2 upg_catalog.<=(upg_catalog.timetz, upg_catalog.timetz),
   OPERATOR 3 upg_catalog.=(upg_catalog.timetz, upg_catalog.timetz),
   OPERATOR 4 upg_catalog.>=(upg_catalog.timetz, upg_catalog.timetz),
   OPERATOR 5 upg_catalog.>(upg_catalog.timetz, upg_catalog.timetz),
   FUNCTION 1 upg_catalog.timetz_cmp(upg_catalog.timetz, upg_catalog.timetz);
 CREATE OPERATOR CLASS _timetz_ops DEFAULT FOR TYPE upg_catalog._timetz USING gin AS
   OPERATOR 1 upg_catalog.&&(upg_catalog.anyarray, upg_catalog.anyarray),
   OPERATOR 2 upg_catalog.@>(upg_catalog.anyarray, upg_catalog.anyarray),
   OPERATOR 3 upg_catalog.<@(upg_catalog.anyarray, upg_catalog.anyarray) RECHECK,
   OPERATOR 4 upg_catalog.=(upg_catalog.anyarray, upg_catalog.anyarray) RECHECK,
   FUNCTION 1 upg_catalog.timetz_cmp(upg_catalog.timetz, upg_catalog.timetz),
   FUNCTION 2 upg_catalog.ginarrayextract(upg_catalog.anyarray, pg_catalog.internal),
   FUNCTION 3 upg_catalog.ginarrayextract(upg_catalog.anyarray, pg_catalog.internal),
   FUNCTION 4 upg_catalog.ginarrayconsistent(pg_catalog.internal, upg_catalog.int2, pg_catalog.internal),
   STORAGE timetz;
 CREATE OPERATOR CLASS bit_ops DEFAULT FOR TYPE upg_catalog."bit" USING bitmap AS
   OPERATOR 1 upg_catalog.<(upg_catalog.bit, upg_catalog.bit),
   OPERATOR 2 upg_catalog.<=(upg_catalog.bit, upg_catalog.bit),
   OPERATOR 3 upg_catalog.=(upg_catalog.bit, upg_catalog.bit),
   OPERATOR 4 upg_catalog.>=(upg_catalog.bit, upg_catalog.bit),
   OPERATOR 5 upg_catalog.>(upg_catalog.bit, upg_catalog.bit),
   FUNCTION 1 upg_catalog.bitcmp(upg_catalog."bit", upg_catalog."bit");
 CREATE OPERATOR CLASS bit_ops DEFAULT FOR TYPE upg_catalog."bit" USING btree AS
   OPERATOR 1 upg_catalog.<(upg_catalog.bit, upg_catalog.bit),
   OPERATOR 2 upg_catalog.<=(upg_catalog.bit, upg_catalog.bit),
   OPERATOR 3 upg_catalog.=(upg_catalog.bit, upg_catalog.bit),
   OPERATOR 4 upg_catalog.>=(upg_catalog.bit, upg_catalog.bit),
   OPERATOR 5 upg_catalog.>(upg_catalog.bit, upg_catalog.bit),
   FUNCTION 1 upg_catalog.bitcmp(upg_catalog."bit", upg_catalog."bit");
 CREATE OPERATOR CLASS _bit_ops DEFAULT FOR TYPE upg_catalog._bit USING gin AS
   OPERATOR 1 upg_catalog.&&(upg_catalog.anyarray, upg_catalog.anyarray),
   OPERATOR 2 upg_catalog.@>(upg_catalog.anyarray, upg_catalog.anyarray),
   OPERATOR 3 upg_catalog.<@(upg_catalog.anyarray, upg_catalog.anyarray) RECHECK,
   OPERATOR 4 upg_catalog.=(upg_catalog.anyarray, upg_catalog.anyarray) RECHECK,
   FUNCTION 1 upg_catalog.bitcmp(upg_catalog."bit", upg_catalog."bit"),
   FUNCTION 2 upg_catalog.ginarrayextract(upg_catalog.anyarray, pg_catalog.internal),
   FUNCTION 3 upg_catalog.ginarrayextract(upg_catalog.anyarray, pg_catalog.internal),
   FUNCTION 4 upg_catalog.ginarrayconsistent(pg_catalog.internal, upg_catalog.int2, pg_catalog.internal),
   STORAGE "bit";
 CREATE OPERATOR CLASS varbit_ops DEFAULT FOR TYPE upg_catalog.varbit USING bitmap AS
   OPERATOR 1 upg_catalog.<(upg_catalog.varbit, upg_catalog.varbit),
   OPERATOR 2 upg_catalog.<=(upg_catalog.varbit, upg_catalog.varbit),
   OPERATOR 3 upg_catalog.=(upg_catalog.varbit, upg_catalog.varbit),
   OPERATOR 4 upg_catalog.>=(upg_catalog.varbit, upg_catalog.varbit),
   OPERATOR 5 upg_catalog.>(upg_catalog.varbit, upg_catalog.varbit),
   FUNCTION 1 upg_catalog.varbitcmp(upg_catalog.varbit, upg_catalog.varbit);
 CREATE OPERATOR CLASS varbit_ops DEFAULT FOR TYPE upg_catalog.varbit USING btree AS
   OPERATOR 1 upg_catalog.<(upg_catalog.varbit, upg_catalog.varbit),
   OPERATOR 2 upg_catalog.<=(upg_catalog.varbit, upg_catalog.varbit),
   OPERATOR 3 upg_catalog.=(upg_catalog.varbit, upg_catalog.varbit),
   OPERATOR 4 upg_catalog.>=(upg_catalog.varbit, upg_catalog.varbit),
   OPERATOR 5 upg_catalog.>(upg_catalog.varbit, upg_catalog.varbit),
   FUNCTION 1 upg_catalog.varbitcmp(upg_catalog.varbit, upg_catalog.varbit);
 CREATE OPERATOR CLASS _varbit_ops DEFAULT FOR TYPE upg_catalog._varbit USING gin AS
   OPERATOR 1 upg_catalog.&&(upg_catalog.anyarray, upg_catalog.anyarray),
   OPERATOR 2 upg_catalog.@>(upg_catalog.anyarray, upg_catalog.anyarray),
   OPERATOR 3 upg_catalog.<@(upg_catalog.anyarray, upg_catalog.anyarray) RECHECK,
   OPERATOR 4 upg_catalog.=(upg_catalog.anyarray, upg_catalog.anyarray) RECHECK,
   FUNCTION 1 upg_catalog.varbitcmp(upg_catalog.varbit, upg_catalog.varbit),
   FUNCTION 2 upg_catalog.ginarrayextract(upg_catalog.anyarray, pg_catalog.internal),
   FUNCTION 3 upg_catalog.ginarrayextract(upg_catalog.anyarray, pg_catalog.internal),
   FUNCTION 4 upg_catalog.ginarrayconsistent(pg_catalog.internal, upg_catalog.int2, pg_catalog.internal),
   STORAGE varbit;
 CREATE OPERATOR CLASS numeric_ops DEFAULT FOR TYPE upg_catalog."numeric" USING bitmap AS
   OPERATOR 1 upg_catalog.<(upg_catalog.numeric, upg_catalog.numeric),
   OPERATOR 2 upg_catalog.<=(upg_catalog.numeric, upg_catalog.numeric),
   OPERATOR 3 upg_catalog.=(upg_catalog.numeric, upg_catalog.numeric),
   OPERATOR 4 upg_catalog.>=(upg_catalog.numeric, upg_catalog.numeric),
   OPERATOR 5 upg_catalog.>(upg_catalog.numeric, upg_catalog.numeric),
   FUNCTION 1 upg_catalog.numeric_cmp(upg_catalog."numeric", upg_catalog."numeric");
 CREATE OPERATOR CLASS numeric_ops DEFAULT FOR TYPE upg_catalog."numeric" USING hash AS
   OPERATOR 1 upg_catalog.=(upg_catalog.numeric, upg_catalog.numeric),
   FUNCTION 1 upg_catalog.hash_numeric(upg_catalog."numeric");
 CREATE OPERATOR CLASS numeric_ops DEFAULT FOR TYPE upg_catalog."numeric" USING btree AS
   OPERATOR 1 upg_catalog.<(upg_catalog.numeric, upg_catalog.numeric),
   OPERATOR 2 upg_catalog.<=(upg_catalog.numeric, upg_catalog.numeric),
   OPERATOR 3 upg_catalog.=(upg_catalog.numeric, upg_catalog.numeric),
   OPERATOR 4 upg_catalog.>=(upg_catalog.numeric, upg_catalog.numeric),
   OPERATOR 5 upg_catalog.>(upg_catalog.numeric, upg_catalog.numeric),
   FUNCTION 1 upg_catalog.numeric_cmp(upg_catalog."numeric", upg_catalog."numeric");
 CREATE OPERATOR CLASS xlogloc_ops DEFAULT FOR TYPE upg_catalog.gpxlogloc USING btree AS
   OPERATOR 1 upg_catalog.<(upg_catalog.gpxlogloc, upg_catalog.gpxlogloc),
   OPERATOR 2 upg_catalog.<=(upg_catalog.gpxlogloc, upg_catalog.gpxlogloc),
   OPERATOR 3 upg_catalog.=(upg_catalog.gpxlogloc, upg_catalog.gpxlogloc),
   OPERATOR 4 upg_catalog.>=(upg_catalog.gpxlogloc, upg_catalog.gpxlogloc),
   OPERATOR 5 upg_catalog.>(upg_catalog.gpxlogloc, upg_catalog.gpxlogloc),
   FUNCTION 1 upg_catalog.btgpxlogloccmp(upg_catalog.gpxlogloc, upg_catalog.gpxlogloc);
 CREATE OPERATOR CLASS array_ops DEFAULT FOR TYPE upg_catalog.anyarray USING bitmap AS
   OPERATOR 1 upg_catalog.<(upg_catalog.anyarray, upg_catalog.anyarray),
   OPERATOR 2 upg_catalog.<=(upg_catalog.anyarray, upg_catalog.anyarray),
   OPERATOR 3 upg_catalog.=(upg_catalog.anyarray, upg_catalog.anyarray),
   OPERATOR 4 upg_catalog.>=(upg_catalog.anyarray, upg_catalog.anyarray),
   OPERATOR 5 upg_catalog.>(upg_catalog.anyarray, upg_catalog.anyarray),
   FUNCTION 1 upg_catalog.btarraycmp(upg_catalog.anyarray, upg_catalog.anyarray);
 CREATE OPERATOR CLASS array_ops DEFAULT FOR TYPE upg_catalog.anyarray USING btree AS
   OPERATOR 1 upg_catalog.<(upg_catalog.anyarray, upg_catalog.anyarray),
   OPERATOR 2 upg_catalog.<=(upg_catalog.anyarray, upg_catalog.anyarray),
   OPERATOR 3 upg_catalog.=(upg_catalog.anyarray, upg_catalog.anyarray),
   OPERATOR 4 upg_catalog.>=(upg_catalog.anyarray, upg_catalog.anyarray),
   OPERATOR 5 upg_catalog.>(upg_catalog.anyarray, upg_catalog.anyarray),
   FUNCTION 1 upg_catalog.btarraycmp(upg_catalog.anyarray, upg_catalog.anyarray);
