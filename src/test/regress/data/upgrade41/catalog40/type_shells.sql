-- Define type shells for types defined in the 4.0 Catalog:
--
-- Derived via the following SQL run within a 4.0 catalog
/*
SELECT 'CREATE TYPE upg_catalog.' || quote_ident(typname) || ';'
FROM pg_type t
JOIN pg_namespace n on (t.typnamespace = n.oid)
WHERE typtype in ('b', 'p') and typname !~ '^_' and n.nspname = 'pg_catalog'
order by 1
;
*/
 CREATE TYPE upg_catalog."any";
 CREATE TYPE upg_catalog."bit";
 CREATE TYPE upg_catalog."char";
 CREATE TYPE upg_catalog."interval";
 CREATE TYPE upg_catalog."numeric";
 CREATE TYPE upg_catalog."time";
 CREATE TYPE upg_catalog."timestamp";
 CREATE TYPE upg_catalog."varchar";
 CREATE TYPE upg_catalog.abstime;
 CREATE TYPE upg_catalog.aclitem;
 CREATE TYPE upg_catalog.anyarray;
 CREATE TYPE upg_catalog.anyelement;
 CREATE TYPE upg_catalog.bool;
 CREATE TYPE upg_catalog.box;
 CREATE TYPE upg_catalog.bpchar;
 CREATE TYPE upg_catalog.bytea;
 CREATE TYPE upg_catalog.cid;
 CREATE TYPE upg_catalog.cidr;
 CREATE TYPE upg_catalog.circle;
 CREATE TYPE upg_catalog.cstring;
 CREATE TYPE upg_catalog.date;
 CREATE TYPE upg_catalog.float4;
 CREATE TYPE upg_catalog.float8;
 CREATE TYPE upg_catalog.gpaotid;
 CREATE TYPE upg_catalog.gpxlogloc;
 CREATE TYPE upg_catalog.inet;
 CREATE TYPE upg_catalog.int2;
 CREATE TYPE upg_catalog.int2vector;
 CREATE TYPE upg_catalog.int4;
 CREATE TYPE upg_catalog.int8;
 CREATE TYPE upg_catalog.internal;
 CREATE TYPE upg_catalog.language_handler;
 CREATE TYPE upg_catalog.line;
 CREATE TYPE upg_catalog.lseg;
 CREATE TYPE upg_catalog.macaddr;
 CREATE TYPE upg_catalog.money;
 CREATE TYPE upg_catalog.name;
 CREATE TYPE upg_catalog.oid;
 CREATE TYPE upg_catalog.oidvector;
 CREATE TYPE upg_catalog.opaque;
 CREATE TYPE upg_catalog.path;
 CREATE TYPE upg_catalog.point;
 CREATE TYPE upg_catalog.polygon;
 CREATE TYPE upg_catalog.record;
 CREATE TYPE upg_catalog.refcursor;
 CREATE TYPE upg_catalog.regclass;
 CREATE TYPE upg_catalog.regoper;
 CREATE TYPE upg_catalog.regoperator;
 CREATE TYPE upg_catalog.regproc;
 CREATE TYPE upg_catalog.regprocedure;
 CREATE TYPE upg_catalog.regtype;
 CREATE TYPE upg_catalog.reltime;
 CREATE TYPE upg_catalog.smgr;
 CREATE TYPE upg_catalog.text;
 CREATE TYPE upg_catalog.tid;
 CREATE TYPE upg_catalog.timestamptz;
 CREATE TYPE upg_catalog.timetz;
 CREATE TYPE upg_catalog.tinterval;
 CREATE TYPE upg_catalog.trigger;
 CREATE TYPE upg_catalog.unknown;
 CREATE TYPE upg_catalog.varbit;
 CREATE TYPE upg_catalog.void;
 CREATE TYPE upg_catalog.xid;

