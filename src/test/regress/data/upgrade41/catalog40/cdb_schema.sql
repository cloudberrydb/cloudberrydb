--
-- This is the system_views.sql file from 4.0
--
-- It has been modified to prefix each view name with "upg_catalog." which 
-- is a necessary requirement since the search path does not seem to be 
-- honored.
--
-- The creation of plpgsql and altering the default resource queue have 
-- been removed.

-- --------------------------------------------------------------------
--
-- cdb_schema.sql
--
-- Define mpp administrative schema and several SQL functions to aid 
-- in maintaining the mpp administrative schema.  
--
-- This is version 2 of the schema.
--
-- TODO Error checking is rudimentary and needs improvment.
--
--
-- --------------------------------------------------------------------

-------------------------------------------------------------------
-- database
-------------------------------------------------------------------
CREATE OR REPLACE VIEW upg_catalog.gp_pgdatabase AS 
    SELECT *
      FROM gp_pgdatabase() AS L(dbid smallint, isprimary boolean, content smallint, valid boolean, definedprimary boolean);

GRANT SELECT ON upg_catalog.gp_pgdatabase TO PUBLIC;

------------------------------------------------------------------
-- distributed transaction related
------------------------------------------------------------------
CREATE OR REPLACE VIEW upg_catalog.gp_distributed_xacts AS 
    SELECT *
      FROM gp_distributed_xacts() AS L(distributed_xid xid, distributed_id text, state text, gp_session_id int, xmin_distributed_snapshot xid);

GRANT SELECT ON upg_catalog.gp_distributed_xacts TO PUBLIC;


CREATE OR REPLACE VIEW upg_catalog.gp_transaction_log AS 
    SELECT *
      FROM gp_transaction_log() AS L(segment_id smallint, dbid smallint, transaction xid, status text);

GRANT SELECT ON upg_catalog.gp_transaction_log TO PUBLIC;

CREATE OR REPLACE VIEW upg_catalog.gp_distributed_log AS 
    SELECT *
      FROM gp_distributed_log() AS L(segment_id smallint, dbid smallint, distributed_xid xid, distributed_id text, status text, local_transaction xid);

GRANT SELECT ON upg_catalog.gp_distributed_log TO PUBLIC;
