
SET search_path = workfile;

BEGIN;

DROP FUNCTION gp_workfile_mgr_test_allsegs(text);
DROP FUNCTION gp_workfile_mgr_test(text);

DROP SCHEMA workfile;

COMMIT; 
