-- ----------------------------------------------------------------------
-- Test: assign/alter a role to a resource group
-- ----------------------------------------------------------------------

--start_ignore
DROP ROLE IF EXISTS rg_test_role;
--end_ignore
CREATE ROLE rg_test_role;
SELECT rolresgroup FROM pg_authid WHERE rolname = 'rg_test_role';
ALTER ROLE rg_test_role RESOURCE GROUP non_exist_group;
SELECT rolresgroup FROM pg_authid WHERE rolname = 'rg_test_role';
ALTER ROLE rg_test_role RESOURCE GROUP admin_group;
SELECT rolresgroup FROM pg_authid WHERE rolname = 'rg_test_role';
ALTER ROLE rg_test_role RESOURCE GROUP none;
SELECT rolresgroup FROM pg_authid WHERE rolname = 'rg_test_role';

DROP ROLE rg_test_role;
CREATE ROLE rg_test_role SUPERUSER;
SELECT rolresgroup FROM pg_authid WHERE rolname = 'rg_test_role';
ALTER ROLE rg_test_role RESOURCE GROUP default_group;
SELECT rolresgroup FROM pg_authid WHERE rolname = 'rg_test_role';
ALTER ROLE rg_test_role RESOURCE GROUP none;
SELECT rolresgroup FROM pg_authid WHERE rolname = 'rg_test_role';

DROP ROLE rg_test_role;
CREATE ROLE rg_test_role RESOURCE GROUP non_exist_group;

-- nonsuper user should not be assigned to admin group
CREATE ROLE rg_test_role RESOURCE GROUP admin_group;
