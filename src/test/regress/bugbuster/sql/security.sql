-- ***** From TIME_BASED_CONSTRAINT suite *****
CREATE USER invalidUser1 WITH LOGIN DENY BETWEEN DAY 'Monday' TIME '00:00' AND DAY 'Monday' TIME '25:00';
CREATE USER invalidUser2 WITH LOGIN DENY BETWEEN DAY 'Monday' TIME '01:65 AM' AND DAY 'Monday' TIME '12:00 PM';
CREATE USER invalidUser3 WITH LOGIN DENY BETWEEN DAY 'Monday' TIME '-01:00' AND DAY 'Monday' TIME '06:00 PM';
CREATE ROLE invalidRole1 WITH LOGIN DENY BETWEEN DAY 'Monday' TIME '00:00' AND DAY 'Monday' TIME '25:00';
CREATE ROLE invalidRole2 WITH LOGIN DENY BETWEEN DAY 'Monday' TIME '01:65 AM' AND DAY 'Monday' TIME '12:00 PM';
CREATE ROLE invalidRole3 WITH LOGIN DENY BETWEEN DAY 'Monday' TIME '-01:00' AND DAY 'Monday' TIME '06:00 PM';

--start_ignore
DROP USER IF EXISTS testuser1;
DROP USER IF EXISTS testuser2;
--end_ignore
CREATE USER testuser1 WITH LOGIN NOSUPERUSER NOCREATEROLE;
CREATE USER testuser2 WITH LOGIN DENY DAY 6;

--start_ignore
DROP USER IF EXISTS testuser2;
--end_ignore
CREATE USER testuser2 WITH LOGIN DENY DAY 'Saturday';

--start_ignore
DROP USER IF EXISTS testuser1;
--end_ignore
CREATE USER testuser1 WITH LOGIN DENY DAY 'Monday';

SELECT COUNT(*)
FROM pg_auth_time_constraint d join pg_roles r ON (d.authid = r.oid)
WHERE r.rolname in ('testuser1');

ALTER USER testuser1 DROP DENY FOR DAY 'Tuesday';
--start_ignore
DROP USER testuser1;
--end_ignore

-- ***** From PASSWORD_ENCRYPTION suite *****
show password_hash_algorithm;

set password_hash_algorithm = 'MD5';

show password_hash_algorithm;

--start_ignore
drop role if exists md5_role1;
--end_ignore

create role md5_role1 with password 'md5password' login;

select rolname,rolpassword from pg_authid where rolname like 'md5%' order by rolname;


show password_hash_algorithm;

set password_hash_algorithm = 'SHA-256';

show password_hash_algorithm;

--start_ignore
drop role if exists sha256_role1;
--end_ignore

create role sha256_role1 with password 'sha256password' login;

select rolname,rolpassword from pg_authid where rolname like 'sha256%' order by rolname;

