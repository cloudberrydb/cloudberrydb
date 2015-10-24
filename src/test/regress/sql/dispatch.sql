-- Misc tests related to dispatching queries to segments.

-- Test quoting of GUC values and databse names when they're sent to segments

-- There used to be a bug in the quoting when the search_path setting was sent
-- to the segment. It was not easily visible when search_path was set with a
-- SET command, only when the setting was sent as part of the startup packet.
-- Set search_path as a per-user setting so that we can test that.
CREATE DATABASE "dispatch test db";
ALTER DATABASE "dispatch test db" SET search_path="my schema",public;

\c "dispatch test db"

CREATE SCHEMA "my schema";

-- Create a table with the same name in both schemas, "my schema" and public.
CREATE TABLE "my table" (t text);
INSERT INTO "my table" VALUES ('myschema.mytable');

CREATE TABLE public."my table" (t text);
INSERT INTO public."my table" VALUES ('public.mytable');

SELECT t as unquoted FROM "my table";
SELECT t as myschema FROM "my schema"."my table";
SELECT t as public FROM public."my table";

DROP TABLE "my table";
DROP TABLE public."my table";

-- Create another table with the same name. To make sure the DROP worked
-- and dropped the correct table.
CREATE TABLE "my table" (id integer);
DROP TABLE "my table";

-- Clean up
\c regression
DROP DATABASE "dispatch test db"
