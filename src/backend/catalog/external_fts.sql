/*
 * external fts view
 *
 * Portions Copyright (c) 2006-2010, Greenplum inc.
 * Portions Copyright (c) 2012-Present VMware, Inc. or its affiliates.
 * Copyright (c) 1996-2021, PostgreSQL Global Development Group
 *
 * src/backend/catalog/external_fts.sql
 *
 */

CREATE VIEW gp_segment_configuration AS
    SELECT * FROM gp_get_segment_configuration();

REVOKE ALL ON gp_segment_configuration FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION gp_get_segment_configuration() FROM PUBLIC;