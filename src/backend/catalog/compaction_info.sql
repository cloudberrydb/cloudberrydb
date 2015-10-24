--------------------------------------------------------------------------------
-- Greenplum Database
-- Copyright (C) 2009, 2015 by Greenplum, Inc. All rights reserved.
--
-- This software is subject to change without notice. It is furnished
-- under a license agreement, and may be used or copied only in
-- accordance with the terms of that agreement. Upgrades are provided
-- only at regularly scheduled software release dates. No part of this
-- file may be reproduced, transmitted, or translated in any form or
-- by any means, electronic, mechanical, manual, optical, or otherwise
-- without the prior written permission of Greenplum, Inc.
--
-- This sql file is intented to populate new functions introduced in
-- Greenplum Database version 4.3.4.2. Execute this file on a system
-- that was upgraded using binary swap from 4.3.x release. This file
-- need not be run against (a) fresh installs of version 4.3.4.2 or
-- newer and (b) major version upgrades from 4.2.x to 4.3.4.2 or
-- newer.
--
--------------------------------------------------------------------------------

BEGIN;

CREATE TYPE gp_toolkit.__gp_aovisimap_hidden_t AS (seg int, hidden bigint, total bigint);
CREATE FUNCTION gp_toolkit.__gp_aovisimap_hidden_typed(oid)
    RETURNS SETOF gp_toolkit.__gp_aovisimap_hidden_t AS $$
    SELECT * FROM gp_toolkit.__gp_aovisimap_hidden_info($1);
$$ LANGUAGE SQL;

CREATE FUNCTION gp_toolkit.__gp_aovisimap_compaction_info(ao_oid oid,
    OUT content int, OUT datafile int, OUT compaction_possible boolean,
    OUT hidden_tupcount bigint, OUT total_tupcount bigint, OUT percent_hidden numeric)
    RETURNS SETOF RECORD AS $$
DECLARE
    hinfo_row RECORD;
    threshold float;
BEGIN
    EXECUTE 'show gp_appendonly_compaction_threshold' INTO threshold;
    FOR hinfo_row IN SELECT gp_segment_id,
    gp_toolkit.__gp_aovisimap_hidden_typed(ao_oid)::gp_toolkit.__gp_aovisimap_hidden_t
    FROM gp_dist_random('gp_id') LOOP
        content := hinfo_row.gp_segment_id;
        datafile := (hinfo_row.__gp_aovisimap_hidden_typed).seg;
        hidden_tupcount := (hinfo_row.__gp_aovisimap_hidden_typed).hidden;
        total_tupcount := (hinfo_row.__gp_aovisimap_hidden_typed).total;
        compaction_possible := false;
        IF total_tupcount > 0 THEN
            percent_hidden := (100 * hidden_tupcount / total_tupcount::numeric)::numeric(5,2);
        ELSE
            percent_hidden := 0::numeric(5,2);
        END IF;
        IF percent_hidden > threshold THEN
            compaction_possible := true;
        END IF;
        RETURN NEXT;
    END LOOP;
    RAISE NOTICE 'gp_appendonly_compaction_threshold = %', threshold;
    RETURN;
END;
$$ LANGUAGE plpgsql;

-- Finalize install
COMMIT;

