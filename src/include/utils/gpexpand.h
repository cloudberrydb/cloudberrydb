/*-------------------------------------------------------------------------
 *
 * gpexpand.h
 *	  Helper functions for gpexpand.
 *
 *
 * Copyright (c) 2018-Present Pivotal Software, Inc.
 *
 * src/include/utils/gpexpand.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef GPEXPAND_H
#define GPEXPAND_H

extern Datum gp_expand_lock_catalog(PG_FUNCTION_ARGS);

extern void gp_expand_protect_catalog_changes(Relation relation);

#endif   /* GPEXPAND_H */

