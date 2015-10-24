/*-------------------------------------------------------------------------
 *
 * cdbcat.h
 *	  routines for reading info from Greenplum Database schema tables
 *
 * Copyright (c) 2005-2008, Greenplum inc
 *
 *-------------------------------------------------------------------------
 */

#ifndef CDBCAT_H
#define CDBCAT_H

#include "access/genam.h"
#include "access/heapam.h"
#include "catalog/gp_id.h"
#include "catalog/gp_policy.h"

extern void checkPolicyForUniqueIndex(Relation rel, AttrNumber *indattr,
									  int nidxatts, bool isprimary, 
									  bool has_exprs, bool has_pkey,
									  bool has_ukey);

#endif   /* CDBCAT_H */
