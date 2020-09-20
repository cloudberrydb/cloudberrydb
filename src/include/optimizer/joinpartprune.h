/*-------------------------------------------------------------------------
 *
 * joinpartprune.h
 *	  Transforms to produce plans that achieve dynamic partition elimination.
 *
 * Portions Copyright (c) 2011-2013, EMC Corporation
 * Portions Copyright (c) 2012-Present Pivotal Software, Inc.
 *
 *
 * IDENTIFICATION
 *	    src/include/optimizer/joinpartprune.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef JOINPARTPRUNE_H
#define JOINPARTPRUNE_H

#include "nodes/plannodes.h"
#include "nodes/relation.h"

extern bool inject_partition_selectors_for_join(PlannerInfo *root,
									JoinPath *join_path,
									List **partSelectors_p);

extern Plan *create_partition_selector_plan(PlannerInfo *root, PartitionSelectorPath *pspath);

#endif /* JOINPARTPRUNE_H */
