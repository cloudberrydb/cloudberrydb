/*-------------------------------------------------------------------------
 *
 * cdbplan.h
 *	  definitions for cdbplan.c utilities
 *
 * Copyright (c) 2004-2008, Greenplum inc
 *
 *
 * NOTES
 *	  See src/backend/utils/misc/guc.c for variable external specification.
 *
 *-------------------------------------------------------------------------
 */

#ifndef CDBPLAN_H
#define CDBPLAN_H

#include "optimizer/walkers.h"





typedef struct XsliceInAppendContext
{
	plan_tree_base_prefix base;
	
	/* The slice number for the node that is being processed */
	int currentSliceNo;

	/* A set of slice numbers that contain cross-slice shared nodes */
	Bitmapset *slices;
} XsliceInAppendContext;


extern Node * plan_tree_mutator(Node *node, Node *(*mutator) (), void *context);

extern bool set_hasxslice_in_append_walker(Node *node, void *ctx);

#endif   /* CDBPLAN_H */
