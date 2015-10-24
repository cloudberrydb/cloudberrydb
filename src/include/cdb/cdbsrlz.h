/*-------------------------------------------------------------------------
 *
 * cdbsrlz.h
 *	  definitions for paln serialization utilities
 *
 * Copyright (c) 2004-2008, Greenplum inc
 *
 * NOTES
 *
 *-------------------------------------------------------------------------
 */

#ifndef CDBSRLZ_H
#define CDBSRLZ_H

#include "lib/stringinfo.h"
#include "nodes/pg_list.h"

extern char *serializeNode(Node *node, int *size, int *uncompressed_size);
extern Node *deserializeNode(const char *strNode, int size);



#endif   /* CDBSRLZ_H */
